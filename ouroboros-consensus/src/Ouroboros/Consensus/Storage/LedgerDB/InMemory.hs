{-# LANGUAGE AllowAmbiguousTypes        #-}
{-# LANGUAGE ConstraintKinds            #-}
{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE DeriveAnyClass             #-}
{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE EmptyDataDeriving          #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE FunctionalDependencies     #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE NamedFieldPuns             #-}
{-# LANGUAGE QuantifiedConstraints      #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TupleSections              #-}
{-# LANGUAGE TypeApplications           #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE UndecidableInstances       #-}

module Ouroboros.Consensus.Storage.LedgerDB.InMemory (
    -- * LedgerDB proper
    LedgerDbCfg (..)
  , combineLedgerDBs
  , ledgerDbWithAnchor
  , oldLedgerDbWithAnchor
    -- ** opaque
  , LedgerDB
  , NewLedgerDB
  , OldLedgerDB
    -- * Ledger DB types (TODO: we might want to place this somewhere else)
  , DbChangelog
  , DbReader (..)
  , ReadKeySets
  , ReadsKeySets (..)
  , RewoundTableKeySets (..)
  , UnforwardedReadSets (..)
  , defaultReadKeySets
  , initialDbChangelog
  , initialDbChangelogWithEmptyState
  , ledgerDbFlush
    -- ** Serialisation
  , decodeSnapshotBackwardsCompatible
  , encodeSnapshot
    -- ** Queries
  , ledgerDbAnchor
  , ledgerDbBimap
  , ledgerDbCurrent
  , ledgerDbCurrentNew
  , ledgerDbCurrentOld
  , ledgerDbPast
  , ledgerDbPrefix
  , ledgerDbPrune
  , ledgerDbSnapshots
  , ledgerDbTip
  , oldLedgerDbCurrent
    -- ** Running updates
  , AnnLedgerError (..)
  , Ap (..)
  , ResolveBlock
  , ResolvesBlocks (..)
  , ThrowsLedgerError (..)
  , defaultResolveBlocks
  , defaultResolveWithErrors
  , defaultThrowLedgerErrors
    -- ** Updates
  , ExceededRollback (..)
  , ledgerDbPush
  , ledgerDbSwitch
    -- * Initialization
  , newApplyBlock
  , oldApplyBlock
  , pushLedgerStateNew
  , pushLedgerStateOld
    -- * Exports for the benefit of tests
    -- ** Additional queries
  , ledgerDbIsSaturated
  , ledgerDbMaxRollback
    -- ** Pure API
  , ledgerDbPush'
  , ledgerDbPushMany'
  , ledgerDbSwitch'
  ) where

import           Codec.Serialise.Decoding (Decoder)
import qualified Codec.Serialise.Decoding as Dec
import           Codec.Serialise.Encoding (Encoding)
import           Control.Monad.Except (throwError)
import           Control.Monad.Reader hiding (ap)
import           Control.Monad.Trans.Except
import           Data.Functor.Identity
import           Data.Kind (Constraint, Type)
import           Data.Word
import           GHC.Generics (Generic)
import           NoThunks.Class (NoThunks)

import           Ouroboros.Network.AnchoredSeq (Anchorable (..),
                     AnchoredSeq (..))
import qualified Ouroboros.Network.AnchoredSeq as AS

import           Cardano.Slotting.Slot (WithOrigin (At))
import           Control.Exception
import           Ouroboros.Consensus.Block
import           Ouroboros.Consensus.Config
import           Ouroboros.Consensus.Ledger.Abstract
import           Ouroboros.Consensus.Storage.LedgerDB.Types (PushGoal (..),
                     PushStart (..), Pushing (..),
                     UpdateLedgerDbTraceEvent (..))
import           Ouroboros.Consensus.Util
import           Ouroboros.Consensus.Util.CBOR (decodeWithOrigin)
import           Ouroboros.Consensus.Util.Versioned

{-------------------------------------------------------------------------------
  Ledger DB types
-------------------------------------------------------------------------------}

-- | Internal state of the ledger DB
--
-- The ledger DB looks like
--
-- > anchor |> snapshots <| current
--
-- where @anchor@ records the oldest known snapshot and @current@ the most
-- recent. The anchor is the oldest point we can roll back to.
--
-- We take a snapshot after each block is applied and keep in memory a window
-- of the last @k@ snapshots. We have verified empirically (#1936) that the
-- overhead of keeping @k snapshots in memory is small, i.e., about 5%
-- compared to keeping a snapshot every 100 blocks. This is thanks to sharing
-- between consecutive snapshots.
--
-- As an example, suppose we have @k = 6@. The ledger DB grows as illustrated
-- below, where we indicate the anchor number of blocks, the stored snapshots,
-- and the current ledger.
--
-- > anchor |> #   [ snapshots ]                   <| tip
-- > ---------------------------------------------------------------------------
-- > G      |> (0) [ ]                             <| G
-- > G      |> (1) [ L1]                           <| L1
-- > G      |> (2) [ L1,  L2]                      <| L2
-- > G      |> (3) [ L1,  L2,  L3]                 <| L3
-- > G      |> (4) [ L1,  L2,  L3,  L4]            <| L4
-- > G      |> (5) [ L1,  L2,  L3,  L4,  L5]       <| L5
-- > G      |> (6) [ L1,  L2,  L3,  L4,  L5,  L6]  <| L6
-- > L1     |> (6) [ L2,  L3,  L4,  L5,  L6,  L7]  <| L7
-- > L2     |> (6) [ L3,  L4,  L5,  L6,  L7,  L8]  <| L8
-- > L3     |> (6) [ L4,  L5,  L6,  L7,  L8,  L9]  <| L9   (*)
-- > L4     |> (6) [ L5,  L6,  L7,  L8,  L9,  L10] <| L10
-- > L5     |> (6) [*L6,  L7,  L8,  L9,  L10, L11] <| L11
-- > L6     |> (6) [ L7,  L8,  L9,  L10, L11, L12] <| L12
-- > L7     |> (6) [ L8,  L9,  L10, L12, L12, L13] <| L13
-- > L8     |> (6) [ L9,  L10, L12, L12, L13, L14] <| L14
--
-- The ledger DB must guarantee that at all times we are able to roll back @k@
-- blocks. For example, if we are on line (*), and roll back 6 blocks, we get
--
-- > L3 |> []
data LedgerDB (l :: LedgerStateKind) = LedgerDB {
      -- | Old-style LedgerDB
      ledgerDbCheckpoints :: Maybe (AnchoredSeq
                               (WithOrigin SlotNo)
                               (Checkpoint (l ValuesMK))
                               (Checkpoint (l ValuesMK)))
      -- | New-style LedgerDB
    , ledgerDbChangelog   :: DbChangelog l
    }
  deriving (Generic)

type OldLedgerDB l = AnchoredSeq (WithOrigin SlotNo) (Checkpoint (l ValuesMK)) (Checkpoint (l ValuesMK))
type NewLedgerDB l = DbChangelog l

deriving instance (Eq       (l EmptyMK), Eq       (l ValuesMK)) => Eq       (LedgerDB l)
deriving instance (NoThunks (l EmptyMK), NoThunks (l ValuesMK)) => NoThunks (LedgerDB l)

instance ShowLedgerState l => Show (LedgerDB l) where
  showsPrec = error "showsPrec @LedgerDB"

-- | Internal newtype wrapper around a ledger state @l@ so that we can define a
-- non-blanket 'Anchorable' instance.
newtype Checkpoint l = Checkpoint {
      unCheckpoint :: l
    }
  deriving (Generic)

deriving instance          Eq       (l ValuesMK) => Eq       (Checkpoint (l ValuesMK))
deriving anyclass instance NoThunks (l ValuesMK) => NoThunks (Checkpoint (l ValuesMK))

instance ShowLedgerState l => Show (Checkpoint (l ValuesMK)) where
  showsPrec = error "showsPrec @CheckPoint"

instance GetTip (l ValuesMK) => Anchorable (WithOrigin SlotNo) (Checkpoint (l ValuesMK)) (Checkpoint (l ValuesMK)) where
  asAnchor = id
  getAnchorMeasure _ = getTipSlot . unCheckpoint

{-------------------------------------------------------------------------------
  LedgerDB proper
-------------------------------------------------------------------------------}

-- | Ledger DB starting at the specified ledger state
ledgerDbWithAnchor :: GetTip (l ValuesMK) => l ValuesMK -> LedgerDB l
ledgerDbWithAnchor anchor = LedgerDB {
      ledgerDbCheckpoints = Just $ Empty (Checkpoint anchor)
    , ledgerDbChangelog   = initialDbChangelog (getTipSlot anchor) anchor
    }

-- | Old LedgerDB starting at the given ledger state
oldLedgerDbWithAnchor
  :: GetTip (l ValuesMK)
  => l ValuesMK
  -> OldLedgerDB l
oldLedgerDbWithAnchor = Empty . Checkpoint

-- | Combine both databases, computing which one should be run if they are out
-- of sync.
combineLedgerDBs
  :: forall l blk.
  ( HeaderHash (l ValuesMK) ~ HeaderHash blk
  , HeaderHash (l EmptyMK) ~ HeaderHash blk
  , StandardHash blk
  , GetTip (l EmptyMK)
  , GetTip (l 'ValuesMK)
  )
  => Maybe (OldLedgerDB l) -- ^
  -> NewLedgerDB l -- ^
  -> LedgerDB l
combineLedgerDBs (Just ledgerDbCheckpoints) ledgerDbChangelog =
   assert ((castPoint @(l ValuesMK) @blk . getTip . oldLedgerDbCurrent $ ledgerDbCheckpoints) ==
           (castPoint @(l EmptyMK) @blk . getTip . seqLast . dbChangelogStates $ ledgerDbChangelog))
    LedgerDB (Just ledgerDbCheckpoints) ledgerDbChangelog
combineLedgerDBs Nothing ledgerDbChangelog =
  LedgerDB Nothing ledgerDbChangelog

{-------------------------------------------------------------------------------
  Compute signature

  Depending on the parameters (apply by value or by reference, previously
  applied or not) we get different signatures.
-------------------------------------------------------------------------------}

-- | Resolve a block
--
-- Resolving a block reference to the actual block lives in @m@ because
-- it might need to read the block from disk (and can therefore not be
-- done inside an STM transaction).
--
-- NOTE: The ledger DB will only ask the 'ChainDB' for blocks it knows
-- must exist. If the 'ChainDB' is unable to fulfill the request, data
-- corruption must have happened and the 'ChainDB' should trigger
-- validation mode.
type ResolveBlock m blk = RealPoint blk -> m blk

-- | Annotated ledger errors
data AnnLedgerError (l :: LedgerStateKind) blk = AnnLedgerError {
      -- | The ledger DB just /before/ this block was applied
      --
      -- Note that we will always return here a full LedgerDB. This means
      -- precisely that if we fail to apply blocks during initialization (where
      -- we might apply only to the old/new database instead to both or just the
      -- new), this error cannot be constructed and therefore we will crash with
      -- an @error@ call. If this is the case, we have an immutable database
      -- that is not a valid chain and therefore we are already in trouble.
      annLedgerState  :: LedgerDB l

      -- | Reference to the block that had the error
    , annLedgerErrRef :: RealPoint blk

      -- | The ledger error itself
    , annLedgerErr    :: LedgerErr l
    }

instance (Eq (LedgerErr l), StandardHash blk) => Eq (AnnLedgerError l blk) where
  (AnnLedgerError _ ref err) == (AnnLedgerError _ ref' err') =
    ref == ref' && err == err'

instance Show (LedgerErr l) => Show (AnnLedgerError l blk) where
  show (AnnLedgerError _ _ e) = show e

-- | Monads in which we can resolve blocks
--
-- To guide type inference, we insist that we must be able to infer the type
-- of the block we are resolving from the type of the monad.
class Monad m => ResolvesBlocks m blk | m -> blk where
  resolveBlock :: ResolveBlock m blk

instance Monad m => ResolvesBlocks (ReaderT (ResolveBlock m blk) m) blk where
  resolveBlock r = ReaderT $ \f -> f r

instance ResolvesBlocks m blk => ResolvesBlocks (ExceptT e m) blk where
  resolveBlock = lift . resolveBlock

defaultResolveBlocks :: ResolveBlock m blk
                     -> ReaderT (ResolveBlock m blk) m a
                     -> m a
defaultResolveBlocks = flip runReaderT

class Monad m => ThrowsLedgerError m l blk where
  throwLedgerError :: LedgerDB l -> RealPoint blk -> LedgerErr l -> m a

defaultThrowLedgerErrors :: ExceptT (AnnLedgerError l blk) m a
                         -> m (Either (AnnLedgerError l blk) a)
defaultThrowLedgerErrors = runExceptT

defaultResolveWithErrors :: ResolveBlock m blk
                         -> ExceptT (AnnLedgerError l blk)
                                    (ReaderT (ResolveBlock m blk) m)
                                    a
                         -> m (Either (AnnLedgerError l blk) a)
defaultResolveWithErrors resolve =
      defaultResolveBlocks resolve
    . defaultThrowLedgerErrors

instance Monad m => ThrowsLedgerError (ExceptT (AnnLedgerError l blk) m) l blk where
   throwLedgerError l r e = throwError $ AnnLedgerError l r e

-- | 'Ap' is used to pass information about blocks to ledger DB updates
--
-- The constructors serve two purposes:
--
-- * Specify the various parameters
--   a. Are we passing the block by value or by reference?
--   b. Are we applying or reapplying the block?
--
-- * Compute the constraint @c@ on the monad @m@ in order to run the query:
--   a. If we are passing a block by reference, we must be able to resolve it.
--   b. If we are applying rather than reapplying, we might have ledger errors.
data Ap :: (Type -> Type) -> LedgerStateKind -> Type -> Constraint -> Type where
  ReapplyVal ::           blk -> Ap m l blk ( ReadsKeySets m l )
  ApplyVal   ::           blk -> Ap m l blk ( ReadsKeySets m l
                                            , ThrowsLedgerError m l blk )
  ReapplyRef :: RealPoint blk -> Ap m l blk ( ResolvesBlocks m blk
                                            , ReadsKeySets m l
                                            )
  ApplyRef   :: RealPoint blk -> Ap m l blk ( ResolvesBlocks m blk
                                            , ThrowsLedgerError m l blk
                                            , ReadsKeySets m l
                                            )

  -- | 'Weaken' increases the constraint on the monad @m@.
  --
  -- This is primarily useful when combining multiple 'Ap's in a single
  -- homogeneous structure.
  Weaken :: (c' => c) => Ap m l blk c -> Ap m l blk c'

{-------------------------------------------------------------------------------
  Internal utilities for 'Ap'
-------------------------------------------------------------------------------}

toRealPoint :: HasHeader blk => Ap m l blk c -> RealPoint blk
toRealPoint (ReapplyVal blk) = blockRealPoint blk
toRealPoint (ApplyVal blk)   = blockRealPoint blk
toRealPoint (ReapplyRef rp)  = rp
toRealPoint (ApplyRef rp)    = rp
toRealPoint (Weaken ap)      = toRealPoint ap

-- | Apply block to the current ledger state
--
-- We take in the entire 'LedgerDB' because we record that as part of errors.
applyBlock :: forall m c l blk
            . (ApplyBlock l blk, TickedTableStuff l, Monad m, c)
           => LedgerCfg l
           -> Ap m l blk c
           -> LedgerDB l
           -> m (Either (AnnLedgerError l blk) (Maybe (l ValuesMK), l TrackingMK))
applyBlock cfg ap db = do
  meOld <- maybe (return Nothing) (fmap Just . oldApplyBlock cfg ap) $ ledgerDbCheckpoints db
  eNew  <- newApplyBlock cfg ap $ ledgerDbChangelog db
  case meOld of
    Nothing         ->
      case eNew of
        Left (b, err) -> case ap of
          ApplyRef{} -> return $ Left $ AnnLedgerError db b err
          ApplyVal{} -> return $ Left $ AnnLedgerError db b err
          _          -> error "Unreachable"
        Right new     -> return $ Right (Nothing, new)
    Just res ->
      case (res, eNew) of
        (Left (b, err), Left (_, err'))
          | err == err' -> case ap of
              ApplyRef{} -> return $ Left $ AnnLedgerError db b err
              ApplyVal{} -> return $ Left $ AnnLedgerError db b err
              _          -> error "Unreachable"
          | otherwise -> error "Block application error was different in the implementations"
        (Right (pre,post), Right new) ->

          assert (post == applyDiffs pre (trackingTablesToDiffs new))
                       . return
                       $ Right (Just post, new)
        (Left (b, err), _) ->
             error
          $  "Block application succeeded in the new implementation and failed in the old implementation for block "
          <> show b
          <> " with error "
          <> show err
        (_, Left (b, err)) ->
             error
          $  "Block application succeeded in the old implementation and failed in the new implementation for block "
          <> show b
          <> " with error "
          <> show err

oldApplyBlock :: forall m c l blk
            . (ApplyBlock l blk, TickedTableStuff l, Monad m, c)
           => LedgerCfg l
           -> Ap m l blk c
           -> OldLedgerDB l
           -> m (Either (RealPoint blk, LedgerErr l) (l ValuesMK, l ValuesMK))
oldApplyBlock cfg ap db = fmap (oldTip,) <$> runExceptT (oldApplyBlock' cfg ap oldTip)
  where
    oldTip = either unCheckpoint unCheckpoint . AS.head $ db

oldApplyBlock' :: forall m c l blk
            . (ApplyBlock l blk, TickedTableStuff l, Monad m, c)
           => LedgerCfg l
           -> Ap m l blk c
           -> l ValuesMK
           -> ExceptT (RealPoint blk, LedgerErr l) m (l ValuesMK)
oldApplyBlock' cfg ap oldTip = case ap of
        ReapplyVal b -> return $ forgetLedgerStateTracking $ tickThenReapply cfg b oldTip

        ApplyVal b -> forgetLedgerStateTracking <$>
                      (  either (throwError . (blockRealPoint b,)) return
                        $ runExcept
                        $ tickThenApply cfg b
                        $ oldTip)

        ReapplyRef r  -> do
          b <- resolveBlock r -- TODO: ask: would it make sense to recursively call applyBlock using ReapplyVal?

          return $ forgetLedgerStateTracking $ tickThenReapply cfg b oldTip

        ApplyRef r -> do
          b <- resolveBlock r

          forgetLedgerStateTracking <$>
            ( either (throwError . (blockRealPoint b,)) return
              $ runExcept
              $ tickThenApply cfg b
              $ oldTip)
        Weaken ap' -> oldApplyBlock' cfg ap' oldTip

newApplyBlock :: forall m c l blk
            . (ApplyBlock l blk, TickedTableStuff l, Monad m, c)
           => LedgerCfg l
           -> Ap m l blk c
           -> NewLedgerDB l
           -> m (Either (RealPoint blk, LedgerErr l) (l TrackingMK))
newApplyBlock cfg ap db = case ap of
    ReapplyVal b -> withBlockReadSets b $ \lh ->
                                            return $ return $ tickThenReapply cfg b lh

    ApplyVal b -> withBlockReadSets b $ \lh -> runExceptT $
                                          ( either (throwError . (blockRealPoint b,)) return
                                            $ runExcept
                                            $ tickThenApply cfg b lh)

    ReapplyRef r  -> do
      b <- resolveBlock r -- TODO: ask: would it make sense to recursively call applyBlock using ReapplyVal?

      withBlockReadSets b $ \lh ->
                              return $ return $ tickThenReapply cfg b lh

    ApplyRef r -> do
      b <- resolveBlock r

      withBlockReadSets b $ \lh -> runExceptT $
                              either (throwError . (blockRealPoint b,)) return $ runExcept $
                              tickThenApply cfg b lh

    Weaken ap' -> newApplyBlock cfg ap' db

  where
    withBlockReadSets
      :: ReadsKeySets m l
      => blk
      -> (l ValuesMK -> m (Either (RealPoint blk, LedgerErr l) (l TrackingMK)))
      -> m (Either (RealPoint blk, LedgerErr l) (l TrackingMK))
    withBlockReadSets b f = do
      let ks = getBlockKeySets b :: TableKeySets l
      let aks = rewindTableKeySets db ks :: RewoundTableKeySets l
      urs <- readDb aks
      case withHydratedLedgerState urs f of
        Nothing ->
          -- We performed the rewind;read;forward sequence in this function. So
          -- the forward operation should not fail. If this is the case we're in
          -- the presence of a problem that we cannot deal with at this level,
          -- so we throw an error.
          --
          -- When we introduce pipelining, if the forward operation fails it
          -- could be because the DB handle was modified by a DB flush that took
          -- place when __after__ we read the unforwarded keys-set from disk.
          -- However, performing rewind;read;forward with the same __locked__
          -- changelog should always succeed.
          error "Changelog rewind;read;forward sequence failed."
        Just res -> res

    withHydratedLedgerState
      :: UnforwardedReadSets l
      -> (l ValuesMK -> a)
      -> Maybe a
    withHydratedLedgerState urs f = do
      rs <- forwardTableKeySets db urs
      return $ f $ withLedgerTables (seqLast . dbChangelogStates $ db) rs


{-------------------------------------------------------------------------------
  HD Interface that I need (Could be moved to  Ouroboros.Consensus.Ledger.Basics )
-------------------------------------------------------------------------------}

data Seq (a :: LedgerStateKind) s

seqLast :: Seq state (state mk) -> state mk
seqLast = undefined

seqAt :: SeqNo state -> Seq state (state EmptyMK) -> state EmptyMK
seqAt = undefined

seqAnchorAt :: SeqNo state -> Seq state (state EmptyMK) -> Seq state (state EmptyMK)
seqAnchorAt = undefined

seqLength :: Seq state (state EmptyMK) -> Int
seqLength = undefined

data DbChangelog (l :: LedgerStateKind)
  deriving (Eq, Generic, NoThunks)

newtype RewoundTableKeySets l = RewoundTableKeySets (AnnTableKeySets l ()) -- KeySetSanityInfo l

initialDbChangelog
  :: WithOrigin SlotNo -> l ValuesMK -> DbChangelog l
initialDbChangelog = undefined

initialDbChangelogWithEmptyState :: WithOrigin SlotNo -> l EmptyMK -> NewLedgerDB l
initialDbChangelogWithEmptyState = undefined

rewindTableKeySets
  :: DbChangelog l -> TableKeySets l -> RewoundTableKeySets l
rewindTableKeySets = undefined

newtype UnforwardedReadSets l = UnforwardedReadSets (AnnTableReadSets l ())

forwardTableKeySets
  :: DbChangelog l -> UnforwardedReadSets l -> Maybe (TableReadSets l)
forwardTableKeySets = undefined

extendDbChangelog
  :: SeqNo l
  -> l DiffMK
  -- -> Maybe (l SnapshotsMK) TOOD: We won't use this parameter in the first iteration.
  -> DbChangelog l
  -> DbChangelog l
extendDbChangelog = undefined

dbChangelogStateAnchor :: DbChangelog state -> SeqNo state
dbChangelogStateAnchor = undefined

dbChangelogStates :: DbChangelog state -> Seq state (state EmptyMK)
dbChangelogStates = undefined

dbChangelogRollBack :: Point blk -> DbChangelog state -> DbChangelog state
dbChangelogRollBack = undefined

newtype SeqNo (state :: LedgerStateKind) = SeqNo { unSeqNo :: Word64 }
  deriving (Eq, Ord, Show)

class HasSeqNo (state :: LedgerStateKind) where
  stateSeqNo :: state table -> SeqNo state

-- TODO: flushing the changelog will invalidate other copies of 'LedgerDB'. At
-- the moment the flush-locking concern is outside the scope of this module.
-- Clients need to ensure they flush in a safe manner.
--
ledgerDbFlush
  :: Monad m => (DbChangelog l -> m (DbChangelog l)) -> LedgerDB l -> m (LedgerDB l)
ledgerDbFlush changelogFlush db = do
  ledgerDbChangelog' <- changelogFlush (ledgerDbChangelog db)
  return $! db { ledgerDbChangelog = ledgerDbChangelog' }

class ReadsKeySets m l where

  readDb :: ReadKeySets m l

type ReadKeySets m l = RewoundTableKeySets l -> m (UnforwardedReadSets l)

newtype DbReader m l a = DbReader { runDbReader :: ReaderT (ReadKeySets m l) m a}
  deriving newtype (Functor, Applicative, Monad)

instance ReadsKeySets (DbReader m l) l where
  readDb rks = DbReader $ ReaderT $ \f -> f rks

-- TODO: this is leaking details on how we want to compose monads at the higher levels.
instance (Monad m, ReadsKeySets m l) => ReadsKeySets (ReaderT r m) l where
  readDb = lift . readDb

instance (Monad m, ReadsKeySets m l) => ReadsKeySets (ExceptT e m) l where
  readDb = lift . readDb

defaultReadKeySets :: ReadKeySets m l -> DbReader m l a -> m a
defaultReadKeySets f dbReader = runReaderT (runDbReader dbReader) f

{-------------------------------------------------------------------------------
  Queries
-------------------------------------------------------------------------------}

-- | The ledger state at the tip of the chain
ledgerDbCurrent :: LedgerDB l -> l EmptyMK
ledgerDbCurrent =  ledgerDbCurrentNew . ledgerDbChangelog

-- | The complete ledger state at the tip of the old-style database
ledgerDbCurrentOld :: GetTip (l ValuesMK) => LedgerDB l -> Maybe (l ValuesMK)
ledgerDbCurrentOld = fmap oldLedgerDbCurrent . ledgerDbCheckpoints

ledgerDbCurrentNew :: DbChangelog state -> state EmptyMK
ledgerDbCurrentNew = seqLast . dbChangelogStates

oldLedgerDbCurrent :: GetTip (l ValuesMK) => OldLedgerDB l -> l ValuesMK
oldLedgerDbCurrent = either unCheckpoint unCheckpoint . AS.head

-- | Information about the state of the ledger at the anchor
ledgerDbAnchor :: LedgerDB l -> l EmptyMK
ledgerDbAnchor LedgerDB{..} = seqAt (dbChangelogStateAnchor ledgerDbChangelog) (dbChangelogStates ledgerDbChangelog)

-- | All snapshots currently stored by the ledger DB (new to old)
--
-- This also includes the snapshot at the anchor. For each snapshot we also
-- return the distance from the tip.
ledgerDbSnapshots :: LedgerDB l -> [(Word64, l EmptyMK)]
ledgerDbSnapshots LedgerDB{} = undefined

-- | How many blocks can we currently roll back?
ledgerDbMaxRollback :: GetTip (l ValuesMK) => LedgerDB l -> Word64
ledgerDbMaxRollback LedgerDB{..} =
  let
    old = fromIntegral . AS.length <$> ledgerDbCheckpoints
    new = fromIntegral
        $ seqLength
        $ seqAnchorAt (dbChangelogStateAnchor ledgerDbChangelog) (dbChangelogStates ledgerDbChangelog)
  in
    assert (old == Nothing || old == Just new) new

-- | Reference to the block at the tip of the chain
ledgerDbTip :: IsLedger l => LedgerDB l -> Point (l EmptyMK)
ledgerDbTip = getTip . ledgerDbCurrent

-- | Have we seen at least @k@ blocks?
ledgerDbIsSaturated :: (forall mk. GetTip (l mk)) => SecurityParam -> LedgerDB l -> Bool
ledgerDbIsSaturated (SecurityParam k) db =
    ledgerDbMaxRollback db >= k

-- | Get a past ledger state
--
--  \( O(\log(\min(i,n-i)) \)
--
-- When no ledger state (or anchor) has the given 'Point', 'Nothing' is
-- returned.
ledgerDbPast :: (HasHeader blk, IsLedger l, HeaderHash l ~ HeaderHash blk)
  => Point blk
  -> LedgerDB l
  -> Maybe (l EmptyMK)
ledgerDbPast pt db = ledgerDbCurrent <$> ledgerDbPrefix pt db

-- | Get the Diffs corresponding to the ledger state at the given point.
--
-- This should be used to check for agreement between the old and new style
-- -- databases when applying a block.
-- ledgerDbPastDiffs :: (HasHeader blk, IsLedger l, HeaderHash l ~ HeaderHash blk)
--   => Point blk
--   -> LedgerDB l
--   -> Maybe (l DiffMK)
-- ledgerDbPastDiffs pt db = ledgerDbCurrentDiffs <$> ledgerDbPrefix pt db

-- -- | Extract the Diffs between the last and previous ledger states on the
-- -- LedgerDB.
-- ledgerDbCurrentDiffs :: LedgerDB l -> l DiffMK
-- ledgerDbCurrentDiffs = seqLast . dbChangelogTableDiffs . ledgerDbChangelog

-- -- | Get a past ledger state
-- --
-- --  \( O(\log(\min(i,n-i)) \)
-- --
-- -- When no ledger state (or anchor) has the given 'Point', 'Nothing' is
-- -- returned.
-- ledgerDbPastOld :: GetTip (l ValuesMK) => Point blk -> LedgerDB l -> Maybe (l ValuesMK)
-- ledgerDbPastOld pt db =
--       join $ fmap (fmap unCheckpoint) $ listToMaybe . AS.lookupByMeasure (pointSlot pt) <$> ledgerDbCheckpoints db

-- | Get a prefix of the LedgerDB
--
--  \( O(\log(\min(i,n-i)) \)
--
-- When no ledger state (or anchor) has the given 'Point', 'Nothing' is
-- returned.
ledgerDbPrefix ::
     forall l blk. (HasHeader blk, IsLedger l, HeaderHash l ~ HeaderHash blk)
  => Point blk
  -> LedgerDB l
  -> Maybe (LedgerDB l)
ledgerDbPrefix pt db
    | pt == (castPoint $ getTip (ledgerDbAnchor db))
    = let old = Empty . AS.anchor <$> ledgerDbCheckpoints db
          new = initialDbChangelogWithEmptyState undefined undefined -- TODO: @js fill these
      in
        Just $ combineLedgerDBs @l @blk old new
    | otherwise
    =  do
        checkpoints' <- AS.rollback
                          (pointSlot pt)
                          ((== pt) . castPoint . getTip . unCheckpoint . either id id) <$>
                          (ledgerDbCheckpoints db)

        return $ LedgerDB
                  { ledgerDbCheckpoints = checkpoints'
                  , ledgerDbChangelog   = dbChangelogRollBack pt $ ledgerDbChangelog db
                  }


-- | Transform the underlying 'AnchoredSeq' using the given functions.
ledgerDbBimap ::
     Anchorable (WithOrigin SlotNo) a b
  => (l ValuesMK -> a)
  -> (l ValuesMK -> b)
  -> AnchoredSeq (WithOrigin SlotNo)
                 (Checkpoint (l ValuesMK))
                 (Checkpoint (l ValuesMK))
  -> AnchoredSeq (WithOrigin SlotNo) a b
ledgerDbBimap f g =
    -- Instead of exposing 'ledgerDbCheckpoints' directly, this function hides
    -- the internal 'Checkpoint' type.
    AS.bimap (f . unCheckpoint) (g . unCheckpoint)


-- | Prune snapshots until at we have at most @k@ snapshots in the LedgerDB,
-- excluding the snapshots stored at the anchor.
ledgerDbPrune :: GetTip (l ValuesMK) => SecurityParam -> LedgerDB l -> LedgerDB l
ledgerDbPrune (SecurityParam k) db = db {
      ledgerDbCheckpoints = AS.anchorNewest k <$> ledgerDbCheckpoints db
    }

 -- NOTE: we must inline 'ledgerDbPrune' otherwise we get unexplained thunks in
 -- 'LedgerDB' and thus a space leak. Alternatively, we could disable the
 -- @-fstrictness@ optimisation (enabled by default for -O1). See #2532.
{-# INLINE ledgerDbPrune #-}

{-------------------------------------------------------------------------------
  Internal updates
-------------------------------------------------------------------------------}

-- | Push an updated ledger state to the old database
pushLedgerStateOld ::
     IsLedger l
  => SecurityParam
  -> l ValuesMK -- ^ Updated ledger state
  -> OldLedgerDB l -> OldLedgerDB l
pushLedgerStateOld (SecurityParam k) currentOld' db  =
    AS.anchorNewest k $ db AS.:> Checkpoint currentOld'

-- | Push an updated ledger state to the new database
pushLedgerStateNew ::
     (IsLedger l, TickedTableStuff l)
  => l TrackingMK -- ^ Updated ledger state
  -> NewLedgerDB l -> NewLedgerDB l
pushLedgerStateNew currentNew'  =
     extendDbChangelog (stateSeqNo currentNew')
                                              (trackingTablesToDiffs currentNew')


instance IsLedger l => HasSeqNo l where
  stateSeqNo l =
    case getTipSlot l of
      Origin        -> SeqNo 0
      At (SlotNo n) -> SeqNo (n + 1)

{-------------------------------------------------------------------------------
  Internal: rolling back
-------------------------------------------------------------------------------}

-- | Rollback
--
-- Returns 'Nothing' if maximum rollback is exceeded.
rollback :: forall l. GetTip (l ValuesMK) => Word64 -> LedgerDB l -> Maybe (LedgerDB l)
rollback n db@LedgerDB{..}
    | n <= ledgerDbMaxRollback db
    = Just db {
          ledgerDbCheckpoints = AS.dropNewest (fromIntegral n) <$> ledgerDbCheckpoints,
          ledgerDbChangelog = undefined
        }
    | otherwise
    = Nothing


{-------------------------------------------------------------------------------
  Updates
-------------------------------------------------------------------------------}

-- | Exceeded maximum rollback supported by the current ledger DB state
--
-- Under normal circumstances this will not arise. It can really only happen
-- in the presence of data corruption (or when switching to a shorter fork,
-- but that is disallowed by all currently known Ouroboros protocols).
--
-- Records both the supported and the requested rollback.
data ExceededRollback = ExceededRollback {
      rollbackMaximum   :: Word64
    , rollbackRequested :: Word64
    }

ledgerDbPush :: forall m c l blk
              . (ApplyBlock l blk, TickedTableStuff l, Monad m, c)
             => LedgerDbCfg l
             -> Ap m l blk c
             -> LedgerDB l
             -> m (LedgerDB l)
ledgerDbPush cfg ap db@(LedgerDB {..}) = do
  applyResult <- applyBlock (ledgerDbCfg cfg) ap db
  case applyResult of
    Left (AnnLedgerError d b e) -> case ap of
      ApplyVal _ -> throwLedgerError d b e
      ApplyRef _ -> throwLedgerError d b e
      _          -> error "Reapplying a block failed"
    Right (resOld, resNew) -> return $ db {
        ledgerDbCheckpoints = do
            c <- ledgerDbCheckpoints
            o <- resOld
            return $ pushLedgerStateOld undefined o c
      , ledgerDbChangelog = pushLedgerStateNew resNew ledgerDbChangelog
      }

-- | Push a bunch of blocks (oldest first)
ledgerDbPushMany ::
     forall m c l blk . (ApplyBlock l blk, TickedTableStuff l, Monad m, c)
  => (Pushing blk -> m ())
  -> LedgerDbCfg l
  -> [Ap m l blk c] -> LedgerDB l -> m (LedgerDB l)
ledgerDbPushMany trace cfg aps initDb = (repeatedlyM pushAndTrace) aps initDb
  where
    pushAndTrace ap db = do
      let pushing = Pushing . toRealPoint $ ap
      trace pushing
      ledgerDbPush cfg ap db

-- | Switch to a fork
ledgerDbSwitch :: (ApplyBlock l blk, TickedTableStuff l, Monad m, c)
               => LedgerDbCfg l
               -> Word64          -- ^ How many blocks to roll back
               -> (UpdateLedgerDbTraceEvent blk -> m ())
               -> [Ap m l blk c]  -- ^ New blocks to apply
               -> LedgerDB l
               -> m (Either ExceededRollback (LedgerDB l))
ledgerDbSwitch cfg numRollbacks trace newBlocks db =
    case rollback numRollbacks db of
      Nothing ->
        return $ Left $ ExceededRollback {
            rollbackMaximum   = ledgerDbMaxRollback db
          , rollbackRequested = numRollbacks
          }
      Just db' -> case newBlocks of
        [] -> pure $ Right db'
        -- no blocks to apply to ledger state, return current LedgerDB
        (firstBlock:_) -> do
          let start   = PushStart . toRealPoint $ firstBlock
              goal    = PushGoal  . toRealPoint . last $ newBlocks
          Right <$> ledgerDbPushMany (trace . (StartedPushingBlockToTheLedgerDb start goal))
                                     cfg
                                     newBlocks
                                     db'

{-------------------------------------------------------------------------------
  LedgerDB Config
-------------------------------------------------------------------------------}

data LedgerDbCfg l = LedgerDbCfg {
      ledgerDbCfgSecParam :: !SecurityParam
    , ledgerDbCfg         :: !(LedgerCfg l)
    -- ledgerDbFlushingPolicy :: FP
    --
    -- or
    --
    --
    -- ledgerDbTryFlush  :: dbhandle -> DbChangelog l -> m ()
    }
  deriving (Generic)

deriving instance NoThunks (LedgerCfg l) => NoThunks (LedgerDbCfg l)

type instance HeaderHash (LedgerDB l) = HeaderHash l

instance IsLedger l => GetTip (LedgerDB l) where
  getTip = castPoint . getTip . ledgerDbCurrent

{-------------------------------------------------------------------------------
  Support for testing
-------------------------------------------------------------------------------}

pureBlock :: blk -> Ap m l blk (ReadsKeySets m l)
pureBlock = ReapplyVal

ledgerDbPush' :: (ApplyBlock l blk, TickedTableStuff l, ReadsKeySets Identity l)
              => LedgerDbCfg l -> blk -> LedgerDB l -> LedgerDB l
ledgerDbPush' cfg b = runIdentity . ledgerDbPush cfg (pureBlock b)

ledgerDbPushMany' :: (ApplyBlock l blk, TickedTableStuff l, ReadsKeySets Identity l)
                  => LedgerDbCfg l -> [blk] -> LedgerDB l -> LedgerDB l
ledgerDbPushMany' cfg bs =
  runIdentity . ledgerDbPushMany (const $ pure ()) cfg (map pureBlock bs)

ledgerDbSwitch' :: forall l blk
                 . (ApplyBlock l blk, TickedTableStuff l, ReadsKeySets Identity l)
                => LedgerDbCfg l
                -> Word64 -> [blk] -> LedgerDB l -> Maybe (LedgerDB l)
ledgerDbSwitch' cfg n bs db =
    case runIdentity $ ledgerDbSwitch cfg n (const $ pure ()) (map pureBlock bs) db
    of
      Left  ExceededRollback{} -> Nothing
      Right db'                -> Just db'

{-------------------------------------------------------------------------------
  Serialisation
-------------------------------------------------------------------------------}

-- | Version 1: uses versioning ('Ouroboros.Consensus.Util.Versioned') and only
-- encodes the ledger state @l@.
snapshotEncodingVersion1 :: VersionNumber
snapshotEncodingVersion1 = 1

-- | Encoder to be used in combination with 'decodeSnapshotBackwardsCompatible'.
encodeSnapshot :: (l -> Encoding) -> l -> Encoding
encodeSnapshot encodeLedger l =
    encodeVersion snapshotEncodingVersion1 (encodeLedger l)

-- | To remain backwards compatible with existing snapshots stored on disk, we
-- must accept the old format as well as the new format.
--
-- The old format:
-- * The tip: @WithOrigin (RealPoint blk)@
-- * The chain length: @Word64@
-- * The ledger state: @l@
--
-- The new format is described by 'snapshotEncodingVersion1'.
--
-- This decoder will accept and ignore them. The encoder ('encodeSnapshot') will
-- no longer encode them.
decodeSnapshotBackwardsCompatible ::
     forall l blk.
     Proxy blk
  -> (forall s. Decoder s l)
  -> (forall s. Decoder s (HeaderHash blk))
  -> forall s. Decoder s l
decodeSnapshotBackwardsCompatible _ decodeLedger decodeHash =
    decodeVersionWithHook
      decodeOldFormat
      [(snapshotEncodingVersion1, Decode decodeVersion1)]
  where
    decodeVersion1 :: forall s. Decoder s l
    decodeVersion1 = decodeLedger

    decodeOldFormat :: Maybe Int -> forall s. Decoder s l
    decodeOldFormat (Just 3) = do
        _ <- withOriginRealPointToPoint <$>
               decodeWithOrigin (decodeRealPoint @blk decodeHash)
        _ <- Dec.decodeWord64
        decodeLedger
    decodeOldFormat mbListLen =
        fail $
          "decodeSnapshotBackwardsCompatible: invalid start " <>
          show mbListLen
