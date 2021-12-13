{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# OPTIONS_GHC -Wno-orphans #-}

-- | Node configuration common to all (era, protocol) combinations deriving from
-- Shelley.
module Ouroboros.Consensus.Shelley.Node.Common
  ( ShelleyLeaderCredentials (..),
    ProtocolParamsShelleyBased (..),
    shelleyBlockIssuerVKey,
    -- -- * SOP machinery
    -- NP2(..),
    -- (:..:)
  )
where

import qualified Cardano.Ledger.Keys as SL
import qualified Cardano.Ledger.Shelley.API as SL
import Cardano.Ledger.Slot
import Data.Text (Text)
import Ouroboros.Consensus.Block
  ( CannotForge,
    ForgeStateInfo,
    ForgeStateUpdateError,
  )
import Ouroboros.Consensus.Config (maxRollbacks)
import Ouroboros.Consensus.Config.SupportsNode
import Ouroboros.Consensus.Node.InitStorage
import qualified Ouroboros.Consensus.Protocol.Ledger.HotKey as HotKey
import Ouroboros.Consensus.Protocol.Praos.Common
  ( PraosCanBeLeader (praosCanBeLeaderColdVerKey),
  )
import Ouroboros.Consensus.Shelley.Eras (EraCrypto)
import Ouroboros.Consensus.Shelley.Ledger
  ( ShelleyBlock,
    ShelleyCompatible,
    shelleyNetworkMagic,
    shelleyStorageConfigSecurityParam,
    shelleyStorageConfigSlotsPerKESPeriod,
    shelleySystemStart,
    verifyBlockIntegrity,
  )
import Ouroboros.Consensus.Shelley.Protocol.Abstract (ProtocolHeaderSupportsProtocol (CannotForgeError))
import Ouroboros.Consensus.Storage.ImmutableDB

{-------------------------------------------------------------------------------
  Credentials
-------------------------------------------------------------------------------}

data ShelleyLeaderCredentials c = ShelleyLeaderCredentials
  { -- | The unevolved signing KES key (at evolution 0).
    --
    -- Note that this is not inside 'ShelleyCanBeLeader' since it gets evolved
    -- automatically, whereas 'ShelleyCanBeLeader' does not change.
    shelleyLeaderCredentialsInitSignKey :: SL.SignKeyKES c,
    shelleyLeaderCredentialsCanBeLeader :: PraosCanBeLeader c,
    -- | Identifier for this set of credentials.
    --
    -- Useful when the node is running with multiple sets of credentials.
    shelleyLeaderCredentialsLabel :: Text
  }

shelleyBlockIssuerVKey ::
  ShelleyLeaderCredentials c -> SL.VKey 'SL.BlockIssuer c
shelleyBlockIssuerVKey =
  praosCanBeLeaderColdVerKey . shelleyLeaderCredentialsCanBeLeader

{-------------------------------------------------------------------------------
  BlockForging
-------------------------------------------------------------------------------}

type instance CannotForge (ShelleyBlock proto era) = CannotForgeError proto

type instance ForgeStateInfo (ShelleyBlock proto era) = HotKey.KESInfo

type instance ForgeStateUpdateError (ShelleyBlock proto era) = HotKey.KESEvolutionError

{-------------------------------------------------------------------------------
  ConfigSupportsNode instance
-------------------------------------------------------------------------------}

instance ConfigSupportsNode (ShelleyBlock proto era) where
  getSystemStart = shelleySystemStart
  getNetworkMagic = shelleyNetworkMagic

{-------------------------------------------------------------------------------
  NodeInitStorage instance
-------------------------------------------------------------------------------}

instance ShelleyCompatible era proto => NodeInitStorage (ShelleyBlock proto era) where
  -- We fix the chunk size to @10k@ so that we have the same chunk size as
  -- Byron. Consequently, a Shelley net will have the same chunk size as the
  -- Byron-to-Shelley net with the same @k@.
  nodeImmutableDbChunkInfo =
    simpleChunkInfo
      . EpochSize
      . (* 10)
      . maxRollbacks
      . shelleyStorageConfigSecurityParam

  nodeCheckIntegrity cfg =
    verifyBlockIntegrity (shelleyStorageConfigSlotsPerKESPeriod cfg)

{-------------------------------------------------------------------------------
  Protocol parameters
-------------------------------------------------------------------------------}

-- | Parameters common to all Shelley-based ledgers.
--
-- When running a chain with multiple Shelley-based eras, in addition to the
-- per-era protocol parameters, one value of 'ProtocolParamsShelleyBased' will
-- be needed, which is shared among all Shelley-based eras.
--
-- The @era@ parameter determines from which era the genesis config will be
-- used.
data ProtocolParamsShelleyBased era = ProtocolParamsShelleyBased
  { shelleyBasedGenesis :: SL.ShelleyGenesis era,
    -- | The initial nonce, typically derived from the hash of Genesis
    -- config JSON file.
    --
    -- WARNING: chains using different values of this parameter will be
    -- mutually incompatible.
    shelleyBasedInitialNonce :: SL.Nonce,
    shelleyBasedLeaderCredentials :: [ShelleyLeaderCredentials (EraCrypto era)]
  }

{-------------------------------------------------------------------------------
  SOP machinery
-------------------------------------------------------------------------------}

-- -- | Variation on '(:.:)' for an second argument of kind (* -> * -> *).
-- newtype (:..:) (f :: l -> Type) (g :: j -> k -> l) (j1 :: j) (k1 :: k) =
--   Comp2 (f (g j1 k1))
--     deriving Generic

-- -- | Variation on 'Data.SOP.NP.NP' applied to tupled lists and arguments of kind
-- -- (* -> * -> *).
-- data NP2 :: (k -> l -> Type) -> [(k, l)] -> Type where
--   Nil :: NP2 f '[]
--   (:*) :: f x y -> NP2 f xs -> NP2 f ('(x,y) ': xs)
