{-# LANGUAGE DataKinds                #-}
{-# LANGUAGE DisambiguateRecordFields #-}
{-# LANGUAGE FlexibleContexts         #-}
{-# LANGUAGE NamedFieldPuns           #-}
{-# LANGUAGE OverloadedStrings        #-}
{-# LANGUAGE RecordWildCards          #-}
{-# LANGUAGE ScopedTypeVariables      #-}
{-# LANGUAGE TypeApplications         #-}
{-# LANGUAGE TypeFamilies             #-}

module Test.Consensus.Shelley.Examples (
    -- * Setup
    codecConfig
  , testShelleyGenesis
    -- * Examples
  , examplesAllegra
  , examplesAlonzo
  , examplesMary
  , examplesShelley
  ) where

import qualified Data.Map as Map
import qualified Data.Set as Set
import           GHC.Stack (HasCallStack)

import           Cardano.Slotting.EpochInfo (fixedEpochInfo)
import           Cardano.Slotting.Time (mkSlotLength)

import           Cardano.Ledger.Alonzo.Genesis (AlonzoGenesis (..))
import           Cardano.Ledger.Alonzo.Language (Language (PlutusV1))
import           Cardano.Ledger.Alonzo.Scripts (CostModel (..), ExUnits (..),
                     Prices (..))
import           Cardano.Ledger.BaseTypes (NonNegativeInterval, boundRational)
import           Cardano.Ledger.Coin (Coin (..))
import qualified Cardano.Ledger.Era as Core
import           Cardano.Ledger.Shelley.Genesis (mkShelleyGlobals)

import           Ouroboros.Network.Block (Serialised (..))

import           Ouroboros.Consensus.Block
import           Ouroboros.Consensus.HeaderValidation
import           Ouroboros.Consensus.Ledger.Extended
import           Ouroboros.Consensus.Ledger.SupportsMempool
import           Ouroboros.Consensus.Storage.Serialisation
import           Ouroboros.Consensus.Util.Time (secondsToNominalDiffTime)

import           Test.Cardano.Ledger.Shelley.Orphans ()

import           Ouroboros.Consensus.Shelley.Eras
import           Ouroboros.Consensus.Shelley.Ledger

import           Test.Util.Orphans.Arbitrary ()
import           Test.Util.Serialisation.Golden (labelled, unlabelled)
import qualified Test.Util.Serialisation.Golden as Golden
import           Test.Util.Serialisation.Roundtrip (SomeResult (..))

import           Ouroboros.Consensus.Protocol.TPraos (TPraosState (TPraosState))

import           Test.Cardano.Ledger.Allegra.Examples.Consensus
                     (ledgerExamplesAllegra)
import           Test.Cardano.Ledger.Alonzo.Examples.Consensus
                     (ledgerExamplesAlonzo)
import           Test.Cardano.Ledger.Mary.Examples.Consensus
                     (ledgerExamplesMary)
import           Test.Cardano.Ledger.Shelley.Examples.Consensus
                     (ShelleyLedgerExamples (..), ShelleyResultExamples (..),
                     ledgerExamplesShelley, testShelleyGenesis)

{-------------------------------------------------------------------------------
  Examples
-------------------------------------------------------------------------------}

codecConfig :: CodecConfig (ShelleyBlock Ouroboros.Consensus.Shelley.Eras.StandardShelley)
codecConfig = ShelleyCodecConfig

fromShelleyLedgerExamples
  :: ShelleyBasedEra era
  => Core.TranslationContext era
  -> ShelleyLedgerExamples era
  -> Golden.Examples (ShelleyBlock era)
fromShelleyLedgerExamples tc ShelleyLedgerExamples {
                            sleResultExamples = ShelleyResultExamples{..}
                            , ..} =
  Golden.Examples {
      exampleBlock            = unlabelled blk
    , exampleSerialisedBlock  = unlabelled serialisedBlock
    , exampleHeader           = unlabelled $ getHeader blk
    , exampleSerialisedHeader = unlabelled serialisedHeader
    , exampleHeaderHash       = unlabelled hash
    , exampleGenTx            = unlabelled tx
    , exampleGenTxId          = unlabelled $ txId tx
    , exampleApplyTxErr       = unlabelled sleApplyTxError
    , exampleQuery            = queries
    , exampleResult           = results
    , exampleAnnTip           = unlabelled annTip
    , exampleLedgerConfig     = unlabelled ledgerConfig
    , exampleLedgerState      = unlabelled ledgerState
    , exampleChainDepState    = unlabelled chainDepState
    , exampleExtLedgerState   = unlabelled extLedgerState
    , exampleSlotNo           = unlabelled slotNo
    }
  where
    blk = mkShelleyBlock sleBlock
    hash = ShelleyHash sleHashHeader
    serialisedBlock = Serialised "<BLOCK>"
    tx = mkShelleyTx sleTx
    slotNo = SlotNo 42
    serialisedHeader =
      SerialisedHeaderFromDepPair $ GenDepPair (NestedCtxt CtxtShelley) (Serialised "<HEADER>")
    queries = labelled [
          ("GetLedgerTip",              SomeSecond GetLedgerTip)
        , ("GetEpochNo",                SomeSecond GetEpochNo)
        , ("GetCurrentPParams",         SomeSecond GetCurrentPParams)
        , ("GetProposedPParamsUpdates", SomeSecond GetProposedPParamsUpdates)
        , ("GetStakeDistribution",      SomeSecond GetStakeDistribution)
        , ("GetNonMyopicMemberRewards", SomeSecond $ GetNonMyopicMemberRewards sleRewardsCredentials)
        , ("GetGenesisConfig",          SomeSecond GetGenesisConfig)
      ]
    results = labelled [
          ("LedgerTip",              SomeResult GetLedgerTip (blockPoint blk))
        , ("EpochNo",                SomeResult GetEpochNo 10)
        , ("EmptyPParams",           SomeResult GetCurrentPParams srePParams)
        , ("ProposedPParamsUpdates", SomeResult GetProposedPParamsUpdates sreProposedPPUpdates)
        , ("StakeDistribution",      SomeResult GetStakeDistribution srePoolDistr)
        , ("NonMyopicMemberRewards", SomeResult (GetNonMyopicMemberRewards Set.empty)
                                     (NonMyopicMemberRewards $ sreNonMyopicRewards))
        , ("GenesisConfig",          SomeResult GetGenesisConfig (compactGenesis sreShelleyGenesis))
        ]
    annTip = AnnTip {
        annTipSlotNo  = SlotNo 14
      , annTipBlockNo = BlockNo 6
      , annTipInfo    = hash
      }
    ledgerState = ShelleyLedgerState {
        shelleyLedgerTip        = NotOrigin ShelleyTip {
                                    shelleyTipSlotNo  = SlotNo 9
                                  , shelleyTipBlockNo = BlockNo 3
                                  , shelleyTipHash    = hash
                                  }
    , shelleyLedgerState      = sleNewEpochState
    , shelleyLedgerTransition = ShelleyTransitionInfo {shelleyAfterVoting = 0}
    }
    chainDepState = TPraosState (NotOrigin 1) sleChainDepState
    extLedgerState = ExtLedgerState
                       ledgerState
                       (genesisHeaderState chainDepState)
    ledgerConfig = exampleShelleyLedgerConfig tc

examplesShelley :: Golden.Examples (ShelleyBlock StandardShelley)
examplesShelley = fromShelleyLedgerExamples () ledgerExamplesShelley

examplesAllegra :: Golden.Examples (ShelleyBlock StandardAllegra)
examplesAllegra = fromShelleyLedgerExamples () ledgerExamplesAllegra

examplesMary :: Golden.Examples (ShelleyBlock StandardMary)
examplesMary = fromShelleyLedgerExamples () ledgerExamplesMary

examplesAlonzo :: Golden.Examples (ShelleyBlock StandardAlonzo)
examplesAlonzo = fromShelleyLedgerExamples tc ledgerExamplesAlonzo
  where
    tc = AlonzoGenesis {
            coinsPerUTxOWord     = Coin 1
          , costmdls             = Map.fromList [(PlutusV1, CostModel (Map.fromList [("A", 79), ("V", 78)]))]
          , prices               = Prices (boundRational' 90) (boundRational' 91)
          , maxTxExUnits         = ExUnits 123 123
          , maxBlockExUnits      = ExUnits 223 223
          , maxValSize           = 1234
          , collateralPercentage = 20
          , maxCollateralInputs  = 30
          }

    boundRational' :: HasCallStack => Rational -> NonNegativeInterval
    boundRational' x = case boundRational x of
      Nothing -> error $ "Expected non-negative value but got: " <> show x
      Just x' -> x'

exampleShelleyLedgerConfig :: Core.TranslationContext era -> ShelleyLedgerConfig era
exampleShelleyLedgerConfig translationContext = ShelleyLedgerConfig {
      shelleyLedgerCompactGenesis = compactGenesis testShelleyGenesis
    , shelleyLedgerGlobals = mkShelleyGlobals
        testShelleyGenesis
        epochInfo
        26
    , shelleyLedgerTranslationContext = translationContext
    }
  where
    epochInfo  = fixedEpochInfo (EpochSize 4) slotLength
    slotLength = mkSlotLength (secondsToNominalDiffTime 7)
