{-# LANGUAGE DataKinds               #-}
{-# LANGUAGE DuplicateRecordFields   #-}
{-# LANGUAGE FlexibleContexts        #-}
{-# LANGUAGE FlexibleInstances       #-}
{-# LANGUAGE GADTs                   #-}
{-# LANGUAGE MultiParamTypeClasses   #-}
{-# LANGUAGE PolyKinds               #-}
{-# LANGUAGE ScopedTypeVariables     #-}
{-# LANGUAGE TypeFamilies            #-}
{-# LANGUAGE UndecidableInstances    #-}
{-# LANGUAGE UndecidableSuperClasses #-}

{-# OPTIONS_GHC -Wno-orphans #-}

module Ouroboros.Consensus.Shelley.Node (
    MaxMajorProtVer (..)
  , ProtocolParamsAllegra (..)
  , ProtocolParamsAlonzo (..)
  , ProtocolParamsMary (..)
  , ProtocolParamsShelley (..)
  , ProtocolParamsShelleyBased (..)
  , SL.Nonce (..)
  , SL.ProtVer (..)
  , SL.ShelleyGenesis (..)
  , SL.ShelleyGenesisStaking (..)
  , SL.emptyGenesisStaking
  , ShelleyLeaderCredentials (..)
  , protocolClientInfoShelley
  , protocolInfoShelley
  , protocolInfoShelleyBased
  , registerGenesisStaking
  , registerInitialFunds
  , validateGenesis
  ) where



import           Ouroboros.Consensus.Block
import           Ouroboros.Consensus.Node.ProtocolInfo
import           Ouroboros.Consensus.Node.Run

import qualified Cardano.Ledger.Shelley.API as SL

import           Ouroboros.Consensus.Protocol.TPraos
import           Ouroboros.Consensus.Shelley.Ledger
import           Ouroboros.Consensus.Shelley.Ledger.Inspect ()
import           Ouroboros.Consensus.Shelley.Ledger.NetworkProtocolVersion ()
import           Ouroboros.Consensus.Shelley.Node.Serialisation ()
import           Ouroboros.Consensus.Shelley.Node.TPraos
{-------------------------------------------------------------------------------
  ProtocolInfo
-------------------------------------------------------------------------------}



protocolClientInfoShelley :: ProtocolClientInfo (ShelleyBlock proto era)
protocolClientInfoShelley =
    ProtocolClientInfo {
      -- No particular codec configuration is needed for Shelley
      pClientInfoCodecConfig = ShelleyCodecConfig
    }

{-------------------------------------------------------------------------------
  RunNode instance
-------------------------------------------------------------------------------}

instance ShelleyCompatible era proto => BlockSupportsMetrics (ShelleyBlock proto era) where
  isSelfIssued cfg hdr =
       case csvSelfIssued $ selectView cfg hdr of
         SelfIssued    -> IsSelfIssued
         NotSelfIssued -> IsNotSelfIssued

instance ShelleyCompatible era proto => RunNode (ShelleyBlock proto era)
