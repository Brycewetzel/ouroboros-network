{-# LANGUAGE DataKinds #-}

module Ouroboros.Consensus.Protocol.Praos.Views (
    HeaderView (..)
  , LedgerView (..)
  ) where

import           Cardano.Crypto.KES (SignedKES)
import           Cardano.Crypto.VRF (CertifiedVRF, VRFAlgorithm (VerKeyVRF))
import           Cardano.Ledger.Crypto (KES, VRF)
import           Cardano.Ledger.Keys (KeyRole (BlockIssuer), VKey)
import qualified Cardano.Ledger.Shelley.API as SL
import           Cardano.Protocol.TPraos.BHeader (PrevHash)
import           Cardano.Protocol.TPraos.OCert (OCert)
import           Cardano.Slotting.Slot (SlotNo)
import           Data.ByteString (ByteString)
import           Ouroboros.Consensus.Protocol.Praos.VRF (UnifiedVRF)

-- | View of the block header required by the Praos protocol.
data HeaderView crypto = HeaderView
  { -- | Hash of the previous block
    hvPrevHash    :: !(PrevHash crypto),
    -- | verification key of block issuer
    hvVK          :: !(VKey 'BlockIssuer crypto),
    -- | VRF verification key for block issuer
    hvVrfVK       :: !(VerKeyVRF (VRF crypto)),
    -- | VRF result
    hvVrfRes      :: !(CertifiedVRF (VRF crypto) UnifiedVRF),
    -- | operational certificate
    hvOCert       :: !(OCert crypto),
    -- | Slot
    hvSlotNo      :: !SlotNo,
    -- | Bytes of the header which must be signed
    hvSignedBytes :: !ByteString,
    -- | KES Signature of the header
    hvSignature   :: !(SignedKES (KES crypto) ByteString)
  }

newtype LedgerView crypto = LedgerView
  { -- | Stake distribution
    lvPoolDistr :: SL.PoolDistr crypto
  } deriving Show
