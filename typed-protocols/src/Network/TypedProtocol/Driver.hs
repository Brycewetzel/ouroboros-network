{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneKindSignatures #-}
{-# LANGUAGE TypeInType #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE EmptyCase #-}

-- | Actions for running 'Peer's with a 'Driver'
--
module Network.TypedProtocol.Driver (

  -- * Introduction
  -- $intro

  -- * Driver interface
  Driver(..),
  SomeMessage(..),

  -- * Running a peer 
  runPeerWithDriver,

  -- * Re-exports
  DecodeStep (..)
  ) where

import Data.Singletons

import Network.TypedProtocol.Core
import Network.TypedProtocol.Peer
import Network.TypedProtocol.Codec (SomeMessage (..), DecodeStep (..))


-- $intro
--
-- A 'Peer' is a particular implementation of an agent that engages in a
-- typed protocol. To actualy run one we need a source and sink for the typed
-- protocol messages. These are provided by a 'Channel' and a 'Codec'. The
-- 'Channel' represents one end of an untyped duplex message transport, and
-- the 'Codec' handles conversion between the typed protocol messages and
-- the untyped channel.
--
-- So given the 'Peer' and a compatible 'Codec' and 'Channel' we can run the
-- peer in some appropriate monad. The peer and codec have to agree on
-- the same protocol and role in that protocol. The codec and channel have to
-- agree on the same untyped medium, e.g. text or bytes. All three have to
-- agree on the same monad in which they will run.
--
-- This module provides drivers for normal and pipelined peers. There is
-- very little policy involved here so typically it should be possible to
-- use these drivers, and customise things by adjusting the peer, or codec
-- or channel.
--
-- It is of course possible to write custom drivers and the code for these ones
-- may provide a useful starting point. The 'runDecoder' function may be a
-- helpful utility for use in custom drives.
--


--
-- Driver interface
--

data Driver ps (pr :: PeerRole) bytes failure dstate m =
        Driver {
          -- | Send a message.
          --
          -- It allows to update 'dstate'.  This is useful to record when
          -- a message was sent, and check if the response fits within its time
          -- budget,  which allows to have the same timeout policy whether
          -- a message is pipelined or not.
          --
          sendMessage    :: forall (st :: ps) (st' :: ps).
                            ( SingI (PeerHasAgency st)
                            , SingI (ProtocolState st')
                            )
                         => (ReflRelativeAgency (StateAgency st)
                                                 WeHaveAgency
                                                (Relative pr (StateAgency st)))
                         -> Message ps st st'
                         -> dstate
                         -> m dstate

        , -- | Receive a message, a blocking action which reads from the network
          -- and runs the incremental decoder until a full message is decoded.
          -- As an input it might receive a 'DecodeStep' previously started with
          -- 'tryRecvMessage'.
          --
          recvMessage    :: forall (st :: ps).
                            SingI (PeerHasAgency st)
                         => (ReflRelativeAgency (StateAgency st)
                                                 TheyHaveAgency
                                                (Relative pr (StateAgency st)))
                         -> Either ( DecodeStep bytes failure m (SomeMessage st)
                                   , dstate
                                   )
                                   dstate
                         -> m (SomeMessage st, dstate)

        , -- | 'tryRecvMessage' is used to interpret @'Collect' _ (Just k') k@.
          -- If it returns we will continue with @k@, otherwise we keep the
          -- decoder state @DecodeStep@ and continue pipelining using @k'@.
          --
          -- 'tryRecvMessage' ought to be non-blocking.
          --
          tryRecvMessage :: forall (st :: ps).
                            SingI (PeerHasAgency st)
                         => (ReflRelativeAgency (StateAgency st)
                                                 TheyHaveAgency
                                                (Relative pr (StateAgency st)))
                         -> Either    ( DecodeStep bytes failure m (SomeMessage st)
                                      , dstate
                                      )
                                      dstate
                         -> m (Either ( DecodeStep bytes failure m (SomeMessage st)
                                      , dstate
                                      )
                                      ( SomeMessage st
                                      , dstate
                                      ))
            
        , startDState    :: dstate
        }


--
-- Running peers
--

-- | Run a peer with the given driver.
--
-- This runs the peer to completion (if the protocol allows for termination).
--
runPeerWithDriver
  :: forall ps (st :: ps) pr pl bytes failure dstate m a.
     Monad m
  => Driver ps pr bytes failure dstate m
  -> Peer ps pr pl Empty st m a
  -> dstate
  -> m (a, dstate)
runPeerWithDriver Driver{sendMessage, recvMessage, tryRecvMessage} =
    flip goEmpty
  where
    goEmpty
       :: forall st'.
          dstate
       -> Peer ps pr pl 'Empty st' m a
       -> m (a, dstate)
    goEmpty !dstate (Effect k) = k >>= goEmpty dstate

    goEmpty !dstate (Done _ x) = return (x, dstate)

    goEmpty !dstate (Yield refl msg k) = do
      dstate' <- sendMessage refl msg dstate
      goEmpty dstate' k

    goEmpty !dstate (Await refl k) = do
      (SomeMessage msg, dstate') <- recvMessage refl (Right dstate)
      goEmpty dstate' (k msg)

    goEmpty !dstate (YieldPipelined refl msg k) = do
      !dstate' <- sendMessage refl msg dstate
      go (SingCons SingEmpty) (Right dstate') k


    go :: forall st1 st2 st3 q'.
          SingQueue (Tr st1 st2 <| q')
       -> Either ( DecodeStep bytes failure m (SomeMessage st1)
                 , dstate
                 )
                 dstate
       -> Peer ps pr pl (Tr st1 st2 <| q') st3 m a
       -> m (a, dstate)
    go q !dstate (Effect k) = k >>= go q dstate

    go q !dstate (YieldPipelined
                  refl
                  (msg :: Message ps st3 st')
                  (k   :: Peer ps pr pl ((Tr st1 st2 <| q') |> Tr st' st'') st'' m a))
                = do
      !dstate' <- sendMessage refl msg (getDState dstate)
      go (q |> (SingTr :: SingTrans (Tr st' st'')))
         (setDState dstate' dstate) k

    go (SingCons q) !dstate (Collect refl Nothing k) = do
      (SomeMessage msg, dstate') <- recvMessage refl dstate
      go (SingCons q) (Right dstate') (k msg)

    go q@(SingCons q') !dstate (Collect refl (Just k') k) = do
      r <- tryRecvMessage refl dstate
      case r of
        Left dstate' ->
          go q (Left dstate') k'
        Right (SomeMessage msg, dstate') ->
          go (SingCons q') (Right dstate') (k msg)


    go (SingCons SingEmpty)     (Right dstate) (CollectDone k) =
      goEmpty dstate k

    go (SingCons q@SingCons {}) (Right dstate) (CollectDone k) =
      go q (Right dstate) k

    go  SingCons {}              Left {}        CollectDone {} =
      -- 'CollectDone' can only be executed once `Collect` was effective, which
      -- means we cannot receive a partial decoder here.
      error "runPeerWithDriver: unexpected parital decoder"

    --
    -- lenses
    --

    getDState :: Either (x, dstate) dstate -> dstate
    getDState (Left (_, dstate)) = dstate
    getDState (Right dstate)     = dstate

    setDState :: dstate -> Either (x, dstate) dstate -> Either (x, dstate) dstate
    setDState dstate (Left (x, _dstate)) = Left (x, dstate)
    setDState dstate (Right _dstate)    = Right dstate
