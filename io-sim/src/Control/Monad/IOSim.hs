{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Control.Monad.IOSim (
  -- * Simulation monad
  IOSim,
  STMSim,
  -- ** Run simulation
  runSim,
  runSimOrThrow,
  runSimStrictShutdown,
  Failure(..),
  runSimTrace,
  runSimTraceST,
  liftST,
  traceM,
  traceSTM,
  -- * Simulation time
  setCurrentTime,
  unshareClock,
  -- * Simulation trace
  Trace,
  Octopus (Trace, TraceMainReturn, TraceMainException, TraceDeadlock),
  Value(..),
  EventCtx(..),
  TraceEvent(..),
  ThreadLabel,
  Labelled (..),
  traceEvents,
  traceResult,
  selectTraceEvents,
  selectTraceEvents',
  selectTraceEventsDynamic,
  selectTraceEventsDynamic',
  selectTraceEventsSay,
  selectTraceEventsSay',
  octoSelectTraceEvents,
  octoSelectTraceEventsDynamic,
  octoSelectTraceEventsSay,
  printTraceEventsSay,
  -- * Eventlog
  EventlogEvent(..),
  EventlogMarker(..),
  -- * Low-level API
  execReadTVar,
  -- * Deprecated interfaces
  SimM,
  SimSTM
  ) where

import           Prelude

import           Data.Dynamic (fromDynamic)
import           Data.List (intercalate)
import           Data.Bifoldable
import           Data.Typeable (Typeable)

import           Control.Exception (throw)

import           Control.Monad.ST.Lazy

import           Control.Monad.Class.MonadThrow as MonadThrow
import           Control.Monad.Class.MonadTime

import           Control.Monad.IOSim.Internal
import           Data.List.Octopus


selectTraceEvents
    :: (TraceEvent -> Maybe b)
    -> Trace a
    -> [b]
selectTraceEvents fn =
      bifoldr ( \ v _
               -> case v of
                    MainException _ e _       -> throw (FailureException e)
                    Deadlock      _   threads -> throw (FailureDeadlock threads)
                    MainReturn    _ _ _       -> []
              )
              ( \ b acc -> b : acc )
              []
    . octoSelectTraceEvents fn

selectTraceEvents'
    :: (TraceEvent -> Maybe b)
    -> Trace a
    -> [b]
selectTraceEvents' fn =
      bifoldr ( \ _ _   -> []  )
              ( \ b acc -> b : acc )
              []
    . octoSelectTraceEvents fn

-- | Select all the traced values matching the expected type. This relies on
-- the sim's dynamic trace facility.
--
-- For convenience, this throws exceptions for abnormal sim termination.
--
selectTraceEventsDynamic :: forall a b. Typeable b => Trace a -> [b]
selectTraceEventsDynamic = selectTraceEvents fn
  where
    fn :: TraceEvent -> Maybe b
    fn (EventLog dyn) = fromDynamic dyn
    fn _              = Nothing

-- | Like 'selectTraceEventsDynamic' but returns partial trace if an excpetion
-- is found in it.
--
selectTraceEventsDynamic' :: forall a b. Typeable b => Trace a -> [b]
selectTraceEventsDynamic' = selectTraceEvents' fn
  where
    fn :: TraceEvent -> Maybe b
    fn (EventLog dyn) = fromDynamic dyn
    fn _              = Nothing

-- | Get a trace of 'EventSay'.
--
-- For convenience, this throws exceptions for abnormal sim termination.
--
selectTraceEventsSay :: Trace a -> [String]
selectTraceEventsSay = selectTraceEvents fn
  where
    fn :: TraceEvent -> Maybe String
    fn (EventSay s) = Just s
    fn _            = Nothing

-- | Like 'selectTraceEventsSay' but return partial trace if an exception is
-- found in it.
--
selectTraceEventsSay' :: Trace a -> [String]
selectTraceEventsSay' = selectTraceEvents' fn
  where
    fn :: TraceEvent -> Maybe String
    fn (EventSay s) = Just s
    fn _            = Nothing

-- | Print all 'EventSay' to the console.
--
-- For convenience, this throws exceptions for abnormal sim termination.
--
printTraceEventsSay :: Trace a -> IO ()
printTraceEventsSay = mapM_ print . selectTraceEventsSay


-- | The most general select function.  It is a _total_ function.
--
octoSelectTraceEvents
    :: (TraceEvent -> Maybe b)
    -> Trace a
    -> Octopus (Value a) b
octoSelectTraceEvents fn = bifoldr ( \ v _acc -> Nil v )
                                   ( \ eventCtx acc
                                    -> case fn (ecTraceEvent eventCtx) of
                                         Nothing -> acc
                                         Just b  -> Cons b acc
                                   )
                                   undefined -- it is ignored

-- | Select dynamic events.  It is a _total_ function.
--
octoSelectTraceEventsDynamic :: forall a b. Typeable b
                             => Trace a -> Octopus (Value a) b
octoSelectTraceEventsDynamic = octoSelectTraceEvents fn
  where
    fn :: TraceEvent -> Maybe b
    fn (EventLog dyn) = fromDynamic dyn
    fn _              = Nothing


-- | Select say events.  It is a _total_ function.
--
octoSelectTraceEventsSay :: forall a.  Trace a -> Octopus (Value a) String
octoSelectTraceEventsSay = octoSelectTraceEvents fn
  where
    fn :: TraceEvent -> Maybe String
    fn (EventSay s) = Just s
    fn _            = Nothing

-- | Simulation termination with failure
--
data Failure =
       -- | The main thread terminated with an exception
       FailureException SomeException

       -- | The threads all deadlocked
     | FailureDeadlock ![Labelled ThreadId]

       -- | The main thread terminated normally but other threads were still
       -- alive, and strict shutdown checking was requested.
       -- See 'runSimStrictShutdown'
     | FailureSloppyShutdown [Labelled ThreadId]
  deriving Show

instance Exception Failure where
    displayException (FailureException err) = displayException  err
    displayException (FailureDeadlock threads) =
      concat [ "<<io-sim deadlock: "
             , intercalate ", " (show `map` threads)
             , ">>"
             ]
    displayException (FailureSloppyShutdown threads) =
      concat [ "<<io-sim sloppy shutdown: "
             , intercalate ", " (show `map` threads)
             , ">>"
             ]

-- | 'IOSim' is a pure monad.
--
runSim :: forall a. (forall s. IOSim s a) -> Either Failure a
runSim mainAction = traceResult False (runSimTrace mainAction)

-- | For quick experiments and tests it is often appropriate and convenient to
-- simply throw failures as exceptions.
--
runSimOrThrow :: forall a. (forall s. IOSim s a) -> a
runSimOrThrow mainAction =
    case runSim mainAction of
      Left  e -> throw e
      Right x -> x

-- | Like 'runSim' but also fail if when the main thread terminates, there
-- are other threads still running or blocked. If one is trying to follow
-- a strict thread cleanup policy then this helps testing for that.
--
runSimStrictShutdown :: forall a. (forall s. IOSim s a) -> Either Failure a
runSimStrictShutdown mainAction = traceResult True (runSimTrace mainAction)

traceResult :: Bool -> Trace a -> Either Failure a
traceResult strict = go
  where
    go (Trace _ _ _ _ t)                = go t
    go (TraceMainReturn _ _ tids@(_:_))
                               | strict = Left (FailureSloppyShutdown tids)
    go (TraceMainReturn _ x _)          = Right x
    go (TraceMainException _ e _)       = Left (FailureException e)
    go (TraceDeadlock   _   threads)    = Left (FailureDeadlock threads)

traceEvents :: Trace a -> [(Time, ThreadId, Maybe ThreadLabel, TraceEvent)]
traceEvents (Trace time tid tlbl event t) = (time, tid, tlbl, event)
                                          : traceEvents t
traceEvents _                             = []



-- | See 'runSimTraceST' below.
--
runSimTrace :: forall a. (forall s. IOSim s a) -> Trace a
runSimTrace mainAction = runST (runSimTraceST mainAction)
