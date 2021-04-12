{-# LANGUAGE CPP #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}
module Katip.Scribes.SyncHandle.FileOwner where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Debounce
import Control.DeepSeq
import Control.Exception
import Control.Monad
import Data.ByteString.Lazy   as BL
import Data.IORef
import GHC.IO.Handle
import GHC.IO.Handle.FD
import GHC.IO.IOMode

#if MIN_VERSION_base(4,8,0)
import Numeric.Natural
#endif


-- | File owner struct writing data to the file
-- sequentially. Automatic buffer flushing and log rotating.
data FileOwner = FileOwner
  { foDataQueune   :: TBQueue BL.ByteString
  , foControlQueue :: TBQueue ControlMsg
  , foAsync        :: Async ()
    -- ^ Wait for to be sure that worker thread is closed.
  }

#if MIN_VERSION_base(4,8,0)
type QueueLen = Natural
#else
type QueueLen = Int
#endif

data FileOwnerSettings = FileOwnerSettings
  { fosDebounceFreq    :: Maybe Int
  , fosDataQueueLen    :: QueueLen
  , fosControlQueueLen :: QueueLen
  }

defaultFileOwnerSettings :: FileOwnerSettings
defaultFileOwnerSettings = FileOwnerSettings
  { fosDebounceFreq    = Just 200000 -- every 200ms
  , fosDataQueueLen    = 1000
  , fosControlQueueLen = 100
  }

data ControlMsg
  = CloseMsg
  | FlushMsg
  | ReopenMsg
  deriving (Eq, Ord, Show)

newFileOwner :: FilePath -> FileOwnerSettings -> IO FileOwner
newFileOwner fp s = do
  dqueue <- newTBQueueIO $ fosDataQueueLen s
  cqueue <- newTBQueueIO $ fosControlQueueLen s
  let
    newResource = do
      openBinaryFile fp AppendMode
    ack = newResource >>= newIORef
    release ref = do
      h <- readIORef ref
      hFlush h
      hClose h
    go ref = do
      let
        readAllData = do
          a <- readTBQueue dqueue
          -- flushTBQueue never retries
          as <- flushTBQueue dqueue
          return $ a:as
        readMsg
          =   (Left <$> readTBQueue cqueue)
          <|> (Right <$> readAllData)
        debounce action = case fosDebounceFreq s of
          Nothing -> do
            return $ return ()
          Just freq -> mkDebounce $ defaultDebounceSettings
            { debounceAction = action
            , debounceFreq   = freq }
      flush <- debounce $ readIORef ref >>= hFlush
      let
        recur = atomically readMsg >>= \case
          Right bs -> do
            h <- readIORef ref
            BL.hPutStr h $ mconcat bs
            flush -- auto debounced flush
            recur
          Left c -> case c of
            CloseMsg -> do
              lastMsgs <- atomically $ flushTBQueue dqueue
              -- flush never blocks
              case lastMsgs of
                [] -> return ()
                _  -> do
                  h <- readIORef ref
                  BL.hPutStr h $ mconcat lastMsgs
                  return () -- release will flush and close the handler
            FlushMsg -> do
              readIORef ref >>= hFlush
              recur
            ReopenMsg -> do
              newH <- newResource
              oldH <- atomicModifyIORef' ref (\oldH -> (newH, oldH))
              hFlush oldH
              hClose oldH
              recur
      recur
    worker :: IO ()
    worker = try (bracket ack release go) >>= \case
      Left (_ :: SomeException)   -> do
        threadDelay 100000
        -- to not restart the worker function too often
        worker
        -- We dont want the worker thread become unavailable because
        -- of exceptions. E.g. could not create or write the file.
      Right ()                    -> return ()
  asyncRet <- async worker
  return $ FileOwner
    { foDataQueune   = dqueue
    , foControlQueue = cqueue
    , foAsync        = asyncRet
    }

fileOwnerControl :: FileOwner -> ControlMsg -> IO ()
fileOwnerControl fo msg = do
  atomically $ writeTBQueue (foControlQueue fo) msg
  when (msg == CloseMsg) $ do
    -- Wait for worker thread to finish
    void $ waitCatch $ foAsync fo

writeFileOwner :: FileOwner -> BL.ByteString -> IO ()
writeFileOwner fo bs = do
  a <- return $!! bs
  -- The deepseq here is to be sure that writing thread will not
  -- calculate thunks and will not get into the blackhole or
  -- something. Precalculating is the responsibility of the sending
  -- thread, because there may be several sending threads and only one
  -- writing.
  atomically $ writeTBQueue (foDataQueune fo) $!! a
