-- |
--   This module implements redis locks as specified at https://redis.io/topics/distlock
--   The algorithm is experimental and not formally analyzed.
module Database.Redis.Redlock
  ( Lock,
    lockValidity,
    lockResource,
    lockToken,
    RedlockError (..),
    lock,
    unlock,
    defaultRedlockCfg,
    RedlockCfg (..),
  )
where

import Control.Concurrent (threadDelay)
import Control.Exception (Exception, try)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Bifunctor (second)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Char (ord)
import Data.Data (Typeable)
import Data.Either (partitionEithers)
import Database.Redis
  ( Condition (Nx),
    Connection,
    Redis,
    RedisCtx,
    Reply,
    SetOpts (SetOpts),
    Status,
    eval,
    runRedis,
    setOpts,
  )

data Lock e m = Lock
  { lockValidity :: Integer,
    lockResource :: ByteString,
    lockToken :: ByteString,
    reconnect :: [m (Either e Connection)]
  }

data RedlockError e
  = -- | Locking attempts exceeded retryCount
    ResourceNotLocked
  | -- | Could not lock majority of instancees.
    MultiError [Either e Reply]
  deriving (Eq, Show, Typeable)

data RedlockCfg = RedlockCfg
  { -- | How long to wait (in *milliseconds*) before trying to unlock a ressource.
    retryDelay :: Int,
    -- | How many attempts to unlock before failure
    retryCount :: Int
  }
  deriving (Eq, Show)

defaultRedlockCfg :: RedlockCfg
defaultRedlockCfg = RedlockCfg {retryDelay = 200, retryCount = 3}

-- | Release lock on a resource
unlock ::
  (Exception e, MonadIO m) =>
  Lock e m ->
  m (Maybe (RedlockError e), [Status])
unlock Lock {lockResource = rsrc, lockToken = token, reconnect = recon} =
  tryRedises recon (unlockInstance rsrc token)

-- | Acquire lock on a ressource
lock ::
  (Monad m, MonadIO m, Exception e) =>
  RedlockCfg ->
  -- | *Finite* list for establishing connections to all redis masters.
  --   This is re-evaluated frequently!
  [m (Either e Connection)] ->
  -- | Retrieves current time in *milliseconds*
  m Integer ->
  -- | Key of the ressource
  ByteString ->
  -- | unique lock token string
  ByteString ->
  -- | time to live in milliseconds
  Integer ->
  -- | retrieves a lock or errors.
  m (Either (RedlockError e) (Lock e m))
lock RedlockCfg {retryCount = maxRetries, retryDelay = delaymsecs} connects currentMillis resource token ttlmsecs =
  go 0
  where
    clockDriftFactor = 0.01 :: Double
    delayMicros = delaymsecs * (10 ^ (3 :: Integer))
    quorum
      | length connects == 1 = 1
      | otherwise = (length connects `div` 2) + 1
    drift = floor $ fromIntegral ttlmsecs * clockDriftFactor + 2
    bulkRedis = tryRedises connects

    go retries
      | retries >= maxRetries = pure $ Left ResourceNotLocked
      | otherwise = do
        startTime <- currentMillis
        (errs, oks) <- bulkRedis (lockInstance resource token ttlmsecs)
        elapsedTime <- (`subtract` startTime) <$> currentMillis
        let validity = ttlmsecs - elapsedTime - drift
        if validity > 0 && length oks >= quorum
          then
            maybe
              (pure $ Right $ Lock validity resource token connects)
              (pure . Left)
              errs
          else do
            _ <- bulkRedis (unlockInstance resource token)
            liftIO $ threadDelay delayMicros
            go (retries + 1)

-- | internal function to associate redis results along with IO errors to a connection
tryRedises ::
  (Exception e, MonadIO m) =>
  [m (Either e Connection)] ->
  Redis (Either Reply a) ->
  m (Maybe (RedlockError e), [a])
tryRedises connects act = do
  (ioErrs, (redisErrs, vals)) <-
    second partitionEithers
      . partitionEithers
      <$> mapM
        ( \conn -> do
            ec <- conn
            case ec of
              Left e -> pure $ Left e
              Right c -> liftIO $ try (runRedis c act)
        )
        connects
  let errs = (Left <$> ioErrs) <> (Right <$> redisErrs)
  if null errs
    then pure (Nothing, vals)
    else pure (Just (MultiError errs), vals)

-- | internal redis query to receive lock to ressource on an instance
lockInstance ::
  RedisCtx m f => ByteString -> ByteString -> Integer -> m (f Status)
lockInstance rsrc token msecs =
  setOpts rsrc token (SetOpts Nothing (Just msecs) (Just Nx))

-- | internal redis query to release lock to ressource on an instance
unlockInstance :: ByteString -> ByteString -> Redis (Either Reply Status)
unlockInstance rsrc token = eval unlockScript [rsrc] [token]
  where
    unlockScript :: ByteString
    unlockScript =
      BS.pack $
        map (fromIntegral . ord) $
          unlines
            [ "if redis.call(\"get\",KEYS[1]) == ARGV[1] then",
              "    return redis.call(\"del\",KEYS[1])",
              "else",
              "    return 0",
              "end"
            ]
