{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -Wno-missing-import-lists #-}

-- |
-- Description : High-level interface for all producer-related functions, using exceptions instead of @Either@
--
-- To implement an ASAP:O producer, you should only need this interface.
-- It exposes no memory-management functions (like free) or pointers, and
-- is thus safe to use.
module Asapo.Producer
  ( Endpoint (..),
    ProducerException (..),
    ProcessingThreads (..),
    RequestHandlerType (..),
    Error (..),
    Metadata (..),
    SourceCredentials (..),
    MessageId (..),
    DeletionFlags (..),
    Producer,
    LogLevel (..),
    FileName (..),
    PipelineStep (..),
    Beamtime (Beamtime),
    DataSource (DataSource),
    Beamline (Beamline),
    StreamInfo (..),
    SourceType (..),
    StreamName (..),
    messageIdFromInt,
    InstanceId (..),
    Token (..),
    DatasetSubstream (..),
    DatasetSize (..),
    VersionInfo (..),
    UpsertMode (..),
    MetadataIngestMode (..),
    AutoIdFlag (..),
    TransferFlag (..),
    StorageFlag (..),
    RequestResponse (..),
    Opcode (..),
    GenericRequestHeader (..),
    withProducer,
    getVersionInfo,
    getStreamInfo,
    getLastStream,
    send,
    deleteStream,
    getBeamtimeMeta,
    getStreamMeta,
    sendFile,
    sendStreamFinishedFlag,
    sendBeamtimeMetadata,
    sendStreamMetadata,
    setLogLevel,
    enableLocalLog,
    enableRemoteLog,
    setCredentials,
    getRequestsQueueSize,
    getRequestsQueueVolumeMb,
    setRequestsQueueLimits,
    waitRequestsFinished,
  )
where

import Asapo.Either.Common
  ( Beamline (Beamline),
    Beamtime (Beamtime),
    DataSource (DataSource),
    InstanceId (..),
    MessageId (..),
    PipelineStep (..),
    SourceCredentials (..),
    SourceType (..),
    StreamInfo (..),
    StreamName (..),
    Token (..),
    messageIdFromInt,
  )
import Asapo.Either.Producer
  ( AutoIdFlag,
    DatasetSize,
    DatasetSubstream,
    DeletionFlags,
    Endpoint,
    Error (Error),
    FileName,
    GenericRequestHeader (..),
    LogLevel (..),
    Metadata,
    MetadataIngestMode,
    Opcode (..),
    ProcessingThreads,
    Producer,
    RequestHandlerType,
    RequestResponse,
    StorageFlag,
    TransferFlag,
    UpsertMode,
    VersionInfo,
    enableLocalLog,
    enableRemoteLog,
    getRequestsQueueSize,
    getRequestsQueueVolumeMb,
    setLogLevel,
    setRequestsQueueLimits,
  )
import qualified Asapo.Either.Producer as PlainProducer
import Control.Applicative (pure)
import Control.Exception (Exception, throw)
import Control.Monad (Monad)
import qualified Data.ByteString as BS
import Data.Either (Either (Left, Right))
import Data.Int (Int)
import Data.Maybe (Maybe)
import Data.Text (Text)
import Data.Time (NominalDiffTime)
import System.IO (IO)
import Text.Show (Show)
import Prelude ()

newtype ProducerException = ProducerException Text deriving (Show)

instance Exception ProducerException

-- | Create a producer and do something with it. This is the main entrypoint into the producer.
withProducer ::
  forall a.
  Endpoint ->
  ProcessingThreads ->
  RequestHandlerType ->
  SourceCredentials ->
  -- | timeout
  NominalDiffTime ->
  (Producer -> IO a) ->
  IO a
withProducer endpoint processingThreads handlerType sourceCredentials timeout onSuccess =
  let onError (Error message) = throw (ProducerException message)
   in PlainProducer.withProducer endpoint processingThreads handlerType sourceCredentials timeout onError onSuccess

maybeThrow :: (Monad m) => m (Either Error b) -> m b
maybeThrow f = do
  result <- f
  case result of
    Left (Error message) -> throw (ProducerException message)
    Right v -> pure v

-- | Retrieve producer version info
getVersionInfo :: Producer -> IO VersionInfo
getVersionInfo producer = maybeThrow (PlainProducer.getVersionInfo producer)

-- | Retrieve info for a single stream
getStreamInfo ::
  Producer ->
  StreamName ->
  -- | Timeout
  NominalDiffTime ->
  IO StreamInfo
getStreamInfo producer stream timeout = maybeThrow (PlainProducer.getStreamInfo producer stream timeout)

-- | Retrieve info for the latest stream
getLastStream ::
  Producer ->
  -- | Timeout
  NominalDiffTime ->
  IO StreamInfo
getLastStream producer timeout = maybeThrow (PlainProducer.getLastStream producer timeout)

-- | Retrieve metadata for the given stream (which might be missing, in which case @Nothing@ is returned)
getStreamMeta ::
  Producer ->
  StreamName ->
  -- | timeout
  NominalDiffTime ->
  IO (Maybe Text)
getStreamMeta producer stream timeout = maybeThrow (PlainProducer.getStreamMeta producer stream timeout)

-- | Retrieve metadata for the given stream (which might be missing, in which case @Nothing@ is returned)
getBeamtimeMeta ::
  Producer ->
  -- | timeout
  NominalDiffTime ->
  IO (Maybe Text)
getBeamtimeMeta producer timeout = maybeThrow (PlainProducer.getBeamtimeMeta producer timeout)

deleteStream ::
  Producer ->
  StreamName ->
  -- | timeout
  NominalDiffTime ->
  [DeletionFlags] ->
  IO Int
deleteStream producer stream timeout deletionFlags = maybeThrow (PlainProducer.deleteStream producer stream timeout deletionFlags)

-- | Send a message containing raw data. Due to newtype and enum usage, all parameter should be self-explanatory
send ::
  Producer ->
  MessageId ->
  FileName ->
  Metadata ->
  DatasetSubstream ->
  DatasetSize ->
  AutoIdFlag ->
  BS.ByteString ->
  TransferFlag ->
  StorageFlag ->
  StreamName ->
  (RequestResponse -> IO ()) ->
  IO Int
send producer messageId fileName metadata datasetSubstream datasetSize autoIdFlag data' transferFlag storageFlag stream callback =
  maybeThrow (PlainProducer.send producer messageId fileName metadata datasetSubstream datasetSize autoIdFlag data' transferFlag storageFlag stream callback)

-- | Send a message containing a file. Due to newtype and enum usage, all parameter should be self-explanatory
sendFile ::
  Producer ->
  MessageId ->
  FileName ->
  Metadata ->
  DatasetSubstream ->
  DatasetSize ->
  AutoIdFlag ->
  -- | Size
  Int ->
  FileName ->
  TransferFlag ->
  StorageFlag ->
  StreamName ->
  (RequestResponse -> IO ()) ->
  IO Int
sendFile producer messageId fileName meta datasetSubstream datasetSize autoIdFlag size fileNameToSend transferFlag storageFlag stream callback =
  maybeThrow (PlainProducer.sendFile producer messageId fileName meta datasetSubstream datasetSize autoIdFlag size fileNameToSend transferFlag storageFlag stream callback)

-- | As the title says, send the "stream finished" flag
sendStreamFinishedFlag :: Producer -> StreamName -> MessageId -> StreamName -> (RequestResponse -> IO ()) -> IO Int
sendStreamFinishedFlag producer stream lastId nextStream callback =
  maybeThrow
    ( PlainProducer.sendStreamFinishedFlag
        producer
        stream
        lastId
        nextStream
        callback
    )

-- | Send or extend beamtime metadata
sendBeamtimeMetadata :: Producer -> Metadata -> MetadataIngestMode -> UpsertMode -> (RequestResponse -> IO ()) -> IO Int
sendBeamtimeMetadata producer metadata ingestMode upsertMode callback = maybeThrow (PlainProducer.sendBeamtimeMetadata producer metadata ingestMode upsertMode callback)

-- | Send or extend stream metadata
sendStreamMetadata :: Producer -> Metadata -> MetadataIngestMode -> UpsertMode -> StreamName -> (RequestResponse -> IO ()) -> IO Int
sendStreamMetadata producer metadata ingestMode upsertMode stream callback = maybeThrow (PlainProducer.sendStreamMetadata producer metadata ingestMode upsertMode stream callback)

-- | Set a different set of credentials
setCredentials :: Producer -> SourceCredentials -> IO Int
setCredentials producer creds = maybeThrow (PlainProducer.setCredentials producer creds)

-- | Wait for all outstanding requests to finish
waitRequestsFinished :: Producer -> NominalDiffTime -> IO Int
waitRequestsFinished producer timeout = maybeThrow (PlainProducer.waitRequestsFinished producer timeout)
