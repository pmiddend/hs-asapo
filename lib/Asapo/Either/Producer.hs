{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Description : High-level interface for all producer-related functions
--
-- To implement an ASAP:O producer, you should only need this interface.
-- It exposes no memory-management functions (like free) or pointers, and
-- is thus safe to use.
module Asapo.Either.Producer
  ( Endpoint (..),
    ProcessingThreads (..),
    RequestHandlerType (..),
    Error (..),
    Metadata (..),
    SourceCredentials,
    DeletionFlags (..),
    Producer,
    LogLevel (..),
    FileName (..),
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
    getRequestsQueueSize,
    getRequestsQueueVolumeMb,
    setRequestsQueueLimits,
    checkError,
    checkErrorWithGivenHandle,
    withProducer,
    enableLocalLog,
    waitRequestsFinished,
    getVersionInfo,
    getStreamInfo,
    getStreamMeta,
    getBeamtimeMeta,
    deleteStream,
    getLastStream,
    send,
    sendFile,
    sendStreamFinishedFlag,
    sendBeamtimeMetadata,
    sendStreamMetadata,
    setLogLevel,
    enableRemoteLog,
    setCredentials,
  )
where

import Asapo.Either.Common (MessageId (MessageId), SourceCredentials, StreamInfo, StreamName (StreamName), nominalDiffToMillis, peekCStringText, retrieveStreamInfoFromC, stringHandleToText, stringHandleToTextUnsafe, withCStringNToText, withConstText, withCredentials, withPtr, withText)
import Asapo.Raw.Common (AsapoErrorHandle, AsapoStreamInfoHandle, AsapoStringHandle, asapo_error_explain, asapo_free_error_handle, asapo_free_stream_info_handle, asapo_free_string_handle, asapo_is_error, asapo_new_error_handle, asapo_new_string_handle)
import Asapo.Raw.Producer
  ( AsapoGenericRequestHeader (AsapoGenericRequestHeader),
    AsapoLogLevel,
    AsapoMessageHeaderHandle,
    AsapoOpcode,
    AsapoProducerHandle,
    AsapoRequestCallbackPayloadHandle,
    asapoLogLevelDebug,
    asapoLogLevelError,
    asapoLogLevelInfo,
    asapoLogLevelNone,
    asapoLogLevelWarning,
    asapo_create_message_header,
    asapo_create_producer,
    asapo_free_message_header_handle,
    asapo_free_producer_handle,
    asapo_producer_delete_stream,
    asapo_producer_enable_local_log,
    asapo_producer_enable_remote_log,
    asapo_producer_get_beamtime_meta,
    asapo_producer_get_last_stream,
    asapo_producer_get_requests_queue_size,
    asapo_producer_get_requests_queue_volume_mb,
    asapo_producer_get_stream_info,
    asapo_producer_get_stream_meta,
    asapo_producer_get_version_info,
    asapo_producer_send,
    asapo_producer_send_beamtime_metadata,
    asapo_producer_send_file,
    asapo_producer_send_stream_finished_flag,
    asapo_producer_send_stream_metadata,
    asapo_producer_set_credentials,
    asapo_producer_set_log_level,
    asapo_producer_set_requests_queue_limits,
    asapo_producer_wait_requests_finished,
    asapo_request_callback_payload_get_original_header,
    asapo_request_callback_payload_get_response,
    createRequestCallback,
    kFilesystem,
    kInsert,
    kOpcodeAuthorize,
    kOpcodeCount,
    kOpcodeDeleteStream,
    kOpcodeGetBufferData,
    kOpcodeGetMeta,
    kOpcodeLastStream,
    kOpcodeStreamInfo,
    kOpcodeTransferData,
    kOpcodeTransferDatasetData,
    kOpcodeTransferMetaData,
    kOpcodeUnknownOp,
    kReplace,
    kStoreInDatabase,
    kStoreInFilesystem,
    kTcp,
    kTransferData,
    kTransferMetaDataOnly,
    kUpdate,
  )
import Control.Applicative (Applicative (pure))
import Control.Exception (bracket)
import Data.Bits ((.|.))
import Data.Bool (Bool)
import qualified Data.ByteString as BS
import Data.ByteString.Unsafe (unsafeUseAsCString)
import Data.Either (Either (Left, Right))
import Data.Eq (Eq ((==)))
import Data.Foldable (Foldable (elem))
import Data.Functor ((<$>))
import Data.Int (Int)
import Data.Maybe (Maybe (Just, Nothing))
import Data.Ord ((>))
import Data.Text (Text)
import Data.Time (NominalDiffTime)
import Data.Word (Word64)
import Foreign (Storable (peek), alloca, castPtr)
import Foreign.C.ConstPtr (ConstPtr (unConstPtr))
import Foreign.Ptr (Ptr)
import System.IO (IO)
import Text.Show (Show)
import Prelude (fromIntegral)

-- | Wrapper around an ASAP:O producer error. Note that there is only
--  an error "explanation" here, no error code, since the C interface
--  does not expose this.
newtype Error = Error Text deriving (Show)

-- | Wrapper around an ASAP:O producer endpoint (usually something like "host:port")
newtype Endpoint = Endpoint Text

-- | Wrapper around the number of ASAP:O processing threads (simply to make call signatures mor readable)
newtype ProcessingThreads = ProcessingThreads Int

-- | This has no documentation in ASAP:O yet
data RequestHandlerType = TcpHandler | FilesystemHandler

-- | Opaque wrapper around an ASAP:O producer
newtype Producer = Producer AsapoProducerHandle

-- | Internal function to check and return either an error (if it's
-- present behind the given handle) or a "result" of some function
-- that produced the error handle
checkErrorWithGivenHandle :: AsapoErrorHandle -> b -> IO (Either Error b)
checkErrorWithGivenHandle errorHandle result = do
  isError <- asapo_is_error errorHandle
  if isError > 0
    then do
      let explanationLength = 1024
      explanation <- withCStringNToText explanationLength \explanationPtr ->
        asapo_error_explain
          errorHandle
          explanationPtr
          (fromIntegral explanationLength)
      pure (Left (Error explanation))
    else pure (Right result)

withErrorHandle :: (AsapoErrorHandle -> IO c) -> IO c
withErrorHandle = bracket asapo_new_error_handle asapo_free_error_handle

-- | Helper function since most ASAP:O functions receive a pointer to
-- an error handle as the last argument, so error checking becomes
-- "abstractable"
checkError :: (Ptr AsapoErrorHandle -> IO b) -> IO (Either Error b)
checkError f = do
  withErrorHandle \errorHandle -> do
    (errorHandlePtr, result) <- withPtr errorHandle f
    checkErrorWithGivenHandle errorHandlePtr result

create :: Endpoint -> ProcessingThreads -> RequestHandlerType -> SourceCredentials -> NominalDiffTime -> IO (Either Error AsapoProducerHandle)
create (Endpoint endpoint) (ProcessingThreads processingThreads) handlerType sourceCredentials timeout = do
  withCredentials sourceCredentials \credentials' ->
    let convertHandlerType TcpHandler = kTcp
        convertHandlerType FilesystemHandler = kFilesystem
     in do
          withText endpoint \endpoint' -> do
            checkError
              ( asapo_create_producer
                  endpoint'
                  (fromIntegral processingThreads)
                  (convertHandlerType handlerType)
                  credentials'
                  (nominalDiffToMillis timeout)
              )

-- | Create a producer and do something with it. This is the main entrypoint into the producer
withProducer ::
  forall a.
  Endpoint ->
  ProcessingThreads ->
  RequestHandlerType ->
  SourceCredentials ->
  -- | timeout
  NominalDiffTime ->
  (Error -> IO a) ->
  (Producer -> IO a) ->
  IO a
withProducer endpoint processingThreads handlerType sourceCredentials timeout onError onSuccess = bracket (create endpoint processingThreads handlerType sourceCredentials timeout) freeProducer handle
  where
    freeProducer :: Either Error AsapoProducerHandle -> IO ()
    freeProducer (Left _) = pure ()
    freeProducer (Right producerHandle) = asapo_free_producer_handle producerHandle
    handle :: Either Error AsapoProducerHandle -> IO a
    handle (Left e) = onError e
    handle (Right v) = onSuccess (Producer v)

withStringHandle :: (AsapoStringHandle -> IO c) -> IO c
withStringHandle = bracket asapo_new_string_handle asapo_free_string_handle

data VersionInfo = VersionInfo
  { versionClient :: Text,
    versionServer :: Text,
    versionSupported :: Bool
  }
  deriving (Show)

-- | Retrieve producer version info
getVersionInfo :: Producer -> IO (Either Error VersionInfo)
getVersionInfo (Producer producerHandle) =
  withStringHandle \clientInfo -> withStringHandle \serverInfo -> alloca \supportedPtr -> do
    result <- checkError (asapo_producer_get_version_info producerHandle clientInfo serverInfo supportedPtr)
    case result of
      Left e -> pure (Left e)
      -- The return value is a CInt which is unnecessary probably?
      Right _integerReturnCode -> do
        supported <- peek supportedPtr
        clientInfo' <- stringHandleToTextUnsafe clientInfo
        serverInfo' <- stringHandleToTextUnsafe serverInfo
        pure (Right (VersionInfo clientInfo' serverInfo' (supported > 0)))

-- | Retrieve info for a single stream
getStreamInfo ::
  Producer ->
  StreamName ->
  -- | Timeout
  NominalDiffTime ->
  IO (Either Error StreamInfo)
getStreamInfo (Producer producer) (StreamName stream) timeout = bracket init destroy f
  where
    init :: IO (Either Error AsapoStreamInfoHandle)
    init = withConstText stream \streamC -> checkError (asapo_producer_get_stream_info producer streamC (nominalDiffToMillis timeout))
    destroy :: Either Error AsapoStreamInfoHandle -> IO ()
    destroy (Right handle) = asapo_free_stream_info_handle handle
    destroy _ = pure ()
    f :: Either Error AsapoStreamInfoHandle -> IO (Either Error StreamInfo)
    f (Left e) = pure (Left e)
    f (Right streamInfoHandle) = Right <$> retrieveStreamInfoFromC streamInfoHandle

-- | Retrieve info for the latest stream
getLastStream ::
  Producer ->
  -- | Timeout
  NominalDiffTime ->
  IO (Either Error StreamInfo)
getLastStream (Producer producer) timeout = bracket init destroy f
  where
    init :: IO (Either Error AsapoStreamInfoHandle)
    init = checkError (asapo_producer_get_last_stream producer (nominalDiffToMillis timeout))
    destroy :: Either Error AsapoStreamInfoHandle -> IO ()
    destroy (Right handle) = asapo_free_stream_info_handle handle
    destroy _ = pure ()
    f :: Either Error AsapoStreamInfoHandle -> IO (Either Error StreamInfo)
    f (Left e) = pure (Left e)
    f (Right streamInfoHandle) = Right <$> retrieveStreamInfoFromC streamInfoHandle

-- | Retrieve metadata for the given stream (which might be missing, in which case @Nothing@ is returned)
getStreamMeta ::
  Producer ->
  StreamName ->
  -- | timeout
  NominalDiffTime ->
  IO (Either Error (Maybe Text))
getStreamMeta (Producer producer) (StreamName stream) timeout = bracket init destroy f
  where
    init :: IO (Either Error AsapoStringHandle)
    init = withConstText stream \streamC -> checkError (asapo_producer_get_stream_meta producer streamC (nominalDiffToMillis timeout))
    destroy :: Either Error AsapoStringHandle -> IO ()
    destroy (Right handle) = asapo_free_string_handle handle
    destroy _ = pure ()
    f :: Either Error AsapoStringHandle -> IO (Either Error (Maybe Text))
    f (Left e) = pure (Left e)
    f (Right string) = Right <$> stringHandleToText string

-- | Retrieve metadata for the given stream (which might be missing, in which case @Nothing@ is returned)
getBeamtimeMeta ::
  Producer ->
  -- | timeout
  NominalDiffTime ->
  IO (Either Error (Maybe Text))
getBeamtimeMeta (Producer producer) timeout = bracket init destroy f
  where
    init :: IO (Either Error AsapoStringHandle)
    init = checkError (asapo_producer_get_beamtime_meta producer (nominalDiffToMillis timeout))
    destroy :: Either Error AsapoStringHandle -> IO ()
    destroy (Right handle) = asapo_free_string_handle handle
    destroy _ = pure ()
    f :: Either Error AsapoStringHandle -> IO (Either Error (Maybe Text))
    f (Left e) = pure (Left e)
    f (Right string) = Right <$> stringHandleToText string

data DeletionFlags
  = -- | Delete metadata also
    DeleteMeta
  | -- | Don't throw an error if the data doesn't exist anyways
    DeleteErrorOnNotExist
  deriving (Eq)

-- | Delete the given stream
deleteStream ::
  Producer ->
  StreamName ->
  -- | timeout
  NominalDiffTime ->
  [DeletionFlags] ->
  IO (Either Error Int)
deleteStream (Producer producer) (StreamName stream) timeout deletionFlags = do
  result <- withConstText stream \streamC ->
    checkError
      ( asapo_producer_delete_stream
          producer
          streamC
          (nominalDiffToMillis timeout)
          (if DeleteMeta `elem` deletionFlags then 1 else 0)
          (if DeleteErrorOnNotExist `elem` deletionFlags then 1 else 0)
      )
  pure (fromIntegral <$> result)

-- | Wrapper around file name (dubious to use @Text@ here, but fine for now)
newtype FileName = FileName Text

-- | Wrapper around metadata to be produced
newtype Metadata = Metadata Text

-- | Wrapper around the substream to use
newtype DatasetSubstream = DatasetSubstream Int

-- | Wrapper around the dataset size to use
newtype DatasetSize = DatasetSize Int

-- | Anti-boolean-blindness for the "auto id" flag in the message header
data AutoIdFlag = UseAutoId | NoAutoId deriving (Eq)

-- | Which data to transfer
data TransferFlag = DataAndMetadata | MetadataOnly

-- | Where to store the data
data StorageFlag = Filesystem | Database | FilesystemAndDatabase

convertSendFlags :: TransferFlag -> StorageFlag -> Word64
convertSendFlags tf sf = convertTransferFlag tf .|. convertStorageFlag sf
  where
    convertTransferFlag :: TransferFlag -> Word64
    convertTransferFlag DataAndMetadata = fromIntegral kTransferData
    convertTransferFlag MetadataOnly = fromIntegral kTransferMetaDataOnly
    convertStorageFlag :: StorageFlag -> Word64
    convertStorageFlag Filesystem = fromIntegral kStoreInFilesystem
    convertStorageFlag Database = fromIntegral kStoreInDatabase
    convertStorageFlag FilesystemAndDatabase = fromIntegral kStoreInDatabase .|. fromIntegral kStoreInFilesystem

-- | Internal function to create a message header handle, which gets used to send data.
withMessageHeaderHandle ::
  MessageId ->
  FileName ->
  Metadata ->
  DatasetSubstream ->
  DatasetSize ->
  AutoIdFlag ->
  Int ->
  (AsapoMessageHeaderHandle -> IO b) ->
  IO b
withMessageHeaderHandle
  (MessageId messageId)
  (FileName fileName)
  (Metadata metadata)
  (DatasetSubstream datasetSubstream)
  (DatasetSize datasetSize)
  autoIdFlag
  dataSize = bracket init destroy
    where
      init :: IO AsapoMessageHeaderHandle
      init = withConstText fileName \fileNameC -> withConstText metadata \metadataC ->
        asapo_create_message_header
          messageId
          (fromIntegral dataSize)
          fileNameC
          metadataC
          (fromIntegral datasetSubstream)
          (fromIntegral datasetSize)
          (if autoIdFlag == UseAutoId then 1 else 0)
      destroy = asapo_free_message_header_handle

data Opcode
  = OpcodeUnknownOp
  | OpcodeTransferData
  | OpcodeTransferDatasetData
  | OpcodeStreamInfo
  | OpcodeLastStream
  | OpcodeGetBufferData
  | OpcodeAuthorize
  | OpcodeTransferMetaData
  | OpcodeDeleteStream
  | OpcodeGetMeta
  | OpcodeCount
  | OpcodePersistStream

convertOpcode :: AsapoOpcode -> Opcode
convertOpcode x | x == kOpcodeUnknownOp = OpcodeUnknownOp
convertOpcode x | x == kOpcodeTransferData = OpcodeTransferData
convertOpcode x | x == kOpcodeTransferDatasetData = OpcodeTransferDatasetData
convertOpcode x | x == kOpcodeStreamInfo = OpcodeStreamInfo
convertOpcode x | x == kOpcodeLastStream = OpcodeLastStream
convertOpcode x | x == kOpcodeGetBufferData = OpcodeGetBufferData
convertOpcode x | x == kOpcodeAuthorize = OpcodeAuthorize
convertOpcode x | x == kOpcodeTransferMetaData = OpcodeTransferMetaData
convertOpcode x | x == kOpcodeDeleteStream = OpcodeDeleteStream
convertOpcode x | x == kOpcodeGetMeta = OpcodeGetMeta
convertOpcode x | x == kOpcodeCount = OpcodeCount
convertOpcode _ = OpcodePersistStream

-- | Information about the send request, to be used in the ASAP:O send callback
data GenericRequestHeader = GenericRequestHeader
  { genericRequestHeaderOpCode :: Opcode,
    genericRequestHeaderDataId :: Int,
    genericRequestHeaderDataSize :: Int,
    genericRequestHeaderMetaSize :: Int,
    genericRequestHeaderCustomData :: [Int],
    genericRequestHeaderMessage :: BS.ByteString,
    genericRequestHeaderStream :: Text,
    genericRequestHeaderApiVersion :: Text
  }

convertRequestHeader :: AsapoGenericRequestHeader -> IO GenericRequestHeader
convertRequestHeader (AsapoGenericRequestHeader opcode dataId dataSize metaSize customData message stream apiVersion) = do
  streamText <- peekCStringText stream
  apiVersionText <- peekCStringText apiVersion
  messageAsBs <- BS.packCStringLen (message, fromIntegral dataSize)
  let customData' :: [Int]
      customData' = fromIntegral <$> customData
  pure
    ( GenericRequestHeader
        (convertOpcode opcode)
        (fromIntegral dataId)
        (fromIntegral dataSize)
        (fromIntegral metaSize)
        customData'
        messageAsBs
        streamText
        apiVersionText
    )

-- | Information about the request and its response, to be used in the ASAP:O send callback
data RequestResponse = RequestResponse
  { responsePayload :: Text,
    responseOriginalRequestHeader :: GenericRequestHeader,
    responseError :: Maybe Error
  }

sendRequestCallback :: (RequestResponse -> IO ()) -> Ptr () -> AsapoRequestCallbackPayloadHandle -> AsapoErrorHandle -> IO ()
sendRequestCallback simpleCallback _data payloadHandle errorHandle = do
  payloadText <- bracket (asapo_request_callback_payload_get_response payloadHandle) asapo_free_string_handle stringHandleToTextUnsafe
  originalHeaderCPtr <- asapo_request_callback_payload_get_original_header payloadHandle
  originalHeaderC <- peek (unConstPtr originalHeaderCPtr)
  originalHeader <- convertRequestHeader originalHeaderC
  errorHandle' <- checkErrorWithGivenHandle errorHandle ()
  case errorHandle' of
    Left e -> simpleCallback (RequestResponse payloadText originalHeader (Just e))
    _ -> simpleCallback (RequestResponse payloadText originalHeader Nothing)

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
  IO (Either Error Int)
send (Producer producer) messageId fileName metadata datasetSubstream datasetSize autoIdFlag data' transferFlag storageFlag (StreamName stream) callback =
  withMessageHeaderHandle
    messageId
    fileName
    metadata
    datasetSubstream
    datasetSize
    autoIdFlag
    (BS.length data')
    \messageHeaderHandle ->
      unsafeUseAsCString data' \data'' -> withConstText stream \streamC -> do
        requestCallback <- createRequestCallback (sendRequestCallback callback)
        ( fromIntegral
            <$>
          )
          <$> checkError
            ( asapo_producer_send
                producer
                messageHeaderHandle
                (castPtr data'')
                (convertSendFlags transferFlag storageFlag)
                streamC
                requestCallback
            )

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
  IO (Either Error Int)
sendFile (Producer producer) messageId fileName meta datasetSubstream datasetSize autoIdFlag size (FileName fileNameToSend) transferFlag storageFlag (StreamName stream) callback =
  withMessageHeaderHandle
    messageId
    fileName
    meta
    datasetSubstream
    datasetSize
    autoIdFlag
    size
    \messageHeaderHandle ->
      withConstText fileNameToSend \fileNameToSendC -> withConstText stream \streamC -> do
        requestCallback <- createRequestCallback (sendRequestCallback callback)
        ( fromIntegral
            <$>
          )
          <$> checkError
            ( asapo_producer_send_file
                producer
                messageHeaderHandle
                fileNameToSendC
                (convertSendFlags transferFlag storageFlag)
                streamC
                requestCallback
            )

-- | As the title says, send the "stream finished" flag
sendStreamFinishedFlag :: Producer -> StreamName -> MessageId -> StreamName -> (RequestResponse -> IO ()) -> IO (Either Error Int)
sendStreamFinishedFlag (Producer producer) (StreamName stream) (MessageId lastId) (StreamName nextStream) callback = do
  requestCallback <- createRequestCallback (sendRequestCallback callback)
  withConstText stream \streamC -> withConstText nextStream \nextStreamC ->
    (fromIntegral <$>)
      <$> checkError
        ( asapo_producer_send_stream_finished_flag
            producer
            streamC
            lastId
            nextStreamC
            requestCallback
        )

data MetadataIngestMode = Insert | Replace | Update

data UpsertMode = UseUpsert | NoUpsert

-- | Send or extend beamtime metadata
sendBeamtimeMetadata :: Producer -> Metadata -> MetadataIngestMode -> UpsertMode -> (RequestResponse -> IO ()) -> IO (Either Error Int)
sendBeamtimeMetadata (Producer producer) (Metadata metadata) ingestMode upsertMode callback = do
  requestCallback <- createRequestCallback (sendRequestCallback callback)
  (fromIntegral <$>)
    <$> withConstText metadata \metadataC ->
      checkError
        ( asapo_producer_send_beamtime_metadata
            producer
            metadataC
            ( case ingestMode of
                Insert -> kInsert
                Replace -> kReplace
                Update -> kUpdate
            )
            ( case upsertMode of
                UseUpsert -> 1
                _ -> 0
            )
            requestCallback
        )

-- | Send or extend stream metadata
sendStreamMetadata :: Producer -> Metadata -> MetadataIngestMode -> UpsertMode -> StreamName -> (RequestResponse -> IO ()) -> IO (Either Error Int)
sendStreamMetadata (Producer producer) (Metadata metadata) ingestMode upsertMode (StreamName stream) callback = do
  requestCallback <- createRequestCallback (sendRequestCallback callback)
  (fromIntegral <$>)
    <$> withConstText metadata \metadataC -> withConstText stream \streamC ->
      checkError
        ( asapo_producer_send_stream_metadata
            producer
            metadataC
            ( case ingestMode of
                Insert -> kInsert
                Replace -> kReplace
                Update -> kUpdate
            )
            ( case upsertMode of
                UseUpsert -> 1
                _ -> 0
            )
            streamC
            requestCallback
        )

data LogLevel
  = LogNone
  | LogError
  | LogInfo
  | LogDebug
  | LogWarning
  deriving (Eq)

convertLogLevel :: LogLevel -> AsapoLogLevel
convertLogLevel x | x == LogNone = asapoLogLevelNone
convertLogLevel x | x == LogError = asapoLogLevelError
convertLogLevel x | x == LogInfo = asapoLogLevelInfo
convertLogLevel x | x == LogDebug = asapoLogLevelDebug
convertLogLevel _ = asapoLogLevelWarning

-- | Set the log level
setLogLevel :: Producer -> LogLevel -> IO ()
setLogLevel (Producer producer) logLevel = asapo_producer_set_log_level producer (convertLogLevel logLevel)

-- | Enable the local log
enableLocalLog :: Producer -> Bool -> IO ()
enableLocalLog (Producer producer) enable = asapo_producer_enable_local_log producer (if enable then 1 else 0)

-- | Enable the remote log
enableRemoteLog :: Producer -> Bool -> IO ()
enableRemoteLog (Producer producer) enable = asapo_producer_enable_remote_log producer (if enable then 1 else 0)

-- | Set a different set of credentials
setCredentials :: Producer -> SourceCredentials -> IO (Either Error Int)
setCredentials (Producer producer) credentials = withCredentials credentials \credentialsHandle ->
  (fromIntegral <$>) <$> checkError (asapo_producer_set_credentials producer credentialsHandle)

-- | Get the request queue size
getRequestsQueueSize :: Producer -> IO Int
getRequestsQueueSize (Producer producer) = fromIntegral <$> asapo_producer_get_requests_queue_size producer

-- | Get the request queue volume in MiB
getRequestsQueueVolumeMb :: Producer -> IO Int
getRequestsQueueVolumeMb (Producer producer) = fromIntegral <$> asapo_producer_get_requests_queue_volume_mb producer

-- | Set different request queue limits
setRequestsQueueLimits :: Producer -> Int -> Int -> IO ()
setRequestsQueueLimits (Producer producer) size volume =
  asapo_producer_set_requests_queue_limits producer (fromIntegral size) (fromIntegral volume)

-- | Wait for all outstanding requests to finish
waitRequestsFinished :: Producer -> NominalDiffTime -> IO (Either Error Int)
waitRequestsFinished (Producer producer) timeout = do
  (fromIntegral <$>) <$> checkError (asapo_producer_wait_requests_finished producer (nominalDiffToMillis timeout))
