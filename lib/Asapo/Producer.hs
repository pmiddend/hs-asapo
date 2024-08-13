{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Asapo.Producer
  ( Endpoint (..),
    ProcessingThreads (..),
    getRequestsQueueSize,
    getRequestsQueueVolumeMb,
    setRequestsQueueLimits,
    RequestHandlerType (..),
    withProducer,
    Metadata (..),
    enableLocalLog,
    Milliseconds (..),
    LogLevel (..),
    MessageId (..),
    FileName (..),
    DatasetSubstream (..),
    DatasetSize (..),
    StreamName (..),
    waitRequestsFinished,
    getVersionInfo,
    VersionInfo (..),
    StreamInfo (..),
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
    UpsertMode (..),
    MetadataIngestMode (..),
    AutoIdFlag (..),
    TransferFlag (..),
    StorageFlag (..),
    RequestResponse (..),
    Opcode (..),
    GenericRequestHeader (..),
    setLogLevel,
    enableRemoteLog,
    setCredentials,
    Error (..),
  )
where

import Asapo.Common (Beamline (Beamline), Beamtime (Beamtime), DataSource (DataSource), InstanceId (InstanceId), PipelineStep (PipelineStep), SourceCredentials (SourceCredentials, beamline, beamtime, dataSource, instanceId, pipelineStep, sourceType, token), SourceType (ProcessedSource, RawSource), Token (Token))
import Asapo.Raw.Common (AsapoErrorHandle, AsapoSourceCredentialsHandle, AsapoStreamInfoHandle, AsapoStringHandle, ConstCString, asapo_create_source_credentials, asapo_error_explain, asapo_free_error_handle, asapo_free_source_credentials, asapo_free_stream_info_handle, asapo_free_string_handle, asapo_is_error, asapo_new_error_handle, asapo_new_string_handle, asapo_stream_info_get_finished, asapo_stream_info_get_last_id, asapo_stream_info_get_name, asapo_stream_info_get_next_stream, asapo_stream_info_get_timestamp_created, asapo_stream_info_get_timestamp_last_entry, asapo_string_c_str, kProcessed, kRaw)
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
import Control.Monad (Monad ((>>=)))
import Data.Bits ((.|.))
import Data.Bool (Bool)
import qualified Data.ByteString as BS
import Data.ByteString.Unsafe (unsafeUseAsCString)
import Data.Either (Either (Left, Right))
import Data.Eq (Eq ((==)))
import Data.Foldable (Foldable (elem))
import Data.Function (($), (.))
import Data.Functor ((<$>))
import Data.Int (Int)
import Data.Maybe (Maybe (Just, Nothing), fromJust)
import Data.Ord ((>))
import Data.String (String)
import Data.Text (Text, pack, unpack)
import Data.Time (NominalDiffTime)
import Data.Time.Clock (UTCTime, addUTCTime)
import Data.Time.LocalTime (zonedTimeToUTC)
import qualified Data.Time.RFC3339 as RFC3339
import Foreign (Storable (peek), alloca, castPtr, free, mallocArray)
import Foreign.C (CChar)
import Foreign.C.ConstPtr (ConstPtr (ConstPtr, unConstPtr))
import Foreign.C.String (CString, peekCString, withCString)
import Foreign.C.Types (CULong)
import Foreign.Marshal (with)
import Foreign.Ptr (Ptr)
import System.Clock (TimeSpec, toNanoSecs)
import System.IO (IO)
import Text.Show (Show)
import Prelude (Fractional ((/)), Num (fromInteger), fromIntegral)

newtype Endpoint = Endpoint Text

newtype ProcessingThreads = ProcessingThreads Int

newtype Milliseconds = Milliseconds Int

data RequestHandlerType = TcpHandler | FilesystemHandler

newtype Producer = Producer AsapoProducerHandle

newtype StreamName = StreamName Text

newtype Error = Error Text

newtype MessageId = MessageId Int deriving (Show)

-- peekConstCString :: ConstPtr CChar -> IO String
-- peekConstCString = peekCString . unConstPtr

peekCStringText :: CString -> IO Text
peekCStringText = (pack <$>) . peekCString

peekConstCStringText :: ConstPtr CChar -> IO Text
peekConstCStringText = (pack <$>) . peekCString . unConstPtr

withConstCString :: String -> (ConstCString -> IO b) -> IO b
withConstCString s f = withCString s (f . ConstPtr)

withText :: Text -> (CString -> IO a) -> IO a
withText t = withCString (unpack t)

withConstText :: Text -> (ConstCString -> IO a) -> IO a
withConstText t = withConstCString (unpack t)

withErrorHandle :: (AsapoErrorHandle -> IO c) -> IO c
withErrorHandle = bracket asapo_new_error_handle asapo_free_error_handle

-- withErrorHandlePtr :: (Ptr AsapoErrorHandle -> IO c) -> IO c
-- withErrorHandlePtr f = withErrorHandle (`with` f)

withCStringN :: Int -> (CString -> IO a) -> IO a
withCStringN size = bracket (mallocArray size) free

withCStringNToText :: Int -> (CString -> IO ()) -> IO Text
withCStringNToText size f =
  withCStringN size \ptr -> do
    f ptr
    pack <$> peekCString ptr

withCredentials :: SourceCredentials -> (AsapoSourceCredentialsHandle -> IO a) -> IO a
withCredentials
  ( SourceCredentials
      { sourceType,
        instanceId = InstanceId instanceId',
        pipelineStep = PipelineStep pipelineStep',
        beamtime = Beamtime beamtime',
        beamline = Beamline beamline',
        dataSource = DataSource dataSource',
        token = Token token'
      }
    )
  f = do
    let convertSourceType RawSource = kRaw
        convertSourceType ProcessedSource = kProcessed
        createCredentialsWithText = withText instanceId' \instanceId'' -> withText pipelineStep' \pipelineStep'' -> withText beamtime' \beamtime'' -> withText beamline' \beamline'' -> withText dataSource' \dataSource'' -> withText token' \token'' ->
          asapo_create_source_credentials
            (convertSourceType sourceType)
            instanceId''
            pipelineStep''
            beamtime''
            beamline''
            dataSource''
            token''
    bracket createCredentialsWithText asapo_free_source_credentials f

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

checkError :: (Ptr AsapoErrorHandle -> IO b) -> IO (Either Error b)
checkError f = do
  withErrorHandle \errorHandle -> with errorHandle \errorHandlePtr -> do
    result <- f errorHandlePtr
    checkErrorWithGivenHandle errorHandle result

create :: Endpoint -> ProcessingThreads -> RequestHandlerType -> SourceCredentials -> Milliseconds -> IO (Either Error AsapoProducerHandle)
create (Endpoint endpoint) (ProcessingThreads processingThreads) handlerType sourceCredentials (Milliseconds milliseconds) = do
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
                  (fromIntegral milliseconds)
              )

withProducer :: forall a. Endpoint -> ProcessingThreads -> RequestHandlerType -> SourceCredentials -> Milliseconds -> (Error -> IO a) -> (Producer -> IO a) -> IO a
withProducer endpoint processingThreads handlerType sourceCredentials milliseconds onError onSuccess = bracket (create endpoint processingThreads handlerType sourceCredentials milliseconds) freeProducer handle
  where
    freeProducer :: Either Error AsapoProducerHandle -> IO ()
    freeProducer _ = pure ()
    handle :: Either Error AsapoProducerHandle -> IO a
    handle (Left e) = onError e
    handle (Right v) = onSuccess (Producer v)

data VersionInfo = VersionInfo
  { versionClient :: Text,
    versionServer :: Text,
    versionSupported :: Bool
  }
  deriving (Show)

withStringHandle :: (AsapoStringHandle -> IO c) -> IO c
withStringHandle = bracket asapo_new_string_handle asapo_free_string_handle

getVersionInfo :: Producer -> IO (Either Error VersionInfo)
getVersionInfo (Producer producerHandle) =
  withStringHandle \clientInfo -> withStringHandle \serverInfo -> alloca \supportedPtr -> do
    result <- checkError (asapo_producer_get_version_info producerHandle clientInfo serverInfo supportedPtr)
    case result of
      Left e -> pure (Left e)
      -- The return value is a CInt which is unnecessary probably?
      Right _integerReturnCode -> do
        supported <- peek supportedPtr
        clientInfoCString <- asapo_string_c_str clientInfo
        serverInfoCString <- asapo_string_c_str serverInfo
        clientInfo' <- peekConstCStringText clientInfoCString
        serverInfo' <- peekConstCStringText serverInfoCString
        pure (Right (VersionInfo clientInfo' serverInfo' (supported > 0)))

data StreamInfo = StreamInfo
  { streamInfoLastId :: MessageId,
    streamInfoName :: Text,
    streamInfoFinished :: Bool,
    streamInfoNextStream :: Text,
    streamInfoCreated :: UTCTime,
    streamInfoLastEntry :: UTCTime
  }
  deriving (Show)

-- Thanks to
--
-- https://github.com/imoverclocked/convert-times/blob/7f9b45bea8e62dbf14a156a8229b68e07efec5a1/app/Main.hs
timespecToUTC :: TimeSpec -> UTCTime
timespecToUTC sc_time =
  let scEpochInUTC :: UTCTime
      scEpochInUTC = zonedTimeToUTC $ fromJust $ RFC3339.parseTimeRFC3339 "1970-01-01T00:00:00.00Z"
      sc2diffTime = fromInteger (toNanoSecs sc_time) / 1000000000 :: NominalDiffTime
   in addUTCTime sc2diffTime scEpochInUTC

retrieveStreamInfoFromC :: AsapoStreamInfoHandle -> IO StreamInfo
retrieveStreamInfoFromC infoHandle = do
  lastId <- asapo_stream_info_get_last_id infoHandle
  name <- asapo_stream_info_get_name infoHandle >>= peekConstCStringText
  nextStream <- asapo_stream_info_get_next_stream infoHandle >>= peekConstCStringText
  finished <- asapo_stream_info_get_finished infoHandle
  created <- alloca \timespecPtr -> do
    asapo_stream_info_get_timestamp_created infoHandle timespecPtr
    timespec <- peek timespecPtr
    pure (timespecToUTC timespec)
  lastEntry <- alloca \timespecPtr -> do
    asapo_stream_info_get_timestamp_last_entry infoHandle timespecPtr
    timespec <- peek timespecPtr
    pure (timespecToUTC timespec)
  pure (StreamInfo (MessageId (fromIntegral lastId)) name (finished > 0) nextStream created lastEntry)

getStreamInfo :: Producer -> StreamName -> Milliseconds -> IO (Either Error StreamInfo)
getStreamInfo (Producer producer) (StreamName stream) (Milliseconds ms) = bracket init destroy f
  where
    init :: IO (Either Error AsapoStreamInfoHandle)
    init = withConstText stream \streamC -> checkError (asapo_producer_get_stream_info producer streamC (fromIntegral ms))
    destroy :: Either Error AsapoStreamInfoHandle -> IO ()
    destroy (Right handle) = asapo_free_stream_info_handle handle
    destroy _ = pure ()
    f :: Either Error AsapoStreamInfoHandle -> IO (Either Error StreamInfo)
    f (Left e) = pure (Left e)
    f (Right streamInfoHandle) = Right <$> retrieveStreamInfoFromC streamInfoHandle

getLastStream :: Producer -> Milliseconds -> IO (Either Error StreamInfo)
getLastStream (Producer producer) (Milliseconds timeout) = bracket init destroy f
  where
    init :: IO (Either Error AsapoStreamInfoHandle)
    init = checkError (asapo_producer_get_last_stream producer (fromIntegral timeout))
    destroy :: Either Error AsapoStreamInfoHandle -> IO ()
    destroy (Right handle) = asapo_free_stream_info_handle handle
    destroy _ = pure ()
    f :: Either Error AsapoStreamInfoHandle -> IO (Either Error StreamInfo)
    f (Left e) = pure (Left e)
    f (Right streamInfoHandle) = Right <$> retrieveStreamInfoFromC streamInfoHandle

getStreamMeta :: Producer -> StreamName -> Milliseconds -> IO (Either Error Text)
getStreamMeta (Producer producer) (StreamName stream) (Milliseconds ms) = bracket init destroy f
  where
    init :: IO (Either Error AsapoStringHandle)
    init = withConstText stream \streamC -> checkError (asapo_producer_get_stream_meta producer streamC (fromIntegral ms))
    destroy :: Either Error AsapoStringHandle -> IO ()
    destroy (Right handle) = asapo_free_string_handle handle
    destroy _ = pure ()
    f :: Either Error AsapoStringHandle -> IO (Either Error Text)
    f (Left e) = pure (Left e)
    f (Right string) = do
      resultC <- asapo_string_c_str string
      result <- peekConstCStringText resultC
      pure (Right result)

getBeamtimeMeta :: Producer -> Milliseconds -> IO (Either Error Text)
getBeamtimeMeta (Producer producer) (Milliseconds ms) = bracket init destroy f
  where
    init :: IO (Either Error AsapoStringHandle)
    init = checkError (asapo_producer_get_beamtime_meta producer (fromIntegral ms))
    destroy :: Either Error AsapoStringHandle -> IO ()
    destroy (Right handle) = asapo_free_string_handle handle
    destroy _ = pure ()
    f :: Either Error AsapoStringHandle -> IO (Either Error Text)
    f (Left e) = pure (Left e)
    f (Right string) = do
      resultC <- asapo_string_c_str string
      result <- peekConstCStringText resultC
      pure (Right result)

data DeletionFlags
  = DeleteMeta
  | DeleteErrorOnNotExist
  deriving (Eq)

deleteStream :: Producer -> StreamName -> Milliseconds -> [DeletionFlags] -> IO (Either Error Int)
deleteStream (Producer producer) (StreamName stream) (Milliseconds timeout) deletionFlags = do
  result <- withConstText stream \streamC ->
    checkError
      ( asapo_producer_delete_stream
          producer
          streamC
          (fromIntegral timeout)
          (if DeleteMeta `elem` deletionFlags then 1 else 0)
          (if DeleteErrorOnNotExist `elem` deletionFlags then 1 else 0)
      )
  pure (fromIntegral <$> result)

newtype FileName = FileName Text

newtype Metadata = Metadata Text

newtype DatasetSubstream = DatasetSubstream Int

newtype DatasetSize = DatasetSize Int

data AutoIdFlag = UseAutoId | NoAutoId deriving (Eq)

data TransferFlag = DataAndMetadata | MetadataOnly

data StorageFlag = Filesystem | Database | FilesystemAndDatabase

convertSendFlags :: TransferFlag -> StorageFlag -> CULong
convertSendFlags tf sf = convertTransferFlag tf .|. convertStorageFlag sf
  where
    convertTransferFlag :: TransferFlag -> CULong
    convertTransferFlag DataAndMetadata = fromIntegral kTransferData
    convertTransferFlag MetadataOnly = fromIntegral kTransferMetaDataOnly
    convertStorageFlag :: StorageFlag -> CULong
    convertStorageFlag Filesystem = fromIntegral kStoreInFilesystem
    convertStorageFlag Database = fromIntegral kStoreInDatabase
    convertStorageFlag FilesystemAndDatabase = fromIntegral kStoreInDatabase .|. fromIntegral kStoreInFilesystem

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
withMessageHeaderHandle (MessageId messageId) (FileName fileName) (Metadata metadata) (DatasetSubstream datasetSubstream) (DatasetSize datasetSize) autoIdFlag dataSize = bracket init destroy
  where
    init :: IO AsapoMessageHeaderHandle
    init = withConstText fileName \fileNameC -> withConstText metadata \metadataC ->
      asapo_create_message_header
        (fromIntegral messageId)
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

data RequestResponse = RequestResponse
  { responsePayload :: Text,
    responseOriginalRequestHeader :: GenericRequestHeader,
    responseError :: Maybe Error
  }

sendRequestCallback :: (RequestResponse -> IO ()) -> Ptr () -> AsapoRequestCallbackPayloadHandle -> AsapoErrorHandle -> IO ()
sendRequestCallback simpleCallback _data payloadHandle errorHandle = do
  payloadText <-
    bracket (asapo_request_callback_payload_get_response payloadHandle) asapo_free_string_handle \payloadHandle' -> do
      payloadC <- asapo_string_c_str payloadHandle'
      peekConstCStringText payloadC
  originalHeaderCPtr <- asapo_request_callback_payload_get_original_header payloadHandle
  originalHeaderC <- peek (unConstPtr originalHeaderCPtr)
  originalHeader <- convertRequestHeader originalHeaderC
  errorHandle' <- checkErrorWithGivenHandle errorHandle ()
  case errorHandle' of
    Left e -> simpleCallback (RequestResponse payloadText originalHeader (Just e))
    _ -> simpleCallback (RequestResponse payloadText originalHeader Nothing)

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

sendFile ::
  Producer ->
  MessageId ->
  FileName ->
  Metadata ->
  DatasetSubstream ->
  DatasetSize ->
  AutoIdFlag ->
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

sendStreamFinishedFlag :: Producer -> StreamName -> MessageId -> StreamName -> (RequestResponse -> IO ()) -> IO (Either Error Int)
sendStreamFinishedFlag (Producer producer) (StreamName stream) (MessageId lastId) (StreamName nextStream) callback = do
  requestCallback <- createRequestCallback (sendRequestCallback callback)
  withConstText stream \streamC -> withConstText nextStream \nextStreamC ->
    (fromIntegral <$>)
      <$> checkError
        ( asapo_producer_send_stream_finished_flag
            producer
            streamC
            (fromIntegral lastId)
            nextStreamC
            requestCallback
        )

data MetadataIngestMode = Insert | Replace | Update

data UpsertMode = UseUpsert | NoUpsert

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

setLogLevel :: Producer -> LogLevel -> IO ()
setLogLevel (Producer producer) logLevel = asapo_producer_set_log_level producer (convertLogLevel logLevel)

enableLocalLog :: Producer -> Bool -> IO ()
enableLocalLog (Producer producer) enable = asapo_producer_enable_local_log producer (if enable then 1 else 0)

enableRemoteLog :: Producer -> Bool -> IO ()
enableRemoteLog (Producer producer) enable = asapo_producer_enable_remote_log producer (if enable then 1 else 0)

setCredentials :: Producer -> SourceCredentials -> IO (Either Error Int)
setCredentials (Producer producer) credentials = withCredentials credentials \credentialsHandle ->
  (fromIntegral <$>) <$> checkError (asapo_producer_set_credentials producer credentialsHandle)

getRequestsQueueSize :: Producer -> IO Int
getRequestsQueueSize (Producer producer) = fromIntegral <$> asapo_producer_get_requests_queue_size producer

getRequestsQueueVolumeMb :: Producer -> IO Int
getRequestsQueueVolumeMb (Producer producer) = fromIntegral <$> asapo_producer_get_requests_queue_volume_mb producer

setRequestsQueueLimits :: Producer -> Int -> Int -> IO ()
setRequestsQueueLimits (Producer producer) size volume =
  asapo_producer_set_requests_queue_limits producer (fromIntegral size) (fromIntegral volume)

waitRequestsFinished :: Producer -> Milliseconds -> IO (Either Error Int)
waitRequestsFinished (Producer producer) (Milliseconds timeout) = do
  (fromIntegral <$>) <$> checkError (asapo_producer_wait_requests_finished producer (fromIntegral timeout))
