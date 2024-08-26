{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- |
-- Description : High-level interface for all consumer-related functions
--
-- To implement an ASAP:O consumer, you should only need this interface.
-- It exposes no memory-management functions (like free) or pointers, and
-- is thus safe to use.
module Asapo.Either.Consumer
  ( FilesystemFlag (..),
    ServerName (..),
    IncludeIncompleteFlag (..),
    SourcePath (..),
    Dataset (..),
    MessageMeta (..),
    DeleteFlag (..),
    ErrorOnNotExistFlag (..),
    Consumer,
    NetworkConnectionType (..),
    StreamFilter (..),
    MessageMetaHandle,
    GroupId,
    Error (..),
    ErrorType (..),
    withConsumer,
    retrieveDataForMessageMeta,
    queryMessages,
    resendNacs,
    getNextDataset,
    retrieveDataFromMeta,
    setStreamPersistent,
    getLastMessageMetaAndData,
    getLastMessageMeta,
    getLastMessageData,
    getLastInGroupMessageMetaAndData,
    getLastInGroupMessageMeta,
    getLastInGroupMessageData,
    getNextMessageMetaAndData,
    getNextMessageMeta,
    getNextMessageData,
    getCurrentDatasetCount,
    getBeamtimeMeta,
    getCurrentSize,
    acknowledge,
    negativeAcknowledge,
    getMessageMetaAndDataById,
    getMessageMetaById,
    getMessageDataById,
    getUnacknowledgedMessages,
    withGroupId,
    queryMessagesHandles,
    setTimeout,
    getLastDataset,
    getLastDatasetInGroup,
    resetLastReadMarker,
    setLastReadMarker,
    getCurrentConnectionType,
    getStreamList,
    deleteStream,
    resolveMetadata,
  )
where

import Asapo.Either.Common (MessageId (MessageId), SourceCredentials, StreamInfo, StreamName (StreamName), nominalDiffToMillis, peekConstCStringText, retrieveStreamInfoFromC, stringHandleToText, timespecToUTC, withCStringNToText, withConstText, withCredentials, withPtr)
import Asapo.Raw.Common (AsapoErrorHandle, AsapoMessageDataHandle (AsapoMessageDataHandle), AsapoStringHandle, ConstCString, asapo_error_explain, asapo_free_error_handle, asapo_free_stream_infos_handle, asapo_free_string_handle, asapo_is_error, asapo_new_error_handle, asapo_stream_infos_get_item)
import Asapo.Raw.Consumer (AsapoConsumerErrorType, AsapoConsumerHandle, AsapoDataSetHandle, AsapoIdListHandle, AsapoMessageMetaHandle (AsapoMessageMetaHandle), AsapoNetworkConnectionType, AsapoStreamFilter, asapo_consumer_acknowledge, asapo_consumer_current_connection_type, asapo_consumer_delete_stream, asapo_consumer_generate_new_group_id, asapo_consumer_get_beamtime_meta, asapo_consumer_get_by_id, asapo_consumer_get_current_dataset_count, asapo_consumer_get_current_size, asapo_consumer_get_last, asapo_consumer_get_last_dataset, asapo_consumer_get_last_dataset_ingroup, asapo_consumer_get_last_ingroup, asapo_consumer_get_next, asapo_consumer_get_next_dataset, asapo_consumer_get_stream_list, asapo_consumer_get_unacknowledged_messages, asapo_consumer_negative_acknowledge, asapo_consumer_query_messages, asapo_consumer_reset_last_read_marker, asapo_consumer_retrieve_data, asapo_consumer_set_last_read_marker, asapo_consumer_set_resend_nacs, asapo_consumer_set_stream_persistent, asapo_consumer_set_timeout, asapo_create_consumer, asapo_dataset_get_expected_size, asapo_dataset_get_id, asapo_dataset_get_item, asapo_dataset_get_size, asapo_error_get_type, asapo_free_consumer_handle, asapo_free_id_list_handle, asapo_free_message_metas_handle, asapo_id_list_get_item, asapo_id_list_get_size, asapo_message_data_get_as_chars, asapo_message_meta_get_buf_id, asapo_message_meta_get_dataset_substream, asapo_message_meta_get_id, asapo_message_meta_get_metadata, asapo_message_meta_get_name, asapo_message_meta_get_size, asapo_message_meta_get_source, asapo_message_meta_get_timestamp, asapo_message_metas_get_item, asapo_message_metas_get_size, asapo_stream_infos_get_size, kAllStreams, kAsapoTcp, kDataNotInCache, kEndOfStream, kFinishedStreams, kInterruptedTransaction, kLocalIOError, kNoData, kPartialData, kStreamFinished, kUnavailableService, kUndefined, kUnfinishedStreams, kUnsupportedClient, kWrongInput)
import Asapo.Raw.FreeHandleHack (p_asapo_free_handle)
import Control.Applicative (Applicative ((<*>)), pure)
import Control.Exception (bracket)
import Control.Monad (Monad ((>>=)), (>=>))
import Data.Bool (Bool, otherwise)
import qualified Data.ByteString as BS
import Data.Either (Either (Left, Right))
import Data.Eq (Eq ((==)))
import Data.Function (($), (.))
import Data.Functor ((<$>))
import Data.Int (Int)
import Data.Maybe (Maybe (Just, Nothing))
import Data.Ord ((>))
import Data.Text (Text)
import Data.Time (NominalDiffTime, UTCTime)
import Data.Traversable (Traversable (traverse))
import Data.Word (Word64)
import Foreign (Storable (peek), alloca)
import Foreign.C (CInt)
import Foreign.C.ConstPtr (ConstPtr (unConstPtr))
import Foreign.ForeignPtr (ForeignPtr, newForeignPtr, withForeignPtr)
import Foreign.Ptr (Ptr)
import System.IO (IO)
import Text.Show (Show)
import Prelude (Enum, Num ((-)), fromIntegral)

-- | Wrapper for a server name (something like "host:port")
newtype ServerName = ServerName Text

-- | Wrapper for a source path (dubious to not use @FilePath@, but let's see)
newtype SourcePath = SourcePath Text

-- | Whether to use the filesystem or do it in-memory
data FilesystemFlag = WithFilesystem | WithoutFilesystem deriving (Eq)

-- | Wrapper around a consumer handle. Create with the @withConsumer@ function(s).
newtype Consumer = Consumer AsapoConsumerHandle

data ErrorType
  = ErrorNoData
  | ErrorEndOfStream
  | ErrorStreamFinished
  | ErrorUnavailableService
  | ErrorInterruptedTransaction
  | ErrorLocalIOError
  | ErrorWrongInput
  | ErrorPartialData
  | ErrorUnsupportedClient
  | ErrorDataNotInCache
  | ErrorUnknownError
  deriving (Show)

convertErrorType :: AsapoConsumerErrorType -> ErrorType
convertErrorType x | x == kNoData = ErrorNoData
convertErrorType x | x == kEndOfStream = ErrorEndOfStream
convertErrorType x | x == kStreamFinished = ErrorStreamFinished
convertErrorType x | x == kUnavailableService = ErrorUnavailableService
convertErrorType x | x == kInterruptedTransaction = ErrorInterruptedTransaction
convertErrorType x | x == kLocalIOError = ErrorLocalIOError
convertErrorType x | x == kWrongInput = ErrorWrongInput
convertErrorType x | x == kPartialData = ErrorPartialData
convertErrorType x | x == kUnsupportedClient = ErrorUnsupportedClient
convertErrorType x | x == kDataNotInCache = ErrorDataNotInCache
convertErrorType _ = ErrorUnknownError

-- | Wrapper around an ASAP:O producer error, with an additional error code
data Error = Error
  { errorMessage :: Text,
    errorType :: ErrorType
  }
  deriving (Show)

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
      errorType' <- asapo_error_get_type errorHandle
      pure (Left (Error explanation (convertErrorType errorType')))
    else pure (Right result)

withErrorHandle :: (AsapoErrorHandle -> IO c) -> IO c
withErrorHandle = bracket asapo_new_error_handle asapo_free_error_handle

checkError :: (Ptr AsapoErrorHandle -> IO b) -> IO (Either Error b)
checkError f = do
  withErrorHandle \errorHandle -> do
    (errorHandlePtr, result) <- withPtr errorHandle f
    checkErrorWithGivenHandle errorHandlePtr result

withSuccess :: (Ptr AsapoErrorHandle -> IO t) -> (t -> IO (Either Error b)) -> IO (Either Error b)
withSuccess toCheck onSuccess = do
  result <- checkError toCheck
  case result of
    Left e -> pure (Left e)
    Right success -> onSuccess success

create :: ServerName -> SourcePath -> FilesystemFlag -> SourceCredentials -> IO (Either Error AsapoConsumerHandle)
create (ServerName serverName) (SourcePath sourcePath) fsFlag creds =
  withCredentials creds \creds' ->
    withConstText serverName \serverNameC ->
      withConstText sourcePath \sourcePathC ->
        checkError (asapo_create_consumer serverNameC sourcePathC (if fsFlag == WithFilesystem then 1 else 0) creds')

-- | Create a consumer and do something with it. This is the main entrypoint into the consumer
withConsumer :: forall a. ServerName -> SourcePath -> FilesystemFlag -> SourceCredentials -> (Error -> IO a) -> (Consumer -> IO a) -> IO a
withConsumer serverName sourcePath filesystemFlag creds onError onSuccess = bracket (create serverName sourcePath filesystemFlag creds) freeConsumer handle
  where
    freeConsumer :: Either Error AsapoConsumerHandle -> IO ()
    freeConsumer (Right h) = asapo_free_consumer_handle h
    freeConsumer _ = pure ()
    handle :: Either Error AsapoConsumerHandle -> IO a
    handle (Left e) = onError e
    handle (Right v) = onSuccess (Consumer v)

-- | Wrapper around a group ID
newtype GroupId = GroupId AsapoStringHandle

-- | Allocate a group ID and call a callback
withGroupId :: forall a. Consumer -> (Error -> IO a) -> (GroupId -> IO a) -> IO a
withGroupId (Consumer consumerHandle) onError onSuccess = bracket createGroupId destroy handle
  where
    createGroupId = withSuccess (asapo_consumer_generate_new_group_id consumerHandle) (pure . Right . GroupId)
    destroy (Right (GroupId stringHandle)) = asapo_free_string_handle stringHandle
    destroy _ = pure ()
    handle (Right v) = onSuccess v
    handle (Left e) = onError e

-- | Set the global consumer timeout
setTimeout :: Consumer -> NominalDiffTime -> IO ()
setTimeout (Consumer consumerHandle) timeout = asapo_consumer_set_timeout consumerHandle (nominalDiffToMillis timeout)

-- | Reset the last read marker for a specific group
resetLastReadMarker :: Consumer -> GroupId -> StreamName -> IO (Either Error Int)
resetLastReadMarker (Consumer consumer) (GroupId groupId) (StreamName streamName) =
  withConstText streamName \streamNameC -> (fromIntegral <$>) <$> checkError (asapo_consumer_reset_last_read_marker consumer groupId streamNameC)

-- | Set the last read marker for the stream
setLastReadMarker :: Consumer -> GroupId -> StreamName -> MessageId -> IO (Either Error Int)
setLastReadMarker (Consumer consumer) (GroupId groupId) (StreamName streamName) (MessageId value) =
  withConstText streamName \streamNameC -> (fromIntegral <$>) <$> checkError (asapo_consumer_set_last_read_marker consumer groupId value streamNameC)

-- | Acknowledge a specific message
acknowledge :: Consumer -> GroupId -> StreamName -> MessageId -> IO (Either Error Int)
acknowledge (Consumer consumer) (GroupId groupId) (StreamName streamName) (MessageId messageId) =
  withConstText streamName \streamNameC -> (fromIntegral <$>) <$> checkError (asapo_consumer_acknowledge consumer groupId messageId streamNameC)

-- | Negatively acknowledge a specific message
negativeAcknowledge ::
  Consumer ->
  GroupId ->
  StreamName ->
  MessageId ->
  -- | delay
  NominalDiffTime ->
  IO (Either Error Int)
negativeAcknowledge (Consumer consumer) (GroupId groupId) (StreamName streamName) (MessageId messageId) delay =
  withConstText streamName \streamNameC -> (fromIntegral <$>) <$> checkError (asapo_consumer_negative_acknowledge consumer groupId messageId (nominalDiffToMillis delay) streamNameC)

-- | Get a list of all unacknowledged message IDs in a range
getUnacknowledgedMessages :: Consumer -> GroupId -> StreamName -> (MessageId, MessageId) -> IO (Either Error [MessageId])
getUnacknowledgedMessages (Consumer consumer) (GroupId groupId) (StreamName streamName) (MessageId from, MessageId to) = bracket init destroy handle
  where
    init :: IO (Either Error AsapoIdListHandle)
    init = withConstText streamName $ checkError . asapo_consumer_get_unacknowledged_messages consumer groupId from to
    destroy :: Either Error AsapoIdListHandle -> IO ()
    destroy (Left _) = pure ()
    destroy (Right handle') = asapo_free_id_list_handle handle'
    handle :: Either Error AsapoIdListHandle -> IO (Either Error [MessageId])
    handle (Left e) = pure (Left e)
    handle (Right idListHandle) = do
      numberOfIds <- asapo_id_list_get_size idListHandle
      Right <$> repeatGetterWithSizeLimit ((MessageId <$>) . asapo_id_list_get_item idListHandle) numberOfIds

-- | Network connection type
data NetworkConnectionType
  = -- | not sure about this
    ConnectionUndefined
  | -- | TCP
    ConnectionTcp
  | -- | not sure about this
    ConnectionFabric

convertConnectionType :: AsapoNetworkConnectionType -> NetworkConnectionType
convertConnectionType x | x == kUndefined = ConnectionUndefined
convertConnectionType x | x == kAsapoTcp = ConnectionTcp
convertConnectionType _ = ConnectionFabric

-- | Retrieve the current consumer connection type
getCurrentConnectionType :: Consumer -> IO NetworkConnectionType
getCurrentConnectionType (Consumer consumerHandle) = convertConnectionType <$> asapo_consumer_current_connection_type consumerHandle

data StreamFilter = FilterAllStreams | FilterFinishedStreams | FilterUnfinishedStreams

convertStreamFilter :: StreamFilter -> AsapoStreamFilter
convertStreamFilter FilterAllStreams = kAllStreams
convertStreamFilter FilterFinishedStreams = kFinishedStreams
convertStreamFilter FilterUnfinishedStreams = kUnfinishedStreams

-- An often-repeating pattern: getting a length value and then either
-- returning an empty list or having another function to get the value
-- per ID. With an annoying off-by-one error.
repeatGetterWithSizeLimit :: (Eq a1, Num a1, Applicative f, Enum a1) => (a1 -> f a2) -> a1 -> f [a2]
repeatGetterWithSizeLimit f n
  | n == 0 = pure []
  | otherwise = traverse f [0 .. n - 1]

-- | Retrieve the list of streams with metadata
getStreamList ::
  Consumer ->
  -- | Pass @Nothing@ to get all streams
  Maybe StreamName ->
  StreamFilter ->
  IO (Either Error [StreamInfo])
getStreamList (Consumer consumer) streamName filter = bracket init destroy handle
  where
    init =
      let realStreamName = case streamName of
            Nothing -> ""
            Just (StreamName streamName') -> streamName'
       in withConstText realStreamName $ \streamNameC -> checkError (asapo_consumer_get_stream_list consumer streamNameC (convertStreamFilter filter))
    destroy (Left _) = pure ()
    destroy (Right handle') = asapo_free_stream_infos_handle handle'
    handle (Left e) = pure (Left e)
    handle (Right streamInfosHandle) = do
      numberOfStreams <- asapo_stream_infos_get_size streamInfosHandle
      Right
        <$> repeatGetterWithSizeLimit
          (asapo_stream_infos_get_item streamInfosHandle >=> retrieveStreamInfoFromC)
          numberOfStreams

-- | Anti-boolean-blindness for delete or not delete metadata when deleting a stream
data DeleteFlag = DeleteMeta | DontDeleteMeta deriving (Eq)

-- | Anti-boolean-blindness for "error on not existing data"
data ErrorOnNotExistFlag = ErrorOnNotExist | NoErrorOnNotExist deriving (Eq)

-- | Delete a given stream
deleteStream :: Consumer -> StreamName -> DeleteFlag -> ErrorOnNotExistFlag -> IO (Either Error Int)
deleteStream (Consumer consumer) (StreamName streamName) deleteFlag errorOnNotExistFlag =
  withConstText streamName \streamNameC ->
    (fromIntegral <$>)
      <$> checkError
        ( asapo_consumer_delete_stream
            consumer
            streamNameC
            (if deleteFlag == DeleteMeta then 1 else 0)
            (if errorOnNotExistFlag == ErrorOnNotExist then 1 else 0)
        )

-- | Set a stream persistent
setStreamPersistent :: Consumer -> StreamName -> IO (Either Error Int)
setStreamPersistent (Consumer consumer) (StreamName streamName) =
  withConstText streamName \streamNameC ->
    (fromIntegral <$>)
      <$> checkError
        ( asapo_consumer_set_stream_persistent
            consumer
            streamNameC
        )

-- | Get the current size (number of messages) of the stream
getCurrentSize :: Consumer -> StreamName -> IO (Either Error Int)
getCurrentSize (Consumer consumer) (StreamName streamName) =
  withConstText streamName \streamNameC ->
    (fromIntegral <$>)
      <$> checkError
        ( asapo_consumer_get_current_size
            consumer
            streamNameC
        )

-- | Anti-boolean-blindness for "include incomplete data sets in list"
data IncludeIncompleteFlag = IncludeIncomplete | ExcludeIncomplete deriving (Eq)

-- | Get number of datasets in stream
getCurrentDatasetCount :: Consumer -> StreamName -> IncludeIncompleteFlag -> IO (Either Error Int)
getCurrentDatasetCount (Consumer consumer) (StreamName streamName) inludeIncomplete =
  withConstText streamName \streamNameC ->
    (fromIntegral <$>)
      <$> checkError
        ( asapo_consumer_get_current_dataset_count
            consumer
            streamNameC
            (if inludeIncomplete == IncludeIncomplete then 1 else 0)
        )

-- | Get beamtime metadata (which can be not set, in which case @Nothing@ is returned)
getBeamtimeMeta :: Consumer -> IO (Either Error (Maybe Text))
getBeamtimeMeta (Consumer consumer) = checkError (asapo_consumer_get_beamtime_meta consumer) >>= traverse stringHandleToText

-- | Metadata handle, can be passed around as a pure value and be used to retrieve actual data for the metadata as a two-step process, using the @retrieveDataForMessageMeta@ function(s)
newtype MessageMetaHandle = MessageMetaHandle (ForeignPtr ()) deriving (Show)

withMessageMetaHandle :: MessageMetaHandle -> (AsapoMessageMetaHandle -> IO a) -> IO a
withMessageMetaHandle (MessageMetaHandle foreignPtr) f = withForeignPtr foreignPtr (f . AsapoMessageMetaHandle)

newMessageMetaHandle :: AsapoMessageMetaHandle -> IO MessageMetaHandle
newMessageMetaHandle (AsapoMessageMetaHandle inputPtr) = do
  MessageMetaHandle <$> newForeignPtr p_asapo_free_handle inputPtr

newtype MessageDataHandle = MessageDataHandle (ForeignPtr ()) deriving (Show)

withMessageDataHandle :: MessageDataHandle -> (AsapoMessageDataHandle -> IO a) -> IO a
withMessageDataHandle (MessageDataHandle foreignPtr) f = withForeignPtr foreignPtr (f . AsapoMessageDataHandle)

newMessageDataHandle :: AsapoMessageDataHandle -> IO MessageDataHandle
newMessageDataHandle (AsapoMessageDataHandle inputPtr) =
  MessageDataHandle <$> newForeignPtr p_asapo_free_handle inputPtr

wrapMessageMetaHandle :: AsapoMessageMetaHandle -> IO MessageMetaHandle
wrapMessageMetaHandle = newMessageMetaHandle

-- | Metadata for a single message
data MessageMeta = MessageMeta
  { messageMetaName :: Text,
    messageMetaTimestamp :: UTCTime,
    messageMetaSize :: Word64,
    messageMetaId :: MessageId,
    messageMetaSource :: Text,
    messageMetaMetadata :: Text,
    messageMetaBufId :: Word64,
    messageMetaDatasetSubstream :: Word64,
    messageMetaHandle :: MessageMetaHandle
  }
  deriving (Show)

-- | Retrieve actual data for the handle
retrieveDataForMessageMeta :: Consumer -> MessageMeta -> IO (Either Error BS.ByteString)
retrieveDataForMessageMeta consumer meta = retrieveDataFromMeta consumer (messageMetaHandle meta)

-- | Get the actual metadata hiding behind a handle (shouldn't be necessary when using the convenience interfaces)
resolveMetadata :: MessageMetaHandle -> IO MessageMeta
resolveMetadata metaHandle = withMessageMetaHandle metaHandle \meta -> do
  (timestamp, _) <- withPtr 0 (asapo_message_meta_get_timestamp meta)
  MessageMeta
    <$> (asapo_message_meta_get_name meta >>= peekConstCStringText)
    <*> pure (timespecToUTC timestamp)
    <*> asapo_message_meta_get_size meta
    <*> (MessageId <$> asapo_message_meta_get_id meta)
    <*> (asapo_message_meta_get_source meta >>= peekConstCStringText)
    <*> (asapo_message_meta_get_metadata meta >>= peekConstCStringText)
    <*> asapo_message_meta_get_buf_id meta
    <*> asapo_message_meta_get_dataset_substream meta
    <*> pure metaHandle

-- | Metadata for a dataset
data Dataset = Dataset
  { datasetId :: Word64,
    datasetExpectedSize :: Word64,
    datasetItems :: [MessageMetaHandle]
  }

retrieveDatasetFromC :: AsapoDataSetHandle -> IO Dataset
retrieveDatasetFromC handle = do
  numberOfItems <- asapo_dataset_get_size handle
  items <- repeatGetterWithSizeLimit (asapo_dataset_get_item handle >=> wrapMessageMetaHandle) numberOfItems
  Dataset <$> asapo_dataset_get_id handle <*> asapo_dataset_get_expected_size handle <*> pure items

-- | Get the next dataset for a stream
getNextDataset ::
  Consumer ->
  GroupId ->
  -- | Wait until dataset has these number of messages (0 for maximum size)
  Word64 ->
  StreamName ->
  IO (Either Error Dataset)
getNextDataset (Consumer consumer) (GroupId groupId) minSize (StreamName streamName) = withConstText streamName \streamNameC -> do
  checkError (asapo_consumer_get_next_dataset consumer groupId minSize streamNameC) >>= traverse retrieveDatasetFromC

-- | Get the last dataset in the stream
getLastDataset ::
  Consumer ->
  -- | Wait until dataset has these number of messages (0 for maximum size)
  Word64 ->
  StreamName ->
  IO (Either Error Dataset)
getLastDataset (Consumer consumer) minSize (StreamName streamName) = withConstText streamName \streamNameC -> do
  checkError (asapo_consumer_get_last_dataset consumer minSize streamNameC) >>= traverse retrieveDatasetFromC

-- | Get the last data ste in the given group
getLastDatasetInGroup ::
  Consumer ->
  GroupId ->
  -- | Wait until dataset has these number of messages (0 for maximum size)
  Word64 ->
  StreamName ->
  IO (Either Error Dataset)
getLastDatasetInGroup (Consumer consumer) (GroupId groupId) minSize (StreamName streamName) = withConstText streamName \streamNameC -> do
  checkError (asapo_consumer_get_last_dataset_ingroup consumer groupId minSize streamNameC) >>= traverse retrieveDatasetFromC

retrieveDataFromHandle :: MessageDataHandle -> IO BS.ByteString
retrieveDataFromHandle dataHandle = do
  withMessageDataHandle dataHandle \dataHandlePtr -> do
    messageCString <- asapo_message_data_get_as_chars dataHandlePtr
    BS.packCString (unConstPtr messageCString)

-- | Retrieve data for the given metadata handle
retrieveDataFromMeta :: Consumer -> MessageMetaHandle -> IO (Either Error BS.ByteString)
retrieveDataFromMeta (Consumer consumer) metaHandle =
  withMessageMetaHandle metaHandle \metaHandlePtr ->
    alloca \dataHandlePtrPtr ->
      withSuccess (asapo_consumer_retrieve_data consumer metaHandlePtr dataHandlePtrPtr) \_result -> do
        dataHandlePtr <- peek dataHandlePtrPtr
        dataHandle <- newMessageDataHandle dataHandlePtr
        Right <$> retrieveDataFromHandle dataHandle

withMessageHandles ::
  StreamName ->
  (Ptr AsapoMessageMetaHandle -> Ptr AsapoMessageDataHandle -> ConstCString -> Ptr AsapoErrorHandle -> IO CInt) ->
  (MessageMetaHandle -> MessageDataHandle -> IO (Either Error a)) ->
  IO (Either Error a)
withMessageHandles (StreamName streamName) g f = do
  alloca \metaHandlePtrPtr ->
    alloca \dataHandlePtrPtr ->
      withConstText streamName \streamNameC ->
        withSuccess (g metaHandlePtrPtr dataHandlePtrPtr streamNameC) \_result -> do
          metaHandlePtr <- peek metaHandlePtrPtr
          dataHandlePtr <- peek dataHandlePtrPtr
          metaHandle <- newMessageMetaHandle metaHandlePtr
          dataHandle <- newMessageDataHandle dataHandlePtr
          f metaHandle dataHandle

withMessageHandlesById :: Consumer -> StreamName -> MessageId -> (MessageMetaHandle -> MessageDataHandle -> IO (Either Error a)) -> IO (Either Error a)
withMessageHandlesById (Consumer consumer) streamName (MessageId messageId) =
  withMessageHandles
    streamName
    (asapo_consumer_get_by_id consumer messageId)

retrieveMessageMetaAndData :: MessageMetaHandle -> MessageDataHandle -> IO (Either a (MessageMeta, BS.ByteString))
retrieveMessageMetaAndData metaHandle dataHandle = do
  data' <- retrieveDataFromHandle dataHandle
  meta <- resolveMetadata metaHandle
  pure (Right (meta, data'))

retrieveMessageMeta :: MessageMetaHandle -> p -> IO (Either a MessageMeta)
retrieveMessageMeta metaHandle _dataHandle = do
  meta <- resolveMetadata metaHandle
  pure (Right meta)

retrieveMessageData :: p -> MessageDataHandle -> IO (Either a BS.ByteString)
retrieveMessageData _metaHandle dataHandle = do
  data' <- retrieveDataFromHandle dataHandle
  pure (Right data')

-- | Given a message ID, retrieve both metadata and data
getMessageMetaAndDataById :: Consumer -> StreamName -> MessageId -> IO (Either Error (MessageMeta, BS.ByteString))
getMessageMetaAndDataById consumer streamName messageId =
  withMessageHandlesById consumer streamName messageId retrieveMessageMetaAndData

-- | Given a message ID, retrieve only the metadata (you can get the data later with 'retrieveDataFromMessageMeta')
getMessageMetaById :: Consumer -> StreamName -> MessageId -> IO (Either Error MessageMeta)
getMessageMetaById consumer streamName messageId = do
  withMessageHandlesById consumer streamName messageId retrieveMessageMeta

-- | Given a message ID, retrieve only the data
getMessageDataById :: Consumer -> StreamName -> MessageId -> IO (Either Error BS.ByteString)
getMessageDataById consumer streamName messageId = do
  withMessageHandlesById consumer streamName messageId retrieveMessageData

-- | Retrieve the last message in the stream, with data and metadata
getLastMessageMetaAndData :: Consumer -> StreamName -> IO (Either Error (MessageMeta, BS.ByteString))
getLastMessageMetaAndData (Consumer consumer) streamName = withMessageHandles streamName (asapo_consumer_get_last consumer) retrieveMessageMetaAndData

-- | Retrieve the last message in the stream, only metadata (you can get the data later with 'retrieveDataFromMessageMeta')
getLastMessageMeta :: Consumer -> StreamName -> IO (Either Error MessageMeta)
getLastMessageMeta (Consumer consumer) streamName = withMessageHandles streamName (asapo_consumer_get_last consumer) retrieveMessageMeta

-- | Retrieve the last message in the stream, only data
getLastMessageData :: Consumer -> StreamName -> IO (Either Error BS.ByteString)
getLastMessageData (Consumer consumer) streamName = withMessageHandles streamName (asapo_consumer_get_last consumer) retrieveMessageData

-- | Retrieve the last message in a given stream and group, with data and metadata
getLastInGroupMessageMetaAndData :: Consumer -> StreamName -> GroupId -> IO (Either Error (MessageMeta, BS.ByteString))
getLastInGroupMessageMetaAndData (Consumer consumer) streamName (GroupId groupId) = withMessageHandles streamName (asapo_consumer_get_last_ingroup consumer groupId) retrieveMessageMetaAndData

-- | Retrieve the last message in a given stream and group, only metadata (you can get the data later with 'retrieveDataFromMessageMeta')
getLastInGroupMessageMeta :: Consumer -> StreamName -> GroupId -> IO (Either Error MessageMeta)
getLastInGroupMessageMeta (Consumer consumer) streamName (GroupId groupId) = withMessageHandles streamName (asapo_consumer_get_last_ingroup consumer groupId) retrieveMessageMeta

-- | Retrieve the last message in a given stream and group, only data
getLastInGroupMessageData :: Consumer -> StreamName -> GroupId -> IO (Either Error BS.ByteString)
getLastInGroupMessageData (Consumer consumer) streamName (GroupId groupId) = withMessageHandles streamName (asapo_consumer_get_last_ingroup consumer groupId) retrieveMessageData

-- | Retrieve the next message in the stream and group, with data and metadata
getNextMessageMetaAndData :: Consumer -> StreamName -> GroupId -> IO (Either Error (MessageMeta, BS.ByteString))
getNextMessageMetaAndData (Consumer consumer) streamName (GroupId groupId) =
  withMessageHandles
    streamName
    (asapo_consumer_get_next consumer groupId)
    retrieveMessageMetaAndData

-- | Retrieve the next message in the stream and group, only metadata (you can get the data later with 'retrieveDataFromMessageMeta')
getNextMessageMeta :: Consumer -> StreamName -> GroupId -> IO (Either Error MessageMeta)
getNextMessageMeta (Consumer consumer) streamName (GroupId groupId) = withMessageHandles streamName (asapo_consumer_get_next consumer groupId) retrieveMessageMeta

-- | Retrieve the next message in the stream and group, only data
getNextMessageData :: Consumer -> StreamName -> GroupId -> IO (Either Error BS.ByteString)
getNextMessageData (Consumer consumer) streamName (GroupId groupId) = withMessageHandles streamName (asapo_consumer_get_next consumer groupId) retrieveMessageData

-- | Query messages, return handles without data
queryMessagesHandles ::
  Consumer ->
  -- | Actual query string, see the docs for syntax
  Text ->
  StreamName ->
  IO (Either Error [MessageMetaHandle])
queryMessagesHandles (Consumer consumer) query (StreamName streamName) = withConstText streamName \streamNameC -> withConstText query \queryC ->
  let init = checkError (asapo_consumer_query_messages consumer queryC streamNameC)
      destroy (Left _) = pure ()
      destroy (Right v) = asapo_free_message_metas_handle v
   in bracket init destroy \case
        Left e -> pure (Left e)
        Right metasHandle' -> do
          numberOfMetas <- asapo_message_metas_get_size metasHandle'
          Right <$> repeatGetterWithSizeLimit (asapo_message_metas_get_item metasHandle' >=> wrapMessageMetaHandle) numberOfMetas

-- | Query messages, return handles without data
queryMessages ::
  Consumer ->
  -- | Actual query string, see the docs for syntax
  Text ->
  StreamName ->
  IO (Either Error [MessageMeta])
queryMessages (Consumer consumer) query (StreamName streamName) = withConstText streamName \streamNameC -> withConstText query \queryC ->
  let init = checkError (asapo_consumer_query_messages consumer queryC streamNameC)
      destroy (Left _) = pure ()
      destroy (Right v) = asapo_free_message_metas_handle v
   in bracket init destroy \case
        Left e -> pure (Left e)
        Right metasHandle' -> do
          numberOfMetas <- asapo_message_metas_get_size metasHandle'
          Right <$> repeatGetterWithSizeLimit (asapo_message_metas_get_item metasHandle' >=> wrapMessageMetaHandle >=> resolveMetadata) numberOfMetas

-- | Reset negative acknowledgements
resendNacs ::
  Consumer ->
  -- | resend yes/no
  Bool ->
  -- | delay
  NominalDiffTime ->
  -- | attempts
  Word64 ->
  IO ()
resendNacs (Consumer consumer) resend delay = asapo_consumer_set_resend_nacs consumer (if resend then 1 else 0) (nominalDiffToMillis delay)
