{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Asapo.Consumer
  ( withConsumer,
    FilesystemFlag (..),
    queryMessages,
    ServerName (..),
    resendNacs,
    IncludeIncompleteFlag (..),
    SourcePath (..),
    Dataset (..),
    getNextDataset,
    MessageMeta (..),
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
    DeleteFlag (..),
    getBeamtimeMeta,
    ErrorOnNotExistFlag (..),
    getCurrentSize,
    Consumer,
    acknowledge,
    StreamFilter (..),
    negativeAcknowledge,
    getMessageMetaAndDataById,
    getMessageMetaById,
    getMessageDataById,
    getUnacknowledgedMessages,
    GroupId,
    withGroupId,
    Error (..),
    ErrorType (..),
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

import Asapo.Common (MessageId (MessageId), SourceCredentials, StreamInfo, StreamName (StreamName), nominalDiffToMillis, peekConstCStringText, retrieveStreamInfoFromC, stringHandleToText, timespecToUTC, withCStringNToText, withConstText, withCredentials, withPtr)
import Asapo.Raw.Common (AsapoErrorHandle, AsapoMessageDataHandle, AsapoStringHandle, ConstCString, asapo_error_explain, asapo_free_error_handle, asapo_free_message_data_handle, asapo_free_stream_infos_handle, asapo_free_string_handle, asapo_is_error, asapo_new_error_handle, asapo_new_message_data_handle, asapo_stream_infos_get_item, p_asapo_free_handle)
import Asapo.Raw.Consumer (AsapoConsumerErrorType, AsapoConsumerHandle, AsapoDataSetHandle, AsapoIdListHandle, AsapoMessageMetaHandle (AsapoMessageMetaHandle), AsapoNetworkConnectionType, AsapoStreamFilter, asapo_consumer_acknowledge, asapo_consumer_current_connection_type, asapo_consumer_delete_stream, asapo_consumer_generate_new_group_id, asapo_consumer_get_beamtime_meta, asapo_consumer_get_by_id, asapo_consumer_get_current_dataset_count, asapo_consumer_get_current_size, asapo_consumer_get_last, asapo_consumer_get_last_dataset, asapo_consumer_get_last_dataset_ingroup, asapo_consumer_get_last_ingroup, asapo_consumer_get_next, asapo_consumer_get_next_dataset, asapo_consumer_get_stream_list, asapo_consumer_get_unacknowledged_messages, asapo_consumer_negative_acknowledge, asapo_consumer_query_messages, asapo_consumer_reset_last_read_marker, asapo_consumer_retrieve_data, asapo_consumer_set_last_read_marker, asapo_consumer_set_resend_nacs, asapo_consumer_set_stream_persistent, asapo_consumer_set_timeout, asapo_create_consumer, asapo_dataset_get_expected_size, asapo_dataset_get_id, asapo_dataset_get_item, asapo_dataset_get_size, asapo_error_get_type, asapo_free_id_list_handle, asapo_free_message_metas_handle, asapo_id_list_get_item, asapo_id_list_get_size, asapo_message_data_get_as_chars, asapo_message_meta_get_buf_id, asapo_message_meta_get_dataset_substream, asapo_message_meta_get_id, asapo_message_meta_get_metadata, asapo_message_meta_get_name, asapo_message_meta_get_size, asapo_message_meta_get_source, asapo_message_meta_get_timestamp, asapo_message_metas_get_item, asapo_message_metas_get_size, asapo_new_message_meta_handle, asapo_stream_infos_get_size, kAllStreams, kAsapoTcp, kDataNotInCache, kEndOfStream, kFinishedStreams, kInterruptedTransaction, kLocalIOError, kNoData, kPartialData, kStreamFinished, kUnavailableService, kUndefined, kUnfinishedStreams, kUnsupportedClient, kWrongInput)
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
import Foreign (Storable (peek), with)
import Foreign.C (CInt)
import Foreign.C.ConstPtr (ConstPtr (unConstPtr))
import Foreign.ForeignPtr (ForeignPtr, newForeignPtr, newForeignPtr_, withForeignPtr)
import Foreign.Ptr (Ptr)
import System.IO (IO)
import Text.Show (Show)
import Prelude (Enum, Num ((-)), Semigroup ((<>)), Show (show), fromIntegral, putStrLn)

newtype ServerName = ServerName Text

newtype SourcePath = SourcePath Text

data FilesystemFlag = WithFilesystem | WithoutFilesystem deriving (Eq)

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

withConsumer :: forall a. ServerName -> SourcePath -> FilesystemFlag -> SourceCredentials -> (Error -> IO a) -> (Consumer -> IO a) -> IO a
withConsumer serverName sourcePath filesystemFlag creds onError onSuccess = bracket (create serverName sourcePath filesystemFlag creds) freeConsumer handle
  where
    freeConsumer :: Either Error AsapoConsumerHandle -> IO ()
    freeConsumer _ = pure ()
    handle :: Either Error AsapoConsumerHandle -> IO a
    handle (Left e) = onError e
    handle (Right v) = onSuccess (Consumer v)

newtype GroupId = GroupId AsapoStringHandle

withGroupId :: forall a. Consumer -> (Error -> IO a) -> (GroupId -> IO a) -> IO a
withGroupId (Consumer consumerHandle) onError onSuccess = bracket createGroupId destroy handle
  where
    createGroupId = withSuccess (asapo_consumer_generate_new_group_id consumerHandle) (pure . Right . GroupId)
    destroy (Right (GroupId stringHandle)) = asapo_free_string_handle stringHandle
    destroy _ = pure ()
    handle (Right v) = onSuccess v
    handle (Left e) = onError e

setTimeout :: Consumer -> NominalDiffTime -> IO ()
setTimeout (Consumer consumerHandle) timeout = asapo_consumer_set_timeout consumerHandle (nominalDiffToMillis timeout)

resetLastReadMarker :: Consumer -> GroupId -> StreamName -> IO (Either Error Int)
resetLastReadMarker (Consumer consumer) (GroupId groupId) (StreamName streamName) =
  withConstText streamName \streamNameC -> (fromIntegral <$>) <$> checkError (asapo_consumer_reset_last_read_marker consumer groupId streamNameC)

setLastReadMarker :: Consumer -> GroupId -> StreamName -> MessageId -> IO (Either Error Int)
setLastReadMarker (Consumer consumer) (GroupId groupId) (StreamName streamName) (MessageId value) =
  withConstText streamName \streamNameC -> (fromIntegral <$>) <$> checkError (asapo_consumer_set_last_read_marker consumer groupId value streamNameC)

acknowledge :: Consumer -> GroupId -> StreamName -> MessageId -> IO (Either Error Int)
acknowledge (Consumer consumer) (GroupId groupId) (StreamName streamName) (MessageId messageId) =
  withConstText streamName \streamNameC -> (fromIntegral <$>) <$> checkError (asapo_consumer_acknowledge consumer groupId messageId streamNameC)

negativeAcknowledge :: Consumer -> GroupId -> StreamName -> MessageId -> NominalDiffTime -> IO (Either Error Int)
negativeAcknowledge (Consumer consumer) (GroupId groupId) (StreamName streamName) (MessageId messageId) delay =
  withConstText streamName \streamNameC -> (fromIntegral <$>) <$> checkError (asapo_consumer_negative_acknowledge consumer groupId messageId (nominalDiffToMillis delay) streamNameC)

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

data NetworkConnectionType = ConnectionUndefined | ConnectionTcp | ConnectionFabric

convertConnectionType :: AsapoNetworkConnectionType -> NetworkConnectionType
convertConnectionType x | x == kUndefined = ConnectionUndefined
convertConnectionType x | x == kAsapoTcp = ConnectionTcp
convertConnectionType _ = ConnectionFabric

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

getStreamList :: Consumer -> Maybe StreamName -> StreamFilter -> IO (Either Error [StreamInfo])
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

data DeleteFlag = DeleteMeta | DontDeleteMeta deriving (Eq)

data ErrorOnNotExistFlag = ErrorOnNotExist | NoErrorOnNotExist deriving (Eq)

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

setStreamPersistent :: Consumer -> StreamName -> IO (Either Error Int)
setStreamPersistent (Consumer consumer) (StreamName streamName) =
  withConstText streamName \streamNameC ->
    (fromIntegral <$>)
      <$> checkError
        ( asapo_consumer_set_stream_persistent
            consumer
            streamNameC
        )

getCurrentSize :: Consumer -> StreamName -> IO (Either Error Int)
getCurrentSize (Consumer consumer) (StreamName streamName) =
  withConstText streamName \streamNameC ->
    (fromIntegral <$>)
      <$> checkError
        ( asapo_consumer_get_current_size
            consumer
            streamNameC
        )

data IncludeIncompleteFlag = IncludeIncomplete | ExcludeIncomplete deriving (Eq)

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

getBeamtimeMeta :: Consumer -> IO (Either Error (Maybe Text))
getBeamtimeMeta (Consumer consumer) = checkError (asapo_consumer_get_beamtime_meta consumer) >>= traverse stringHandleToText

newtype MessageMetaHandle = MessageMetaHandle (ForeignPtr ())

withMessageMetaHandle :: MessageMetaHandle -> (AsapoMessageMetaHandle -> IO a) -> IO a
withMessageMetaHandle (MessageMetaHandle foreignPtr) f = withForeignPtr foreignPtr (f . AsapoMessageMetaHandle)

newMessageMetaHandle :: IO MessageMetaHandle
newMessageMetaHandle = do
  AsapoMessageMetaHandle internalHandle <- asapo_new_message_meta_handle
  -- MessageMetaHandle <$> newForeignPtr p_asapo_free_handle internalHandle
  MessageMetaHandle <$> newForeignPtr_ internalHandle

wrapMessageMetaHandle :: AsapoMessageMetaHandle -> IO AsapoMessageMetaHandle
wrapMessageMetaHandle = pure

data MessageMeta = MessageMeta
  { messageMetaName :: Text,
    messageMetaTimestamp :: UTCTime,
    messageMetaSize :: Word64,
    messageMetaId :: MessageId,
    messageMetaSource :: Text,
    messageMetaMetadata :: Text,
    messageMetaBufId :: Word64,
    messageMetaDatasetSubstream :: Word64
  }
  deriving (Show)

resolveMetadata :: AsapoMessageMetaHandle -> IO MessageMeta
resolveMetadata meta = do
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

data Dataset = Dataset
  { datasetId :: Word64,
    datasetExpectedSize :: Word64,
    datasetItems :: [AsapoMessageMetaHandle]
  }

retrieveDatasetFromC :: AsapoDataSetHandle -> IO Dataset
retrieveDatasetFromC handle = do
  numberOfItems <- asapo_dataset_get_size handle
  items <- repeatGetterWithSizeLimit (asapo_dataset_get_item handle >=> wrapMessageMetaHandle) numberOfItems
  Dataset <$> asapo_dataset_get_id handle <*> asapo_dataset_get_expected_size handle <*> pure items

getNextDataset :: Consumer -> GroupId -> Word64 -> StreamName -> IO (Either Error Dataset)
getNextDataset (Consumer consumer) (GroupId groupId) minSize (StreamName streamName) = withConstText streamName \streamNameC -> do
  checkError (asapo_consumer_get_next_dataset consumer groupId minSize streamNameC) >>= traverse retrieveDatasetFromC

getLastDataset :: Consumer -> Word64 -> StreamName -> IO (Either Error Dataset)
getLastDataset (Consumer consumer) minSize (StreamName streamName) = withConstText streamName \streamNameC -> do
  checkError (asapo_consumer_get_last_dataset consumer minSize streamNameC) >>= traverse retrieveDatasetFromC

getLastDatasetInGroup :: Consumer -> GroupId -> Word64 -> StreamName -> IO (Either Error Dataset)
getLastDatasetInGroup (Consumer consumer) (GroupId groupId) minSize (StreamName streamName) = withConstText streamName \streamNameC -> do
  checkError (asapo_consumer_get_last_dataset_ingroup consumer groupId minSize streamNameC) >>= traverse retrieveDatasetFromC

withMessageDataHandle :: (AsapoMessageDataHandle -> IO a) -> IO a
withMessageDataHandle = bracket asapo_new_message_data_handle asapo_free_message_data_handle

retrieveDataFromHandle :: AsapoMessageDataHandle -> IO BS.ByteString
retrieveDataFromHandle dataHandle = do
  messageCString <- asapo_message_data_get_as_chars dataHandle
  BS.packCString (unConstPtr messageCString)

retrieveDataFromMeta :: Consumer -> MessageMetaHandle -> IO (Either Error BS.ByteString)
retrieveDataFromMeta (Consumer consumer) metaHandle =
  withMessageMetaHandle metaHandle \metaHandlePtr ->
    withMessageDataHandle \dataHandle ->
      with dataHandle \dataHandlePtr ->
        withSuccess (asapo_consumer_retrieve_data consumer metaHandlePtr dataHandlePtr) \_result -> do
          changedDataHandle <- peek dataHandlePtr
          Right <$> retrieveDataFromHandle changedDataHandle

withMessageHandles ::
  StreamName ->
  (Ptr AsapoMessageMetaHandle -> Ptr AsapoMessageDataHandle -> ConstCString -> Ptr AsapoErrorHandle -> IO CInt) ->
  (AsapoMessageMetaHandle -> AsapoMessageDataHandle -> IO (Either Error a)) ->
  IO (Either Error a)
withMessageHandles (StreamName streamName) g f = do
  metaHandle <- newMessageMetaHandle
  withMessageMetaHandle metaHandle \metaHandleInternal ->
    with metaHandleInternal \metaHandleInternalPtr ->
      withMessageDataHandle \dataHandle ->
        with dataHandle \dataHandlePtr ->
          withConstText streamName \streamNameC ->
            withSuccess (g metaHandleInternalPtr dataHandlePtr streamNameC) \_result -> do
              newHandle <- peek metaHandleInternalPtr
              newDataHandle <- peek dataHandlePtr
              f newHandle newDataHandle

withMessageHandlesById :: Consumer -> StreamName -> MessageId -> (AsapoMessageMetaHandle -> AsapoMessageDataHandle -> IO (Either Error a)) -> IO (Either Error a)
withMessageHandlesById (Consumer consumer) streamName (MessageId messageId) = withMessageHandles streamName (asapo_consumer_get_by_id consumer messageId)

retrieveMessageMetaAndData :: AsapoMessageMetaHandle -> AsapoMessageDataHandle -> IO (Either a (AsapoMessageMetaHandle, MessageMeta, BS.ByteString))
retrieveMessageMetaAndData metaHandle dataHandle = do
  data' <- retrieveDataFromHandle dataHandle
  meta <- resolveMetadata metaHandle
  pure (Right (metaHandle, meta, data'))

retrieveMessageMeta :: AsapoMessageMetaHandle -> p -> IO (Either a (AsapoMessageMetaHandle, MessageMeta))
retrieveMessageMeta metaHandle _dataHandle = do
  meta <- resolveMetadata metaHandle
  pure (Right (metaHandle, meta))

retrieveMessageData :: p -> AsapoMessageDataHandle -> IO (Either a BS.ByteString)
retrieveMessageData _metaHandle dataHandle = do
  data' <- retrieveDataFromHandle dataHandle
  pure (Right data')

getMessageMetaAndDataById :: Consumer -> StreamName -> MessageId -> IO (Either Error (AsapoMessageMetaHandle, MessageMeta, BS.ByteString))
getMessageMetaAndDataById consumer streamName messageId =
  withMessageHandlesById consumer streamName messageId retrieveMessageMetaAndData

getMessageMetaById :: Consumer -> StreamName -> MessageId -> IO (Either Error (AsapoMessageMetaHandle, MessageMeta))
getMessageMetaById consumer streamName messageId = do
  withMessageHandlesById consumer streamName messageId retrieveMessageMeta

getMessageDataById :: Consumer -> StreamName -> MessageId -> IO (Either Error BS.ByteString)
getMessageDataById consumer streamName messageId = do
  withMessageHandlesById consumer streamName messageId retrieveMessageData

getLastMessageMetaAndData :: Consumer -> StreamName -> IO (Either Error (AsapoMessageMetaHandle, MessageMeta, BS.ByteString))
getLastMessageMetaAndData (Consumer consumer) streamName = withMessageHandles streamName (asapo_consumer_get_last consumer) retrieveMessageMetaAndData

getLastMessageMeta :: Consumer -> StreamName -> IO (Either Error (AsapoMessageMetaHandle, MessageMeta))
getLastMessageMeta (Consumer consumer) streamName = withMessageHandles streamName (asapo_consumer_get_last consumer) retrieveMessageMeta

getLastMessageData :: Consumer -> StreamName -> IO (Either Error BS.ByteString)
getLastMessageData (Consumer consumer) streamName = withMessageHandles streamName (asapo_consumer_get_last consumer) retrieveMessageData

getLastInGroupMessageMetaAndData :: Consumer -> StreamName -> GroupId -> IO (Either Error (AsapoMessageMetaHandle, MessageMeta, BS.ByteString))
getLastInGroupMessageMetaAndData (Consumer consumer) streamName (GroupId groupId) = withMessageHandles streamName (asapo_consumer_get_last_ingroup consumer groupId) retrieveMessageMetaAndData

getLastInGroupMessageMeta :: Consumer -> StreamName -> GroupId -> IO (Either Error (AsapoMessageMetaHandle, MessageMeta))
getLastInGroupMessageMeta (Consumer consumer) streamName (GroupId groupId) = withMessageHandles streamName (asapo_consumer_get_last_ingroup consumer groupId) retrieveMessageMeta

getLastInGroupMessageData :: Consumer -> StreamName -> GroupId -> IO (Either Error BS.ByteString)
getLastInGroupMessageData (Consumer consumer) streamName (GroupId groupId) = withMessageHandles streamName (asapo_consumer_get_last_ingroup consumer groupId) retrieveMessageData

getNextMessageMetaAndData :: Consumer -> StreamName -> GroupId -> IO (Either Error (AsapoMessageMetaHandle, MessageMeta, BS.ByteString))
getNextMessageMetaAndData (Consumer consumer) streamName (GroupId groupId) = withMessageHandles streamName (asapo_consumer_get_next consumer groupId) retrieveMessageMetaAndData

getNextMessageMeta :: Consumer -> StreamName -> GroupId -> IO (Either Error (AsapoMessageMetaHandle, MessageMeta))
getNextMessageMeta (Consumer consumer) streamName (GroupId groupId) = withMessageHandles streamName (asapo_consumer_get_next consumer groupId) retrieveMessageMeta

getNextMessageData :: Consumer -> StreamName -> GroupId -> IO (Either Error BS.ByteString)
getNextMessageData (Consumer consumer) streamName (GroupId groupId) = withMessageHandles streamName (asapo_consumer_get_next consumer groupId) retrieveMessageData

queryMessagesHandles :: Consumer -> Text -> StreamName -> IO (Either Error [AsapoMessageMetaHandle])
queryMessagesHandles (Consumer consumer) query (StreamName streamName) = withConstText streamName \streamNameC -> withConstText query \queryC ->
  let init = checkError (asapo_consumer_query_messages consumer queryC streamNameC)
      destroy (Left _) = pure ()
      destroy (Right v) = asapo_free_message_metas_handle v
   in bracket init destroy \case
        Left e -> pure (Left e)
        Right metasHandle' -> do
          numberOfMetas <- asapo_message_metas_get_size metasHandle'
          Right <$> repeatGetterWithSizeLimit (asapo_message_metas_get_item metasHandle' >=> wrapMessageMetaHandle) numberOfMetas

queryMessages :: Consumer -> Text -> StreamName -> IO (Either Error [MessageMeta])
queryMessages (Consumer consumer) query (StreamName streamName) = withConstText streamName \streamNameC -> withConstText query \queryC ->
  let init = checkError (asapo_consumer_query_messages consumer queryC streamNameC)
      destroy (Left _) = pure ()
      destroy (Right v) = asapo_free_message_metas_handle v
   in bracket init destroy \case
        Left e -> pure (Left e)
        Right metasHandle' -> do
          numberOfMetas <- asapo_message_metas_get_size metasHandle'
          Right <$> repeatGetterWithSizeLimit (asapo_message_metas_get_item metasHandle' >=> wrapMessageMetaHandle >=> resolveMetadata) numberOfMetas

resendNacs :: Consumer -> Bool -> NominalDiffTime -> Word64 -> IO ()
resendNacs (Consumer consumer) resend delay = asapo_consumer_set_resend_nacs consumer (if resend then 1 else 0) (nominalDiffToMillis delay)
