{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -Wno-missing-import-lists #-}

-- |
-- Description : High-level interface for all consumer-related functions, using exceptions instead of @Either@
--
-- To implement an ASAP:O consumer, you should only need this interface.
-- It exposes no memory-management functions (like free) or pointers, and
-- is thus safe to use.
--
-- = Simple Example
--
-- Here's some code for a simple consumer that connects, outputs the available streams, and then gets a specific message by ID with metadata and data:
--
-- >>> :seti -XOverloadedStrings
-- >>> :{
--  module Main where
--  import Asapo.Consumer
--  import Prelude hiding (putStrLn)
--  import Data.Maybe(fromMaybe)
--  import Control.Monad(forM_)
--  import Data.Text(pack)
--  import Data.Text.IO(putStrLn)
--  import Data.Text.Encoding(decodeUtf8)
--  import qualified Data.ByteString as BS
--  main :: IO ()
--  main =
--    withConsumer
--      (ServerName "localhost:8040")
--      (SourcePath "")
--      WithoutFilesystem
--      ( SourceCredentials
--        { sourceType = RawSource,
--          instanceId = InstanceId "auto",
--          pipelineStep = PipelineStep "ps1",
--          beamtime = Beamtime "asapo_test",
--          beamline = Beamline "",
--          dataSource = DataSource "asapo_source",
--          token = Token "token-please-change"
--        }
--      ) $ \consumer -> do
--        beamtimeMeta <- getBeamtimeMeta consumer
--        putStrLn $ "beamtime metadata: " <> (fromMaybe "N/A" beamtimeMeta)
--        streams <- getStreamList consumer Nothing FilterAllStreams
--        forM_ streams $ \stream -> do
--          putStrLn $ "=> stream info " <> pack (show stream)
--          streamSize <- getCurrentSize consumer (streamInfoName stream)
--          putStrLn $ "   stream size: " <> pack (show streamSize)
--          datasetCount <- getCurrentDatasetCount consumer (streamInfoName stream) IncludeIncomplete
--          putStrLn $ "   dataset count: " <> pack (show datasetCount)
--        metaAndData <- getMessageMetaAndDataById consumer (StreamName "default") (messageIdFromInt 1337)
--        putStrLn $ "meta: " <> pack (show (fst metaAndData))
--        putStrLn $ "data: " <> decodeUtf8 (snd metaAndData)
-- :}
module Asapo.Consumer
  ( -- * Error types
    SomeConsumerException,
    NoData,
    EndOfStream,
    StreamFinished,
    UnavailableService,
    InterruptedTransaction,
    LocalIOError,
    WrongInput,
    PartialData,
    UnsupportedClient,
    DataNotInCache,
    UnknownError,

    -- * Types
    Consumer,
    Dataset (..),
    MessageMetaHandle,
    DeleteFlag (..),
    ErrorOnNotExistFlag (..),
    ErrorType (ErrorNoData),
    FilesystemFlag (..),
    PipelineStep (..),
    Beamtime (Beamtime),
    DataSource (DataSource),
    Beamline (Beamline),
    StreamInfo (..),
    GroupId,
    IncludeIncompleteFlag (..),
    MessageMeta (..),
    ServerName (..),
    SourcePath (..),
    SourceType (..),
    StreamName (..),
    InstanceId (..),
    Token (..),
    StreamFilter (..),
    SourceCredentials (..),

    -- * Initialization
    withConsumer,
    withGroupId,

    -- * Getters
    getCurrentSize,
    getCurrentDatasetCount,
    getBeamtimeMeta,
    getNextDataset,
    getLastDataset,
    getLastDatasetInGroup,
    getMessageMetaAndDataById,
    getMessageMetaById,
    getMessageDataById,
    getNextMessageMetaAndData,
    getNextMessageMeta,
    getNextMessageData,
    getLastMessageMetaAndData,
    getLastMessageMeta,
    getLastMessageData,
    getLastInGroupMessageMetaAndData,
    getLastInGroupMessageMeta,
    getLastInGroupMessageData,
    getUnacknowledgedMessages,
    getCurrentConnectionType,
    getStreamList,
    messageIdFromInt,
    queryMessages,
    retrieveDataForMessageMeta,

    -- * Modifiers
    resetLastReadMarker,
    setTimeout,
    setLastReadMarker,
    setStreamPersistent,
    acknowledge,
    negativeAcknowledge,
    deleteStream,
    resendNacs,
  )
where

import Asapo.Either.Common
  ( Beamline (Beamline),
    Beamtime (Beamtime),
    DataSource (DataSource),
    InstanceId (..),
    MessageId,
    PipelineStep (..),
    SourceCredentials (..),
    SourceType (..),
    StreamInfo (..),
    StreamName (..),
    Token (..),
    messageIdFromInt,
  )
import Asapo.Either.Consumer
  ( Consumer,
    Dataset (..),
    DeleteFlag (..),
    Error (Error),
    ErrorOnNotExistFlag (..),
    ErrorType (..),
    FilesystemFlag (..),
    GroupId,
    IncludeIncompleteFlag (..),
    MessageMeta (..),
    MessageMetaHandle,
    ServerName (..),
    SourcePath (..),
    StreamFilter (..),
    getCurrentConnectionType,
    resendNacs,
    setTimeout,
  )
import qualified Asapo.Either.Consumer as PC
import Control.Applicative (pure)
import Control.Exception (Exception (fromException, toException), SomeException, throw)
import Control.Monad (Monad)
import qualified Data.ByteString as BS
import Data.Either (Either (Left, Right))
import Data.Function ((.))
import Data.Int (Int)
import Data.Maybe (Maybe)
import Data.Text (Text)
import Data.Time (NominalDiffTime)
import Data.Typeable (cast)
import Data.Word (Word64)
import System.IO (IO)
import Text.Show (Show, show)
import Prelude ()

-- $setup
-- >>> :seti -XOverloadedStrings
-- >>> import Control.Exception(catch)
-- >>> import Prelude(undefined, (<>), error)
-- >>> import Data.Text.IO(putStrLn)
-- >>> import Data.Text(pack)
-- >>> let consumer = undefined

-- | Parent class for all consumer-related exceptions. This makes catchall possible, as in...
--
-- @
-- setStreamPersistent consumer (StreamName "default")
--   `catch` (\e -> error ("Caught " <> (show (e :: SomeConsumerException))))
-- @
data SomeConsumerException
  = forall e. (Exception e) => SomeConsumerException e

instance Show SomeConsumerException where
  show (SomeConsumerException e) = show e

instance Exception SomeConsumerException

consumerExceptionToException :: (Exception e) => e -> SomeException
consumerExceptionToException = toException . SomeConsumerException

consumerExceptionFromException :: (Exception e) => SomeException -> Maybe e
consumerExceptionFromException x = do
  SomeConsumerException a <- fromException x
  cast a

newtype NoData = NoData Text deriving (Show)

instance Exception NoData where
  toException = consumerExceptionToException
  fromException = consumerExceptionFromException

newtype EndOfStream = EndOfStream Text deriving (Show)

instance Exception EndOfStream where
  toException = consumerExceptionToException
  fromException = consumerExceptionFromException

newtype StreamFinished = StreamFinished Text deriving (Show)

instance Exception StreamFinished where
  toException = consumerExceptionToException
  fromException = consumerExceptionFromException

newtype UnavailableService = UnavailableService Text deriving (Show)

instance Exception UnavailableService where
  toException = consumerExceptionToException
  fromException = consumerExceptionFromException

newtype InterruptedTransaction = InterruptedTransaction Text deriving (Show)

instance Exception InterruptedTransaction where
  toException = consumerExceptionToException
  fromException = consumerExceptionFromException

newtype LocalIOError = LocalIOError Text deriving (Show)

instance Exception LocalIOError where
  toException = consumerExceptionToException
  fromException = consumerExceptionFromException

newtype WrongInput = WrongInput Text deriving (Show)

instance Exception WrongInput where
  toException = consumerExceptionToException
  fromException = consumerExceptionFromException

newtype PartialData = PartialData Text deriving (Show)

instance Exception PartialData where
  toException = consumerExceptionToException
  fromException = consumerExceptionFromException

newtype UnsupportedClient = UnsupportedClient Text deriving (Show)

instance Exception UnsupportedClient where
  toException = consumerExceptionToException
  fromException = consumerExceptionFromException

newtype DataNotInCache = DataNotInCache Text deriving (Show)

instance Exception DataNotInCache where
  toException = consumerExceptionToException
  fromException = consumerExceptionFromException

newtype UnknownError = UnknownError Text deriving (Show)

instance Exception UnknownError where
  toException = consumerExceptionToException
  fromException = consumerExceptionFromException

errorTypeToException :: ErrorType -> Text -> a
errorTypeToException ErrorNoData = throw . NoData
errorTypeToException ErrorEndOfStream = throw . EndOfStream
errorTypeToException ErrorStreamFinished = throw . StreamFinished
errorTypeToException ErrorUnavailableService = throw . UnavailableService
errorTypeToException ErrorInterruptedTransaction = throw . InterruptedTransaction
errorTypeToException ErrorLocalIOError = throw . LocalIOError
errorTypeToException ErrorWrongInput = throw . WrongInput
errorTypeToException ErrorPartialData = throw . PartialData
errorTypeToException ErrorUnsupportedClient = throw . UnsupportedClient
errorTypeToException ErrorDataNotInCache = throw . DataNotInCache
errorTypeToException ErrorUnknownError = throw . UnknownError

-- | Create a consumer and do something with it. This is the main entrypoint into the consumer
withConsumer :: forall a. ServerName -> SourcePath -> FilesystemFlag -> SourceCredentials -> (Consumer -> IO a) -> IO a
withConsumer serverName sourcePath filesystemFlag creds onSuccess =
  let onError (Error errorMessage errorType) = errorTypeToException errorType errorMessage
   in PC.withConsumer serverName sourcePath filesystemFlag creds onError onSuccess

-- | Allocate a group ID and call a callback
withGroupId :: forall a. Consumer -> (GroupId -> IO a) -> IO a
withGroupId consumer onSuccess =
  let onError (Error errorMessage errorType) = errorTypeToException errorType errorMessage
   in PC.withGroupId consumer onError onSuccess

maybeThrow :: (Monad m) => m (Either Error b) -> m b
maybeThrow f = do
  result <- f
  case result of
    Left (Error errorMessage errorType) -> errorTypeToException errorType errorMessage
    Right v -> pure v

-- | Reset the last read marker for the stream
resetLastReadMarker :: Consumer -> GroupId -> StreamName -> IO Int
resetLastReadMarker consumer groupId streamName = maybeThrow (PC.resetLastReadMarker consumer groupId streamName)

-- | Set the last read marker for the stream
setLastReadMarker :: Consumer -> GroupId -> StreamName -> MessageId -> IO Int
setLastReadMarker consumer groupId streamName value = maybeThrow (PC.setLastReadMarker consumer groupId streamName value)

-- | Acknowledge a specific message
acknowledge :: Consumer -> GroupId -> StreamName -> MessageId -> IO Int
acknowledge consumer groupId streamName messageId = maybeThrow (PC.acknowledge consumer groupId streamName messageId)

-- | Negatively acknowledge a specific message
negativeAcknowledge ::
  Consumer ->
  GroupId ->
  StreamName ->
  MessageId ->
  -- | delay
  NominalDiffTime ->
  IO Int
negativeAcknowledge consumer groupId streamName messageId delay = maybeThrow (PC.negativeAcknowledge consumer groupId streamName messageId delay)

-- | Get a list of all unacknowledged message IDs in a range
getUnacknowledgedMessages :: Consumer -> GroupId -> StreamName -> (MessageId, MessageId) -> IO [MessageId]
getUnacknowledgedMessages consumer groupId streamName (from, to) = maybeThrow (PC.getUnacknowledgedMessages consumer groupId streamName (from, to))

-- | Retrieve the list of streams with metadata
getStreamList :: Consumer -> Maybe StreamName -> StreamFilter -> IO [StreamInfo]
getStreamList consumer streamName filter = maybeThrow (PC.getStreamList consumer streamName filter)

-- | Delete a given stream
deleteStream :: Consumer -> StreamName -> DeleteFlag -> ErrorOnNotExistFlag -> IO Int
deleteStream consumer streamName deleteFlag errorOnNotExistFlag = maybeThrow (PC.deleteStream consumer streamName deleteFlag errorOnNotExistFlag)

-- | Set a stream persistent
setStreamPersistent :: Consumer -> StreamName -> IO Int
setStreamPersistent consumer streamName = maybeThrow (PC.setStreamPersistent consumer streamName)

-- | Get the current size (number of messages) of the stream
getCurrentSize :: Consumer -> StreamName -> IO Int
getCurrentSize consumer streamName = maybeThrow (PC.getCurrentSize consumer streamName)

-- | Get number of datasets in stream
getCurrentDatasetCount :: Consumer -> StreamName -> IncludeIncompleteFlag -> IO Int
getCurrentDatasetCount consumer streamName inludeIncomplete = maybeThrow (PC.getCurrentDatasetCount consumer streamName inludeIncomplete)

-- | Get beamtime metadata (which can be not set, in which case @Nothing@ is returned)
getBeamtimeMeta :: Consumer -> IO (Maybe Text)
getBeamtimeMeta consumer = maybeThrow (PC.getBeamtimeMeta consumer)

-- | Get the next dataset for a stream
getNextDataset ::
  Consumer ->
  GroupId ->
  -- | minimum size
  Word64 ->
  StreamName ->
  IO Dataset
getNextDataset consumer groupId minSize streamName = maybeThrow (PC.getNextDataset consumer groupId minSize streamName)

-- | Get the last dataset in the stream
getLastDataset ::
  Consumer ->
  -- | minimum size
  Word64 ->
  StreamName ->
  IO Dataset
getLastDataset consumer minSize streamName = maybeThrow (PC.getLastDataset consumer minSize streamName)

-- | Get the last data ste in the given group
getLastDatasetInGroup ::
  Consumer ->
  GroupId ->
  -- | minimum size
  Word64 ->
  StreamName ->
  IO Dataset
getLastDatasetInGroup consumer groupId minSize streamName = maybeThrow (PC.getLastDatasetInGroup consumer groupId minSize streamName)

-- | Given a message ID, retrieve both metadata and data
getMessageMetaAndDataById :: Consumer -> StreamName -> MessageId -> IO (MessageMeta, BS.ByteString)
getMessageMetaAndDataById consumer streamName messageId = maybeThrow (PC.getMessageMetaAndDataById consumer streamName messageId)

-- | Given a message ID, retrieve only the metadata (you can get the data later with 'retrieveDataFromMessageMeta')
getMessageMetaById :: Consumer -> StreamName -> MessageId -> IO MessageMeta
getMessageMetaById consumer streamName messageId = maybeThrow (PC.getMessageMetaById consumer streamName messageId)

-- | Given a message ID, retrieve only the data
getMessageDataById :: Consumer -> StreamName -> MessageId -> IO BS.ByteString
getMessageDataById consumer streamName messageId = maybeThrow (PC.getMessageDataById consumer streamName messageId)

-- | Retrieve the last message in the stream, with data and metadata
getLastMessageMetaAndData :: Consumer -> StreamName -> IO (MessageMeta, BS.ByteString)
getLastMessageMetaAndData consumer streamName = maybeThrow (PC.getLastMessageMetaAndData consumer streamName)

-- | Retrieve the last message in the stream, only metadata (you can get the data later with 'retrieveDataFromMessageMeta')
getLastMessageMeta :: Consumer -> StreamName -> IO MessageMeta
getLastMessageMeta consumer streamName = maybeThrow (PC.getLastMessageMeta consumer streamName)

-- | Retrieve the last message in the stream, only data
getLastMessageData :: Consumer -> StreamName -> IO BS.ByteString
getLastMessageData consumer streamName = maybeThrow (PC.getLastMessageData consumer streamName)

-- | Retrieve the last message in a given stream and group, with data and metadata
getLastInGroupMessageMetaAndData :: Consumer -> StreamName -> GroupId -> IO (MessageMeta, BS.ByteString)
getLastInGroupMessageMetaAndData consumer streamName groupId = maybeThrow (PC.getLastInGroupMessageMetaAndData consumer streamName groupId)

-- | Retrieve the last message in a given stream and group, only metadata (you can get the data later with 'retrieveDataFromMessageMeta')
getLastInGroupMessageMeta :: Consumer -> StreamName -> GroupId -> IO MessageMeta
getLastInGroupMessageMeta consumer streamName groupId = maybeThrow (PC.getLastInGroupMessageMeta consumer streamName groupId)

-- | Retrieve the last message in a given stream and group, only data
getLastInGroupMessageData :: Consumer -> StreamName -> GroupId -> IO BS.ByteString
getLastInGroupMessageData consumer streamName groupId = maybeThrow (PC.getLastInGroupMessageData consumer streamName groupId)

-- | Retrieve the next message in the stream and group, with data and metadata
getNextMessageMetaAndData :: Consumer -> StreamName -> GroupId -> IO (MessageMeta, BS.ByteString)
getNextMessageMetaAndData consumer streamName groupId = maybeThrow (PC.getNextMessageMetaAndData consumer streamName groupId)

-- | Retrieve the next message in the stream and group, only metadata (you can get the data later with 'retrieveDataFromMessageMeta')
getNextMessageMeta :: Consumer -> StreamName -> GroupId -> IO MessageMeta
getNextMessageMeta consumer streamName groupId = maybeThrow (PC.getNextMessageMeta consumer streamName groupId)

-- | Retrieve the next message in the stream and group, only data
getNextMessageData :: Consumer -> StreamName -> GroupId -> IO BS.ByteString
getNextMessageData consumer streamName groupId = maybeThrow (PC.getNextMessageData consumer streamName groupId)

-- | Query messages, return handles without data
queryMessages ::
  Consumer ->
  -- | Actual query string, see the docs for syntax
  Text ->
  StreamName ->
  IO [MessageMeta]
queryMessages consumer query streamName = maybeThrow (PC.queryMessages consumer query streamName)

-- | Retrieve actual data for the handle
retrieveDataForMessageMeta :: Consumer -> MessageMeta -> IO BS.ByteString
retrieveDataForMessageMeta consumer meta = maybeThrow (PC.retrieveDataForMessageMeta consumer meta)
