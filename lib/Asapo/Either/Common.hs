{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE NamedFieldPuns #-}

module Asapo.Either.Common
  ( SourceType (..),
    InstanceId (..),
    PipelineStep (..),
    Beamline (..),
    withPtr,
    Beamtime (..),
    messageIdFromInt,
    MessageId (..),
    stringHandleToText,
    DataSource (..),
    Token (..),
    timespecToUTC,
    SourceCredentials (..),
    withCredentials,
    withText,
    StreamName (..),
    withCStringN,
    withCStringNToText,
    withConstText,
    withConstCString,
    peekConstCStringText,
    peekCStringText,
    stringHandleToTextUnsafe,
    nominalDiffToMillis,
    retrieveStreamInfoFromC,
    StreamInfo (..),
  )
where

-- \|
-- Description : Utility module with common definitions
--
-- You shouldn't need to explicitly import anything from here
import Asapo.Raw.Common (AsapoSourceCredentialsHandle, AsapoStreamInfoHandle, AsapoStringHandle (AsapoStringHandle), ConstCString, asapo_create_source_credentials, asapo_free_source_credentials, asapo_stream_info_get_finished, asapo_stream_info_get_last_id, asapo_stream_info_get_name, asapo_stream_info_get_next_stream, asapo_stream_info_get_timestamp_created, asapo_stream_info_get_timestamp_last_entry, asapo_string_c_str, kProcessed, kRaw)
import Control.Applicative (pure)
import Control.Exception (bracket)
import Control.Monad (Monad ((>>=)), (>=>))
import Data.Bool (Bool, otherwise)
import Data.Eq ((==))
import Data.Function (($), (.))
import Data.Functor ((<$>))
import Data.Int (Int)
import Data.Maybe (Maybe (Just, Nothing), fromJust)
import Data.Ord ((>))
import Data.String (String)
import Data.Text (Text, pack, unpack)
import Data.Time (NominalDiffTime, zonedTimeToUTC)
import Data.Time.Clock (UTCTime, addUTCTime)
import qualified Data.Time.RFC3339 as RFC3339
import Data.Word (Word64)
import Foreign (Ptr, Storable (peek), nullPtr, with)
import Foreign.C (CChar)
import Foreign.C.ConstPtr (ConstPtr (ConstPtr, unConstPtr))
import Foreign.C.String (CString, peekCString, withCString)
import Foreign.Marshal (alloca, mallocArray)
import Foreign.Marshal.Alloc (free)
import System.Clock (TimeSpec, toNanoSecs)
import System.IO (IO)
import Text.Show (Show)
import Prelude (Fractional ((/)), Integral, Num (fromInteger, (*)), RealFrac (round), fromIntegral)

withText :: Text -> (CString -> IO a) -> IO a
withText t = withCString (unpack t)

nominalDiffToMillis :: (Integral a) => NominalDiffTime -> a
nominalDiffToMillis = round . (* 1000)

newtype MessageId = MessageId Word64 deriving (Show)

messageIdFromInt :: (Integral a) => a -> MessageId
messageIdFromInt = MessageId . fromIntegral

newtype StreamName = StreamName Text deriving (Show)

data SourceType = RawSource | ProcessedSource

newtype InstanceId = InstanceId Text

newtype PipelineStep = PipelineStep Text

newtype Beamtime = Beamtime Text

newtype Beamline = Beamline Text

newtype DataSource = DataSource Text

newtype Token = Token Text

data SourceCredentials = SourceCredentials
  { sourceType :: SourceType,
    instanceId :: InstanceId,
    pipelineStep :: PipelineStep,
    beamtime :: Beamtime,
    beamline :: Beamline,
    dataSource :: DataSource,
    token :: Token
  }

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
        createCredentialsWithText = withText instanceId' \instanceId'' -> withText pipelineStep' \pipelineStep'' -> withText beamtime' \beamtime'' -> withText beamline' \beamline'' -> withText dataSource' \dataSource'' ->
          withText token' $
            asapo_create_source_credentials
              (convertSourceType sourceType)
              instanceId''
              pipelineStep''
              beamtime''
              beamline''
              dataSource''
    bracket createCredentialsWithText asapo_free_source_credentials f

peekCStringText :: CString -> IO Text
peekCStringText = (pack <$>) . peekCString

peekConstCStringText :: ConstPtr CChar -> IO Text
peekConstCStringText = (pack <$>) . peekCString . unConstPtr

withConstCString :: String -> (ConstCString -> IO b) -> IO b
withConstCString s f = withCString s (f . ConstPtr)

withConstText :: Text -> (ConstCString -> IO a) -> IO a
withConstText t = withConstCString (unpack t)

withCStringN :: Int -> (CString -> IO a) -> IO a
withCStringN size = bracket (mallocArray size) free

withCStringNToText :: Int -> (CString -> IO ()) -> IO Text
withCStringNToText size f =
  withCStringN size \ptr -> do
    f ptr
    pack <$> peekCString ptr

data StreamInfo = StreamInfo
  { streamInfoLastId :: MessageId,
    streamInfoName :: StreamName,
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
  pure (StreamInfo (MessageId (fromIntegral lastId)) (StreamName name) (finished > 0) nextStream created lastEntry)

stringHandleToText :: AsapoStringHandle -> IO (Maybe Text)
stringHandleToText handle@(AsapoStringHandle handlePtr)
  | handlePtr == nullPtr = pure Nothing
  | otherwise = Just <$> (asapo_string_c_str handle >>= peekConstCStringText)

stringHandleToTextUnsafe :: AsapoStringHandle -> IO Text
stringHandleToTextUnsafe = asapo_string_c_str >=> peekConstCStringText

withPtr :: (Storable a) => a -> (Ptr a -> IO b) -> IO (a, b)
withPtr h f = with h \hPtr -> do
  result <- f hPtr
  first <- peek hPtr
  pure (first, result)
