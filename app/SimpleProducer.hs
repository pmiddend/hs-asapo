{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Asapo.Common
import Asapo.Producer
import Control.Applicative (Applicative (pure, (<*>)))
import Control.Concurrent (threadDelay)
import Control.Monad (when, (=<<))
import Data.Bits ((.|.))
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8
import Data.Either (Either (Left, Right))
import Data.Foldable (Foldable (length))
import Data.Function (($))
import Data.Functor ((<$>))
import Data.Int (Int)
import Data.Maybe (Maybe (Just, Nothing))
import Data.Ord (Ord ((>)))
import Data.Semigroup (Semigroup ((<>)))
import Data.String (String)
import Data.Text (Text, pack)
import Data.Text.Encoding (encodeUtf8)
import Data.Text.IO as TIO
import Data.Time.Clock (secondsToNominalDiffTime)
import Data.Word (Word64)
import Foreign.C.String (newCString, withCString)
import Foreign.Marshal.Utils (with)
import Foreign.Ptr (castPtr)
import qualified Options.Applicative as Opt
import System.Environment (getArgs)
import System.IO (IO, print, putStrLn)
import Text.Show (Show (show))
import Prelude (Num ((*), (+)), error, fromIntegral)

hstoken :: Token
hstoken = Token "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjk1NzE3MTAyMTYsImp0aSI6Ind0ZmlzdGhpcyIsInN1YiI6ImJ0X2FzYXBvX3Rlc3QiLCJFeHRyYUNsYWltcyI6eyJBY2Nlc3NUeXBlcyI6WyJ3cml0ZSIsIndyaXRlcmF3IiwicmVhZCJdfX0.cz6R_kVf4yh7IJD6bJjDdgTaxPN3txudZx9DE6WaTtk"

data Options = Options
  { optionsMessageId :: Word64,
    optionsMessageContent :: Text,
    optionsEndpoint :: Text,
    optionsStreamName :: Text
  }

optionsParser :: Opt.Parser Options
optionsParser =
  Options
    <$> Opt.option Opt.auto (Opt.long "message-id" <> Opt.help "ID for the message to send (don't re-use, because of duplication messages)")
    <*> Opt.strOption (Opt.long "message-content" <> Opt.help "what to put into the message")
    <*> Opt.strOption (Opt.long "endpoint" <> Opt.help "asapo endpoint")
    <*> Opt.strOption (Opt.long "stream-name")

main :: IO ()
main = realMain =<< Opt.execParser opts
  where
    opts =
      Opt.info
        (optionsParser Opt.<**> Opt.helper)
        ( Opt.fullDesc
            <> Opt.progDesc "Send a string with a specified ID"
            <> Opt.header "simple-producer - a simple message sender"
        )

realMain (Options messageId messageContent endpoint streamName) = do
  withProducer
    (Endpoint endpoint)
    (ProcessingThreads 1)
    TcpHandler
    ( SourceCredentials
        { sourceType = RawSource,
          instanceId = InstanceId "test_instance",
          pipelineStep = PipelineStep "pipeline_step_1",
          beamtime = Beamtime "asapo_test",
          beamline = Beamline "auto",
          dataSource = DataSource "asapo_source",
          token = hstoken
        }
    )
    (secondsToNominalDiffTime 10)
    (\(Error errorMessage) -> TIO.putStrLn errorMessage)
    \producer -> do
      TIO.putStrLn "connected, sending data"
      let responseHandler :: RequestResponse -> IO ()
          responseHandler requestResponse = TIO.putStrLn $ "in response handler, payload " <> responsePayload requestResponse <> ", error " <> pack (show (responseError requestResponse))
      sendResult <-
        send
          producer
          (MessageId messageId)
          (FileName $ "raw/" <> streamName <> "/" <> pack (show messageId) <> ".txt")
          (Metadata "{\"test\": 3.0}")
          (DatasetSubstream 0)
          (DatasetSize 0)
          NoAutoId
          (encodeUtf8 messageContent)
          DataAndMetadata
          FilesystemAndDatabase
          (StreamName streamName)
          responseHandler
      case sendResult of
        Left e -> TIO.putStrLn "error sending"
        Right _ -> TIO.putStrLn "send complete"
      waitResult <- waitRequestsFinished producer (secondsToNominalDiffTime 10)
      case waitResult of
        Left e -> TIO.putStrLn "error waiting"
        Right _ -> TIO.putStrLn "wait complete"
