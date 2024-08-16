{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Asapo.Producer
import Control.Applicative (Applicative ((<*>)))
import Control.Monad (void, when, (=<<))
import Data.Bits ((.|.))
import Data.Either (Either (Left, Right))
import Data.Function (($))
import Data.Functor ((<$>))
import Data.Semigroup (Semigroup ((<>)))
import Data.Text (Text, pack)
import Data.Text.Encoding (encodeUtf8)
import qualified Data.Text.IO as TIO
import Data.Time.Clock (secondsToNominalDiffTime)
import Data.Word (Word64)
import Foreign.C.String (newCString, withCString)
import qualified Options.Applicative as Opt
import System.IO (IO)
import Text.Show (Show (show))
import Prelude ()

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

realMain :: Options -> IO ()
realMain (Options messageId messageContent endpoint streamName) = void $
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
    \producer -> do
      TIO.putStrLn "connected, sending data"
      let responseHandler :: RequestResponse -> IO ()
          responseHandler requestResponse = TIO.putStrLn $ "in response handler, payload " <> responsePayload requestResponse <> ", error " <> pack (show (responseError requestResponse))
      void $
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
      waitRequestsFinished producer (secondsToNominalDiffTime 10)
