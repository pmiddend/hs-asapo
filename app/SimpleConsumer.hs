{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Asapo.Common
import Asapo.Consumer
import Control.Applicative (Applicative ((<*>)))
import Control.Monad (forM_, (=<<))
import Data.Bool (Bool (True))
import Data.Either (Either (Left, Right))
import Data.Function (($))
import Data.Functor ((<$>))
import Data.Maybe (Maybe (Nothing), fromMaybe)
import Data.Semigroup (Semigroup ((<>)))
import Data.Text (Text, pack)
import qualified Data.Text.IO as TIO
import Data.Time.Clock (secondsToNominalDiffTime)
import qualified Options.Applicative as Opt
import System.IO (IO)
import Text.Show (Show (show))
import Prelude ()

hstoken :: Token
hstoken = Token "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjk1NzE3MTAyMTYsImp0aSI6Ind0ZmlzdGhpcyIsInN1YiI6ImJ0X2FzYXBvX3Rlc3QiLCJFeHRyYUNsYWltcyI6eyJBY2Nlc3NUeXBlcyI6WyJ3cml0ZSIsIndyaXRlcmF3IiwicmVhZCJdfX0.cz6R_kVf4yh7IJD6bJjDdgTaxPN3txudZx9DE6WaTtk"

data Options = Options
  { optionsServerName :: Text,
    optionsWithFilesystem :: Bool
  }

optionsParser :: Opt.Parser Options
optionsParser =
  Options
    <$> Opt.strOption (Opt.long "server-name")
    <*> Opt.switch (Opt.long "with-filesystem")

main :: IO ()
main = realMain =<< Opt.execParser opts
  where
    opts =
      Opt.info
        (optionsParser Opt.<**> Opt.helper)
        ( Opt.fullDesc
            <> Opt.progDesc "Consume data from asapo"
            <> Opt.header "simple-consumer - a simple message sender"
        )

outputError :: Error -> IO ()
outputError (Error errorMessage' errorType') = TIO.putStrLn $ "error: " <> errorMessage' <> " code " <> pack (show errorType')

onSuccess :: Text -> IO (Either Error t) -> (t -> IO ()) -> IO ()
onSuccess opName g f = do
  result <- g
  case result of
    Left (Error errorMessage' errorType') -> TIO.putStrLn $ "error in op " <> opName <> ": " <> errorMessage' <> " code " <> pack (show errorType')
    Right v -> f v

realMain :: Options -> IO ()
realMain (Options serverName withFilesystem) = do
  withConsumer
    (ServerName serverName)
    (SourcePath "")
    (if withFilesystem then WithFilesystem else WithoutFilesystem)
    ( SourceCredentials
        { sourceType = RawSource,
          instanceId = InstanceId "auto",
          pipelineStep = PipelineStep "pipeline_step_1",
          beamtime = Beamtime "asapo_test",
          beamline = Beamline "",
          dataSource = DataSource "asapo_source",
          token = hstoken
        }
    )
    outputError
    \consumer -> do
      TIO.putStrLn "inited consumer"

      TIO.putStrLn "misc: setting timeout"
      setTimeout consumer (secondsToNominalDiffTime 0.5)

      onSuccess "getBeamtimeMeta" (getBeamtimeMeta consumer) \meta -> TIO.putStrLn $ "beamtime metadata: " <> (fromMaybe "N/A" meta)

      TIO.putStrLn "listing all available streams:"
      onSuccess "getStreamList" (getStreamList consumer Nothing FilterAllStreams) \streams ->
        forM_ streams \stream -> do
          TIO.putStrLn $ "=> stream info " <> pack (show stream)
          onSuccess "getCurrentSize" (getCurrentSize consumer (streamInfoName stream)) \streamSize ->
            TIO.putStrLn $ "   stream size: " <> pack (show streamSize)
          onSuccess "getCurrentDatasetCount" (getCurrentDatasetCount consumer (streamInfoName stream) IncludeIncomplete) \datasetCount ->
            TIO.putStrLn $ "   dataset count: " <> pack (show datasetCount)

          withGroupId consumer outputError \groupId -> do
            onSuccess "getNextMessageMeta" (getNextMessageMeta consumer (streamInfoName stream) groupId) \(messageMetaHandle, messageMeta) -> do
              TIO.putStrLn "   got message meta"
            onSuccess "getNextMessageMetaAndData" (getNextMessageMetaAndData consumer (streamInfoName stream) groupId) \(messageMetaHandle, messageMeta, messageData) -> do
              TIO.putStrLn "   got message"

      TIO.putStrLn "misc: resending nacs"
      resendNacs consumer True (secondsToNominalDiffTime 1) 10
