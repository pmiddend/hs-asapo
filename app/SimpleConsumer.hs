{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Asapo.Consumer
import Control.Applicative (Applicative ((<*>)))
import Control.Monad (forM_, (=<<), (>>=))
import Data.Bool (Bool (True))
import Data.Either (Either (Left, Right))
import Data.Function (($))
import Data.Functor ((<$>))
import Data.Maybe (Maybe (Nothing), fromMaybe)
import Data.Semigroup (Semigroup ((<>)))
import Data.Text (Text, pack)
import Data.Text.Encoding (decodeUtf8)
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

realMain :: Options -> IO ()
realMain (Options serverName withFilesystem) = do
  withConsumer
    (ServerName serverName)
    (SourcePath "")
    (if withFilesystem then WithFilesystem else WithoutFilesystem)
    ( SourceCredentials
        { sourceType = RawSource,
          instanceId = InstanceId "auto",
          pipelineStep = PipelineStep "ps1",
          beamtime = Beamtime "asapo_test",
          beamline = Beamline "",
          dataSource = DataSource "asapo_source",
          token = hstoken
        }
    )
    \consumer -> do
      TIO.putStrLn "inited consumer"

      TIO.putStrLn "misc: setting timeout"
      setTimeout consumer (secondsToNominalDiffTime 0.5)

      getBeamtimeMeta consumer >>= \meta -> TIO.putStrLn $ "beamtime metadata: " <> (fromMaybe "N/A" meta)

      TIO.putStrLn "listing all available streams:"
      streams <- getStreamList consumer Nothing FilterAllStreams
      forM_ streams \stream -> do
        TIO.putStrLn $ "=> stream info " <> pack (show stream)
        streamSize <- getCurrentSize consumer (streamInfoName stream)
        TIO.putStrLn $ "   stream size: " <> pack (show streamSize)
        datasetCount <- getCurrentDatasetCount consumer (streamInfoName stream) IncludeIncomplete
        TIO.putStrLn $ "   dataset count: " <> pack (show datasetCount)

      -- withGroupId consumer outputError \groupId -> do
      --   onSuccess "getNextMessageMeta" (getNextMessageMeta consumer (streamInfoName stream) groupId) \(messageMetaHandle, messageMeta) -> do
      --     TIO.putStrLn "   got message meta"
      --   onSuccess "getNextMessageMetaAndData" (getNextMessageMetaAndData consumer (streamInfoName stream) groupId) \(messageMetaHandle, messageMeta, messageData) -> do
      --     TIO.putStrLn "   got message"

      (meta, data') <- getMessageMetaAndDataById consumer (StreamName "default") (messageIdFromInt 156)
      TIO.putStrLn $ "meta: " <> pack (show meta)
      TIO.putStrLn $ "data: " <> decodeUtf8 data'

      TIO.putStrLn "misc: resending nacs"
      resendNacs consumer True (secondsToNominalDiffTime 1) 10
