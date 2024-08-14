{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Asapo.Common
import Asapo.Producer
import Control.Applicative (Applicative (pure))
import Control.Concurrent (threadDelay)
import Control.Monad (when)
import Data.Bits ((.|.))
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8
import Data.Either (Either (Left, Right))
import Data.Foldable (Foldable (length))
import Data.Function (($))
import Data.Maybe (Maybe (Just, Nothing))
import Data.Ord (Ord ((>)))
import Data.Semigroup (Semigroup ((<>)))
import Data.String (String)
import Data.Text (pack)
import Data.Text.IO as TIO
import Foreign.C.String (newCString, withCString)
import Foreign.Marshal.Utils (with)
import Foreign.Ptr (castPtr)
import System.IO (IO, print, putStrLn)
import Text.Show (Show (show))
import Prelude (Num ((*), (+)), error, fromIntegral)

hstoken :: Token
hstoken = Token "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjk1NzE3MTAyMTYsImp0aSI6Ind0ZmlzdGhpcyIsInN1YiI6ImJ0X2FzYXBvX3Rlc3QiLCJFeHRyYUNsYWltcyI6eyJBY2Nlc3NUeXBlcyI6WyJ3cml0ZSIsIndyaXRlcmF3IiwicmVhZCJdfX0.cz6R_kVf4yh7IJD6bJjDdgTaxPN3txudZx9DE6WaTtk"

main = do
  withProducer
    (Endpoint "localhost:8400")
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
    (Milliseconds 10000)
    (\(Error errorMessage) -> TIO.putStrLn errorMessage)
    \producer -> do
      TIO.putStrLn "connected, sending data"
      let responseHandler :: RequestResponse -> IO ()
          responseHandler requestResponse = TIO.putStrLn $ "in response handler, payload " <> responsePayload requestResponse <> ", error " <> pack (show (responseError requestResponse))
      sendResult <- send producer (MessageId 1) (FileName "raw/1.txt") (Metadata "{}") (DatasetSubstream 1) (DatasetSize 1) NoAutoId (BS8.pack "hello") DataAndMetadata FilesystemAndDatabase (StreamName "hl") responseHandler
      case sendResult of
        Left e -> TIO.putStrLn "error sending"
        Right _ -> TIO.putStrLn "send complete"
      waitResult <- waitRequestsFinished producer (Milliseconds 10000)
      case waitResult of
        Left e -> TIO.putStrLn "error waiting"
        Right _ -> TIO.putStrLn "wait complete"

-- main :: IO ()
-- main = do
--   err <- asapo_new_error_handle
--   withCString "cfeld-vm04.desy.de:8400" $ \endpoint -> do
--     withCString "asapo_test" $ \beamtime -> do
--       withCString hstoken $ \token -> do
--         withCString "test_producer_instance" $ \instanceId -> do
--           withCString "pipeline_step_1" $ \pipelineStep -> do
--             withCString "" $ \beamline -> do
--               withCString "asapo_source" $ \dataSource -> do
--                 cred <- asapo_create_source_credentials kProcessed instanceId pipelineStep beamtime beamline dataSource token
--                 let processingThreads = 1
--                     timeoutMs = 60000
--                 producer' <- with err $ \errPtr -> do
--                   producer'' <- asapo_create_producer endpoint processingThreads kTcp cred timeoutMs errPtr
--                   isError <- asapo_is_error err
--                   if isError > 0
--                     then do
--                       putStrLn "error"
--                       pure Nothing
--                     else pure (Just producer'')

--                 -- with cred asapo_free_handle

--                 putStrLn "done creating producer"

--                 case producer' of
--                   Nothing -> pure ()
--                   Just producer -> do
--                     let messageId = 301
--                         stringToSend = "teststring"
--                         dataSize = fromIntegral (length stringToSend + 1)
--                         fileName = "raw/file.txt"
--                         metadata = "{\"hehe\": true}"
--                         datasetSubstream = 1
--                         datasetSize = 1
--                         -- is a boolean, so 0 = False
--                         autoId = 0

--                     header <- withCString metadata $ \userMetadata -> withCString fileName $ \fileName' ->
--                       asapo_create_message_header messageId dataSize fileName' userMetadata datasetSubstream datasetSize autoId
--                     print header

--                     let processAfterSendCallback payload payloadHandle errorHandle = do
--                           isError <- asapo_is_error err
--                           when (isError > 0) (putStrLn "error in callback")

--                           putStrLn "process after send"

--                     processAfterSendCallbackPtr <- createRequestCallback processAfterSendCallback

--                     stringToSendC <- newCString stringToSend
--                     withCString "kacke" $ \stream ->
--                       with err $ \errPtr -> do
--                         result <-
--                           asapo_producer_send
--                             producer
--                             header
--                             (castPtr stringToSendC)
--                             (kTransferData .|. kStoreInDatabase)
--                             stream
--                             processAfterSendCallbackPtr
--                             errPtr
--                         exitIfError err
--                         putStrLn $ "result of send " <> show result

--                     with err $ \errPtr -> asapo_producer_wait_requests_finished producer (20 * 1000) errPtr
--                     exitIfError err

--                     putStrLn "waited for request finished"

--                     threadDelay (1000 * 1000 * 100)

--                     putStrLn "waited additional 100s"
