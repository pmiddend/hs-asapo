module Main (main) where

import Asapo.Raw
  ( AsapoErrorHandle,
    asapo_create_message_header,
    asapo_create_producer,
    asapo_create_source_credentials,
    asapo_is_error,
    asapo_new_error_handle,
    asapo_producer_send,
    asapo_producer_wait_requests_finished,
    createRequestCallback,
    kProcessed,
    kStoreInDatabase,
    kTcp,
    kTransferData,
  )
import Control.Applicative (Applicative (pure))
import Control.Concurrent (threadDelay)
import Control.Monad (when)
import Data.Bits ((.|.))
import Data.Foldable (Foldable (length))
import Data.Function (($))
import Data.Maybe (Maybe (Just, Nothing))
import Data.Ord (Ord ((>)))
import Data.Semigroup (Semigroup ((<>)))
import Data.String (String)
import Foreign.C.String (newCString, withCString)
import Foreign.Marshal.Utils (with)
import Foreign.Ptr (castPtr)
import System.IO (IO, print, putStrLn)
import Text.Show (Show (show))
import Prelude (Num ((*), (+)), error, fromIntegral)

hstoken :: String
hstoken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjk1NzE3MTAyMTYsImp0aSI6Ind0ZmlzdGhpcyIsInN1YiI6ImJ0X2FzYXBvX3Rlc3QiLCJFeHRyYUNsYWltcyI6eyJBY2Nlc3NUeXBlcyI6WyJ3cml0ZSIsIndyaXRlcmF3IiwicmVhZCJdfX0.cz6R_kVf4yh7IJD6bJjDdgTaxPN3txudZx9DE6WaTtk"

exitIfError :: AsapoErrorHandle -> IO ()
exitIfError err = do
  isError <- asapo_is_error err
  when (isError > 0) (error "error")

main :: IO ()
main = do
  err <- asapo_new_error_handle
  withCString "cfeld-vm04.desy.de:8400" $ \endpoint -> do
    withCString "asapo_test" $ \beamtime -> do
      withCString hstoken $ \token -> do
        withCString "test_producer_instance" $ \instanceId -> do
          withCString "pipeline_step_1" $ \pipelineStep -> do
            withCString "" $ \beamline -> do
              withCString "asapo_source" $ \dataSource -> do
                cred <- asapo_create_source_credentials kProcessed instanceId pipelineStep beamtime beamline dataSource token
                let processingThreads = 1
                    timeoutMs = 60000
                producer' <- with err $ \errPtr -> do
                  producer'' <- asapo_create_producer endpoint processingThreads kTcp cred timeoutMs errPtr
                  isError <- asapo_is_error err
                  if isError > 0
                    then do
                      putStrLn "error"
                      pure Nothing
                    else pure (Just producer'')

                -- with cred asapo_free_handle

                putStrLn "done creating producer"

                case producer' of
                  Nothing -> pure ()
                  Just producer -> do
                    let messageId = 301
                        stringToSend = "teststring"
                        dataSize = fromIntegral (length stringToSend + 1)
                        fileName = "raw/file.txt"
                        metadata = "{\"hehe\": true}"
                        datasetSubstream = 1
                        datasetSize = 1
                        -- is a boolean, so 0 = False
                        autoId = 0

                    header <- withCString metadata $ \userMetadata -> withCString fileName $ \fileName' ->
                      asapo_create_message_header messageId dataSize fileName' userMetadata datasetSubstream datasetSize autoId
                    print header

                    let processAfterSendCallback payload payloadHandle errorHandle = do
                          isError <- asapo_is_error err
                          when (isError > 0) (putStrLn "error in callback")

                          putStrLn "process after send"

                    processAfterSendCallbackPtr <- createRequestCallback processAfterSendCallback

                    stringToSendC <- newCString stringToSend
                    withCString "kacke" $ \stream ->
                      with err $ \errPtr -> do
                        result <-
                          asapo_producer_send
                            producer
                            header
                            (castPtr stringToSendC)
                            (kTransferData .|. kStoreInDatabase)
                            stream
                            processAfterSendCallbackPtr
                            errPtr
                        exitIfError err
                        putStrLn $ "result of send " <> show result

                    with err $ \errPtr -> asapo_producer_wait_requests_finished producer (20 * 1000) errPtr
                    exitIfError err

                    putStrLn "waited for request finished"

                    threadDelay (1000 * 1000 * 100)

                    putStrLn "waited additional 100s"
