{-# LANGUAGE BlockArguments #-}

module Main (main) where

import Asapo.Raw
import Control.Concurrent (threadDelay)
import Control.Monad (forM_)
import Data.Bits ((.|.))
import Foreign.C.String (peekCString, withCString)
import Foreign.Marshal.Utils (with)
import Foreign.Ptr (castPtr)
import Foreign.Storable (peek)
import System.Clock (TimeSpec (TimeSpec))

hstoken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjk1NzE3MTAyMTYsImp0aSI6Ind0ZmlzdGhpcyIsInN1YiI6ImJ0X2FzYXBvX3Rlc3QiLCJFeHRyYUNsYWltcyI6eyJBY2Nlc3NUeXBlcyI6WyJ3cml0ZSIsIndyaXRlcmF3IiwicmVhZCJdfX0.cz6R_kVf4yh7IJD6bJjDdgTaxPN3txudZx9DE6WaTtk"

main :: IO ()
main = do
  err <- asapo_new_handle
  mm <- asapo_new_handle
  data' <- asapo_new_handle
  withCString "cfeld-vm04:8400" $ \endpoint -> do
    withCString "asapo_test" $ \beamtime -> do
      withCString hstoken $ \token -> do
        withCString "test_consumer_instance" $ \instanceId -> do
          withCString "pipeline_step_1" $ \pipelineStep -> do
            withCString "" $ \beamline -> do
              withCString "asapo_source" $ \dataSource -> do
                cred <- asapo_create_source_credentials kProcessed instanceId pipelineStep beamtime beamline dataSource token
                withCString "auto" $ \sourcePath -> do
                  consumer <- with err $ \errPtr -> asapo_create_consumer endpoint sourcePath 1 cred errPtr
                  with cred asapo_free_handle
                  asapo_consumer_set_timeout consumer 5000

                  group_id <- with err $ \errPtr -> asapo_consumer_generate_new_group_id consumer errPtr

                  infosHandle <- withCString "" $ \fromStream -> with err $ \errPtr -> asapo_consumer_get_stream_list consumer fromStream kAllStreams errPtr

                  size <- asapo_stream_infos_get_size infosHandle

                  putStrLn $ show size <> " stream(s)"

                  forM_ [0 .. size - 1] \index -> do
                    info <- asapo_stream_infos_get_item infosHandle index
                    lastId <- asapo_stream_info_get_last_id info
                    putStrLn $ "stream " <> show index <> " last ID " <> show lastId
                    nameC <- asapo_stream_info_get_name info
                    name <- peekCString nameC
                    putStrLn $ "stream " <> show index <> " name " <> name
                    finished <- asapo_stream_info_get_finished info
                    putStrLn $ "stream " <> show index <> " finished " <> show finished
                    nextStreamC <- asapo_stream_info_get_next_stream info
                    nextStream <- peekCString nextStreamC
                    putStrLn $ "stream " <> show index <> " next stream " <> nextStream
                    timestampCreated <- with (TimeSpec 0 0) \timespecPtr -> do
                      asapo_stream_info_get_timestamp_created info timespecPtr
                      peek timespecPtr
                    putStrLn $ "stream " <> show index <> " created " <> show timestampCreated
                    timestampLast_Entry <- with (TimeSpec 0 0) \timespecPtr -> do
                      asapo_stream_info_get_timestamp_last_entry info timespecPtr
                      peek timespecPtr
                    putStrLn $ "stream " <> show index <> " last_entry " <> show timestampLast_Entry

                  threadDelay (1000 * 1000 * 5)
