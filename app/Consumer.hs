module Main (main) where

import Asapo.Raw
import Control.Concurrent (threadDelay)
import Data.Bits ((.|.))
import Foreign.C.String (withCString)
import Foreign.Marshal.Utils (with)
import Foreign.Ptr (castPtr)

hstoken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjk1NzE3MTAyMTYsImp0aSI6Ind0ZmlzdGhpcyIsInN1YiI6ImJ0X2FzYXBvX3Rlc3QiLCJFeHRyYUNsYWltcyI6eyJBY2Nlc3NUeXBlcyI6WyJ3cml0ZSIsIndyaXRlcmF3IiwicmVhZCJdfX0.cz6R_kVf4yh7IJD6bJjDdgTaxPN3txudZx9DE6WaTtk"

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

                  threadDelay (1000 * 1000 * 5)
