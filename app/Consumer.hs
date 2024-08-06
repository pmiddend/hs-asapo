{-# LANGUAGE BlockArguments #-}

module Main (main) where

import Asapo.Raw (AsapoConsumerHandle, AsapoErrorHandle, asapo_consumer_generate_new_group_id, asapo_consumer_get_next, asapo_consumer_get_stream_list, asapo_consumer_set_timeout, asapo_create_consumer, asapo_create_source_credentials, asapo_is_error, asapo_new_error_handle, asapo_new_message_data_handle, asapo_new_message_meta_handle, asapo_stream_info_get_finished, asapo_stream_info_get_last_id, asapo_stream_info_get_name, asapo_stream_info_get_next_stream, asapo_stream_info_get_timestamp_created, asapo_stream_info_get_timestamp_last_entry, asapo_stream_infos_get_item, asapo_stream_infos_get_size, kAllStreams, kProcessed)
import Control.Concurrent (threadDelay)
import Control.Monad (forM_, when)
import Data.Function (($))
import Data.Ord (Ord ((<), (>)))
import Data.Semigroup (Semigroup ((<>)))
import Data.String (String)
import Foreign.C.ConstPtr (ConstPtr (unConstPtr))
import Foreign.C.String (peekCString, withCString)
import Foreign.Marshal.Utils (with)
import Foreign.Storable (peek)
import System.Clock (TimeSpec (TimeSpec))
import System.IO (IO, print, putStrLn)
import Text.Show (Show (show))
import Prelude (Num ((*), (-)), error)

hstoken :: String
hstoken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjk1NzE3MTAyMTYsImp0aSI6Ind0ZmlzdGhpcyIsInN1YiI6ImJ0X2FzYXBvX3Rlc3QiLCJFeHRyYUNsYWltcyI6eyJBY2Nlc3NUeXBlcyI6WyJ3cml0ZSIsIndyaXRlcmF3IiwicmVhZCJdfX0.cz6R_kVf4yh7IJD6bJjDdgTaxPN3txudZx9DE6WaTtk"

exitIfError :: AsapoErrorHandle -> IO ()
exitIfError err = do
  isError <- asapo_is_error err
  when (isError > 0) (error "error")

listStreams :: AsapoConsumerHandle -> AsapoErrorHandle -> IO ()
listStreams consumer err = do
  infosHandle <- withCString "" $ \fromStream ->
    with err (asapo_consumer_get_stream_list consumer fromStream kAllStreams)
  exitIfError err

  size <- asapo_stream_infos_get_size infosHandle

  putStrLn $ show size <> " stream(s)"

  forM_ [0 .. size - 1] \index -> do
    info <- asapo_stream_infos_get_item infosHandle index
    lastId <- asapo_stream_info_get_last_id info
    putStrLn $ "stream " <> show index <> " last ID " <> show lastId
    nameC <- asapo_stream_info_get_name info
    name <- peekCString (unConstPtr nameC)
    putStrLn $ "stream " <> show index <> " name " <> name
    finished <- asapo_stream_info_get_finished info
    putStrLn $ "stream " <> show index <> " finished " <> show finished
    nextStreamC <- asapo_stream_info_get_next_stream info
    nextStream <- peekCString (unConstPtr nextStreamC)
    putStrLn $ "stream " <> show index <> " next stream " <> nextStream
    timestampCreated <- with (TimeSpec 0 0) \timespecPtr -> do
      asapo_stream_info_get_timestamp_created info timespecPtr
      peek timespecPtr
    putStrLn $ "stream " <> show index <> " created " <> show timestampCreated
    timestampLast_Entry <- with (TimeSpec 0 0) \timespecPtr -> do
      asapo_stream_info_get_timestamp_last_entry info timespecPtr
      peek timespecPtr
    putStrLn $ "stream " <> show index <> " last_entry " <> show timestampLast_Entry

main :: IO ()
main = do
  err <- asapo_new_error_handle
  withCString "cfeld-vm04.desy.de:8400" $ \endpoint -> do
    withCString "asapo_test" $ \beamtime -> do
      withCString hstoken $ \token -> do
        withCString "test_consumer_instance" $ \instanceId -> do
          withCString "pipeline_step_1" $ \pipelineStep -> do
            withCString "test_beamline" $ \beamline -> do
              withCString "asapo_source" $ \dataSource -> do
                cred <- asapo_create_source_credentials kProcessed instanceId pipelineStep beamtime beamline dataSource token
                withCString "auto" $ \sourcePath -> do
                  consumer <- with err $ \errPtr -> asapo_create_consumer endpoint sourcePath 1 cred errPtr
                  exitIfError err
                  -- with cred asapo_free_handle
                  asapo_consumer_set_timeout consumer 5000

                  group_id <- with err $ \errPtr -> asapo_consumer_generate_new_group_id consumer errPtr
                  exitIfError err

                  listStreams consumer err

                  metaHandle <- asapo_new_message_meta_handle
                  dataHandle <- asapo_new_message_data_handle
                  resultOfGetNext <- with err $ \errPtr ->
                    with metaHandle $
                      \metaHandlePtr -> with dataHandle $
                        \dataHandlePtr ->
                          withCString "kacke" $
                            \stream -> asapo_consumer_get_next consumer group_id metaHandlePtr dataHandlePtr stream errPtr
                  exitIfError err

                  when (resultOfGetNext < 0) do
                    error "get next failed"
                  print resultOfGetNext
                  -- metaId <- asapo_message_meta_get_id metaHandle

                  -- print metaId

                  threadDelay (1000 * 1000 * 5)
