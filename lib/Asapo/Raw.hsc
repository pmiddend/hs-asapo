{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CApiFFI #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE ForeignFunctionInterface #-}

module Asapo.Raw where

import Data.Bits (shiftL, (.|.))
import Foreign.C.String (CString)
import Foreign.C.Types (CInt (CInt), CSize (CSize), CUChar (CUChar), CULong (CULong))
import Foreign.Ptr (FunPtr, Ptr)
import System.IO (IO)
import Prelude ()

type AsapoBool = CInt

type AsapoSourceCredentialsHandle = Ptr ()

type AsapoConsumerHandle = Ptr ()

type AsapoStringHandle = Ptr ()

type AsapoErrorHandle = Ptr ()

type AsapoProducerHandle = Ptr ()

type AsapoStreamInfosHandle = Ptr ()

foreign import capi "asapo/common/common_c.h asapo_is_error" asapo_is_error :: AsapoErrorHandle -> IO AsapoBool

foreign import capi "asapo/common/common_c.h asapo_error_explain" asapo_error_explain :: AsapoErrorHandle -> CString -> CSize -> IO ()

foreign import ccall "asapo/consumer_c.h asapo_create_consumer" asapo_create_consumer :: CString -> CString -> AsapoBool -> AsapoSourceCredentialsHandle -> Ptr AsapoErrorHandle -> IO AsapoConsumerHandle

foreign import capi "asapo/consumer_c.h asapo_new_handle" asapo_new_handle :: IO (Ptr ())

foreign import capi "asapo/consumer_c.h asapo_free_handle" asapo_free_handle :: Ptr (Ptr ()) -> IO ()

foreign import capi "asapo/consumer_c.h asapo_create_source_credentials" asapo_create_source_credentials :: CInt -> CString -> CString -> CString -> CString -> CString -> CString -> IO AsapoSourceCredentialsHandle

foreign import capi "asapo/consumer_c.h asapo_consumer_set_timeout" asapo_consumer_set_timeout :: AsapoConsumerHandle -> CULong -> IO ()

foreign import ccall "asapo/consumer_c.h asapo_consumer_generate_new_group_id" asapo_consumer_generate_new_group_id :: AsapoConsumerHandle -> Ptr AsapoErrorHandle -> IO AsapoStringHandle

foreign import ccall "asapo/consumer_c.h asapo_consumer_get_stream_list" asapo_consumer_get_stream_list :: AsapoConsumerHandle -> CString -> CInt -> Ptr AsapoErrorHandle -> IO AsapoStreamInfosHandle

foreign import capi "asapo/consumer_c.h asapo_stream_infos_get_size" asapo_stream_infos_get_size :: AsapoStreamInfosHandle -> IO CSize

kProcessed :: CInt
kProcessed = 0

kRaw :: CInt
kRaw = 1

kAllStreams :: CInt
kAllStreams = 0

kFinishedStreams :: CInt
kFinishedStreams = 1

kUnfinishedStreams :: CInt
kUnfinishedStreams = 2

foreign import ccall "asapo/producer_c.h asapo_create_producer"
  asapo_create_producer ::
    CString ->
    CUChar ->
    CInt ->
    AsapoSourceCredentialsHandle ->
    CULong ->
    Ptr AsapoErrorHandle ->
    IO AsapoProducerHandle

type AsapoRequestCallbackPayloadHandle = Ptr ()

type AsapoRequestCallback = Ptr () -> AsapoRequestCallbackPayloadHandle -> AsapoErrorHandle -> IO ()

foreign import ccall "wrapper" createRequestCallback :: AsapoRequestCallback -> IO (FunPtr AsapoRequestCallback)

type AsapoMessageHeaderHandle = Ptr ()

foreign import ccall "asapo/producer_c.h asapo_create_message_header"
  asapo_create_message_header ::
    -- message ID
    CULong ->
    -- data size
    CULong ->
    -- file name
    CString ->
    -- user metadata
    CString ->
    -- dataset substream
    CULong ->
    -- dataset size
    CULong ->
    -- auto id
    AsapoBool ->
    IO AsapoMessageHeaderHandle

foreign import ccall "asapo/producer_c.h asapo_producer_send"
  asapo_producer_send ::
    AsapoProducerHandle ->
    AsapoMessageHeaderHandle ->
    -- data
    Ptr () ->
    -- ingest mode
    CULong ->
    -- stream
    CString ->
    FunPtr AsapoRequestCallback ->
    Ptr AsapoErrorHandle ->
    IO CInt

kTcp :: CInt
kTcp = 0

kFilesystem :: CInt
kFilesystem = 1

kTransferData :: CULong
kTransferData = 1

kTransferMetaDataOnly :: CULong
kTransferMetaDataOnly = shiftL 1 1

kStoreInFilesystem :: CULong
kStoreInFilesystem = shiftL 1 2

kStoreInDatabase :: CULong
kStoreInDatabase = shiftL 1 3

kDefaultIngestMode :: CULong
kDefaultIngestMode = kTransferData .|. kStoreInFilesystem .|. kStoreInDatabase
