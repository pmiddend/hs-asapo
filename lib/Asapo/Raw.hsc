{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CApiFFI #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Asapo.Raw
  ( AsapoBool,
    AsapoSourceCredentialsHandle,
    AsapoConsumerHandle,
    AsapoMessageDataHandle (AsapoMessageDataHandle),
    asapo_is_error,
    asapo_error_explain,
    AsapoErrorHandle,
    asapo_new_error_handle,
    asapo_create_consumer,
    asapo_new_handle,
    asapo_free_handle,
    asapo_new_message_meta_handle,
    asapo_new_message_data_handle,
    asapo_create_source_credentials,
    asapo_consumer_set_timeout,
    asapo_consumer_generate_new_group_id,
    asapo_consumer_get_stream_list,
    asapo_stream_infos_get_size,
    asapo_stream_infos_get_item,
    asapo_stream_info_get_last_id,
    asapo_stream_info_get_name,
    asapo_stream_info_get_finished,
    asapo_stream_info_get_next_stream,
    asapo_stream_info_get_timestamp_created,
    asapo_stream_info_get_timestamp_last_entry,
    kProcessed,
    kRaw,
    kAllStreams,
    kFinishedStreams,
    kUnfinishedStreams,
    asapo_consumer_get_next,
    asapo_message_meta_get_id,
    asapo_create_producer,
    createRequestCallback,
    asapo_create_message_header,
    asapo_producer_send,
    kTcp,
    kFilesystem,
    kTransferData,
    kTransferMetaDataOnly,
    kStoreInFilesystem,
    kStoreInDatabase,
    kDefaultIngestMode,
    asapo_producer_wait_requests_finished,
  )
where

import Data.Bits (shiftL, (.|.))
import Data.Functor ((<$>))
import Foreign.C.ConstPtr (ConstPtr (ConstPtr))
import Foreign.C.String (CString)
import Foreign.C.Types (CChar, CInt (CInt), CSize (CSize), CUChar (CUChar), CULong (CULong))
import Foreign.Ptr (FunPtr, Ptr)
import Foreign.Storable (Storable)
import System.Clock (TimeSpec)
import System.IO (IO)
import Prelude ()

type ConstCString = ConstPtr CChar

type AsapoBool = CInt

newtype {-# CTYPE "asapo/common/common_c.h" "AsapoSourceCredentialsHandle" #-} AsapoSourceCredentialsHandle = AsapoSourceCredentialsHandle (Ptr ()) deriving (Storable)

newtype {-# CTYPE "asapo/common/common_c.h" "AsapoErrorHandle" #-} AsapoErrorHandle = AsapoErrorHandle (Ptr ()) deriving (Storable)

newtype {-# CTYPE "asapo/common/common_c.h" "AsapoStreamInfoHandle" #-} AsapoStreamInfoHandle = AsapoStreamInfoHandle (Ptr ()) deriving (Storable)

newtype {-# CTYPE "asapo/common/common_c.h" "AsapoStreamInfosHandle" #-} AsapoStreamInfosHandle = AsapoStreamInfosHandle (Ptr ()) deriving (Storable)

newtype {-# CTYPE "asapo/common/common_c.h" "AsapoStringHandle" #-} AsapoStringHandle = AsapoStringHandle (Ptr ()) deriving (Storable)

newtype {-# CTYPE "asapo/consumer_c.h" "AsapoConsumerHandle" #-} AsapoConsumerHandle = AsapoConsumerHandle (Ptr ()) deriving (Storable)

newtype {-# CTYPE "asapo/consumer_c.h" "AsapoMessageMetaHandle" #-} AsapoMessageMetaHandle = AsapoMessageMetaHandle (Ptr ()) deriving (Storable)

newtype {-# CTYPE "asapo/consumer_c.h" "AsapoMessageDataHandle" #-} AsapoMessageDataHandle = AsapoMessageDataHandle (Ptr ()) deriving (Storable)

type AsapoProducerHandle = Ptr ()

foreign import capi "asapo/common/common_c.h asapo_is_error" asapo_is_error :: AsapoErrorHandle -> IO AsapoBool

foreign import capi "asapo/common/common_c.h asapo_error_explain" asapo_error_explain :: AsapoErrorHandle -> CString -> CSize -> IO ()

foreign import capi "asapo/consumer_c.h asapo_create_consumer" asapo_create_consumer :: CString -> CString -> AsapoBool -> AsapoSourceCredentialsHandle -> Ptr AsapoErrorHandle -> IO AsapoConsumerHandle

foreign import capi "asapo/consumer_c.h asapo_new_handle" asapo_new_handle :: IO (Ptr ())

asapo_new_error_handle :: IO AsapoErrorHandle
asapo_new_error_handle = AsapoErrorHandle <$> asapo_new_handle

asapo_new_message_meta_handle :: IO AsapoMessageMetaHandle
asapo_new_message_meta_handle = AsapoMessageMetaHandle <$> asapo_new_handle

asapo_new_message_data_handle :: IO AsapoMessageDataHandle
asapo_new_message_data_handle = AsapoMessageDataHandle <$> asapo_new_handle

foreign import capi "asapo/consumer_c.h asapo_free_handle__" asapo_free_handle :: Ptr (Ptr ()) -> IO ()

foreign import capi "asapo/consumer_c.h asapo_create_source_credentials" asapo_create_source_credentials :: CInt -> CString -> CString -> CString -> CString -> CString -> CString -> IO AsapoSourceCredentialsHandle

foreign import capi "asapo/consumer_c.h asapo_consumer_set_timeout" asapo_consumer_set_timeout :: AsapoConsumerHandle -> CULong -> IO ()

foreign import capi "asapo/consumer_c.h asapo_consumer_generate_new_group_id" asapo_consumer_generate_new_group_id :: AsapoConsumerHandle -> Ptr AsapoErrorHandle -> IO AsapoStringHandle

foreign import capi "asapo/consumer_c.h asapo_consumer_get_stream_list" asapo_consumer_get_stream_list :: AsapoConsumerHandle -> CString -> CInt -> Ptr AsapoErrorHandle -> IO AsapoStreamInfosHandle

foreign import capi "asapo/consumer_c.h asapo_stream_infos_get_size" asapo_stream_infos_get_size :: AsapoStreamInfosHandle -> IO CSize

foreign import capi "asapo/common/common_c.h asapo_stream_infos_get_item"
  asapo_stream_infos_get_item ::
    AsapoStreamInfosHandle ->
    -- index
    CSize ->
    IO AsapoStreamInfoHandle

foreign import capi "asapo/common/common_c.h asapo_stream_info_get_last_id" asapo_stream_info_get_last_id :: AsapoStreamInfoHandle -> IO CULong

foreign import capi "asapo/common/common_c.h asapo_stream_info_get_name" asapo_stream_info_get_name :: AsapoStreamInfoHandle -> IO ConstCString

-- Yes, this has a typo. Corrected in the Haskell function name
foreign import capi "asapo/common/common_c.h asapo_stream_info_get_ffinished" asapo_stream_info_get_finished :: AsapoStreamInfoHandle -> IO AsapoBool

foreign import capi "asapo/common/common_c.h asapo_stream_info_get_next_stream" asapo_stream_info_get_next_stream :: AsapoStreamInfoHandle -> IO ConstCString

foreign import capi "asapo/common/common_c.h asapo_stream_info_get_timestamp_created" asapo_stream_info_get_timestamp_created :: AsapoStreamInfoHandle -> Ptr TimeSpec -> IO ()

foreign import capi "asapo/common/common_c.h asapo_stream_info_get_timestamp_last_entry" asapo_stream_info_get_timestamp_last_entry :: AsapoStreamInfoHandle -> Ptr TimeSpec -> IO ()

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

foreign import capi "asapo/consumer_c.h asapo_consumer_get_next"
  asapo_consumer_get_next ::
    AsapoConsumerHandle ->
    -- group id
    AsapoStringHandle ->
    Ptr AsapoMessageMetaHandle ->
    Ptr AsapoMessageDataHandle ->
    -- stream
    CString ->
    Ptr AsapoErrorHandle ->
    IO CInt

foreign import capi "asapo/consumer_c.h asapo_message_meta_get_id"
  asapo_message_meta_get_id ::
    AsapoMessageMetaHandle ->
    IO CULong

-----------------------------------------------------------------------
-- producer

foreign import capi "asapo/producer_c.h asapo_create_producer"
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

foreign import capi "asapo/producer_c.h asapo_create_message_header"
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

foreign import capi "asapo/producer_c.h asapo_producer_send"
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

foreign import capi "asapo/producer_c.h asapo_producer_wait_requests_finished"
  asapo_producer_wait_requests_finished ::
    AsapoProducerHandle ->
    -- timeout ms
    CULong ->
    -- error
    Ptr AsapoErrorHandle ->
    IO CInt
