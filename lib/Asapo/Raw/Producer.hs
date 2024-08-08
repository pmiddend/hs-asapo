{-# LANGUAGE CApiFFI #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Asapo.Raw.Producer
  ( AsapoProducerHandle (AsapoProducerHandle),
    AsapoRequestCallbackPayloadHandle (AsapoRequestCallbackPayloadHandle),
    AsapoMessageHeaderHandle (AsapoMessageHeaderHandle),
    AsapoRequestCallback,
    createRequestCallback,
    kMaxMessageSize,
    kMaxVersionSize,
    kNCustomParams,
    AsapoRequestHandlerType,
    kTcp,
    kFilesystem,
    AsapoIngestModeFlags,
    kTransferData,
    kTransferMetaDataOnly,
    kStoreInFilesystem,
    kStoreInDatabase,
    AsapoMetaIngestOp,
    kInsert,
    kReplace,
    kUpdate,
    AsapoLogLevel,
    asapoLogLevelNone,
    asapoLogLevelError,
    asapoLogLevelInfo,
    asapoLogLevelDebug,
    asapoLogLevelWarning,
    asapo_create_producer,
    asapo_producer_get_version_info,
    asapo_producer_get_stream_info,
    asapo_producer_get_stream_meta,
    asapo_producer_get_beamtime_meta,
    asapo_producer_delete_stream,
    asapo_producer_get_last_stream,
    asapo_create_message_header,
    asapo_producer_send,
    asapo_producer_send_file,
    asapo_producer_send_stream_finished_flag,
    asapo_producer_send_beamtime_metadata,
    asapo_producer_send_stream_metadata,
    asapo_request_callback_payload_get_response,
    asapo_request_callback_payload_get_original_header,
    asapo_producer_set_log_level,
    asapo_producer_enable_local_log,
    asapo_producer_enable_remote_log,
    asapo_producer_set_credentials,
    asapo_producer_get_requests_queue_size,
    asapo_producer_get_requests_queue_volume_mb,
    asapo_producer_set_requests_queue_limits,
    asapo_producer_wait_requests_finished,
  )
where

import Asapo.Raw.AsapoGenericRequestHeader
  ( AsapoGenericRequestHeader,
  )
import Asapo.Raw.Common
  ( AsapoBool,
    AsapoErrorHandle (AsapoErrorHandle),
    AsapoSourceCredentialsHandle (AsapoSourceCredentialsHandle),
    AsapoStreamInfoHandle (AsapoStreamInfoHandle),
    AsapoStringHandle (AsapoStringHandle),
    ConstCString,
  )
import Foreign.C.ConstPtr (ConstPtr (ConstPtr))
import Foreign.C.String (CString)
import Foreign.C.Types (CInt (CInt), CSize (CSize), CUChar (CUChar), CULong (CULong))
import Foreign.Ptr (FunPtr, Ptr)
import Foreign.Storable (Storable)
import System.IO (IO)
import Prelude ()

newtype {-# CTYPE "asapo/producer_c.h" "AsapoProducerHandle" #-} AsapoProducerHandle = AsapoProducerHandle (Ptr ()) deriving (Storable)

newtype {-# CTYPE "asapo/producer_c.h" "AsapoRequestCallbackPayloadHandle" #-} AsapoRequestCallbackPayloadHandle = AsapoRequestCallbackPayloadHandle (Ptr ()) deriving (Storable)

newtype {-# CTYPE "asapo/producer_c.h" "AsapoMessageHeaderHandle" #-} AsapoMessageHeaderHandle = AsapoMessageHeaderHandle (Ptr ()) deriving (Storable)

type AsapoRequestCallback = Ptr () -> AsapoRequestCallbackPayloadHandle -> AsapoErrorHandle -> IO ()

foreign import ccall "wrapper" createRequestCallback :: AsapoRequestCallback -> IO (FunPtr AsapoRequestCallback)

foreign import capi "asapo/producer_c.h value kMaxMessageSize" kMaxMessageSize :: CSize

foreign import capi "asapo/producer_c.h value kMaxVersionSize" kMaxVersionSize :: CSize

foreign import capi "asapo/producer_c.h value kNCustomParams" kNCustomParams :: CSize

type AsapoRequestHandlerType = CInt

foreign import capi "asapo/producer_c.h value kTcp" kTcp :: AsapoRequestHandlerType

foreign import capi "asapo/producer_c.h value kFilesystem" kFilesystem :: AsapoRequestHandlerType

type AsapoIngestModeFlags = CInt

foreign import capi "asapo/producer_c.h value kTransferData" kTransferData :: AsapoIngestModeFlags

foreign import capi "asapo/producer_c.h value kTransferMetaDataOnly" kTransferMetaDataOnly :: AsapoIngestModeFlags

foreign import capi "asapo/producer_c.h value kStoreInFilesystem" kStoreInFilesystem :: AsapoIngestModeFlags

foreign import capi "asapo/producer_c.h value kStoreInDatabase" kStoreInDatabase :: AsapoIngestModeFlags

-- foreign import capi "asapo/producer_c.h value kDefaultIngestMode" kDefaultIngestMode :: AsapoIngestModeFlags

type AsapoMetaIngestOp = CInt

foreign import capi "asapo/producer_c.h value kInsert" kInsert :: AsapoMetaIngestOp

foreign import capi "asapo/producer_c.h value kReplace" kReplace :: AsapoMetaIngestOp

foreign import capi "asapo/producer_c.h value kUpdate" kUpdate :: AsapoMetaIngestOp

type AsapoLogLevel = CInt

foreign import capi "asapo/producer_c.h value None" asapoLogLevelNone :: AsapoLogLevel

foreign import capi "asapo/producer_c.h value Error" asapoLogLevelError :: AsapoLogLevel

foreign import capi "asapo/producer_c.h value Info" asapoLogLevelInfo :: AsapoLogLevel

foreign import capi "asapo/producer_c.h value Debug" asapoLogLevelDebug :: AsapoLogLevel

foreign import capi "asapo/producer_c.h value Warning" asapoLogLevelWarning :: AsapoLogLevel

foreign import capi "asapo/producer_c.h asapo_create_producer"
  asapo_create_producer ::
    -- endpoint
    CString ->
    -- processing threads
    CUChar ->
    AsapoRequestHandlerType ->
    AsapoSourceCredentialsHandle ->
    -- timeout_ms
    CULong ->
    Ptr AsapoErrorHandle ->
    IO AsapoProducerHandle

foreign import capi "asapo/producer_c.h asapo_producer_get_version_info"
  asapo_producer_get_version_info ::
    AsapoProducerHandle ->
    -- client info
    AsapoStringHandle ->
    -- server info
    AsapoStringHandle ->
    -- supported
    Ptr AsapoBool ->
    Ptr AsapoErrorHandle ->
    IO CInt

foreign import capi "asapo/producer_c.h asapo_producer_get_stream_info"
  asapo_producer_get_stream_info ::
    AsapoProducerHandle ->
    -- stream
    ConstCString ->
    -- timeout ms
    CULong ->
    Ptr AsapoErrorHandle ->
    IO AsapoStreamInfoHandle

foreign import capi "asapo/producer_c.h asapo_producer_get_stream_meta"
  asapo_producer_get_stream_meta ::
    AsapoProducerHandle ->
    -- stream
    ConstCString ->
    -- timeout_ms
    CULong ->
    Ptr AsapoErrorHandle ->
    IO AsapoStringHandle

foreign import capi "asapo/producer_c.h asapo_producer_get_beamtime_meta"
  asapo_producer_get_beamtime_meta ::
    AsapoProducerHandle ->
    -- timeout_ms
    CULong ->
    Ptr AsapoErrorHandle ->
    IO AsapoStringHandle

foreign import capi "asapo/producer_c.h asapo_producer_delete_stream"
  asapo_producer_delete_stream ::
    AsapoProducerHandle ->
    -- stream
    ConstCString ->
    -- timeout_ms
    CULong ->
    -- delete meta
    AsapoBool ->
    -- error_on_not_exist
    AsapoBool ->
    Ptr AsapoErrorHandle ->
    IO CInt

foreign import capi "asapo/producer_c.h asapo_producer_get_last_stream"
  asapo_producer_get_last_stream ::
    AsapoProducerHandle ->
    -- timeout ms
    CULong ->
    Ptr AsapoErrorHandle ->
    IO AsapoStreamInfoHandle

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

foreign import capi "asapo/producer_c.h asapo_producer_send_file"
  asapo_producer_send_file ::
    AsapoProducerHandle ->
    AsapoMessageHeaderHandle ->
    -- file name
    ConstCString ->
    -- ingest mode
    CULong ->
    -- stream
    CString ->
    FunPtr AsapoRequestCallback ->
    Ptr AsapoErrorHandle ->
    IO CInt

foreign import capi "asapo/producer_c.h asapo_producer_send_stream_finished_flag"
  asapo_producer_send_stream_finished_flag ::
    AsapoProducerHandle ->
    -- stream
    ConstCString ->
    -- last ID
    CULong ->
    -- next stream
    ConstCString ->
    FunPtr AsapoRequestCallback ->
    Ptr AsapoErrorHandle ->
    IO CInt

foreign import capi "asapo/producer_c.h asapo_producer_send_beamtime_metadata"
  asapo_producer_send_beamtime_metadata ::
    AsapoProducerHandle ->
    -- metadata
    ConstCString ->
    AsapoMetaIngestOp ->
    -- upsert
    AsapoBool ->
    FunPtr AsapoRequestCallback ->
    Ptr AsapoErrorHandle ->
    IO CInt

foreign import capi "asapo/producer_c.h asapo_producer_send_stream_metadata"
  asapo_producer_send_stream_metadata ::
    AsapoProducerHandle ->
    -- metadata
    ConstCString ->
    AsapoMetaIngestOp ->
    -- upsert
    AsapoBool ->
    -- stream
    ConstCString ->
    FunPtr AsapoRequestCallback ->
    Ptr AsapoErrorHandle ->
    IO CInt

foreign import capi "asapo/producer_c.h asapo_request_callback_payload_get_response"
  asapo_request_callback_payload_get_response :: AsapoRequestCallbackPayloadHandle -> IO AsapoStringHandle

foreign import capi "asapo/producer_c.h asapo_request_callback_payload_get_original_header"
  asapo_request_callback_payload_get_original_header :: AsapoRequestCallbackPayloadHandle -> IO (ConstPtr AsapoGenericRequestHeader)

foreign import capi "asapo/producer_c.h asapo_producer_set_log_level"
  asapo_producer_set_log_level :: AsapoProducerHandle -> AsapoLogLevel -> IO ()

foreign import capi "asapo/producer_c.h asapo_producer_enable_local_log"
  asapo_producer_enable_local_log :: AsapoProducerHandle -> AsapoBool -> IO ()

foreign import capi "asapo/producer_c.h asapo_producer_enable_remote_log"
  asapo_producer_enable_remote_log :: AsapoProducerHandle -> AsapoBool -> IO ()

foreign import capi "asapo/producer_c.h asapo_producer_set_credentials"
  asapo_producer_set_credentials ::
    AsapoProducerHandle ->
    AsapoSourceCredentialsHandle ->
    Ptr AsapoErrorHandle ->
    IO CInt

foreign import capi "asapo/producer_c.h asapo_producer_get_requests_queue_size"
  asapo_producer_get_requests_queue_size :: AsapoProducerHandle -> IO CULong

foreign import capi "asapo/producer_c.h asapo_producer_get_requests_queue_volume_mb"
  asapo_producer_get_requests_queue_volume_mb :: AsapoProducerHandle -> IO CULong

foreign import capi "asapo/producer_c.h asapo_producer_set_requests_queue_limits"
  asapo_producer_set_requests_queue_limits ::
    AsapoProducerHandle ->
    -- size
    CULong ->
    -- volume
    CULong ->
    IO ()

foreign import capi "asapo/producer_c.h asapo_producer_wait_requests_finished"
  asapo_producer_wait_requests_finished ::
    AsapoProducerHandle ->
    -- timeout ms
    CULong ->
    -- error
    Ptr AsapoErrorHandle ->
    IO CInt
