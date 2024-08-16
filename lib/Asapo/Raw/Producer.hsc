{-# LANGUAGE CApiFFI #-}
{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

-- hsc2hs needs the include to find the C structure
#include <asapo/producer_c.h>

module Asapo.Raw.Producer
  ( AsapoProducerHandle (AsapoProducerHandle),
    AsapoRequestCallbackPayloadHandle (AsapoRequestCallbackPayloadHandle),
    AsapoMessageHeaderHandle (AsapoMessageHeaderHandle),
    AsapoRequestCallback,
    createRequestCallback,
    kMaxMessageSize,
    asapo_free_producer_handle,
    kNCustomParams,
    kDefaultIngestMode,
    kMaxVersionSize,
    asapo_free_message_header_handle,
    AsapoRequestHandlerType,
    kTcp,
    kFilesystem,
    AsapoIngestModeFlags,
    kTransferData,
    AsapoOpcode,
    kOpcodeUnknownOp,
    kOpcodeTransferData,
    kOpcodeTransferDatasetData,
    kOpcodeStreamInfo,
    kOpcodeLastStream,
    kOpcodeGetBufferData,
    kOpcodeAuthorize,
    kOpcodeTransferMetaData,
    kOpcodeDeleteStream,
    kOpcodeGetMeta,
    kOpcodeCount,
    kOpcodePersistStream,
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
    AsapoGenericRequestHeader(..)
  )
where

import Data.Functor((<$>))
import Data.Word(Word64)
import Asapo.Raw.Common
  ( AsapoBool,
    AsapoErrorHandle (AsapoErrorHandle),
    AsapoSourceCredentialsHandle (AsapoSourceCredentialsHandle),
    AsapoStreamInfoHandle (AsapoStreamInfoHandle),
    AsapoStringHandle (AsapoStringHandle),
    ConstCString,
    asapo_free_handle
  )
import Foreign.C.ConstPtr (ConstPtr (ConstPtr))
import Foreign.C.String (CString)
import Foreign.C.Types (CInt (CInt), CSize (CSize), CUChar (CUChar))
import Foreign.Ptr (FunPtr, Ptr, plusPtr)
import Foreign (with, peekArray)
import Foreign.Storable (Storable(alignment, peek, peekByteOff, poke, sizeOf))
import System.IO (IO)
import Prelude (error, fromIntegral)
import Control.Applicative((<*>), pure)

newtype {-# CTYPE "asapo/producer_c.h" "AsapoProducerHandle" #-} AsapoProducerHandle = AsapoProducerHandle (Ptr ()) deriving (Storable)

asapo_free_producer_handle :: AsapoProducerHandle -> IO ()
asapo_free_producer_handle (AsapoProducerHandle ptr) = with ptr \ptr' -> asapo_free_handle ptr'

newtype {-# CTYPE "asapo/producer_c.h" "AsapoRequestCallbackPayloadHandle" #-} AsapoRequestCallbackPayloadHandle = AsapoRequestCallbackPayloadHandle (Ptr ()) deriving (Storable)

newtype {-# CTYPE "asapo/producer_c.h" "AsapoMessageHeaderHandle" #-} AsapoMessageHeaderHandle = AsapoMessageHeaderHandle (Ptr ()) deriving (Storable)

asapo_free_message_header_handle :: AsapoMessageHeaderHandle -> IO ()
asapo_free_message_header_handle (AsapoMessageHeaderHandle ptr) = with ptr \ptr' -> asapo_free_handle ptr'

type AsapoRequestCallback = Ptr () -> AsapoRequestCallbackPayloadHandle -> AsapoErrorHandle -> IO ()

foreign import ccall "wrapper" createRequestCallback :: AsapoRequestCallback -> IO (FunPtr AsapoRequestCallback)

foreign import capi "asapo/producer_c.h value kMaxMessageSize" kMaxMessageSize :: CSize

foreign import capi "asapo/producer_c.h value kMaxVersionSize" kMaxVersionSize :: CSize

foreign import capi "asapo/producer_c.h value kNCustomParams" kNCustomParams :: CSize

type AsapoOpcode = CInt

foreign import capi "asapo/producer_c.h value kOpcodeUnknownOp" kOpcodeUnknownOp :: AsapoOpcode

foreign import capi "asapo/producer_c.h value kOpcodeTransferData" kOpcodeTransferData :: AsapoOpcode

foreign import capi "asapo/producer_c.h value kOpcodeTransferDatasetData" kOpcodeTransferDatasetData :: AsapoOpcode

foreign import capi "asapo/producer_c.h value kOpcodeStreamInfo" kOpcodeStreamInfo :: AsapoOpcode

foreign import capi "asapo/producer_c.h value kOpcodeLastStream" kOpcodeLastStream :: AsapoOpcode

foreign import capi "asapo/producer_c.h value kOpcodeGetBufferData" kOpcodeGetBufferData :: AsapoOpcode

foreign import capi "asapo/producer_c.h value kOpcodeAuthorize" kOpcodeAuthorize :: AsapoOpcode

foreign import capi "asapo/producer_c.h value kOpcodeTransferMetaData" kOpcodeTransferMetaData :: AsapoOpcode

foreign import capi "asapo/producer_c.h value kOpcodeDeleteStream" kOpcodeDeleteStream :: AsapoOpcode

foreign import capi "asapo/producer_c.h value kOpcodeGetMeta" kOpcodeGetMeta :: AsapoOpcode

foreign import capi "asapo/producer_c.h value kOpcodeCount" kOpcodeCount :: AsapoOpcode

foreign import capi "asapo/producer_c.h value kOpcodePersistStream" kOpcodePersistStream :: AsapoOpcode

data AsapoGenericRequestHeader = AsapoGenericRequestHeader
  { asapoGenericRequestHeaderOpCode :: !AsapoOpcode,
    asapoGenericRequestHeaderDataId :: !Word64,
    asapoGenericRequestHeaderDataSize :: !Word64,
    asapoGenericRequestHeaderMetaSize :: !Word64,
    asapoGenericRequestHeaderCustomData :: ![Word64],
    asapoGenericRequestHeaderMessage :: !CString,
    asapoGenericRequestHeaderStream :: !CString,
    asapoGenericRequestHeaderApiVersion :: !CString
  }

instance Storable AsapoGenericRequestHeader where
  sizeOf _ = (# size struct AsapoGenericRequestHeader)
  alignment _ = (# alignment struct AsapoGenericRequestHeader)
  peek ptr = do
    customData <- peekArray (fromIntegral kNCustomParams) ((# ptr struct AsapoGenericRequestHeader, custom_data) ptr)
    opCode <- (# peek struct AsapoGenericRequestHeader, op_code) ptr
    dataId <- (# peek struct AsapoGenericRequestHeader, data_id) ptr
    dataSize <- (# peek struct AsapoGenericRequestHeader, data_size) ptr
    metaSize <- (# peek struct AsapoGenericRequestHeader, meta_size) ptr
    let message' = (# ptr struct AsapoGenericRequestHeader, message) ptr
    let stream' = (# ptr struct AsapoGenericRequestHeader, stream) ptr
    let apiVersion' = (# ptr struct AsapoGenericRequestHeader, api_version) ptr
    AsapoGenericRequestHeader
      <$> pure opCode
      <*> pure dataId
      <*> pure dataSize
      <*> pure metaSize
      <*> pure customData
      <*> pure message'
      <*> pure stream'
      <*> pure apiVersion'
  poke _ = error "why was AsapoGenericRequestHeader poked? it's supposed to be read-only"


type AsapoRequestHandlerType = CInt

foreign import capi "asapo/producer_c.h value kTcp" kTcp :: AsapoRequestHandlerType

foreign import capi "asapo/producer_c.h value kFilesystem" kFilesystem :: AsapoRequestHandlerType

type AsapoIngestModeFlags = CInt

foreign import capi "asapo/producer_c.h value kTransferData" kTransferData :: AsapoIngestModeFlags

foreign import capi "asapo/producer_c.h value kTransferMetaDataOnly" kTransferMetaDataOnly :: AsapoIngestModeFlags

foreign import capi "asapo/producer_c.h value kStoreInFilesystem" kStoreInFilesystem :: AsapoIngestModeFlags

foreign import capi "asapo/producer_c.h value kStoreInDatabase" kStoreInDatabase :: AsapoIngestModeFlags

foreign import capi "asapo/producer_c.h value kDefaultIngestMode" kDefaultIngestMode :: AsapoIngestModeFlags

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
    Word64 ->
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
    Word64 ->
    Ptr AsapoErrorHandle ->
    IO AsapoStreamInfoHandle

foreign import capi "asapo/producer_c.h asapo_producer_get_stream_meta"
  asapo_producer_get_stream_meta ::
    AsapoProducerHandle ->
    -- stream
    ConstCString ->
    -- timeout_ms
    Word64 ->
    Ptr AsapoErrorHandle ->
    IO AsapoStringHandle

foreign import capi "asapo/producer_c.h asapo_producer_get_beamtime_meta"
  asapo_producer_get_beamtime_meta ::
    AsapoProducerHandle ->
    -- timeout_ms
    Word64 ->
    Ptr AsapoErrorHandle ->
    IO AsapoStringHandle

foreign import capi "asapo/producer_c.h asapo_producer_delete_stream"
  asapo_producer_delete_stream ::
    AsapoProducerHandle ->
    -- stream
    ConstCString ->
    -- timeout_ms
    Word64 ->
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
    Word64 ->
    Ptr AsapoErrorHandle ->
    IO AsapoStreamInfoHandle

foreign import capi "asapo/producer_c.h asapo_create_message_header"
  asapo_create_message_header ::
    -- message ID
    Word64 ->
    -- data size
    Word64 ->
    -- file name
    ConstCString ->
    -- user metadata
    ConstCString ->
    -- dataset substream
    Word64 ->
    -- dataset size
    Word64 ->
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
    Word64 ->
    -- stream
    ConstCString ->
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
    Word64 ->
    -- stream
    ConstCString ->
    FunPtr AsapoRequestCallback ->
    Ptr AsapoErrorHandle ->
    IO CInt

foreign import capi "asapo/producer_c.h asapo_producer_send_stream_finished_flag"
  asapo_producer_send_stream_finished_flag ::
    AsapoProducerHandle ->
    -- stream
    ConstCString ->
    -- last ID
    Word64 ->
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
  asapo_producer_get_requests_queue_size :: AsapoProducerHandle -> IO Word64

foreign import capi "asapo/producer_c.h asapo_producer_get_requests_queue_volume_mb"
  asapo_producer_get_requests_queue_volume_mb :: AsapoProducerHandle -> IO Word64

foreign import capi "asapo/producer_c.h asapo_producer_set_requests_queue_limits"
  asapo_producer_set_requests_queue_limits ::
    AsapoProducerHandle ->
    -- size
    Word64 ->
    -- volume
    Word64 ->
    IO ()

foreign import capi "asapo/producer_c.h asapo_producer_wait_requests_finished"
  asapo_producer_wait_requests_finished ::
    AsapoProducerHandle ->
    -- timeout ms
    Word64 ->
    -- error
    Ptr AsapoErrorHandle ->
    IO CInt
