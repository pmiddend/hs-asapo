{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE CApiFFI #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}

{-# HLINT ignore "Use camelCase" #-}

module Asapo.Raw.Common
  ( AsapoBool,
    AsapoSourceCredentialsHandle (AsapoSourceCredentialsHandle),
    AsapoMessageDataHandle (AsapoMessageDataHandle),
    asapo_is_error,
    asapo_string_c_str,
    mkAsapoFreeWrapper,
    asapo_message_data_get_as_chars,
    asapo_free_stream_infos_handle,
    asapo_new_string_handle,
    asapo_free_string_handle,
    asapo_free_stream_info_handle,
    asapo_free_message_data_handle,
    asapo_error_explain,
    asapo_string_size,
    AsapoErrorHandle (AsapoErrorHandle),
    AsapoStreamInfosHandle (AsapoStreamInfosHandle),
    AsapoStringHandle (AsapoStringHandle),
    ConstCString,
    asapo_free_source_credentials,
    asapo_new_error_handle,
    asapo_free_error_handle,
    asapo_string_from_c_str,
    asapo_new_handle,
    asapo_free_handle,
    asapo_new_message_data_handle,
    asapo_create_source_credentials,
    AsapoStreamInfoHandle (AsapoStreamInfoHandle),
    asapo_stream_infos_get_item,
    asapo_stream_info_get_last_id,
    asapo_stream_info_get_name,
    asapo_stream_info_get_finished,
    asapo_stream_info_get_next_stream,
    asapo_stream_info_get_timestamp_created,
    asapo_stream_info_get_timestamp_last_entry,
    kProcessed,
    kRaw,
  )
where

import Data.Functor ((<$>))
import Foreign (FunPtr, with)
import Foreign.C.ConstPtr (ConstPtr (ConstPtr))
import Foreign.C.String (CString)
import Foreign.C.Types (CChar, CInt (CInt), CSize (CSize), CULong (CULong))
import Foreign.Ptr (Ptr)
import Foreign.Storable (Storable)
import System.Clock (TimeSpec)
import System.IO (IO)
import Prelude ()

-- common
type ConstCString = ConstPtr CChar

type AsapoBool = CInt

newtype {-# CTYPE "asapo/common/common_c.h" "AsapoSourceCredentialsHandle" #-} AsapoSourceCredentialsHandle = AsapoSourceCredentialsHandle (Ptr ()) deriving (Storable)

newtype {-# CTYPE "asapo/common/common_c.h" "AsapoErrorHandle" #-} AsapoErrorHandle = AsapoErrorHandle (Ptr ()) deriving (Storable)

newtype {-# CTYPE "asapo/common/common_c.h" "AsapoStringHandle" #-} AsapoStringHandle = AsapoStringHandle (Ptr ()) deriving (Storable)

asapo_new_string_handle :: IO AsapoStringHandle
asapo_new_string_handle = AsapoStringHandle <$> asapo_new_handle

asapo_free_string_handle :: AsapoStringHandle -> IO ()
asapo_free_string_handle (AsapoStringHandle ptr) = with ptr asapo_free_handle

-- -- FIXME: this is a custom function I added
-- -- foreign import capi "asapo/common/common_c.h &asapo_free_handle___" p_asapo_free_handle :: FunPtr (Ptr () -> IO ())

-- foreign import capi "asapo/common/common_c.h &hs_asapo_free_handle_with_ptr___" p_asapo_free_handle :: FunPtr (Ptr () -> IO ())

newtype {-# CTYPE "asapo/common/common_c.h" "AsapoStreamInfoHandle" #-} AsapoStreamInfoHandle = AsapoStreamInfoHandle (Ptr ()) deriving (Storable)

asapo_free_stream_info_handle :: AsapoStreamInfoHandle -> IO ()
asapo_free_stream_info_handle (AsapoStreamInfoHandle ptr) = with ptr asapo_free_handle

asapo_free_stream_infos_handle :: AsapoStreamInfosHandle -> IO ()
asapo_free_stream_infos_handle (AsapoStreamInfosHandle ptr) = with ptr asapo_free_handle

newtype {-# CTYPE "asapo/common/common_c.h" "AsapoStreamInfosHandle" #-} AsapoStreamInfosHandle = AsapoStreamInfosHandle (Ptr ()) deriving (Storable)

newtype {-# CTYPE "asapo/consumer_c.h" "AsapoMessageDataHandle" #-} AsapoMessageDataHandle = AsapoMessageDataHandle (Ptr ()) deriving (Storable)

asapo_new_message_data_handle :: IO AsapoMessageDataHandle
asapo_new_message_data_handle = AsapoMessageDataHandle <$> asapo_new_handle

asapo_free_message_data_handle :: AsapoMessageDataHandle -> IO ()
asapo_free_message_data_handle (AsapoMessageDataHandle ptr) = with ptr asapo_free_handle

type AsapoSourceType = CInt

foreign import capi "asapo/common/common_c.h value kProcessed" kProcessed :: AsapoSourceType

foreign import capi "asapo/common/common_c.h value kRaw" kRaw :: AsapoSourceType

foreign import capi "asapo/common/common_c.h asapo_free_handle__" asapo_free_handle :: Ptr (Ptr ()) -> IO ()

foreign import ccall "wrapper" mkAsapoFreeWrapper :: (Ptr () -> IO ()) -> IO (FunPtr (Ptr () -> IO ()))

foreign import capi "asapo/common/common_c.h asapo_new_handle" asapo_new_handle :: IO (Ptr ())

asapo_new_error_handle :: IO AsapoErrorHandle
asapo_new_error_handle = AsapoErrorHandle <$> asapo_new_handle

asapo_free_error_handle :: AsapoErrorHandle -> IO ()
asapo_free_error_handle (AsapoErrorHandle ptr) = with ptr asapo_free_handle

foreign import capi "asapo/common/common_c.h asapo_error_explain" asapo_error_explain :: AsapoErrorHandle -> CString -> CSize -> IO ()

foreign import capi "asapo/common/common_c.h asapo_is_error" asapo_is_error :: AsapoErrorHandle -> IO AsapoBool

foreign import capi "asapo/common/common_c.h asapo_string_from_c_str" asapo_string_from_c_str :: ConstCString -> IO AsapoStringHandle

foreign import capi "asapo/common/common_c.h asapo_string_c_str" asapo_string_c_str :: AsapoStringHandle -> IO ConstCString

foreign import capi "asapo/common/common_c.h asapo_string_size" asapo_string_size :: AsapoStringHandle -> IO CSize

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

foreign import capi "asapo/common/common_c.h asapo_create_source_credentials"
  asapo_create_source_credentials ::
    AsapoSourceType ->
    -- instance ID
    CString ->
    -- pipeline step
    CString ->
    -- beamtime
    CString ->
    -- beamline
    CString ->
    -- data source
    CString ->
    -- token
    CString ->
    IO AsapoSourceCredentialsHandle

asapo_free_source_credentials :: AsapoSourceCredentialsHandle -> IO ()
asapo_free_source_credentials (AsapoSourceCredentialsHandle ptr) = with ptr asapo_free_handle

foreign import capi "asapo/common/common_c.h asapo_message_data_get_as_chars" asapo_message_data_get_as_chars :: AsapoMessageDataHandle -> IO ConstCString
