{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CApiFFI #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Asapo.Raw.Consumer where

import Asapo.Raw.Common
import Data.Functor ((<$>))
import Foreign.C.ConstPtr (ConstPtr (ConstPtr))
import Foreign.C.String (CString)
import Foreign.C.Types (CInt (CInt), CLong (CLong), CSize (CSize), CULong (CULong))
import Foreign.Ptr (Ptr)
import Foreign.Storable (Storable)
import System.Clock (TimeSpec)
import System.IO (IO)
import Prelude ()

foreign import capi "asapo/consumer_c.h asapo_stream_infos_get_size" asapo_stream_infos_get_size :: AsapoStreamInfosHandle -> IO CSize

newtype {-# CTYPE "asapo/consumer_c.h" "AsapoConsumerHandle" #-} AsapoConsumerHandle = AsapoConsumerHandle (Ptr ()) deriving (Storable)

newtype {-# CTYPE "asapo/consumer_c.h" "AsapoMessageMetaHandle" #-} AsapoMessageMetaHandle = AsapoMessageMetaHandle (Ptr ()) deriving (Storable)

newtype {-# CTYPE "asapo/consumer_c.h" "AsapoMessageMetasHandle" #-} AsapoMessageMetasHandle = AsapoMessageMetasHandle (Ptr ()) deriving (Storable)

asapo_new_message_meta_handle :: IO AsapoMessageMetaHandle
asapo_new_message_meta_handle = AsapoMessageMetaHandle <$> asapo_new_handle

newtype {-# CTYPE "asapo/consumer_c.h" "AsapoIdListHandle" #-} AsapoIdListHandle = AsapoIdListHandle (Ptr ()) deriving (Storable)

newtype {-# CTYPE "asapo/consumer_c.h" "AsapoDataSetHandle" #-} AsapoDataSetHandle = AsapoDataSetHandle (Ptr ()) deriving (Storable)

newtype {-# CTYPE "asapo/consumer_c.h" "AsapoPartialErrorDataHandle" #-} AsapoPartialErrorDataHandle = AsapoPartialErrorDataHandle (Ptr ()) deriving (Storable)

newtype {-# CTYPE "asapo/consumer_c.h" "AsapoConsumerErrorDataHandle" #-} AsapoConsumerErrorDataHandle = AsapoConsumerErrorDataHandle (Ptr ()) deriving (Storable)

type AsapoConsumerErrorType = CInt

foreign import capi "asapo/consumer_c.h value kNoData" kNoData :: AsapoConsumerErrorType

foreign import capi "asapo/consumer_c.h value kEndOfStream" kEndOfStream :: AsapoConsumerErrorType

foreign import capi "asapo/consumer_c.h value kStreamFinished" kStreamFinished :: AsapoConsumerErrorType

foreign import capi "asapo/consumer_c.h value kUnavailableService" kUnavailableService :: AsapoConsumerErrorType

foreign import capi "asapo/consumer_c.h value kInterruptedTransaction" kInterruptedTransaction :: AsapoConsumerErrorType

foreign import capi "asapo/consumer_c.h value kLocalIOError" kLocalIOError :: AsapoConsumerErrorType

foreign import capi "asapo/consumer_c.h value kWrongInput" kWrongInput :: AsapoConsumerErrorType

foreign import capi "asapo/consumer_c.h value kPartialData" kPartialData :: AsapoConsumerErrorType

foreign import capi "asapo/consumer_c.h value kUnsupportedClient" kUnsupportedClient :: AsapoConsumerErrorType

foreign import capi "asapo/consumer_c.h value kDataNotInCache" kDataNotInCache :: AsapoConsumerErrorType

foreign import capi "asapo/consumer_c.h value kUnknownError" kUnknownError :: AsapoConsumerErrorType

type AsapoStreamFilter = CInt

foreign import capi "asapo/consumer_c.h value kAllStreams" kAllStreams :: AsapoStreamFilter

foreign import capi "asapo/consumer_c.h value kFinishedStreams" kFinishedStreams :: AsapoStreamFilter

foreign import capi "asapo/consumer_c.h value kUnfinishedStreams" kUnfinishedStreams :: AsapoStreamFilter

type AsapoNetworkConnectionType = CInt

foreign import capi "asapo/consumer_c.h value kUndefined" kUndefined :: AsapoNetworkConnectionType

foreign import capi "asapo/consumer_c.h value kAsapoTcp" kAsapoTcp :: AsapoNetworkConnectionType

foreign import capi "asapo/consumer_c.h value kFabric" kFabric :: AsapoNetworkConnectionType

foreign import capi "asapo/consumer_c.h asapo_error_get_type" asapo_error_get_type :: AsapoErrorHandle -> IO AsapoConsumerErrorType

foreign import capi "asapo/consumer_c.h asapo_create_consumer"
  asapo_create_consumer ::
    -- server_name
    CString ->
    -- source_path
    CString ->
    -- has filesystem
    AsapoBool ->
    AsapoSourceCredentialsHandle ->
    Ptr AsapoErrorHandle ->
    IO AsapoConsumerHandle

foreign import capi "asapo/consumer_c.h asapo_consumer_generate_new_group_id" asapo_consumer_generate_new_group_id :: AsapoConsumerHandle -> Ptr AsapoErrorHandle -> IO AsapoStringHandle

foreign import capi "asapo/consumer_c.h asapo_consumer_set_timeout" asapo_consumer_set_timeout :: AsapoConsumerHandle -> CULong -> IO ()

foreign import capi "asapo/consumer_c.h asapo_consumer_reset_last_read_marker"
  asapo_consumer_reset_last_read_marker ::
    AsapoConsumerHandle ->
    -- group_id
    AsapoStringHandle ->
    -- stream
    ConstCString ->
    Ptr AsapoErrorHandle ->
    IO CInt

foreign import capi "asapo/consumer_c.h asapo_consumer_set_last_read_marker"
  asapo_consumer_set_last_read_marker ::
    AsapoConsumerHandle ->
    -- group_id
    AsapoStringHandle ->
    -- value
    CULong ->
    -- stream
    ConstCString ->
    Ptr AsapoErrorHandle ->
    IO CInt

foreign import capi "asapo/consumer_c.h asapo_consumer_acknowledge"
  asapo_consumer_acknowledge ::
    AsapoConsumerHandle ->
    -- group_id
    AsapoStringHandle ->
    -- id
    CULong ->
    -- stream
    ConstCString ->
    Ptr AsapoErrorHandle ->
    IO CInt

foreign import capi "asapo/consumer_c.h asapo_consumer_negative_acknowledge"
  asapo_consumer_negative_acknowledge ::
    AsapoConsumerHandle ->
    -- group_id
    AsapoStringHandle ->
    -- id
    CULong ->
    -- delay_ms
    CULong ->
    -- stream
    ConstCString ->
    Ptr AsapoErrorHandle ->
    IO CInt

foreign import capi "asapo/consumer_c.h asapo_consumer_get_unacknowledged_messages"
  asapo_consumer_get_unacknowledged_messages ::
    AsapoConsumerHandle ->
    -- group_id
    AsapoStringHandle ->
    -- from id
    CULong ->
    -- to id
    CULong ->
    -- stream
    ConstCString ->
    Ptr AsapoErrorHandle ->
    IO AsapoIdListHandle

foreign import capi "asapo/consumer_c.h asapo_id_list_get_size" asapo_id_list_get_size :: AsapoIdListHandle -> IO CSize

foreign import capi "asapo/consumer_c.h asapo_id_list_get_item" asapo_id_list_get_item :: AsapoIdListHandle -> CSize -> IO CULong

foreign import capi "asapo/consumer_c.h asapo_consumer_get_last_acknowledged_message"
  asapo_consumer_get_last_acknowledged_message ::
    AsapoConsumerHandle ->
    -- group_id
    AsapoStringHandle ->
    -- stream
    ConstCString ->
    Ptr AsapoErrorHandle ->
    IO CLong

-- foreign import capi "asapo/consumer_c.h asapo_consumer_force_no_rdma" asapo_consumer_force_no_rdma :: AsapoConsumerHandle -> IO ()

foreign import capi "asapo/consumer_c.h asapo_consumer_current_connection_type" asapo_consumer_current_connection_type :: AsapoConsumerHandle -> IO AsapoNetworkConnectionType

-- Again, a typo in the original (no "asapo_" prefix, but corrected in the Haskell code)
-- foreign import capi "asapo/consumer_c.h enable_new_monitoring_api_format" asapo_enable_new_monitoring_api_format :: AsapoConsumerHandle -> AsapoBool -> Ptr AsapoErrorHandle -> IO CInt

foreign import capi "asapo/consumer_c.h asapo_consumer_get_stream_list"
  asapo_consumer_get_stream_list ::
    AsapoConsumerHandle ->
    -- from
    ConstCString ->
    AsapoStreamFilter ->
    Ptr AsapoErrorHandle ->
    IO AsapoStreamInfosHandle

foreign import capi "asapo/consumer_c.h asapo_consumer_delete_stream"
  asapo_consumer_delete_stream ::
    AsapoConsumerHandle ->
    -- stream
    ConstCString ->
    -- delete_meta
    AsapoBool ->
    -- error_on_not_exist
    AsapoBool ->
    Ptr AsapoErrorHandle ->
    IO CInt

foreign import capi "asapo/consumer_c.h asapo_consumer_set_stream_persistent"
  asapo_consumer_set_stream_persistent ::
    AsapoConsumerHandle ->
    -- stream
    ConstCString ->
    Ptr AsapoErrorHandle ->
    IO CInt

foreign import capi "asapo/consumer_c.h asapo_consumer_get_current_size"
  asapo_consumer_get_current_size ::
    AsapoConsumerHandle ->
    -- stream
    ConstCString ->
    Ptr AsapoErrorHandle ->
    IO CLong

foreign import capi "asapo/consumer_c.h asapo_consumer_get_current_dataset_count"
  asapo_consumer_get_current_dataset_count ::
    AsapoConsumerHandle ->
    -- stream
    ConstCString ->
    -- include_incomplete
    AsapoBool ->
    Ptr AsapoErrorHandle ->
    IO CLong

foreign import capi "asapo/consumer_c.h asapo_consumer_get_beamtime_meta" asapo_consumer_get_beamtime_meta :: AsapoConsumerHandle -> Ptr AsapoErrorHandle -> IO AsapoStringHandle

foreign import capi "asapo/consumer_c.h asapo_consumer_retrieve_data" asapo_consumer_retrieve_data :: AsapoConsumerHandle -> AsapoMessageMetaHandle -> Ptr AsapoMessageDataHandle -> Ptr AsapoErrorHandle -> IO CInt

foreign import capi "asapo/consumer_c.h asapo_consumer_get_next_dataset"
  asapo_consumer_get_next_dataset ::
    AsapoConsumerHandle ->
    -- group_id
    AsapoStringHandle ->
    -- min_size
    CULong ->
    -- stream
    ConstCString ->
    Ptr AsapoErrorHandle ->
    IO AsapoDataSetHandle

foreign import capi "asapo/consumer_c.h asapo_consumer_get_last_dataset"
  asapo_consumer_get_last_dataset ::
    AsapoConsumerHandle ->
    -- min_size
    CULong ->
    -- stream
    ConstCString ->
    Ptr AsapoErrorHandle ->
    IO AsapoDataSetHandle

foreign import capi "asapo/consumer_c.h asapo_consumer_get_last_dataset_ingroup"
  asapo_consumer_get_last_dataset_ingroup ::
    AsapoConsumerHandle ->
    -- group_id
    AsapoStringHandle ->
    -- min_size
    CULong ->
    -- stream
    ConstCString ->
    Ptr AsapoErrorHandle ->
    IO AsapoDataSetHandle

foreign import capi "asapo/consumer_c.h asapo_consumer_get_by_id"
  asapo_consumer_get_by_id ::
    AsapoConsumerHandle ->
    -- id
    CULong ->
    Ptr AsapoMessageMetaHandle ->
    Ptr AsapoMessageDataHandle ->
    -- stream
    ConstCString ->
    Ptr AsapoErrorHandle ->
    IO CInt

foreign import capi "asapo/consumer_c.h asapo_consumer_get_last"
  asapo_consumer_get_last ::
    AsapoConsumerHandle ->
    Ptr AsapoMessageMetaHandle ->
    Ptr AsapoMessageDataHandle ->
    -- stream
    ConstCString ->
    Ptr AsapoErrorHandle ->
    IO CInt

foreign import capi "asapo/consumer_c.h asapo_consumer_get_last_ingroup"
  asapo_consumer_get_last_ingroup ::
    AsapoConsumerHandle ->
    -- group_id
    AsapoStringHandle ->
    Ptr AsapoMessageMetaHandle ->
    Ptr AsapoMessageDataHandle ->
    -- stream
    ConstCString ->
    Ptr AsapoErrorHandle ->
    IO CInt

foreign import capi "asapo/consumer_c.h asapo_consumer_get_next"
  asapo_consumer_get_next ::
    AsapoConsumerHandle ->
    -- group id
    AsapoStringHandle ->
    Ptr AsapoMessageMetaHandle ->
    Ptr AsapoMessageDataHandle ->
    -- stream
    ConstCString ->
    Ptr AsapoErrorHandle ->
    IO CInt

foreign import capi "asapo/consumer_c.h asapo_consumer_query_messages"
  asapo_consumer_query_messages ::
    AsapoConsumerHandle ->
    -- query
    ConstCString ->
    -- stream
    ConstCString ->
    Ptr AsapoErrorHandle ->
    IO AsapoMessageMetasHandle

foreign import capi "asapo/consumer_c.h asapo_consumer_set_resend_nacs"
  asapo_consumer_set_resend_nacs ::
    AsapoConsumerHandle ->
    -- resend
    AsapoBool ->
    -- delay_ms
    CULong ->
    -- resend_attempts
    CULong ->
    IO ()

foreign import capi "asapo/consumer_c.h asapo_message_data_get_as_chars" asapo_message_data_get_as_chars :: AsapoMessageDataHandle -> IO ConstCString

foreign import capi "asapo/consumer_c.h asapo_message_meta_get_name" asapo_message_meta_get_name :: AsapoMessageMetaHandle -> IO ConstCString

foreign import capi "asapo/consumer_c.h asapo_message_meta_get_timestamp" asapo_message_meta_get_timestamp :: AsapoMessageMetaHandle -> Ptr TimeSpec -> IO ()

foreign import capi "asapo/consumer_c.h asapo_message_meta_get_size" asapo_message_meta_get_size :: AsapoMessageMetaHandle -> IO CULong

foreign import capi "asapo/consumer_c.h asapo_message_meta_get_id" asapo_message_meta_get_id :: AsapoMessageMetaHandle -> IO CULong

foreign import capi "asapo/consumer_c.h asapo_message_meta_get_source" asapo_message_meta_get_source :: AsapoMessageMetaHandle -> IO ConstCString

foreign import capi "asapo/consumer_c.h asapo_message_meta_get_metadata" asapo_message_meta_get_metadata :: AsapoMessageMetaHandle -> IO ConstCString

foreign import capi "asapo/consumer_c.h asapo_message_meta_get_buf_id" asapo_message_meta_get_buf_id :: AsapoMessageMetaHandle -> IO CULong

foreign import capi "asapo/consumer_c.h asapo_message_meta_get_dataset_substream" asapo_message_meta_get_dataset_substream :: AsapoMessageMetaHandle -> IO CULong

foreign import capi "asapo/consumer_c.h asapo_dataset_get_id" asapo_dataset_get_id :: AsapoDataSetHandle -> IO CULong

foreign import capi "asapo/consumer_c.h asapo_dataset_get_expected_size" asapo_dataset_get_expected_size :: AsapoDataSetHandle -> IO CULong

foreign import capi "asapo/consumer_c.h asapo_dataset_get_size" asapo_dataset_get_size :: AsapoDataSetHandle -> IO CSize

foreign import capi "asapo/consumer_c.h asapo_dataset_get_item" asapo_dataset_get_item :: AsapoDataSetHandle -> CSize -> IO AsapoMessageMetaHandle

foreign import capi "asapo/consumer_c.h asapo_message_metas_get_size" asapo_message_metas_get_size :: AsapoMessageMetasHandle -> IO CSize

foreign import capi "asapo/consumer_c.h asapo_message_metas_get_item" asapo_message_metas_get_item :: AsapoMessageMetasHandle -> CSize -> IO AsapoMessageMetaHandle

foreign import capi "asapo/consumer_c.h asapo_error_get_payload_from_partial_error" asapo_error_get_payload_from_partial_error :: AsapoErrorHandle -> IO AsapoPartialErrorDataHandle

foreign import capi "asapo/consumer_c.h asapo_partial_error_get_id" asapo_partial_error_get_id :: AsapoPartialErrorDataHandle -> IO CULong

foreign import capi "asapo/consumer_c.h asapo_partial_error_get_expected_size" asapo_partial_error_get_expected_size :: AsapoPartialErrorDataHandle -> IO CULong

foreign import capi "asapo/consumer_c.h asapo_error_get_payload_from_consumer_error" asapo_error_get_payload_from_consumer_error :: AsapoErrorHandle -> IO AsapoConsumerErrorDataHandle

foreign import capi "asapo/consumer_c.h asapo_consumer_error_get_id" asapo_consumer_error_get_id :: AsapoConsumerErrorDataHandle -> IO CULong

foreign import capi "asapo/consumer_c.h asapo_consumer_error_get_next_stream" asapo_consumer_error_get_next_stream :: AsapoConsumerErrorDataHandle -> IO ConstCString
