{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CApiFFI #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Asapo.Raw.Producer where

import Asapo.Raw.Common
import Data.Bits (shiftL, (.|.))
import Data.Functor ((<$>))
import Foreign.C.ConstPtr (ConstPtr (ConstPtr))
import Foreign.C.String (CString)
import Foreign.C.Types (CChar, CInt (CInt), CLong (CLong), CSize (CSize), CUChar (CUChar), CULong (CULong))
import Foreign.Ptr (FunPtr, Ptr)
import Foreign.Storable (Storable)
import System.Clock (TimeSpec)
import System.IO (IO)
import Prelude ()

newtype {-# CTYPE "asapo/producer_c.h" "AsapoProducerHandle" #-} AsapoProducerHandle = AsapoProducerHandle (Ptr ()) deriving (Storable)

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
