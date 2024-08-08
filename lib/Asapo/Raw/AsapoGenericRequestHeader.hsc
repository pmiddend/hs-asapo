{-# LANGUAGE BangPatterns #-}
module Asapo.Raw.AsapoGenericRequestHeader(AsapoGenericRequestHeader(..)) where

import Asapo.Raw.AsapoOpcode(AsapoOpcode)
import Foreign.C.Types (CULong)
import Control.Applicative((<*>))
import Data.Functor((<$>))
import Foreign.C.String (CString)
import Foreign.Storable (Storable (alignment, peek, peekByteOff, poke, sizeOf))
import Prelude(error)

data AsapoGenericRequestHeader = AsapoGenericRequestHeader
  { asapoGenericRequestHeaderOpCode :: !AsapoOpcode,
    asapoGenericRequestHeaderDataId :: !CULong,
    asapoGenericRequestHeaderDataSize :: !CULong,
    asapoGenericRequestHeaderMetaSize :: !CULong,
    asapoGenericRequestHeaderCustomData :: !CULong,
    asapoGenericRequestHeaderMessage :: !CString,
    asapoGenericRequestHeaderStream :: !CString,
    asapoGenericRequestHeaderApiVersion :: !CString
  }

-- hsc2hs needs the include to find the C structure
#include <asapo/producer_c.h>

instance Storable AsapoGenericRequestHeader where
  sizeOf _ = (# size struct AsapoGenericRequestHeader)
  alignment _ = (# alignment struct AsapoGenericRequestHeader)
  peek ptr =
    AsapoGenericRequestHeader
      <$> (# peek struct AsapoGenericRequestHeader, op_code) ptr
      <*> (# peek struct AsapoGenericRequestHeader, data_id) ptr
      <*> (# peek struct AsapoGenericRequestHeader, data_size) ptr
      <*> (# peek struct AsapoGenericRequestHeader, meta_size) ptr
      <*> (# peek struct AsapoGenericRequestHeader, custom_data) ptr
      <*> (# peek struct AsapoGenericRequestHeader, message) ptr
      <*> (# peek struct AsapoGenericRequestHeader, stream) ptr
      <*> (# peek struct AsapoGenericRequestHeader, api_version) ptr
  poke _ = error "why was AsapoGenericRequestHeader poked? it's supposed to be read-only"
