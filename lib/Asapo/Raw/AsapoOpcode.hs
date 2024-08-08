{-# LANGUAGE CApiFFI #-}
{-# LANGUAGE ForeignFunctionInterface #-}

module Asapo.Raw.AsapoOpcode
  ( AsapoOpcode,
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
  )
where

import Foreign.C.Types (CInt (CInt))
import Prelude ()

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
