{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE CApiFFI #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

#include <asapo/common/common_c.h>

-- |
-- Description : Here be dragons
--
-- Actually, not really dragons, just a small hack in order to free up memory without patching asapo or writing a whole C module
module Asapo.Raw.FreeHandleHack(p_asapo_free_handle) where
import Foreign(Ptr, FunPtr)
import Prelude()
import System.IO(IO)

#def void hs_asapo_free_handle_with_ptr(void *handle) { asapo_free_handle__(&handle); }

foreign import capi "asapo/common/common_c.h &hs_asapo_free_handle_with_ptr" p_asapo_free_handle :: FunPtr (Ptr () -> IO ())
