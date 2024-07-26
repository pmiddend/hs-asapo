# hs-asapo - Haskell bindings to ASAP:O

These are bindings for DESY’s [ASAP:O](https://gitlab.desy.de/asapo/asapo), a middleware platform for high-performance data analysis.

## Docs

The raw C bindings are located in `lib/Asapo/Raw.hsc` and are hand-written. A high-level interface is located in `lib/Asapo/Consumer.hs` and `lib/asapo/Producer.hs`. You should use the latter in your code.

## Supported

Currently, a few select functions are supported, and only in C form. More to come.
