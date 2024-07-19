#!/usr/bin/env gxi
;; -*- Gerbil -*-

(import :std/build-script)

(defbuild-script
  `("os/lib.ss"
    (exe:
     "os/main"
     bin: "os"
     "-ld-options"
     "-lleveldb -lstdc++ -lz -lm -lutil"
     )))
