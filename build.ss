#!/usr/bin/env gxi
;; -*- Gerbil -*-

(import :std/build-script)

(defbuild-script
  `("os/lib.ss"
    (exe:
     "os/main"
     bin: "os"
     "-ld-options"
     "-lyaml -lleveldb -lstdc++ -lssl -lcrypto -lz -lm -lutil"
     )))
