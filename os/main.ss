;;; -*- Gerbil -*-
(import :std/error
        :clan/db/leveldb
        :clan/text/yaml
        :gerbil/gambit
        :ober/oberlib
        :std/actor
        :std/cli/getopt
        :std/debug/heap
        :std/debug/memleak
        :std/format
        :std/generic/dispatch
        :std/getopt
        :std/iter
        :std/logger
        :std/misc/list
        :std/misc/lru
        :std/net/address
        :std/net/httpd
        :std/pregexp
        :std/srfi/1
        :std/srfi/95
        :std/sugar
        :std/text/json
        ./lib)
(export main)

;; build manifest; generated during the build
;; defines version-manifest which you can use for exact versioning
;;(include "../manifest.ss")
(def program-name "kunabi")

(def (main . args)
  (def load
    (command 'load help: "Load all files in dir. "
	     (argument 'directory help: "Directory where the Slack files reside")))

  (call-with-getopt process-args args
		    program: "os"
		    help: "Slack Channel log parser"
		    load))

(def (process-args cmd opt)
  (let-hash opt
    (case cmd
      ((load)
       (load-slack .directory)))))
