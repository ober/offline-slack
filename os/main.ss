;;; -*- Gerbil -*-
(import :std/error
        :clan/db/leveldb
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
        :ober/os/lib
        )
(export main)

(def program-name "kunabi")

(def (main . args)
  (def load
    (command 'load help: "Load all files in dir. "
	         (argument 'directory help: "Directory where the Slack files reside")))
  (def ls
    (command 'ls help: "list all records"))

  (call-with-getopt process-args args
		            program: "os"
		            help: "Slack Channel log parser"
		            load))

(def (process-args cmd opt)
  (let-hash opt
    (case cmd
      ((ls)
       (ls))
      ((load)
       (load-slack .directory)))))
