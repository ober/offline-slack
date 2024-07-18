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

(def program-name "os")

(def (main . args)
  (def load
    (command 'load help: "Load all files in dir. "
	           (argument 'directory help: "Directory where the Slack files reside")))

  (def dbg
    (command 'dbg help: "Fetch value for key"
	           (argument 'tag help: "Key to resolve")))

  (def cs
    (command 'cs help: "Channel Message counts"))

  (def msgs
    (command 'msgs help: "Fetch value for key"
	           (argument 'channel help: "Channel to fetch messages")))

  (def lc
    (command 'lc help: "list channels"))

  (def ic
    (command 'ic help: "list channels"))

  (def ls
    (command 'ls help: "list all records"))

  (def st
    (command 'st help: "Show total db entry count"))

  (call-with-getopt process-args args
		                program: "os"
		                help: "Slack Channel log parser"
                    cs
                    dbg
                    ic
                    lc
                    ls
                    msgs
                    st
                    load))

(def (process-args cmd opt)
  (let-hash opt
    (case cmd
      ((cs)
       (cs))
      ((dbg)
       (dbg .tag))
      ((lc)
       (lc))
      ((ic)
       (index-channels))
      ((ls)
       (list-records))
      ((load)
       (load-slack .directory))
      ((msgs)
       (msgs .channel))
      ((st)
       (st)))))
