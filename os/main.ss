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

  (def ts
    (command 'ts help: "Team member counts"))

  (def repair
    (command 'repair help: "Repair the db"))

  (def msgs
    (command 'msgs help: "Fetch value for key"
	           (argument 'channel help: "Channel to fetch messages")))

  (def search
    (command 'search help: "Search for Word"
	           (argument 'word help: "Search and fetch messages with word")))

  (def words
    (command 'words help: "List all words in the index"))

  (def channels
    (command 'channels help: "list channels"))

  (def iw
    (command 'iw help: "index words"))

  (def ls
    (command 'ls help: "list all records"))

  (def teams
    (command 'teams help: "list teams"))

  (def st
    (command 'st help: "Show total db entry count"))

  (call-with-getopt process-args args
		                program: "os"
		                help: "Slack Channel log parser"
                    channels
                    cs
                    dbg
                    iw
                    load
                    ls
                    repair
                    msgs
                    search
                    st
                    teams
                    ts
                    words
                    ))

(def (process-args cmd opt)
  (let-hash opt
    (case cmd
      ((dbg)
       (dbg .tag))
      ((channels)
       (lc))
      ((cs)
       (cs))
      ((teams)
       (lt))
      ((iw)
       (index-words))
      ((ls)
       (list-records))
      ((load)
       (load-slack .directory))
      ((repair)
       (repairdb))
      ((msgs)
       (msgs .channel))
      ((st)
       (st))
      ((search)
       (search .word))
      ((ts)
       (ts))
      ((words)
       (words))
       )))
