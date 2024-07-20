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

  (def msgs
    (command 'msgs help: "Fetch value for key"
	           (argument 'channel help: "Channel to fetch messages")))

  (def words
    (command 'words help: "List all words in the index"))

  (def channels
    (command 'channel help: "list channels"))

  (def ic
    (command 'ic help: "index channels"))

  (def it
    (command 'it help: "index teams"))

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
                    ic
                    it
                    iw
                    load
                    ls
                    msgs
                    st
                    teams
                    ts
                    words
                    ))

(def (process-args cmd opt)
  (let-hash opt
    (case cmd
      ((cs)
       (cs))
      ((dbg)
       (dbg .tag))
      ((channels)
       (lc))
      ((teams)
       (lt))
      ((iw)
       (index-words))
      ((ic)
       (index-channels))
      ((it)
       (index-teams))
      ((ls)
       (list-records))
      ((load)
       (load-slack .directory))
      ((msgs)
       (msgs .channel))
      ((st)
       (st))
      ((ts)
       (ts))
      ((words)
       (words))
       )))
