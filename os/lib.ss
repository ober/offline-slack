;;; -*- Gerbil -*-

(import
  :clan/db/leveldb
  :gerbil/gambit
  :ober/oberlib
  :std/actor-v18/io
  :std/crypto
  :std/debug/heap
  :std/debug/memleak
  :std/format
  :std/generic/dispatch
  :std/io
  :std/iter
  :std/misc/list
  :std/misc/threads
  :std/pregexp
  :std/srfi/1
  (only-in :std/srfi/19 date->string)
  :std/srfi/95
  (only-in :std/srfi/132 list-sort)
  :std/sugar
  :std/text/hex
  :std/text/json
  :std/text/utf8
  :std/text/zlib
  )

(export #t)

(def version "0.01")
(def nil '#(nil))
(def use-write-backs #t)
(def delim "#")

(def (db-init)
  (dp "in db-init")
  (leveldb-writebatch))

(def wb (db-init))

(def (def-num num)
  (if (string? num)
    (string->number num)
    num))

(def write-back-count 0)
(def max-wb-size (def-num (getenv "k_max_wb" 100000)))
(def tmax (def-num (getenv "tmax" 12)))
(def indices-hash (make-hash-table))

(def (find-slack-files dir)
  (find-files
   dir
   (lambda (filename)
     (and (equal? (path-extension filename) ".json")
	        (not (equal? (path-strip-directory filename) ".json"))))))

(def (load-slack dir)
  "Entry point for processing cloudtrail files"
  (parameterize ((read-json-key-as-symbol? #t))
    (let (2G (expt 2 31))
      (when (< (##get-min-heap) 2G)
        (##set-min-heap! 2G)))

    (dp (format ">-- load-slack: ~a" dir))
    (let* ((count 0)
	         (slack-files (find-slack-files dir))
           (pool []))
      (for (file slack-files)
        (dp (format "slackfile: ~a" file))
        (cond-expand
          (gerbil-smp
           (while (< tmax (length (all-threads)))
	           (thread-yield!))
           (let ((thread (spawn (lambda () (read-slack-file file)))))
	           (set! pool (cons thread pool))))
          (else
           (read-slack-file file)))
        (flush-all?)
        (set! count 0))
      (cond-expand (gerbil-smp (for-each thread-join! pool)))
      (db-write)
      (db-close))))

(def (file-already-processed? file)
  (dp "in file-already-processed?")
  (let ((seen (db-key? (format "F-~a" (path-strip-extension (path-strip-directory file))))))
    seen))

(def (mark-file-processed file)
  (dp "in mark-file-processed")
  (let ((filename (path-strip-extension (path-strip-directory file))))
    (dp (format "marking ~A~%" filename))
    (db-batch (format "F-~a" filename) "t")))

(def (load-slack-file port)
  (try
   (read-json port)
   (catch (e)
     (display-exception e))))

(def (read-slack-file file)
  (ensure-db)
  (displayln "trying to load " file)
  (unless (file-already-processed? file)
    (let ((btime (time->seconds (current-time)))
	        (count 0))
      (call-with-input-file file
	      (lambda (file-input)
          (let* ((ch (path-strip-extension (path-strip-directory file)))
                 (data (load-slack-file file-input)))
            (when (hash-table? data)
              (let-hash data
                (when (and .?messages (list? .?messages) (> (length .?messages) 0))
                  (for (msg .?messages)
                    (set! count (+ count 1))
                    (process-msg ch .?name msg))
                  (db-batch (format "ch~a~a" delim ch) .?name)
                  (db-batch (format "n~a~a" delim .?name) ch)
                  (mark-file-processed ch)))))))

      (let ((delta (- (time->seconds (current-time)) btime)))
        (displayln
         "rps: " (float->int (/ count delta ))
         " size: " count
         " delta: " delta
         " threads: " (length (all-threads))
	       " file: " file
	       )))))

(def (db-open)
  (dp ">-- db-open")
  (let ((db-dir (or (getenv "slackdb" #f) (format "~a/slackdb/" (user-info-home (user-info (user-name)))))))
    (dp (format "db-dir is ~a" db-dir))
    (unless (file-exists? db-dir)
      (create-directory* db-dir))
    (let ((location (format "~a/records" db-dir)))
      (leveldb-open location (leveldb-options
			                        paranoid-checks: #f
			                        max-open-files: (def-num (getenv "k_max_files" 500000))
			                        bloom-filter-bits: (def-num (getenv "k_bloom_bits" #f))
			                        compression: #t
			                        block-size: (def-num (getenv "k_block_size" #f))
			                        write-buffer-size: (def-num (getenv "k_write_buffer" (* 102400 1024 16)))
			                        lru-cache-capacity: (def-num (getenv "k_lru_cache" 10000)))))))

(def db (db-open))

(def (ensure-db)
  (unless db
    (set! db (db-open))))

(def (db-get key)
  (dp (format "db-get: ~a" key))
  (let ((ret (leveldb-get db (format "~a" key))))
    (if (u8vector? ret)
      (unmarshal-value ret)
      #f)))

(def (db-key? key)
  (dp (format ">-- db-key? with ~a" key))
  (leveldb-key? db (format "~a" key)))

(def (db-write)
  (dp "in db-write")
  (leveldb-write db wb))

(def (db-close)
  (dp "in db-close")
  (leveldb-close db))

;; leveldb stuff
(def (get-leveldb key)
  (displayln "get-leveldb: " key)
  (try
   (let* ((bytes (leveldb-get db (format "~a" key)))
          (val (if (u8vector? bytes)
                 (unmarshal-value bytes)
                 nil)))
     val)
   (catch (e)
     (raise e))))

(def (repairdb)
  "Repair the db"
  (let ((db-dir (or (getenv "slackdb" #f) (format "~a/slackdb/" (user-info-home (user-info (user-name)))))))
    (leveldb-repair-db (format "~a/records" db-dir))))

(def (process-msg channel name msg)
  (if (hash-table? msg)
    (let-hash msg
      (unless .?text
        (dp (format "Missing text: ~a" (hash->string msg))))

      (let ((h (hash
                (text (or .?text "File Uploaded"))))
            (req-id (format "m~a~a~a~a~a~a"
                            delim
                            channel
                            delim
                            (or .?user .?username .?bod_id .?client_msg_id .?sub_type "Unknown")
                            delim (or .?ts 0))))

        (when (and .?team name)
          (db-batch (format "tn~a~a~a~a" delim .team delim name) #t))
        (db-batch req-id h)

        (when (and .?team .?user)
          (let ((key (format "tu~a~a~a~a" delim .team delim .user)))
            (db-batch key #t)))

        (unless (or .?user .?sub_type .?client_msg_id .?username .?bot_id)
          (displayln (hash->string msg)))

        (when .?text
          (let ((words (pregexp-split "[ \t\n\r]+" .text)))
            (for (word words)
              (register-word word req-id))))

        (set! write-back-count (+ write-back-count 1))

        ))))

(def (kl partial)
  (for-each displayln (keylike partial)))

(def (keylike partial)
  "Return all keys like partial."
  (let ((entries (lookup-keys partial)))
    entries))

(def (register-word word req-id)
  (let* ((key (format "w~a~a~a~a" delim word delim req-id)))
    (db-batch key #t)))

(def (words)
  (let ((werds (lookup-keys (format "w~a" delim))))
    (for-each displayln werds)))

(def (db-batch key value)
  (unless (string? key) (dp (format "key: ~a val: ~a" (##type-id key) (##type-id value))))
  (leveldb-writebatch-put wb key (marshal-value value)))

(def (db-put key value)
  (dp (format "<----> db-put: key: ~a val: ~a" key value))
  (leveldb-put db key (marshal-value value)))

(def (flush-all?)
  (dp (format "write-back-count && max-wb-size ~a ~a" write-back-count max-wb-size))
  (when (> write-back-count max-wb-size)
    (displayln "writing.... " write-back-count)
    (let ((old wb))
      (spawn
       (lambda ()
	       (leveldb-write db old)))
      (set! wb (leveldb-writebatch))
      (set! write-back-count 0))))

(def (ls)
  (list-records))

(def (list-records)
  "Print all records"
  (let (itor (leveldb-iterator db))
    (leveldb-iterator-seek-first itor)
    (let lp ()
      (leveldb-iterator-next itor)
      (let ((key (utf8->string (leveldb-iterator-key itor)))
            (val (unmarshal-value (leveldb-iterator-value itor))))
        (if (hash-table? val)
          (displayln (format "k: ~a v: ~a" key (hash->list val)))
          (displayln (format "k: ~a v: ~a" key val))))
      (when (leveldb-iterator-valid? itor)
        (lp)))))

(def (countdb)
  "Get a count of how many records are in db"
  (let ((mod 1000000)
	      (itor (leveldb-iterator db)))
    (leveldb-iterator-seek-first itor)
    (let lp ((count 1))
      (when (= (modulo count mod) 0)
	      (displayln count))
      (leveldb-iterator-next itor)
      (if (leveldb-iterator-valid? itor)
        (lp (1+ count))
        count))))

(def (st)
  (displayln "Totals: " " records: " (countdb)))

(def (ts)
  (let ((outs [[ "Team" "Members" ]])
        (teams (list-teams)))
    (for (team teams)
      (let ((th (db-get (format "t~a~a" delim team))))
        (when (hash-table? th)
          (let-hash th
            (set! outs (cons [
                              .$name
                              (length .$members)
                              ] outs))))))
    (style-output outs "org-mode")))

(def (msgs channel)
  (let* ((outs [[ "Date" "Name" "Text" ]])
         (ch (db-get (format "n~a~a" delim channel)))
         (pat (format "m~a~a~a" delim ch delim))
         (entries (lookup-keys pat)))

    (for (entry entries)
      (let* ((msg (db-get entry))
             (fields (pregexp-split delim entry))
             (user (nth 2 fields))
             (date (nth 3 fields)))
        (set! outs (cons [
                          (date-parse-epoch date)
                          user
                          (let-hash msg .?text)
                          ] outs))))
    (style-output outs "org-mode")))

(def (lt)
  (for-each displayln (list-teams)))

(def (list-teams)
  (let ((teams (uniq-by-nth-prefix (format "tn~a" delim) 1)))
    teams))

(def (uniq-by-nth-prefix prefix pos)
  (dp (format ">-- uniq-by-nth-prefix: ~a" prefix))
  (let ((itor (leveldb-iterator db)))
    (leveldb-iterator-seek itor (format "~a" prefix))
    (let lp ((res []))
      (if (leveldb-iterator-valid? itor)
        (let ((k (utf8->string (leveldb-iterator-key itor))))
          (if (pregexp-match prefix k)
            (let ((mid (nth pos (pregexp-split delim k))))
              (unless (member mid res string-ci=?)
                (set! res (cons mid res)))
	            (leveldb-iterator-next itor)
	            (lp res))
	          res))
        res))))

(def (lookup-keys key)
  (dp (format ">-- lookup-keys: ~a" key))
  (let ((itor (leveldb-iterator db)))
    (leveldb-iterator-seek itor (format "~a" key))
    (let lp ((res []))
      (if (leveldb-iterator-valid? itor)
        (let ((k (utf8->string (leveldb-iterator-key itor))))
          (if (pregexp-match key k)
            (begin
              (unless (member k res)
                (set! res (cons k res)))
                   (leveldb-iterator-next itor)
                   (lp res))
                 res))
        res))))

(def (sort-uniq-reverse lst)
  (reverse (unique! (sort! lst eq?))))

(def (db-rm key)
  (dp (format "<----> db-rm: key: ~a" key))
  (leveldb-delete db key))

(def (dbg key)
  (displayln (db-get key)))

;; channels
(def (lc)
  (for-each displayln (list-channels-names)))

(def (cs)
  (let ((outs [[ "Channel" "Count" ]])
        (channels (list-channels-names)))
    (for (channel channels)
      (displayln "doing: " channel)
      (let* ((ch (db-get (format "n~a~a" delim channel)))
             (count (length (lookup-keys (format "m~a~a~a" delim ch delim)))))
        (set! outs (cons [
                          channel
                          count
                          ] outs))))
    (style-output outs "org-mode")))

(def (list-channels-hashes)
  (let* ((key (format "ch~a" delim))
         (channels (lookup-keys key))
         (results []))

    (for (channel channels)
      (let* ((chan (pregexp-split delim channel))
             (ch (nth 1 chan))
             (name (db-get (format "~a~a" key ch))))
        (set! results (cons ch results))))
    results))

(def (list-channels-names)
  (let* ((key (format "ch~a" delim))
         (channels (lookup-keys key))
         (results []))

    (for (channel channels)
      (let* ((chan (pregexp-split delim channel))
             (th (nth 1 chan))
             (name (db-get (format "~a~a" key th))))
        (set! results (cons (format "~a" name) results))))
    (list-sort string=? results)))

;; teams

(def (search word)
  (let ((outs [[ "Date" "Channel" "Name" "Text" ]])
        (matches (lookup-keys (format "w~a~a~a" delim word delim))))
    (for (entry matches)
      (let* ((fields (pregexp-split delim entry))
             (user (nth 4 fields))
             (head (nth 2 fields))
             (ch (nth 3 fields))
             (cn (db-get (format "ch~a~a" delim ch)))
             (date (nth 5 fields))
             (req-id (format "~a~a~a~a~a~a~a"
                             head delim
                             ch delim
                             user delim
                             date))
             (message (db-get req-id)))
	      (when (hash-table? message)
          (let-hash message
            (set! outs (cons [
                              (date-parse-epoch date)
                              cn
                              user
                              .?text
                              ] outs))))))
    (style-output outs "org-mode")))

(def (date-parse-epoch epoch)
  (date->string (epoch->date (nth 0 (pregexp-split "\\." (format "~a" epoch))))
                "~Y-~m-~d ~H:~M:~S"))
