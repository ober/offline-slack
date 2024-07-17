;;; -*- Gerbil -*-

(import
  :clan/db/leveldb
  :clan/text/yaml
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
  :std/srfi/95
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
(def wb (db-init))

(def write-back-count 0)
(def max-wb-size (def-num (getenv "k_max_wb" 100000)))
(def tmax (def-num (getenv "tmax" 12)))
(def indices-hash (make-hash-table))

(def (def-num num)
  (if (string? num)
    (string->number num)
    num))



;;; Your library support code
;;; ...

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
	     (slack-files (find-slack-files "."))
         (pool []))
    (for (file slack-files)
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
  (let ((seen (db-key? (format "F-~a" file))))
    seen))

(def (mark-file-processed file)
  (dp "in mark-file-processed")
  (format "marking ~A~%" file)
  (db-batch (format "F-~a" file) "t"))

(def (get-channel-name file)
  (let* ((content (open-input-string file))
         (json (read-json content))
         (name (let-hash json .?name)))
    name))

(def (load-slack-file file)
  (hash-ref (read-json (open-input-string file)) 'messages))

(def (read-slack-file file)
(ensure-db)
(unless (file-already-processed? file)
  (let ((btime (time->seconds (current-time)))
	    (count 0))
    (call-with-input-file file
	  (lambda (file-input)
        (let ((messages (load-slack-file file-input))
              (channel (get-channel-name file-input)))
          (for-each
	        (lambda (msg)
              (set! count (+ count 1))
              (process-msg channel msg))
	        messages))
        (mark-file-processed file)))

    (let ((delta (- (time->seconds (current-time)) btime)))
      (displayln
       "rps: " (float->int (/ count delta ))
       " size: " count
       " delta: " delta
       " threads: " (length (all-threads))
	   " file: " file
	   )))))

(def (ensure-db)
  (unless db
    (set! db (db-open))))

(def db (db-open))

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

(def (db-get key)
  (dp (format "db-get: ~a" key))
  (let ((ret (leveldb-get db (format "~a" key))))
    (if (u8vector? ret)
      (unmarshal-value ret)
      "N/A")))

(def (db-key? key)
  (dp (format ">-- db-key? with ~a" key))
  (leveldb-key? db (format "~a" key)))

(def (db-write)
  (dp "in db-write")
  (leveldb-write db wb))

(def (db-close)
  (dp "in db-close")
  (leveldb-close db))

(def (db-init)
  (dp "in db-init")
  (leveldb-writebatch))

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
  (let ((db-dir (or (getenv "osdb" #f) (format "~a/os-db/" (user-info-home (user-info (user-name)))))))
    (leveldb-repair-db (format "~a/records" db-dir))))

(def (process-msg channel msg)
  (if (hash-table? msg)
    (let-hash msg
      (let ((h (hash
                (text .?text)))
            (req-id (format "~a#~a#~a" .?channel .?ts .?user )))

        ;; (unless (getenv "osro" #f)
        ;;   (set! write-back-count (+ write-back-count 1))
        (db-batch req-id h)
        ;;   (when (string=? user "")
        ;;     (displayln "Error: missing user: " user))
        ;;   (when (string? user)
	    ;;     (db-batch (format "u#~a#~a" user epoch) req-id))
        ;;   (when (string? .?eventName)
	    ;;     (db-batch (format "en#~a#~a" .?eventName epoch) req-id))
        ;;   (when (string? .?errorCode)
	    ;;     (db-batch (format "ec#~a#~a" .errorCode epoch) req-id)))
        ))))

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
