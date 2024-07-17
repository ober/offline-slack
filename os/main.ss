;;; -*- Gerbil -*-
(import :std/error
        :std/sugar
        :std/cli/getopt
        ./lib)
(export main)

;; build manifest; generated during the build
;; defines version-manifest which you can use for exact versioning
(include "../manifest.ss")

(def (main . args)
  (call-with-getopt os-main args
    program: "os"
    help: "An offline Slack reader"
    ;; commands/options/flags for your program; see :std/cli/getopt
    ;; ...
    ))

(def* os-main
  ((opt)
   (os-main/options opt))
  ((cmd opt)
   (os-main/command cmd opt)))

;;; Implement this if your CLI doesn't have commands
(def (os-main/options opt)
  (error "Implement me!"))

;;; Implement this if your CLI has commands
(def (os-main/command cmd opt)
     (displayln "hello " opt))
     ;;(error "Implement me!"))
