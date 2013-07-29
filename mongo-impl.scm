(use lolevel
     srfi-4 srfi-18
     miscmacros
     data-structures
     lazy-seq

     extras
     
     bson)

#>
#include "mongo.h"
<#

(include "common.scm")

;; wrapper for the mongo struct
(define-record-type mongo (%make-mongo blob)
  %mongo?
  (blob %mongo-blob))

;; TODO: hold a reference to mongo here, otherwise it may be freed by
;; the finalizer while still used by the cursor. will happen if mongo
;; is referenced by cursor (in C) alone
(define-record-type mongo-cursor
  (%make-mongo-cursor blob)
  %mongo-cursor?
  (blob %mongo-cursor-blob))


(define-foreign-type
  mongo
  (c-pointer "mongo")
  (lambda (x) (location (%mongo-blob x))))

(define-foreign-type
  mongo-cursor
  (c-pointer "mongo_cursor")
  (lambda (x) (location (%mongo-cursor-blob x))))

(define mongo-client! (foreign-lambda mongo-return-success? "mongo_client" mongo c-string int))
(define mongo-destroy! (foreign-lambda void "mongo_destroy" mongo))

(define mongo-init! (foreign-lambda void "mongo_init" mongo))

(define-foreign-lambda int mongo_get_err mongo)
(define-foreign-lambda int mongo_get_server_err mongo)
(define-foreign-lambda c-string mongo_get_server_err_string mongo)
(define-foreign-lambda void mongo_clear_errors mongo)
(define mongo_get_err_string (foreign-lambda* c-string ((mongo conn)) "return(conn->errstr);"))
(define cursor-error (foreign-lambda* int ((mongo-cursor cursor)) "return(cursor->err);"))

(define errors
  (letrec-syntax
      ((el (syntax-rules ()
             ((_) '())
             ((_ name rest ...)
              (cons (cons (foreign-value name int) (quote name))
                    (el rest ...))))))
    (el MONGO_CONN_SUCCESS
        MONGO_CONN_NO_SOCKET
        MONGO_CONN_FAIL
        MONGO_CONN_ADDR_FAIL
        MONGO_CONN_NOT_MASTER
        MONGO_CONN_BAD_SET_NAME
        MONGO_CONN_NO_PRIMARY
        MONGO_IO_ERROR
        MONGO_SOCKET_ERROR
        MONGO_READ_SIZE_ERROR
        MONGO_COMMAND_FAILED
        MONGO_WRITE_ERROR
        MONGO_NS_INVALID
        MONGO_BSON_INVALID
        MONGO_BSON_NOT_FINISHED
        MONGO_BSON_TOO_LARGE
        MONGO_WRITE_CONCERN_INVALID)))


;; check conn for an err and try to find its text-form
(define (check-error conn)
  (let ((ec (mongo_get_err conn)))
    (if (not (zero? ec))
        (error (mongo_get_err_string conn) (alist-ref ec errors)))))


(define (make-mongo)
  ;; TODO: destroy u8vector from mongo-finalizer instead of using u8vector's
  ;; own, so we don't duplicate finalizers
  (let ((mongo (%make-mongo (make-u8vector sizeof-mongo 0 #t #t))))
    (set-finalizer! mongo (lambda (x) (print "destroying mongo: " x) (mongo-destroy! x)))
    (mongo-init! mongo)
    mongo))

(define (mongo-client #!optional (host "127.0.0.1") (port 27017))
  (let ((mongo (make-mongo)))
    (mongo-client! mongo host port)
    (check-error mongo)
    mongo))

(define-foreign-lambda mongo-return-success? mongo_reconnect        mongo)
(define-foreign-lambda mongo-return-success? mongo_check_connection mongo)
(define-foreign-lambda void mongo_disconnect       mongo)


(define (mongo-reconnect!? #!optional (mongo (current-mongo)))
  (if (not (mongo_check_connection mongo))
      (mongo_reconnect mongo)))



;; does a remote-query
(define-foreign-lambda int mongo_check_last_erro mongo c-string)

(define-record-printer (mongo x out)
  (format out "#<mongo ~A>" (if (mongo_check_connection x) "connected" "disconnected")))

(define mongo-cursor-init!    (foreign-lambda void "mongo_cursor_init" mongo-cursor mongo c-string))
(define mongo-cursor-destroy! (foreign-lambda mongo-return-success? "mongo_cursor_destroy" mongo-cursor))
(define mongo-cursor-next!    (foreign-lambda mongo-return-success? "mongo_cursor_next" mongo-cursor))

(define mongo-cursor-nonnull?    (foreign-lambda* bool ((mongo-cursor cursor))
                                           "return(cursor->current.data);"))

(define %bson_init_finished_data
  (foreign-lambda mongo-return-success? bson_init_finished_data scheme-pointer scheme-pointer bool))

;; returns #f or #<bson>
(define (mongo-cursor-current cursor)
  ;; before calling cursor-next, "current->data.data" will be 0, we
  ;; check for that first because otherwise mongo_cursor_bson returns
  ;; an invalid pointer
  (and (mongo-cursor-nonnull? cursor)
       ((foreign-lambda bson "mongo_cursor_bson" mongo-cursor) cursor)))

(define current-mongo (make-parameter #f))
(define (current-mongo/default)
  (or (current-mongo)
      (begin (current-mongo (mongo-client))
             (current-mongo))))

;; do a query and return a mongo-cursor
(define (mongo-find* nm #!optional query fields (mongo (current-mongo/default)))
  (mongo-reconnect!?)
  (let ((cursor (%make-mongo-cursor (make-u8vector sizeof-mongo-cursor 0 #t #t))))
    (set-finalizer! cursor (lambda (x) (print "destroying cursor: " x) (mongo-cursor-destroy! x)))
    (mongo-cursor-init! cursor mongo nm)
    cursor))

(define (mongo-find-one nm #!optional query fields)
  (let ((cursor (mongo-find* nm query fields)))
    (and (mongo-cursor-next! cursor)
         (mongo-cursor-current cursor))))

;; do a query and return a lazy-seq
(define (mongo-find nm #!optional query fields)
  (let ((crs (mongo-find* nm query fields)))
   (let next ()
     (lazy-seq
      (if (mongo-cursor-next! crs)
          (cons (mongo-cursor-current crs) (next))
          '())))))

;; TODO: make the struct wrappers
(define-foreign-type mongo-write-concern (c-pointer "mongo_write_concern"))

(define-foreign-lambda mongo-return-success? mongo_insert mongo c-string bson mongo-write-concern)

(define (mongo-insert nm b #!optional (mongo (current-mongo/default)))
  (mongo-reconnect!? mongo)
  (mongo_clear_errors mongo)
  (mongo_insert mongo nm b #f)
  (check-error mongo))


