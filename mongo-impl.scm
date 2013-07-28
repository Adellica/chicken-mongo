(use lolevel
     srfi-4 srfi-18
     miscmacros
     data-structures
     lazy-seq
     
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

(define (make-mongo)
  (let ((mongo (%make-mongo (make-u8vector sizeof-mongo 0 #t #t))))
    (set-finalizer! mongo (lambda (x) (print "destroying mongo: " x) (mongo-destroy! x)))
    (mongo-init! mongo)
    mongo))

(define (mongo-client #!optional (host "127.0.0.1") (port 27017))
  (let ((mongo (make-mongo)))
    (mongo-client! mongo host port)
    mongo))

(define mongo-cursor-init!    (foreign-lambda void "mongo_cursor_init" mongo-cursor mongo c-string))
(define mongo-cursor-destroy! (foreign-lambda mongo-return-success? "mongo_cursor_destroy" mongo-cursor))
(define mongo-cursor-next!    (foreign-lambda mongo-return-success? "mongo_cursor_next" mongo-cursor))

(define mongo-cursor-nonnull?    (foreign-lambda* bool ((mongo-cursor cursor))
                                           "return(cursor->current.data);"))

(define %bson_size (foreign-lambda int bson_size (c-pointer "bson")))
(define %bson_init_finished_data
  (foreign-lambda mongo-return-success? bson_init_finished_data scheme-pointer scheme-pointer bool))

(define-foreign-type bson (c-pointer "bson")
  (lambda (x) (let ((blob (make-string (foreign-value "sizeof(bson)" int))))
           (%bson_init_finished_data blob ;; bson struct
                                     x    ;; raw bson data
                                     #f) ;; bson struct does not own string
           (location blob)))             ;; pointer to bson struct
  (lambda (ptr)
    ;; copy bson binary data into string
    (let* ((len (%bson_size ptr))
           (blob (make-string len)))
      (move-memory! ((foreign-lambda c-pointer bson_data (c-pointer "bson")) ptr) ;; from 
                    blob ;; to
                    len)
      blob)))

;; returns #f or #<bson>
(define (mongo-cursor-current cursor)
  ;; before calling cursor-next, "current->data.data" will be 0, we
  ;; check for that first because otherwise mongo_cursor_bson returns
  ;; an invalid pointer
  (and (mongo-cursor-nonnull? cursor)
       ((foreign-lambda bson "mongo_cursor_bson" mongo-cursor) cursor)))

(define current-mongo (make-parameter (mongo-client)))

;; do a query and return a mongo-cursor
(define (mongo-find* nm #!optional query fields)
  (let ((cursor (%make-mongo-cursor (make-u8vector sizeof-mongo-cursor 0 #t #t))))
    (set-finalizer! cursor (lambda (x) (print "destroying cursor: " x) (mongo-cursor-destroy! x)))
    (mongo-cursor-init! cursor (current-mongo) nm)
    cursor))

(define (mongo-find-one nm #!optional query fields)
  (let ((cursor (mongo-find nm query fields)))
    (and (mongo-cursor-next! cursor)
         (mongo-cursor-current cursor))))

(define mongo-print (foreign-lambda void "bson_print" bson))

(define mongo-cursor-current*
  (let ((c 0))
    (lambda (x)
      (set! c (add1 c))
      (if (< c 10)
          (conc "item " c)
          #f))))

;; do a query and return a lazy-seq
(define (mongo-find nm #!optional query fields)
  (let ((crs (mongo-find* nm query fields)))
   (let next ()
     (lazy-seq
      (if (mongo-cursor-next! crs)
          (cons (mongo-cursor-current crs) (next))
          '())))))
