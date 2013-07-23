(use lolevel srfi-4 miscmacros srfi-18)

#>

#include <stdio.h>
#include "mongo.h"

static void tutorial_empty_query( mongo *conn) {
  mongo_cursor cursor[1];
  mongo_cursor_init( cursor, conn, "logging.swipes" );

  while( mongo_cursor_next( cursor ) == MONGO_OK )
  printf ("hello from x %d\n", cursor->current.data);
 //bson_print( cursor->current );

  mongo_cursor_destroy( cursor );
}

int main2() {
  mongo conn[1];
  int status = mongo_client( conn, "127.0.0.1", 27017 );

  if( status != MONGO_OK ) {
      switch ( conn->err ) {
        case MONGO_CONN_NO_SOCKET:  printf( "no socket\n" ); return 1;
        case MONGO_CONN_FAIL:       printf( "connection failed\n" ); return 1;
        case MONGO_CONN_NOT_MASTER: printf( "not master\n" ); return 1;
      }
  }

  tutorial_empty_query (conn);

  mongo_destroy( conn );

  return 0;
}

<#

(define sizeof-mongo        (foreign-value "sizeof(mongo)"        int))
(define sizeof-mongo-cursor (foreign-value "sizeof(mongo_cursor)" int))
(define sizeof-bson         (foreign-value "sizeof(bson)"         int))

(define-record-type mongo        (%make-mongo blob)        %mongo?        (blob %mongo-blob))

;; TODO: hold a reference to mongo here, otherwise it may be freed by
;; the finalizer while still used by the cursor. will happen if mongo
;; is referenced by cursor (in C) alone
(define-record-type mongo-cursor (%make-mongo-cursor blob) %mongo-cursor? (blob %mongo-cursor-blob))
(define-record-type bson
  (%make-bson pointer blob)
  %bson?
  (pointer %bson-pointer)
  (blob %bson-blob))

(define-foreign-type mongo-return-success? int
  (lambda (x) (error "passing status as argument unexpeced"))
  (lambda (int-status) (if (= (foreign-value "MONGO_OK" int) int-status) #t #f)))

(define-foreign-type
  mongo
  (c-pointer "mongo")
  (lambda (x) (location (%mongo-blob x))))

(define-foreign-type
  mongo-cursor
  (c-pointer "mongo_cursor")
  (lambda (x) (location (%mongo-cursor-blob x))))

(define-foreign-type
  bson
  (c-pointer "bson")
  (lambda (x) (%bson-pointer x))
  (lambda (x) (%make-bson x #f)))

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

;; returns #f or #<bson>
(define (mongo-cursor-current cursor)
  ;; before calling cursor-next, "current->data.data" will be 0, we
  ;; check for that first because otherwise mongo_cursor_bson returns
  ;; an invalid pointer
  (and (mongo-cursor-nonnull? cursor)
       ((foreign-lambda bson "mongo_cursor_bson" mongo-cursor) cursor)))

(define current-mongo (make-parameter (mongo-client)))

(define (mongo-find nm #!optional query fields)
  (let ((cursor (%make-mongo-cursor (make-u8vector sizeof-mongo-cursor 0 #t #t))))
    (set-finalizer! cursor (lambda (x) (print "destroying cursor: " x) (mongo-cursor-destroy! x)))
    (mongo-cursor-init! cursor (current-mongo) nm)
    cursor))

(define (mongo-find-one nm #!optional query fields)
  (let ((cursor (mongo-find nm query fields)))
    (and (mongo-cursor-next! cursor)
         (mongo-cursor-current cursor))))



(define mongo-print (foreign-lambda void "bson_print" bson))

(print "sizeof-mongo:" sizeof-mongo)
(print "new mongo: " (make-mongo))

(define cursor (mongo-find "logging.swipes"))


(print "new cursor: " cursor)
(print "current b4: " (mongo-cursor-current cursor))
(mongo-cursor-next! cursor)
(print "current a8: " (mongo-cursor-current cursor))
(mongo-print (mongo-cursor-current cursor))
;;(print "one " (mongo-find-one "logging.swipes"))

(print "***** testing scheme cursor iteration")
(define bson-list
 (let ((cursor (mongo-find "test.cards")))
   (let loop ((lst '()))
     (cond
      ((mongo-cursor-next! cursor)
       ;;(print "iteration x")
            ;;(mongo-print (mongo-cursor-current cursor))
            (loop (cons (mongo-cursor-current cursor) lst)))
      (else lst)))))

(include "bson.scm")
;;(for-each (lambda (b) (mongo-print b)) bson-list)

;; (print "connecting")
;; (mongo-client "127.0.0.1" 27017)
;; (thread-sleep! 1)
;; (print "done")

;; (write "runnin main2")
;; (print ((foreign-lambda int main2)))


`(begin (current-mongo (mongo "localhost" 1356))
        (current-mongo-database "logging")
        (cursor-for-each
         (lambda (x) (print x))
         (mongo-find "logging.swipe" `(name: "Kristian"))))
(expand `(define-record-type foobar (%make-mongo blob) (%mongo-record?)))
