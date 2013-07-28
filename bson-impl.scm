(use srfi-1
     srfi-4
     lolevel ;; for move-memory!
     data-structures
     extras)

;; memory owner summary:
;;
;; bson struct: gc (bson-record with make-blob)
;; bson data-pointer: non-gc
;; bson stack-pointer: either (gc / non-gc)
;;               gc: points to the bson struct (32 stack elements
;;                   embedded in struct bson)
;;               non-gc: dynamically alloced memory, owned by c driver
;; the stackPtr struct field can point to either of those. the
;; stackPtr gets promoted in this order: 0 (no stack), -1 (embedded
;; stack), other (dynamically allocated stack)

#>
#include <stdio.h>
#include "mongo.h"
<#


(include "common.scm")

(define-record-type bson
  (%make-bson data)
  %bson?
  (data %bson-data))

(define-foreign-type
  bson
  scheme-pointer
  (lambda (x) (%bson-data x)) ;; <-- don't use u8vectors with scheme-pointers!
  (lambda (x) (%make-bson x)))


;; oid's
(define-record-type oid
  (%make-oid blob)
  oid?
  (blob oid-blob))

(define-record-printer (oid x out)
  (format out "#<oid ~A>" (oid-blob x)))

(define-foreign-type oid (c-pointer "bson_oid_t")
  (lambda (oid)
    (assert (= (number-of-bytes (oid-blob oid)) 12))
    (location (oid-blob oid)))
  (lambda (ptr) (%make-oid (copy-blob ptr 12))))

(define (oid blob)
  (assert (blob? blob))
  (%make-oid blob))

;; --

(define-foreign-type bson_bool_t int)

(define-syntax define-bson-op
  (syntax-rules ()
    ((_ name atypes ...)
     (define-foreign-lambda mongo-return-success? name bson atypes ...))))

(define-syntax define-appender
  (syntax-rules ()
    ((_ name args ...)
     ;; return-type func operand name
     (define-bson-op name    c-string args ...))))

(define bson_init (foreign-lambda mongo-return-success? bson_init bson))
(define-foreign-lambda mongo-return-success? bson_finish bson)
(define-foreign-lambda void bson_destroy bson)


(define-foreign-lambda int bson_size bson)

(define-appender bson_append_int    int)
(define-appender bson_append_long   integer64)
(define-appender bson_append_double double)
(define-appender bson_append_string c-string)
(define-appender bson_append_symbol c-string)
(define-appender bson_append_bool   bool)

(define-appender bson_append_null)
(define-appender bson_append_undefined)

(define-appender bson_append_start_object)
(define-appender bson_append_start_array)

(define-bson-op bson_append_finish_object)
(define-bson-op bson_append_finish_array)

;; create an initialized, unfinished bson record with finalizers
(define stack-ptr (foreign-lambda* int ((bson b)) "return ( ((bson*)b)->stackPtr );"))

(define (bson-init)
  ;;                                                 init nongc free?
  (let ((bson (%make-bson (make-blob sizeof-bson))))
    (set-finalizer! bson (lambda (x)
                           (print "destrying bson with strp " (stack-ptr x))
                           (bson_destroy x)))
    (bson_init bson)
    bson))

(define (bson obj)
  (let ((b (bson-init))) ;; <- everyone mutates this (must be finished)
    (let recurse ((obj obj))
      (for-each
       (lambda (pair)
         (let* ((key (car pair))
                (key (if (string? key) key (symbol->string key)))
                (val (cdr pair)))
           (cond

            ((string? val) (bson_append_string b key val))
            ((symbol? val) (bson_append_symbol b key (symbol->string val)))

            ((flonum? val) (bson_append_double b key val))
            ;; TODO use int for small values?
            ((fixnum? val) (bson_append_long b key val))

            ((boolean? val) (bson_append_bool b key val))

            ((eq? (void) val) (bson_append_null b key))

            ((pair? val)
             (bson_append_start_object b key)
             (recurse val)
             (bson_append_finish_object b))

            ((vector? val)
             (bson_append_start_array b key)
             ;; TODO: make this not suck completely:
             (recurse (map (lambda (v i) (cons (number->string i) v))
                           (vector->list val)
                           (iota (vector-length val))))
             (bson_append_finish_array b))

            (else (error "don't know how to convert to bson" val)))))
       obj))
    (bson_finish b)
    b))

(define-record-type bson-iterator
  (%make-bson-iterator blob)
  %bson-iterator?
  (blob %bson-iterator-blob))

(define-foreign-type bson-iterator (c-pointer "bson_iterator")
  (lambda (x) (location (%bson-iterator-blob x)))
  (lambda (x) (error "cannot return bson_iterator yet")))


(define-syntax define-bson_types
  (syntax-rules ()
    ((_) (void))
    ((_ name rest ...) (begin (define name (foreign-value name int))
                              (define-bson_types rest ...)))))


(define-bson_types
  BSON_EOO      BSON_DOUBLE     BSON_STRING     BSON_OBJECT
  BSON_ARRAY    BSON_BINDATA    BSON_UNDEFINED  BSON_OID
  BSON_BOOL     BSON_DATE       BSON_NULL       BSON_REGEX
  BSON_DBREF    BSON_CODE       BSON_SYMBOL     BSON_CODEWSCOPE
  BSON_INT      BSON_TIMESTAMP  BSON_LONG       BSON_MAXKEY
  BSON_MINKEY)


(define-foreign-type bson_type int)
(define-foreign-lambda void bson_iterator_init bson-iterator bson)

(define-syntax define-bson-iterator-operation
  (syntax-rules ()
    ((_) (void))
    ((_ type name rest ...)
     (begin (define-foreign-lambda type name bson-iterator)
            (define-bson-iterator-operation rest ...)))))


(define-bson-iterator-operation
  bson_type bson_iterator_more
  bson_type bson_iterator_next
  bson_type bson_iterator_type

  c-string     bson_iterator_key
  c-string     bson_iterator_string
  bson_bool_t  bson_iterator_bool_raw
  double       bson_iterator_double_raw
  int          bson_iterator_int_raw
  long         bson_iterator_long_raw
  int          bson_iterator_time ;; todo
  oid          bson_iterator_oid
  )
(define-foreign-lambda void bson_iterator_subiterator
  bson-iterator  ;; original iterator
  bson-iterator) ;; new sub-iterator destination


(define (init-bson-iterator bson)
  (let ((it (%make-bson-iterator (make-blob sizeof-bson-iterator))))
    ;; no finalizer (iterators don't need to be destroyed?).
    ;; WARNING:
    ;; iterators don't keep a reference to the bson owner (only its
    ;; data, which is non-gc (because we init_with_copy or get it the
    ;; bson object from the bson-driver itself) ). still, we need to
    ;; take care with this!
    (bson_iterator_init it bson)
    it))

(define (init-bson-subiterator it)
  (let ((subit (%make-bson-iterator (make-blob sizeof-bson-iterator))))
    (bson_iterator_subiterator it subit)
    subit))

(define (bson->obj bson)
  (let obj-loop ((i (init-bson-iterator bson)))
    (let loop ((res '()))
      (bson_iterator_next i)
      (let ((type (bson_iterator_type i)))
        (if (= 0 type)
            (reverse res)
            (let ((value
                   (cond ((= BSON_STRING type) (bson_iterator_string i))
                         ((= BSON_BOOL type)   (if (= 0 (bson_iterator_bool_raw i)) #f #t))
                         ((= BSON_DOUBLE type) (bson_iterator_double_raw i))
                         ((= BSON_INT type)    (bson_iterator_int_raw i))
                         ((= BSON_LONG type)   (bson_iterator_long_raw i))
                         ((= BSON_NULL type)   (void))

                         ((= BSON_OID type)    (bson_iterator_oid i))

                         ((= BSON_OBJECT type) (obj-loop (init-bson-subiterator i)))
                         ((= BSON_ARRAY type)  (list->vector (map cdr (obj-loop (init-bson-subiterator i)))))
                         ((= 0 type) #f)
                         (else (error "unknown type" type))))
                  (key (string->symbol (bson_iterator_key i))))
              (loop (cons (cons key  value) ;; new pair
                          res               ;; rest of alist
                          ))))))))
;; convert a bson object into a string-blob
(define (bson->blob b #!optional (maker make-string))
  (let* ((len (bson_size b))
         (blob (maker len)))
    ;; copy data content
    (move-memory! ((foreign-lambda* c-pointer ((bson b)) "return(((bson*)b)->data);") b) ;; TODO use api call
                  blob
                  len)
    blob))

;; we assume that all data pointed to be a bson-struct is non-gc
;; (affects the iterator, for example)
(define-foreign-lambda mongo-return-success? bson_init_finished_data_with_copy bson scheme-pointer)

(define (blob->bson bson-blob)
  (assert (or (string? bson-blob) (blob? bson-blob)))
  (let ((b (%make-bson (make-blob sizeof-bson))))
    (or (bson_init_finished_data_with_copy b bson-blob)
        (error "could not initialize bson with data" bson-blob))
    (set-finalizer! b (lambda (x)
                        (print "destrying copied bson with strp " (stack-ptr x))
                        (bson_destroy x)))
    b))


;; debugging utils
(define mongo-print (foreign-lambda void "bson_print" bson))
