(use srfi-1
     srfi-4
     lolevel ;; for move-memory!
     )

#>
#include <stdio.h>
#include "mongo.h"
<#
(include "common.scm")


(define sizeof-bson          (foreign-value "sizeof(bson)"          int))
(define sizeof-bson-iterator (foreign-value "sizeof(bson_iterator)" int))


(define-record-type bson
  (%make-bson data)
  %bson?
  (data %bson-data))

(define-foreign-type
  bson
  scheme-pointer
  (lambda (x) (%bson-data x)) ;; <-- don't use u8vectors with scheme-pointers!
  (lambda (x) (%make-bson x)))

(define-foreign-type bson_bool_t bool)

(define bson-init!    (foreign-lambda mongo-return-success? "bson_init" bson))
(define bson-finish!  (foreign-lambda mongo-return-success? "bson_finish" bson))
(define bson-destroy! (foreign-lambda void "bson_destroy" bson))


(define-syntax define-foreign-lambda
  (syntax-rules ()
   ((_ rtype name atypes ...)
    (define name (foreign-lambda rtype name atypes ...)))))

(define-syntax define-bson-op
  (syntax-rules ()
    ((_ name atypes ...)
     (define name (foreign-lambda mongo-return-success? name bson atypes ...)))))

(define-syntax define-appender
  (syntax-rules ()
    ((_ name args ...)
     ;; return-type func operand name
     (define-bson-op name    c-string args ...))))

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
                           (bson-destroy! x)))
    (bson-init! bson)
    bson))

(define (bson obj)
  (print "<bson ")
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
    (bson-finish! b)
    (print ">")
    b))

(define-record-type bson-iterator
  (%make-bson-iterator blob)
  %bson-iterator?
  (blob %bson-iterator-blob))

(define-foreign-type bson-iterator (c-pointer "bson_iterator")
  (lambda (x) (location (%bson-iterator-blob x)))
  (lambda (x) (error "cannot return bson_iterator yet")))

(define-foreign-lambda void bson_iterator_init bson-iterator bson)
(define-foreign-lambda bson_bool_t bson_iterator_more bson-iterator)


(define (init-bson-iterator bson)
  (let ((it (%make-bson-iterator (make-blob sizeof-bson-iterator))))
    ;; no finalizer (iterators don't need to be destroyed?).
    ;; WARNING:
    ;; iterators don't keep a reference to the bson owner (only its
    ;; data, which is non-gc). still, we need to take care with this!
    (bson_iterator_init it bson)
    it))

(define (bson->obj bson)
  (let* ((p (%bson-pointer bson))
         (i (init-bson-iterator bson)))
    #f))

(define b (bson `((key . "string-value!")
                  (number-key . 123.3)
                  (bool-key1 . #f)
                  (bool-key2 . #t)
                  (nested-document . ((kkk1 . 1)
                                      (kkk2 . 3)
                                      (kkk3 . "ooh la la!")
                                      (kkk4 . #f)))
                  (array . #(one two 1 2 #t #f "hello world"))
                  (null . ,(void)))))

;; convert a bson object into a string-blob
(define (bson->blob b #!optional (maker make-string))
  (let* ((len (bson_size b))
         (blob (maker len)))
    ;; copy data content
    (move-memory! ((foreign-lambda* c-pointer ((bson b)) "return(((bson*)b)->data);") b)
                  blob
                  len)
    blob))

(bson->blob (bson `((key . "value"))))

(define-foreign-lambda mongo-return-success? bson_init_finished_data_with_copy bson scheme-pointer)

(define (blob->bson bson-blob)
  (assert (or (string? bson-blob) (blob? bson-blob)))
  (let ((b (%make-bson (make-blob sizeof-bson))))
    (or (bson_init_finished_data_with_copy b bson-blob)
        (error "could not initialize bson with data" bson-blob))
    (set-finalizer! bson (lambda (x)
                           (print "destrying copied bson with strp " (stack-ptr x))
                           (bson-destroy! x)))
    b))

(use miscmacros)

`(repeat 10000 (bson `((a . ((aa . 1)
                            (bb . 2))))))
;; (repeat 10000 (bson-init))
;; (gc #t)

(repeat 0
        (let ((bb (stack-ptr (blob->bson (bson->blob b)))))
          (print (stack-ptr b))))


(repeat 0 (bson->blob (bson `((a .  ((aa . 1)
                                     (bb . 2)))))))

(use bitstring)

(bson->blob (bson `((a . "b"))))


