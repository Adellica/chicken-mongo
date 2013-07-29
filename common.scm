

(define-foreign-type mongo-return-success? int
  (lambda (x) (error "passing status as argument unexpeced"))
  (lambda (int-status) (if (= (foreign-value "MONGO_OK" int) int-status) #t #f)))

;; shortcut version of foreign-lambda:

(define-syntax define-foreign-lambda
  (syntax-rules ()
   ((_ rtype name atypes ...)
    (define name (foreign-lambda rtype name atypes ...)))))


(define sizeof-mongo         (foreign-value "sizeof(mongo)"         int))
(define sizeof-mongo-cursor  (foreign-value "sizeof(mongo_cursor)"  int))


(define sizeof-bson          (foreign-value "sizeof(bson)"          int))
(define sizeof-bson-iterator (foreign-value "sizeof(bson_iterator)" int))


;; helper functions
(define (copy-blob pointer length)
  (let ((blob (make-blob length)))
    (move-memory! pointer blob length)
    blob))


(define-foreign-type
  bson
  (c-pointer "bson")
  (lambda (x) (let ((blob/ptr (bson-blob x)))
           (cond ((blob? blob/ptr) (location blob/ptr))
                 ((pointer-like? blob/ptr) blob/ptr)
                 (else (error "unknown bson blob" blob/ptr)))))
  (lambda (x) (make-bson x)))

(define-foreign-lambda int bson_size bson)
