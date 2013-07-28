

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
