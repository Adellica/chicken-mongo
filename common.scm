

(define-foreign-type mongo-return-success? int
  (lambda (x) (error "passing status as argument unexpeced"))
  (lambda (int-status) (if (= (foreign-value "MONGO_OK" int) int-status) #t #f)))
