(use bitstring)

(define (obj->bsontype obj)
  (cond ((flonum? obj) #x01)
        ((string? obj) #x02)
        ((pair? obj)   #x03)
        ((vector? obj) #x04)
        ((blob? obj)   #x05)
        (else (error "unknown bson type" obj))))

(define ($serialize obj)
  (cond ((flonum? obj) (bitconstruct (obj double)))
        ((string? obj) (bitconstruct ((string-length obj) 32 little)
                                     ((string->bitstring obj) bitstring)
                                     (0 8)))
        (else (error "don't know how to serialize" obj))))

(define ($document alist)
  (apply bitstring-append
         (map (lambda (pair)
                (bitconstruct ((obj->bsontype (cdr pair)) 8) ;; type (1 byte)
                              ((string->bitstring (car pair)) bitstring) ;; key
                              (0 8) ;; key delimiter (cstring)
                              (($serialize (cdr pair)) bitstring)))
              alist)))


(define (document alist)
  (let ((bs ($document alist)))
    (bitconstruct ((+ 5 (fx/ (bitstring-length bs) 8)) 32 little) ;; 4 + 1
                  (bs bitstring)
                  (0 8))))

(define obj `(("key-a" . "b")
              ("key-c" . "d")))
(define expected (bson->blob (bson obj)))

(blob->string (bditstring->blob (document obj)))
