(use srfi-1)


;; (define bson (mongo-find-one "logging.swipes"))
;; (mongo-print bson)


"\x0f\x00\x00\x00\x05a\x00\x02\x00\x00\x00\x00hi\x00"


(define bson-init!    (foreign-lambda mongo-return-success? "bson_init" bson))
(define bson-finish!  (foreign-lambda mongo-return-success? "bson_finish" bson))
(define bson-destroy! (foreign-lambda void "bson_destroy" bson))

(define-syntax define-bson-op
  (syntax-rules ()
    ((_ name atypes ...)
     (define name (foreign-lambda mongo-return-success? name bson atypes ...)))))

(define-syntax define-appender
  (syntax-rules ()
    ((_ name args ...)
     ;; return-type func operand name
     (define-bson-op name    c-string args ...))))

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
(define (bson-init)
  ;;                                      init nongc free?
  (let* ((blob (make-u8vector sizeof-bson #f   #t    #t))
         (bson (%make-bson (location blob) blob)))
    (set-finalizer! bson (lambda (x)
                           (bson-destroy! x)))
    (bson-init! bson)
    bson))

(define (bson obj)
  (let ((b (bson-init))) ;; <- everyone mutates this
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
    b))

(mongo-print (bson `((key . "string-value!")
                     (number-key . 123.3)
                     (bool-key1 . #f)
                     (bool-key2 . #t)
                     (nested-document . ((kkk1 . 1)
                                         (kkk2 . 3)
                                         (kkk3 . "ooh la la!")
                                         (kkk4 . #f)))
                     (array . #(one two 1 2 #t #f "hello world"))
                     (null . ,(void)))))


