(use miscmacros bson)

;; how about running this with valgrind
;; and see if anything leaks or is broken

(include "all-types.scm")

(repeat 1000 (bson->obj (bson all-types)))
(repeat 1000 (bson->obj (blob->bson (bson->blob (bson all-types)))))
(gc #t)
