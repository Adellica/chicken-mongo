(use test bson)

(include "all-types.scm")

;; test bson and back
(test all-types (bson->obj (bson all-types)))

;; test bson and back, via blob
(test all-types (bson->obj (blob->bson (bson->blob (bson all-types)))))


(test-exit)
