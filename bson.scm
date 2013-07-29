
(module bson (bson bson->blob blob->bson bson->obj
                   bson? make-bson bson-blob
                   oid oid-blob oid?)

(import chicken scheme foreign)
(include "bson-impl.scm")

)
