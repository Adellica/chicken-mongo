
(module bson (bson bson->blob blob->bson bson->obj
                   oid oid-blob oid?)

(import chicken scheme foreign)
(include "bson-impl.scm")

)
