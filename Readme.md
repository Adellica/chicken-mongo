  [Chicken Scheme]:http://call-cc.org

# chicken-mongo

[Chicken Scheme] bindings for [MongoDB](http://mongodb.org). Currently
in experimantal--stage. The bindings wrap the official
[Mongo-C-Driver](https://github.com/mongodb/mongo-c-driver) library.

While most functionality is still missing, it might be a good starting
point for a serious mongodb interface from [Chicken Scheme].

## mongo-client

```scheme
(current-mongo) -> #f
(mongo-find "test.foo") -> #<lazy-seq ...>
(current-mongo) -> #<mongo connected>
```

## bson

Some bson-types are present:

- nested docs (alists)
- array (vectors)
- strings
- doubles
- numbers (long)
- boolean (#f / #t)
- null ((void))


```scheme
(define b (bson `((key1 . "value") (key2 . 1.5))))
b -> #<bson>
(bson->blob (bson `((a . "b")))) -> "\x0e\x00\x00\x00\x02a\x00\x02\x00\x00\x00b\x00\x00"
```
