+++
date = "2015-03-19T14:27:51-04:00"
title = "Documents"
[menu.main]
  parent = "BSON"
  weight = 20
  pre = "<i class='fa'></i>"
+++

## Documents

The scala driver includes two scala specific representations for BSON documents. Following convention from the scala collections library,
there are immutable and mutable implementations of documents. The underlying implementations of the scala Document use the type safe 
[`BsonDocument`]({{< coreapiref "org/bson/BsonDocument" >}}) class. The scala bson classes are available from the `org.mongodb.scala.bson` 
namespace, which includes type aliases and companion objects.  In general this should suffice but for advanced use cases you may need to 
use `org.bson` directly.

{{% note %}}
The scala `Document` classes implement `TraversableLike[(String, BsonValue)]` and the general API mirrors that of a `Map[String, BsonValue]`.
However, unlike `Map` implementations of `TraversableLike` enables strict type safety as there is no variance in the value type.
{{% /note %}}

[`BsonValue`]({{< apiref "org/mongodb/scala/bson/index" >}}) is the type safe representation of a Bson type from the `org.bson` library, it represents specific value types. The most commonly used value types are: 
   
| BSON type | Scala type                        |
|-----------|-----------------------------------|
| Document  | `org.mongodb.scala.bson.Document` |
| Array     | `List`                            |
| Date      | `Date` or `int` (ms since epoch)  |
| Boolean   | `Boolean`                         |
| Double    | `Double`                          |
| Int32     | `Integer`                         |
| Int64     | `Long`                            |
| String    | `String`                          |
| Binary    | `Array[Byte]`                     |
| ObjectId  | `ObjectId`                        |
| Null      | `None`                            |

It is actually possible to change or extend these mappings, this will be covered in detail below.

There are two main `Document` classes:

## Immutable Documents

Like the Scala collections library the immutable class is the favoured class.  For convenience it is aliased to `org.mongodb.scala.Document`
and `org.mongodb.scala.bson.Document` as well as being available from `org.mongodb.scala.bson.collection.immutable.Document`. Instances of 
this type are guaranteed to be immutable for everyone. Such a collection will never change after it is created. Therefore, you can rely on 
the fact that accessing the same collection value repeatedly at different points in time will always yield a collection with the same elements.

```scala
import org.mongodb.scala.bson._

val doc1 = Document("AL" -> BsonString("Alabama"))
val doc2 = doc1 + ("AK" -> BsonString("Alaska"))
val doc3 = doc2 ++ Document("AR" -> BsonString("Arkansas"), "AZ" -> BsonString("Arizona"))
```

## Mutable Documents

To get the mutable `Document` version, you need to import it explicitly from `org.mongodb.scala.collections.mutable.Document`.  The mutable 
`Document` can be updated or extended in place. This means you can change, add, or remove elements of the `Document` as a side effect. Like 
scala collections, when dealing with mutable types you need to understand which code changes which collection and when.

```scala
import org.mongodb.scala.bson._
import org.mongodb.scala.bson.collection.mutable.Document

val doc = Document("AL" -> BsonString("Alabama"))
val doc1 = doc + ("AK" -> BsonString("Alaska"))   // doc not mutated but new doc created
doc1 ++= Document("AR" -> BsonString("Arkansas"), 
                  "AZ" -> BsonString("Arizona"))  // doc1 mutated as ++= changes in place. 
```

## Implicit conversions

For many of the `BsonValue` types there are obvious direct mappings from a Scala type. For example, a `String` maps to `BsonString`, an `Int`
maps to `BsonInt32` and a `Long` maps to a `BsonInt64`.  For convenience these types can be used directly with `Documents` and they are 
converted by the contract traits in the [`BsonMagnets`]({{< apiref "org/mongodb/scala/bson/BsonMagnets$" >}}) object. As long as there is
an implicit [`BsonTransformer`]({{< apiref "org/mongodb/scala/bson/BsonTransformer" >}}) in scope for any given type, then that type can be 
converted into a `BsonValue`.

The following `BsonTransformers` are in scope by default:


| Scala type            |      | BsonValue                                        |
|-----------------------|------|--------------------------------------------------|
| `Boolean`             | `=>` | `BsonBoolean`                                    |
| `String`              | `=>` | `BsonString`                                     |
| `Array[Byte]`         | `=>` | `BsonBinary`                                     |
| `Regex`               | `=>` | `BsonRegex`                                      |
| `Date`                | `=>` | `BsonDateTime`                                   |
| `ObjectId`            | `=>` | `BsonObjectId`                                   |
| `Int`                 | `=>` | `BsonInt32`                                      |
| `Long`                | `=>` | `BsonInt64`                                      |
| `Double`              | `=>` | `BsonDouble`                                     |
| `None`                | `=>` | `BsonNull`                                       |
| `immutable.Document`  | `=>` | `BsonDocument`                                   |
| `mutable.Document`    | `=>` | `BsonDocument`                                   |
| `Option[T]`           | `=>` | `BsonValue` where `T` has a `BsonTransformer`    |
| `Seq[(String, T)]`    | `=>` | `BsonDocument` where `T` has a `BsonTransformer` |
| `Seq[T]`              | `=>` | `BsonArray` where `T` has a `BsonTransformer`    |
| `BsonValue`           | `=>` | `BsonValue`                                      |



```scala
import org.mongodb.scala.Document

val doc1 = Document("AL" -> "Alabama")
val doc2 = doc1 + ("AK" -> "Alaska")
val doc3 = doc2 ++ Document("AR" -> "Arkansas", "population" -> 2.966)
```

This is achieved by making use of the _"Magnet Pattern"_:

> The magnet pattern is an alternative approach to method overloading. Rather than defining several identically named methods with different parameter lists you define only one method with only one parameter.
> <br><br>
> This parameter is called the magnet. Its type is the magnet type, a dedicated type constructed purely as the target of a number of implicit conversions defined in the magnets companion object, which are called the magnet branches and which model the various “overloads”.

Source: [The Magnet Pattern](http://spray.io/blog/2012-12-13-the-magnet-pattern/)


In the API where we would normally expect a single value or a key value pair or many key value pairs eg: (`BsonValue`, `(String, BsonValue)` 
or `Iterable[(String, BsonValue)]`) we require anything that can become those types via _"`CanBeX`"_ traits that handle the implicit 
conversions necessary to conform to the correct types. These traits are [`CanBeBsonValue`]({{< apiref "org/mongodb/scala/bson/BsonMagnets$$CanBeBsonValue" >}}), 
[`CanBeBsonElement`]({{< apiref ""org/mongodb/scala/bson/BsonMagnets$$CanBeBsonElement" >}}) and
[`CanBeBsonElements`]({{< apiref ""org/mongodb/scala/bson/BsonMagnets$$CanBeBsonElements" >}}). 

One such example is adding a key value pair to a Document or a list of values:

```scala
val doc1 = Document("AL" -> "Alabama")
val doc2 = Document("codes" -> List("AL", "AK", "AR"))
```

### Bson

The driver also contains a small but powerful interface called `Bson`. Any class 
that represents a BSON document, whether included in the driver itself or from a third party, can implement this interface and can then 
be used any place in the high-level API where a BSON document is required. For example:

```scala
collection.find(Document("x" -> 1))
collection.find(Filters.eq("x", 1))
```
