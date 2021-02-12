+++
date = "2015-03-17T15:36:56Z"
title = "Quick Start"
[menu.main]
  parent = "MongoDB Scala Driver"
  identifier = "Scala Quick Start"
  weight = 5
  pre = "<i class='fa'></i>"
+++

# MongoDB Scala Driver Quick Start

The following code snippets come from the [`QuickTour.java`]({{< srcref "driver-scala/src/it/tour/QuickTour.java" >}}) example code
that can be found with the Scala driver source on github.

{{% note %}}
See the [installation guide]({{< relref "driver-scala/getting-started/installation.md" >}}) for instructions on how to install the MongoDB Scala Driver.
{{% /note %}}

{{% note class="important" %}}
This guide uses the `Observable` implicits as covered in the [Quick Start Primer]({{< relref "driver-scala/getting-started/quick-start-primer.md" >}}).
{{% /note %}}


## Prerequisites

- A running MongoDB on localhost using the default port for MongoDB `27017`

- MongoDB Scala Driver.  See [Installation]({{< relref "driver-scala/getting-started/installation.md" >}}) for instructions on how to install the MongoDB driver.

- The following import statements:

New MongoClient API (since 3.7):

```scala
import org.mongodb.scala._
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Projections._
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala.model.Updates._
import org.mongodb.scala.model._

import scala.collection.JavaConverters._
```

## Make a Connection

Use the [`MongoClient`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoClient$" >}}) companion object to make a connection to a running MongoDB instance.

The `MongoClient` instance represents a pool of connections to the database; you will only need one instance of class `MongoClient` even with multiple concurrent operations.

{{% note class="important" %}}

 Typically you only create one `MongoClient` instance for a given MongoDB deployment (e.g. standalone, replica set, or a sharded cluster) and use it across your application. However, if you do create multiple instances:

 - All resource usage limits (e.g. max connections, etc.) apply per `MongoClient` instance.

 - To dispose of an instance, call `MongoClient.close()` to clean up resources.
{{% /note %}}


### Connect to a Single MongoDB instance

The following example shows several ways to connect to a single MongoDB server.

To connect to a single MongoDB instance:

- You can instantiate a MongoClient object without any parameters to connect to a MongoDB instance running on localhost on port ``27017``:

```scala
val mongoClient: MongoClient = MongoClient()
```

- You can explicitly specify the hostname to connect to a MongoDB instance running on the specified host on port ``27017``:

```scala
val mongoClient: MongoClient = MongoClient(
  MongoClientSettings.builder()
    .applyToClusterSettings((builder: ClusterSettings.Builder) => builder.hosts(List(new ServerAddress("hostOne")).asJava))
    .build())
```

- You can explicitly specify the hostname and the port:

```scala
val mongoClient: MongoClient = MongoClient(
  MongoClientSettings.builder()
    .applyToClusterSettings((builder: ClusterSettings.Builder) => builder.hosts(List(new ServerAddress("hostOne", 27017)).asJava))
    .build())
```

- You can specify the [`ConnectionString`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/package$$ConnectionString$.html" >}}):

```scala
val mongoClient: MongoClient = MongoClient("mongodb://hostOne:27017")
```

## Access a Database

Once you have a ``MongoClient`` instance connected to a MongoDB deployment, use the [`MongoClient.getDatabase()`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoClient$.html#getDatabase-java.lang.String-" >}}) method to access a database.

Specify the name of the database to the ``getDatabase()`` method. If a database does not exist, MongoDB creates the database when you first store data for that database.

The following example accesses the ``mydb`` database:

```scala
 val database: MongoDatabase = mongoClient.getDatabase("mydb")
 ```

`MongoDatabase` instances are immutable.

## Access a Collection

Once you have a `MongoDatabase` instance, use its the [`getCollection(String collectionName)`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoDatabase.html#getCollection[TResult](collectionName:String)(implicite:org.mongodb.scala.bson.DefaultHelper.DefaultsTo[TResult,org.mongodb.scala.Document],implicitct:scala.reflect.ClassTag[TResult]):org.mongodb.scala.MongoCollection[TResult]" >}})
method to access a collection.

Specify the name of the collection to the `getCollection()` method. If a collection does not exist, MongoDB creates the collection when you first store data for that collection.

For example, using the `database` instance, the following statement accesses the collection named `test` in the `mydb` database:

```scala
val collection: MongoCollection[Document] = database.getCollection("test")
```

`MongoCollection` instances are immutable.

## Create a Document

To create the document using the Scala driver, use the [`Document`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/bson/collection/immutable/Document">}}) class.

For example, consider the following JSON document:

```javascript
  {
   "name" : "MongoDB",
   "type" : "database",
   "count" : 1,
   "info" : { x : 203, y : 102 }
  }
```

Using the [`Document`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/bson/collection/immutable/Document" >}}) class:

```scala
val doc: Document = Document("_id" -> 0, "name" -> "MongoDB", "type" -> "database",
                             "count" -> 1, "info" -> Document("x" -> 203, "y" -> 102))
```


## Insert a Document

Once you have the `MongoCollection` object, you can insert documents into the collection.

### Insert One Document

To insert the document into the collection, use the `insertOne()` method. Using the `results()` implicit helper we block until the observer is completed:

```scala
collection.insertOne(doc).results()
```

{{% note class="warning" %}}
The Scala driver provides two document types - an immutable [`Document`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/bson/collection/immutable/Document" >}}) 
and a mutable [`Document`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/bson/collection/mutable/Document" >}}).

When using an immutable document then you should explicitly add an `_id` value, if you need to know that `_id` value in the future.

If an `_id` is not present on insertion then driver will add one automatically create one and pass it to the server, but that `_id` will not be 
passed back to the user.
{{% /note %}}

{{% note class="important" %}}
In the API all methods returning a `Observables` are "cold" streams meaning that nothing happens until they are subscribed to.

The example below does nothing:

```scala
val observable: Observable[InsertOneResult] = collection.insertOne(doc)
```

Only when a `Observable` is subscribed to and data requested will the operation happen:

```scala
observable.subscribe(new Observer[InsertOneResult] {

  override def onSubscribe(subscription: Subscription): Unit = subscription.request(1)

  override def onNext(result: InsertOneResult): Unit = println(s"onNext $result")

  override def onError(e: Throwable): Unit = println("Failed")
  
  override def onComplete(): Unit = println("Completed")
})
```

Once the document has been inserted the `onNext` method will be called and it will
print "Inserted!" followed by the result. Finally the `onComplete` method will print "Completed".  
If there was an error for any reason the `onError` method would print "Failed"..

{{% /note %}}

### Insert Multiple Documents

To add multiple documents, you can use the collection's [`insertMany()`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html#insertMany-java.util.List-" >}}) method which takes a list of documents to insert.

The following example will add multiple documents of the form:

```javascript
{ "i" : value }
```

Create the documents in a loop.

```scala
val documents = (1 to 100) map { i: Int => Document("i" -> i) }
```

To insert these documents to the collection, pass the list of documents to the
`insertMany()` method.

```scala
val insertObservable = collection.insertMany(documents)
```

As we haven't subscribed yet no documents have been inserted, lets chain together two operations, inserting and counting.

## Count Documents in A Collection

Once we've inserted the `documents` list we should, have a total of 101 documents in the collection (the 100 we did in the loop, plus
the first one). We can check to see if we have them all using the
[count()]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html#count():org.mongodb.scala.Observable[Long]" >}}) method. 

Lets chain the two operations together using a [`for`](http://docs.scala-lang.org/tutorials/tour/sequence-comprehensions.html) comprehension. The 
following code should insert the documents then count the number of documents and print the results:

```scala
val insertAndCount = for {
  insertResult <- insertObservable
  countResult <- collection.countDocuments()
} yield countResult

println(s"total # of documents after inserting 100 small ones (should be 101):  ${insertAndCount.headResult()}")
```

## Query the Collection

To query the collection, you can use the collection's [`find()`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html#find[C](filter:org.bson.conversions.Bson)(implicite:org.mongodb.scala.bson.DefaultHelper.DefaultsTo[C,org.mongodb.scala.collection.immutable.Document],implicitct:scala.reflect.ClassTag[C]):org.mongodb.scala.FindObservable[C]" >}}) method. You can call the method without any arguments to query all documents in a collection or pass a filter to query for documents that match the filter criteria.

### Find the First Document in a Collection

To return the first document in the collection, use the [`find()`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html#find[C](filter:org.bson.conversions.Bson)(implicite:org.mongodb.scala.bson.DefaultHelper.DefaultsTo[C,org.mongodb.scala.collection.immutable.Document],implicitct:scala.reflect.ClassTag[C]):org.mongodb.scala.FindObservable[C]" >}}) method without any parameters and chain to `find()` method the [`first()`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/FindObservable.html#first():org.mongodb.scala.Observable[TResult]" >}}) method.

{{% note class="tip" %}}
The `find().first()` construct is useful for queries that should only match a single document or if you are interested in the first document only.
{{% /note %}}

The following example prints the first document found in the collection.

```scala
collection.find().first().printHeadResult()
```

The example should print the following document:

```json
{ "_id" : { "$oid" : "551582c558c7b4fbacf16735" },
  "name" : "MongoDB",
  "type" : "database",
  "count" : 1,
  "info" : { "x" : 203, "y" : 102 } }
```

{{% note %}}

The `_id` element has been added automatically by MongoDB to your
document and your value will differ from that shown. MongoDB reserves field
names that start with
`"_"` and `"$"` for internal use.
{{% /note %}}

### Find All Documents in a Collection

To retrieve all the documents in the collection, we will use the
`find()` method. The `find()` method returns a `FindObservable` instance that
provides a fluent interface for chaining or controlling find operations. 
The following code retrieves all documents in the collection and prints them out
(101 documents):

```scala
collection.find().printResults()
```

## Specify a Query Filter

To query for documents that match certain conditions, pass a filter object to the `find()` method. To facilitate creating filter objects, the Scala driver provides the [`Filters`]({{< relref "builders/filters.md" >}}) helper.

### Get A Single Document That Matches a Filter

For example, to find the first document where the field ``i`` has the value `71`, pass an [`equal`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/model/Filters$.html#equal[TItem](fieldName:String,value:TItem):org.mongodb.scala.bson.conversions.Bson" >}}) filter object to specify the equality condition:

```scala
collection.find(equal("i", 71)).first().printHeadResult()
```
The example prints one document:

```json
{ "_id" : { "$oid" : "5515836e58c7b4fbc756320b" }, "i" : 71 }
```

### Get All Documents That Match a Filter

The following example returns and prints all documents where ``"i" > 50``:

```scala
// now use a range query to get a larger subset
collection.find(gt("i", 50)).printResults()
```
which should print the documents where `i > 50`.

To specify a filter for a range, such as ``50 < i <= 100``, you can use the [`and`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/model/Filters$.html#and(filters:org.mongodb.scala.bson.conversions.Bson*):org.mongodb.scala.bson.conversions.Bson" >}}) helper:

```scala
collection.find(and(gt("i", 50), lte("i", 100))).printResults()
```

## Update Documents

To update documents in a collection, you can use the collection's 
[`updateOne`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html#updateOne(filter:org.bson.conversions.Bson,update:org.bson.conversions.Bson):org.mongodb.scala.Observable[org.mongodb.scala.result.UpdateResult]" >}}) and [`updateMany`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html#updateMany(filter:org.bson.conversions.Bson,update:org.bson.conversions.Bson):org.mongodb.scala.Observable[org.mongodb.scala.result.UpdateResult]" >}}) methods.

Pass to the methods:

- A filter object to determine the document or documents to update. To facilitate creating filter objects, the Scala driver provides the [`Filters`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/model/Filters$" >}}) helper. To specify an empty filter (i.e. match all documents in a collection), use an empty `Document` object.

- An update document that specifies the modifications. For a list of the available operators, see [update operators]({{<docsref "reference/operator/update-field" >}}).

The update methods return an [`UpdateResult`]({{< apiref "mongodb-driver-core" "com/mongodb/client/result/UpdateResult.html" >}}) which provides information about the operation including the number of documents modified by the update.

### Update a Single Document

To update at most a single document, use the [`updateOne`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html#updateOne(filter:org.bson.conversions.Bson,update:org.bson.conversions.Bson):org.mongodb.scala.Observable[org.mongodb.scala.result.UpdateResult]" >}})

The following example updates the first document that meets the filter ``i`` equals ``10`` and sets the value of ``i`` to ``110``:

```scala
collection.updateOne(equal("i", 10), set("i", 110)).printHeadResult("Update Result: ")
```


### Update Multiple Documents

To update all documents matching the filter, use the [`updateMany`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html#updateMany(filter:org.bson.conversions.Bson,update:org.bson.conversions.Bson):org.mongodb.scala.Observable[org.mongodb.scala.result.UpdateResult]" >}}) method.

The following example increments the value of ``i`` by ``100`` for all documents where  =``i`` is less than ``100``:

```scala
collection.updateMany(lt("i", 100), inc("i", 100)).printHeadResult("Update Result: ")
```

## Delete Documents

To delete documents from a collection, you can use the collection's[`deleteOne`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html#deleteOne(filter:org.bson.conversions.Bson):org.mongodb.scala.Observable[org.mongodb.scala.result.DeleteResult]" >}}) and [`deleteMany`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html#deleteMany(filter:org.bson.conversions.Bson):org.mongodb.scala.Observable[org.mongodb.scala.result.DeleteResult]" >}}) methods.

Pass to the methods a filter object to determine the document or documents to delete. To facilitate creating filter objects, the Scala driver provides the [`Filters`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/model/Filters$.html" >}}) helper. To specify an empty filter (i.e. match all documents in a collection), use an empty [`Document`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/bson/index.html#Document:org.mongodb.scala.bson.collection.immutable.Document.type" >}}) object.

The delete methods return a [`DeleteResult`]({{< apiref "mongodb-driver-core" "com/mongodb/client/result/DeleteResult.html" >}})
which provides information about the operation including the number of documents deleted.

### Delete a Single Document That Match a Filter

To delete at most a single document that match the filter, use the[`deleteOne`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html#deleteOne(filter:org.bson.conversions.Bson):org.mongodb.scala.Observable[org.mongodb.scala.result.DeleteResult]" >}}) method:

The following example deletes at most one document that meets the filter ``i`` equals ``110``:

```scala
collection.deleteOne(equal("i", 110)).printHeadResult("Delete Result: ")
```

### Delete All Documents That Match a Filter

To delete all documents matching the filter use the [`deleteMany`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html#deleteMany(filter:org.bson.conversions.Bson):org.mongodb.scala.Observable[org.mongodb.scala.result.DeleteResult]" >}}) method.

The following example deletes all documents where ``i`` is greater or equal to ``100``:

```scala
collection.deleteMany(gte("i", 100)).printHeadResult("Delete Result: ")
```

## Create Indexes

To create an index on a field or fields, pass an index specification document to the [`createIndex()`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html#createIndex(key:org.mongodb.scala.bson.conversions.Bson):org.mongodb.scala.SingleObservable[String]" >}}) method. An index key specification document contains the fields to index and the index type for each field:

```scala
Document(<field1> -> <type1>, <field2>, <type2>, ...)
```

- For an ascending index type, specify ``1`` for ``<type>``.
- For a descending index type, specify ``-1`` for ``<type>``.

The following example creates an ascending index on the ``i`` field:

```scala
collection.createIndex(Document("i" -> 1)).printHeadResult("Create Index Result: %s")
```

For a list of other index types, see [Create Indexes]({{< ref "driver-scala/tutorials/indexes.md" >}})

### Additional Information

For additional tutorials about using MongoDB with Case Classes, see the [Case Class Quick Start]({{< ref "driver-scala/getting-started/quick-start-case-class.md" >}}).

For additional tutorials (such as to use the aggregation framework, specify write concern, etc.), see [Scala Driver Tutorials]({{< ref "driver-scala/tutorials/index.md" >}})
