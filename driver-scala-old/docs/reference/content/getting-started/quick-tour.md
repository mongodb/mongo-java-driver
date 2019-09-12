+++
date = "2015-03-17T15:36:56Z"
title = "Quick Tour"
[menu.main]
  parent = "Getting Started"
  identifier = "Quick Tour"
  weight = 30
  pre = "<i class='fa'></i>"
+++

# Quick Tour

The following code snippets come from the `QuickTour.scala` example code that can be found with the 
[driver source]({{< srcref "examples/src/test/scala/tour/QuickTour.scala">}}).

{{% note %}}
See the [installation guide]({{< relref "getting-started/installation-guide.md" >}})
for instructions on how to install the MongoDB Scala Driver.

This guide uses the `Helper` implicits as covered in the [Quick Tour Primer]({{< relref "getting-started/quick-tour-primer.md" >}}).
{{% /note %}}

## Make a Connection

The following example shows multiple ways to connect to the database `mydb` on the local machine, using the 
[`MongoClient`]({{< apiref "org/mongodb/scala/MongoClient$" >}}) companion object.


```scala
import org.mongodb.scala._

// To directly connect to the default server localhost on port 27017
val mongoClient: MongoClient = MongoClient()

// Use a Connection String
val mongoClient: MongoClient = MongoClient("mongodb://localhost")

// or provide custom MongoClientSettings
val settings: MongoClientSettings = MongoClientSettings.builder()
    .applyToClusterSettings(b => b.hosts(List(new ServerAddress("localhost")).asJava).
    .build()
val mongoClient: MongoClient = MongoClient(settings)

val database: MongoDatabase = mongoClient.getDatabase("mydb")
```

At this point, the `database` object will be a connection to a MongoDB server for the specified database.

{{% note %}}
The API only returns implementations of [`Observable[T]`]({{< apiref "org/mongodb/scala/Observable">}}) when network IO required for the 
operation. For `getDatabase("mydb")` there is no network IO required. A `MongoDatabase` instance provides methods to interact with a database
but the database might not actually exist and will only be created on the insertion of data via some means; e.g. the creation of a 
collection or the insertion of documents.
{{% /note %}}

### MongoClient

The [`MongoClient`]({{< apiref "org/mongodb/scala/MongoClient">}}) instance actually represents a pool of connections
for a given MongoDB server deployment; you will only need one instance of class
`MongoClient` even with multiple concurrently executing asynchronous operations.

{{% note class="important" %}}
Typically you only create one `MongoClient` instance for a given database
cluster and use it across your application. When creating multiple instances:

-   All resource usage limits (max connections, etc) apply per `MongoClient` instance
-   To dispose of an instance, make sure you call `MongoClient.close()` to clean up resources
{{% /note %}}

## Get a Collection

To get a collection to operate upon, specify the name of the collection to
the [`getCollection(String collectionName)`]({{< apiref "org/mongodb/scala/MongoDatabase.html#getCollection[TResult](collectionName:String)(implicite:org.mongodb.scala.Helpers.DefaultsTo[TResult,org.mongodb.scala.collection.immutable.Document],implicitct:scala.reflect.ClassTag[TResult]):org.mongodb.scala.MongoCollection[TResult]">}})
method:

The following example gets the collection `test`:

```scala
val collection: MongoCollection[Document] = database.getCollection("test");
```

## Insert a Document

Once you have the collection object, you can insert documents into the
collection. For example, consider the following JSON document; the document
contains a field `info` which is an embedded document:

``` javascript
{
   "name" : "MongoDB",
   "type" : "database",
   "count" : 1,
   "info" : {
               x : 203,
               y : 102
             }
}
```

To create the document using the Scala driver, use the [`Document`]({{< apiref "org/mongodb/scala/bson/collection/immutable/Document">}}) class. You
can use this class to create the embedded document as well.

{{% note class="warning" %}}
The Scala driver provides two document types - an immutable [`Document`]({{< apiref "org/mongodb/scala/bson/collection/immutable/Document">}}) 
and a mutable [`Document`]({{< apiref "org/mongodb/scala/bson/collection/mutable/Document">}}).

When using an immutable document then you should explicitly add an `_id` value, if you need to know that `_id` value in the future.

If an `_id` is not present on insertion then driver will add one automatically create one and pass it to the server, but that `_id` will not be 
passed back to the user.
{{% /note %}}

```scala
val doc: Document = Document("_id" -> 0, "name" -> "MongoDB", "type" -> "database",
                             "count" -> 1, "info" -> Document("x" -> 203, "y" -> 102))
```


To insert the document into the collection, use the `insertOne()` method. 
Using the `results()` implicit we block until the observer is completed:

```scala
collection.insertOne(doc).results();
```

{{% note class="important" %}}
In the API all methods returning a `Observables` are "cold" streams meaning that nothing happens until they are Subscribed to.

The example below does nothing:

```scala
val observable: SingleObservable[Completed] = collection.insertOne(doc)
```

Only when an `Observable` is subscribed to and data requested will the operation happen:

```scala
// Explictly subscribe:
observable.subscribe(new Observer[Completed] {

  override def onNext(result: Completed): Unit = println("Inserted")

  override def onError(e: Throwable): Unit = println("Failed")

  override def onComplete(): Unit = println("Completed")
})
```

Once the document has been inserted the `onNext` method will be called and it will
print "Inserted!" followed by the `onCompleted` method which will print "Completed". If there was an error for any reason the `onError` 
method would print "Failed".

{{% /note %}}

## Add Multiple Documents

To add multiple documents, you can use the `insertMany()` method.

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
[count()]({{< apiref "org/mongodb/scala/MongoCollection.html#count():org.mongodb.scala.Observable[Long]">}}) method. 

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

Use the [find()]({{< apiref "org/mongodb/scala/MongoCollection.html#find[C](filter:org.bson.conversions.Bson)(implicite:org.mongodb.scala.Helpers.DefaultsTo[C,org.mongodb.scala.collection.immutable.Document],implicitct:scala.reflect.ClassTag[C]):org.mongodb.scala.FindObservable[C]">}})
method to query the collection.

### Find the First Document in a Collection

To get the first document in the collection, call the
[first()]({{< apiref "org/mongodb/scala/FindObservable.html#first():org.mongodb.scala.Observable[TResult]">}})
method on the [FindObservable]({{< apiref "org/mongodb/scala/FindObservable">}})
operation. `collection.find().first()` returns the first document or if no document is found the `Observable` just completes.
This is useful for queries that should only match a single document, or if you are interested in the first document only.

Using the `printHeadResult()` implicit we block until the observer is completed and then print the first result:
```scala
collection.find().first().printHeadResult()
```

The example will print the following document:

```json
{ "_id" : 0, "name" : "MongoDB", "type" : "database", "count" : 1, "info" : { "x" : 203, "y" : 102 } }
```

{{% note %}}
The `_id` of the document is "0" as we explicitly set one before we inserted this document.
MongoDB reserves field names that start with "_" and "$" for internal use.
{{% /note %}}

### Find All Documents in a Collection

To retrieve all the documents in the collection, we will use the
`find()` method. The `find()` method returns a `FindObservable` instance that
provides a fluent interface for chaining or controlling find operations. 
The following code retrieves all documents in the collection and prints them out
(101 documents). Using the `printResults()` implicit we block until the observer is completed and then print each result:
```scala
collection.find().printResults()
```

## Get A Single Document with a Query Filter

We can create a filter to pass to the find() method to get a subset of
the documents in our collection. For example, if we wanted to find the
document for which the value of the "i" field is 71, we would do the
following:

```scala
import org.mongodb.scala.model.Filters._

collection.find(equal("i", 71)).first().printHeadResult()
```

will eventually print just one document:

```json
{ "_id" : { "$oid" : "5515836e58c7b4fbc756320b" }, "i" : 71 }
```

{{% note %}}
Use the [`Filters`]({{< relref "builders/filters.md">}}), [`Sorts`]({{< relref "builders/sorts.md">}}),
[`Projections`]({{< relref "builders/projections.md">}}) and [`Updates`]({{< relref "builders/updates.md">}})
helpers for simple and concise ways of building up queries.
{{% /note %}}

## Get a Set of Documents with a Query

We can use the query to get a set of documents from our collection. For
example, if we wanted to get all documents where `"i" > 50`, we could
write:

```scala
collection.find(gt("i", 50)).printResults()
```
which should print the documents where `i > 50`.

We could also get a range, say `50 < i <= 100`:

```scala
collection.find(and(gt("i", 50), lte("i", 100))).printResults()
```

## Sorting documents

We can also use the [Sorts]({{< apiref "org/mongodb/scala/model/Sorts$">}}) helpers to sort documents.
We add a sort to a find query by calling the `sort()` method on a `FindObservable`.  Below we use the [`exists()`]({{< apiref "org/mongodb/scala/model.Filters$.html#exists(fieldName:String,exists:Boolean):org.bson.conversions.Bson">}}) helper and use the sort
[`descending("i")`]({{< apiref "org/mongodb/scala/model.Sorts$.html#descending(fieldNames:String*):org.bson.conversions.Bson">}}) helper to sort our documents:

```scala
import org.mongodb.scala.model.Sorts._

collection.find(exists("i")).sort(descending("i")).first().printHeadResult()
```

## Projecting fields

Sometimes we don't need all the data contained in a document. The [Projections]({{< apiref "org/mongodb/scala/model/Projections$">}}) 
helpers can be used to build the projection parameter for the find operation and limit the fields returned.  
Below we'll sort the collection, exclude the `_id` field and output the first matching document:

```scala
import org.mongodb.scala.model.Projections._

collection.find().projection(excludeId()).first().printHeadResult()
```

## Aggregations

Sometimes we need to aggregate the data stored in MongoDB. The [`Aggregates`]({{< relref "builders/aggregation.md" >}}) helper provides 
builders for each of type of aggregation stage.

Below we'll do a simple two step transformation that will calculate the value of `i * 10`. First we find all Documents 
where `i > 0` by using the [`Aggregates.filter`]({{< relref "builders/aggregation.md#filter" >}}) 
helper. Then we reshape the document by using [`Aggregates.project`]({{< relref "builders/aggregation.md#project" >}}) 
in conjunction with the [`$multiply`]({{< docsref "reference/operator/aggregation/multiply/" >}}) operator to calculate the "`ITimes10`" 
value:

```scala
import org.mongodb.scala.model.Aggregates._

collection.aggregate(Seq(filter(gt("i", 0)),
  project(Document("""{ITimes10: {$multiply: ["$i", 10]}}""")))
).printResults()
```

For [`$group`]({{< relref "builders/aggregation.md#group" >}}) operations use the 
[`Accumulators`]({{< apiref "com/mongodb/client/model/Accumulators" >}}) helper for any 
[accumulator operations]({{< docsref "reference/operator/aggregation/group/#accumulator-operator" >}}). Below we sum up all the values of 
`i` by using the [`Aggregates.group`]({{< relref "builders/aggregation.md#group" >}}) helper in conjunction with the 
[`Accumulators.sum`]({{< apiref "com/mongodb/client/model/Accumulators#sum-java.lang.String-TExpression-" >}}) helper:

```scala
collection.aggregate(List(group(null, sum("total", "$i")))).printHeadResult()
```

{{% note %}}
Currently, there are no helpers for [aggregation expressions]({{< docsref "meta/aggregation-quick-reference/#aggregation-expressions" >}}). 
Use the [`Document.parse()`]({{< relref "bson/extended-json.md" >}}) helper to quickly build aggregation expressions from extended JSON.
{{% /note %}}

## Updating documents

There are numerous [update operators](http://docs.mongodb.org/manual/reference/operator/update-field/)
supported by MongoDB.  We can use the [Updates]({{< apiref "org/mongodb/scala/model/Updates$">}}) helpers to help update documents in the database.

To update at most a single document (may be 0 if none match the filter), use the 
[`updateOne`]({{< apiref "org/mongodb/scala/MongoCollection.html#updateOne(filter:org.bson.conversions.Bson,update:org.bson.conversions.Bson):org.mongodb.scala.Observable[org.mongodb.scala.result.UpdateResult]">}})
method to specify the filter and the update document.  Here we update the first document that meets the filter `i` equals `10` and set the value of `i` to `110`:

```scala
import org.mongodb.scala.model.Updates._

collection.updateOne(equal("i", 10), set("i", 110)).printHeadResult("Update Result: ")
```

To update all documents matching the filter use the [`updateMany`]({{< apiref "org/mongodb/scala/MongoCollection.html#updateMany(filter:org.bson.conversions.Bson,update:org.bson.conversions.Bson):org.mongodb.scala.Observable[org.mongodb.scala.result.UpdateResult]">}})
method.  Here we increment the value of `i` by `100` where `i` is less than `100`.

```scala
collection.updateMany(lt("i", 100), inc("i", 100)).printHeadResult("Update Result: ")
```

The update methods return an [`UpdateResult`]({{< coreapiref "com/mongodb/client/result/UpdateResult.html">}}),
which provides information about the operation including the number of documents modified by the update.

## Deleting documents

To delete at most a single document (may be 0 if none match the filter) use the [`deleteOne`]({{< apiref "org/mongodb/scala/MongoCollection.html#deleteOne(filter:org.bson.conversions.Bson):org.mongodb.scala.Observable[org.mongodb.scala.result.DeleteResult]">}})
method:

```scala
collection.deleteOne(equal("i", 110)).printHeadResult("Delete Result: ")
```

To delete all documents matching the filter use the 
[`deleteMany`]({{< apiref "org/mongodb/scala/MongoCollection.html#deleteMany(filter:org.bson.conversions.Bson):org.mongodb.scala.Observable[org.mongodb.scala.result.DeleteResult]">}}) 
method. Here we delete all documents where `i` is greater or equal to `100`:

```scala
collection.deleteMany(gte("i", 100)).printHeadResult("Delete Result: ")
```

The delete methods return a [`DeleteResult`]({{< coreapiref "com/mongodb/client/result/DeleteResult.html">}}),
which provides information about the operation including the number of documents deleted.


## Bulk operations

These commands allow for the execution of bulk
insert/update/delete operations. There are two types of bulk operations:

1.  Ordered bulk operations.

      Executes all the operation in order and error out on the first write error.

2.   Unordered bulk operations.

      Executes all the operations and reports any the errors.

      Unordered bulk operations do not guarantee order of execution.

Let's look at two simple examples using ordered and unordered
operations:

```scala
val writes: List[WriteModel[_ <: Document]] = List(
      InsertOneModel(Document("_id" -> 4)),
      InsertOneModel(Document("_id"-> 5)),
      InsertOneModel(Document("_id" -> 6)),
      UpdateOneModel(Document("_id" -> 1), set("x", 2)),
      DeleteOneModel(Document("_id" -> 2)),
      ReplaceOneModel(Document("_id" -> 3), Document("_id" -> 3, "x" -> 4))
    )

// 1. Ordered bulk operation - order is guaranteed
collection.bulkWrite(writes).printHeadResult("Bulk write results: ")

// 2. Unordered bulk operation - no guarantee of order of operation
collection.bulkWrite(writes, BulkWriteOptions().ordered(false)).printHeadResult("Bulk write results (unordered): ")
```

{{% note class="important" %}}
Use of the bulkWrite methods is not recommended when connected to pre-2.6 MongoDB servers, as this was the first server version to support 
bulk write commands for insert, update, and delete in a way that allows the driver to implement the correct semantics for BulkWriteResult 
and BulkWriteException. The methods will still work for pre-2.6 servers, but performance will suffer, as each write operation has to be 
executed one at a time.
{{% /note %}}
