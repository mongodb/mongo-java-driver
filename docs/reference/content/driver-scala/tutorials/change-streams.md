+++
date = "2017-08-15T14:19:24-04:00"
title = "Change Streams"
[menu.main]
  parent = "Scala Tutorials"
  identifier = "Scala Change Streams"
  weight = 51
  pre = "<i class='fa'></i>"
+++

## Change Streams

MongoDB 3.6 introduces a new [`$changeStream`](http://dochub.mongodb.org/core/changestreams) aggregation pipeline
operator.

Change streams provide a way to watch changes to documents in a collection. To improve the usability of this new stage, the 
`MongoCollection` API includes a new `watch` method. The `ChangeStreamObservable` sets up the change stream and automatically attempts 
to resume if it encounters a potentially recoverable error.

## Prerequisites

- The example below requires a ``restaurants`` collection in the ``test`` database. To create and populate the collection, follow the directions in [github](https://github.com/mongodb/docs-assets/tree/drivers).

- Include the following import statements:

```scala
import java.util.concurrent.CountDownLatch

import org.mongodb.scala._
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.changestream._
```

{{% note class="important" %}}
This guide uses the `Observable` implicits as covered in the [Quick Start Primer]({{< relref "driver-scala/getting-started/quick-start-primer.md" >}}).
{{% /note %}}

## Connect to a MongoDB Deployment

Connect to a MongoDB deployment and declare and define a `MongoDatabase` and a `MongoCollection` instance.

For example, include the following code to connect to a replicaSet MongoDB deployment running on localhost on ports `27017`, `27018` and `27019`. 
It also defines `database` to refer to the `test` database and `collection` to refer to the `restaurants` collection.

```scala
val mongoClient: MongoClient = MongoClient("mongodb://localhost:27017,localhost:27018,localhost:27019")
val database: MongoDatabase = mongoClient.getDatabase("test")
val collection: MongoCollection[Document] = database.getCollection("restaurants")
```

For additional information on connecting to MongoDB, see [Connect to MongoDB]({{< ref "connect-to-mongodb.md">}}).

## Watch the collection

To create a change stream use one of the [`MongoCollection.watch()`]({{<scapiref "org/mongodb/scala/MongoCollection.html#watch[C](clientSession:org.mongodb.scala.ClientSession,pipeline:Seq[org.mongodb.scala.bson.conversions.Bson])(implicite:org.mongodb.scala.bson.DefaultHelper.DefaultsTo[C,TResult],implicitct:scala.reflect.ClassTag[C]):org.mongodb.scala.ChangeStreamObservable[C]">}}) methods.



In the following example, the change stream prints out all changes it observes.

```scala
case class LatchedObserver() extends Observer[ChangeStreamDocument[Document]] {
  val latch = new CountDownLatch(1)

  override def onSubscribe(subscription: Subscription): Unit = subscription.request(Long.MaxValue) // Request data

  override def onNext(changeDocument: ChangeStreamDocument[Document]): Unit = println(changeDocument)

  override def onError(throwable: Throwable): Unit = {
      println(s"Error: '$throwable")
      latch.countDown()
  }

  override def onComplete(): Unit = latch.countDown()

  def await(): Unit = latch.await()
}

val observer = LatchedObserver()
collection.watch().subscribe(observer)
observer.await() // Block waiting for the latch
```

## Watch the database

New in the 3.8 driver and MongoDB 4.0, applications can open a single change stream to watch all non-system collections of a database. To
create such a change stream use one of the [`MongoDatabase.watch()`]({{<scapiref "org/mongodb/scala/MongoDatabase.html#watch[C]()(implicite:org.mongodb.scala.bson.DefaultHelper.DefaultsTo[C,org.mongodb.scala.Document],implicitct:scala.reflect.ClassTag[C]):org.mongodb.scala.ChangeStreamObservable[C]">}}) methods.

In the following example, the change stream prints out all the changes it observes on the given database.

```scala
val observer = LatchedObserver()
database.watch().subscribe(observer)
observer.await() // Block waiting for the latch
```

## Watch all databases

New in the 3.8 driver and MongoDB 4.0, applications can open a single change stream to watch all non-system collections of all databases 
in a MongoDB deployment. To create such a change stream use one of the 
[`MongoClient.watch()`]({{<scapiref "org/mongodb/scala/MongoClient.html#watch[C]()(implicite:org.mongodb.scala.bson.DefaultHelper.DefaultsTo[C,org.mongodb.scala.Document],implicitct:scala.reflect.ClassTag[C]):org.mongodb.scala.ChangeStreamObservable[C]">}}) methods.

In the following example, the change stream prints out all the changes it observes on the deployment to which the `MongoClient` is
connected

```scala
val observer = LatchedObserver()
client.watch().subscribe(observer)
observer.await() // Block waiting for the latch
```

## Filtering content

The `watch` method can also be passed a list of [aggregation stages]({{< docsref "meta/aggregation-quick-reference" >}}), that can modify 
the data returned by the `$changeStream` operator. Note: not all aggregation operators are supported. See the 
[`$changeStream`](http://dochub.mongodb.org/core/changestreams) documentation for more information.

In the following example the change stream prints out all changes it observes, for `insert`, `update`, `replace` and `delete` operations:

- First it uses a [`$match`]({{< docsref "reference/operator/aggregation/match/" >}}) stage to filter for documents where the `operationType` 
is either an `insert`, `update`, `replace` or `delete`.

- Then, it sets the `fullDocument` to [`FullDocument.UPDATE_LOOKUP`]({{<scapiref "org/mongodb/scala/model/changestream/FullDocument$.html#UPDATE_LOOKUP:com.mongodb.client.model.changestream.FullDocument">}}),
so that the document after the update is included in the results.

```scala
val observer = LatchedObserver()
collection.watch(Seq(Aggregates.filter(Filters.in("operationType", Seq("insert", "update", "replace", "delete")))))
        .fullDocument(FullDocument.UPDATE_LOOKUP).subscribe(observer)
observer.await() // Block waiting for the latch
```
