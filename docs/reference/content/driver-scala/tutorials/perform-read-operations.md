+++
date = "2016-06-09T13:21:16-04:00"
title = "Read Operations"
[menu.main]
  parent = "Scala Tutorials"
  identifier = "Scala Perform Read Operations"
  weight = 15
  pre = "<i class='fa'></i>"
+++

## Find Operations

Find operations retrieve documents from a collection. You can specify a filter to select only those documents that match the filter condition.

## Prerequisites

- The example below requires a ``restaurants`` collection in the ``test`` database. To create and populate the collection, follow the directions in [github](https://github.com/mongodb/docs-assets/tree/drivers).

- Include the following import statements:

     ```scala
     import org.mongodb.scala._
     import org.mongodb.scala.model.Filters._
     import org.mongodb.scala.model.Projections._
     import org.mongodb.scala.model.Sorts._
     ```

{{% note class="important" %}}
This guide uses the `Observable` implicits as covered in the [Quick Start Primer]({{< relref "driver-scala/getting-started/quick-start-primer.md" >}}).
{{% /note %}}

## Connect to a MongoDB Deployment

Connect to a MongoDB deployment and declare and define a `MongoDatabase` instance and a `MongoCollection` instance

For example, include the following code to connect to a standalone MongoDB deployment running on localhost on port `27017` and define `database` to refer to the `test` database and `collection` to refer to the `restaurants` collection:

```scala
val mongoClient: MongoClient = MongoClient()
val database: MongoDatabase = mongoClient.getDatabase("test")
val collection: MongoCollection[Document] = database.getCollection("restaurants")
```

For additional information on connecting to MongoDB, see [Connect to MongoDB]({{< ref "connect-to-mongodb.md">}}).

## Query a Collection

To query the collection, you can use the collection's [`find()`]({{< scapiref "org/mongodb/scala/MongoCollection.html#find[C]()(implicite:org.mongodb.scala.bson.DefaultHelper.DefaultsTo[C,TResult],implicitct:scala.reflect.ClassTag[C]):org.mongodb.scala.FindObservable[C]">}}) method.

You can call the method without any arguments to query all documents in a collection:

```scala
collection.find().printResults()
```

Or pass a filter to query for documents that match the filter criteria:

```scala
collection.find(equal("name", "456 Cookies Shop"))
          .printResults()
```

## Query Filters

To query for documents that match certain conditions, pass a filter document to the [`find()`]({{< scapiref "org/mongodb/scala/MongoCollection.html#find[C]()(implicite:org.mongodb.scala.bson.DefaultHelper.DefaultsTo[C,TResult],implicitct:scala.reflect.ClassTag[C]):org.mongodb.scala.FindObservable[C]">}}) method.

### Empty Filter

To specify an empty filter (i.e. match all documents in a collection), use an empty [`Document`]({{< scapiref "org/mongodb/scala/bson/index.html#Document:org.mongodb.scala.bson.collection.immutable.Document.type" >}}) object.

```scala
collection.find(Document()).printResults()
```
{{% note class="tip"%}}
For the [`find()`]({{< scapiref "org/mongodb/scala/MongoCollection.html#find[C]()(implicite:org.mongodb.scala.bson.DefaultHelper.DefaultsTo[C,TResult],implicitct:scala.reflect.ClassTag[C]):org.mongodb.scala.FindObservable[C]">}}) method, you can also call the method without passing a filter object to match all documents in a collection.
{{% /note %}}

```scala
collection.find().printResults()
```

### `Filters` Helper

To facilitate the creation of filter documents, the Scala driver provides the [`Filters`]({{< scapiref "org/mongodb/scala/model/Filters$.html">}}) class that provides filter condition helper methods.

Consider the following `find` operation which includes a filter `Document` which specifies that:

- the `stars` field is greater than or equal to 2 and less than 5, *AND*

- the `categories` field equals `"Bakery"` (or if `categories` is an array, contains the string `"Bakery"` as an element):

```scala
collection.find(
     Document("stars" -> Document("$gte" -> 2, "$lt"-> 5, "categories" -> "Bakery")))
    .printResults()
```

The following example specifies the same filter condition using the [`Filters`]({{< scapiref "org/mongodb/scala/model/Filters$.html">}}) helper methods:

```scala
collection.find(and(gte("stars", 2), lt("stars", 5), equal("categories", "Bakery")))
          .printResults()
```

For a list of MongoDB query filter operators, refer to the [MongoDB Manual]({{<docsref "reference/operator/query">}}). For the associated `Filters` helpers, see [`Filters`]({{< scapiref "org/mongodb/scala/model/Filters$.html">}}).
See also the  [Query Documents Tutorial]({{<docsref "tutorial/query-documents">}}) for an overview of querying in MongoDB, including specifying filter conditions on arrays and embedded documents.

## FindObservable

The [`find()`]({{< scapiref "org/mongodb/scala/MongoCollection.html#find[C]()(implicite:org.mongodb.scala.bson.DefaultHelper.DefaultsTo[C,TResult],implicitct:scala.reflect.ClassTag[C]):org.mongodb.scala.FindObservable[C]">}}) method returns an instance of the [`FindObservable`]({{< scapiref "org/mongodb/scala/FindObservable.html" >}}) class. The class provides various methods that you can chain to the `find()` method to modify the output or behavior of the query, such as [`sort()`]({{<scapiref "org/mongodb/scala/FindObservable.html#sort(sort:org.mongodb.scala.bson.conversions.Bson):org.mongodb.scala.FindObservable[TResult]">}})  or [`projection()`]({{<scapiref "org/mongodb/scala/FindObservable.html#projection(projection:org.mongodb.scala.bson.conversions.Bson):org.mongodb.scala.FindObservable[TResult]">}}), as well as for iterating the results via the `subscribe` method.

### Projections

By default, queries in MongoDB return all fields in matching documents. To specify the fields to return in the matching documents, you can specify a [projection document]({{<docsref "tutorial/project-fields-from-query-results/#projection-document">}}).

Consider the following `find` operation which includes a projection `Document` which specifies that the matching documents return only the `name` field, `stars` field, and the `categories` field.

```scala
collection.find(and(gte("stars", 2), lt("stars", 5), equal("categories", "Bakery")))
          .projection(Document("name" -> 1, "stars" -> 1, "categories" -> 1, "_id" -> 0))
          .printResults()
```

To facilitate the creation of projection documents, the Scala driver provides the
[`Projections`]({{<scapiref "org/mongodb/scala/model/Projections$.html">}}) class.

```scala
collection.find(and(gte("stars", 2), lt("stars", 5), equal("categories", "Bakery")))
          .projection(fields(include("name", "stars", "categories"), excludeId()))
          .printResults()
```

In the projection document, you can also specify a projection expression using a [projection operator]({{<docsref "reference/operator/projection/">}})

For an example on using the [`Projections.metaTextScore`]({{<scapiref "org/mongodb/scala/model/Projections$.html#metaTextScore(fieldName:String):org.mongodb.scala.bson.conversions.Bson">}}),
see the [Text Search tutorial]({{<relref "driver-scala/tutorials/text-search.md">}}).

### Sorts

To sort documents, pass a [sort specification document]({{<docsref "reference/method/cursor.sort/#cursor.sort">}}) to the [`FindObservable.sort()`]({{<scapiref "org/mongodb/scala/FindObservable.html#sort(sort:org.mongodb.scala.bson.conversions.Bson):org.mongodb.scala.FindObservable[TResult]">}}) method.  The Scala driver provides [`Sorts`]({{< relref "builders/sorts.md">}}) helpers to facilitate the sort specification document.

```scala
collection.find(and(gte("stars", 2), lt("stars", 5), equal("categories", "Bakery")))
          .sort(ascending("name"))
          .printResults()
```

### Sort with Projections

The [`FindObservable`]({{< scapiref "org/mongodb/scala/FindObservable.html" >}}) methods themselves return `FindObservable` objects, and as such, you can append multiple `FindObservable` methods to the `find()` method.

```scala
collection.find(and(gte("stars", 2), lt("stars", 5), equal("categories", "Bakery")))
          .sort(ascending("name"))
          .projection(fields(include("name", "stars", "categories"), excludeId()))
          .printResults()
```

## Read Preference

For read operations on [replica sets]({{<docsref "replication/">}}) or [sharded clusters]({{<docsref "sharding/">}}), applications can configure the [read preference]({{<docsref "reference/read-preference">}}) at three levels:

- In a [`MongoClient()`]({{< scapiref "org/mongodb/scala/MongoClient$.html">}})

  - Via [`MongoClientSettings`]({{<scapiref "org/mongodb/scala/MongoClientSettings$.html">}}):

      ```scala
      val mongoClient = MongoClient(MongoClientSettings.builder()
                                         .applyConnectionString(ConnectionString("mongodb://host1,host2"))
                                         .readPreference(ReadPreference.secondary())
                                         .build())
      ```

  - Via [`ConnectionString`]({{< scapiref "org/mongodb/scala/ConnectionString$.html">}}), as in the following example:

      ```scala
      val mongoClient = MongoClient("mongodb://host1:27017,host2:27017/?readPreference=secondary")
      ```

- In a [`MongoDatabase`]({{<scapiref "org/mongodb/scala/MongoDatabase.html">}}) via its [`withReadPreference`]({{<scapiref "org/mongodb/scala/MongoDatabase.html#withReadPreference(readPreference:org.mongodb.scala.ReadPreference):org.mongodb.scala.MongoDatabase">}}) method.

    ```scala
    val database = mongoClient.getDatabase("test")
                              .withReadPreference(ReadPreference.secondary())
    ```

- In a [`MongoCollection`]({{<scapiref "org/mongodb/scala/MongoCollection.html">}}) via its [`withReadPreference`]({{<scapiref "org/mongodb/scala/MongoCollection.html#withReadPreference(readPreference:org.mongodb.scala.ReadPreference):org.mongodb.scala.MongoCollection[TResult]">}}) method:

    ```scala
    val collection = database.getCollection("restaurants")
                             .withReadPreference(ReadPreference.secondary())
    ```

`MongoDatabase` and `MongoCollection` instances are immutable. Calling `.withReadPreference()` on an existing `MongoDatabase` or `MongoCollection` instance returns a new instance and does not affect the instance on which the method is called.

For example, in the following, the `collectionWithReadPref` instance has the read preference of primaryPreferred whereas the read preference of the `collection` is unaffected.

```scala
val collectionWithReadPref = collection.withReadPreference(ReadPreference.primaryPreferred())
```

## Read Concern

For read operations on [replica sets]({{<docsref "replication/">}}) or [sharded clusters]({{<docsref "sharding/">}}), applications can configure the [read concern]({{<docsref "reference/read-concern">}}) at three levels:

- In a [`MongoClient()`]({{< scapiref "org/mongodb/scala/MongoClient$.html">}})

  - Via [`MongoClientSettings`]({{<scapiref "org/mongodb/scala/MongoClientSettings$.html">}}):

      ```scala
      val mongoClient = MongoClient(MongoClientSettings.builder()
                                     .applyConnectionString(ConnectionString("mongodb://host1,host2"))
                                     .readConcern(ReadConcern.MAJORITY)
                                     .build())
      ```

  - Via [`ConnectionString`]({{< scapiref "org/mongodb/scala/ConnectionString$.html">}}), as in the following example:

      ```scala
      val mongoClient = MongoClient("mongodb://host1:27017,host2:27017/?readConcernLevel=majority")
      ```

- In a [`MongoDatabase`]({{<scapiref "org/mongodb/scala/MongoDatabase.html">}}) via its [`withReadConcern`]({{<scapiref "org/mongodb/scala/MongoDatabase.html#withReadConcern(readConcern:org.mongodb.scala.ReadConcern):org.mongodb.scala.MongoDatabase">}}) method, as in the following example:

    ```scala
    val database = mongoClient.getDatabase("test")
                              .withReadConcern(ReadConcern.MAJORITY)
    ```

- In a [`MongoCollection`]({{<scapiref "org/mongodb/scala/MongoCollection.html">}}) via its [`withReadConcern`]({{<scapiref "org/mongodb/scala/MongoCollection.html#withReadConcern-com.mongodb.ReadConcern-">}}) method, as in the following example:

    ```scala
    val collection = database.getCollection("restaurants")
                             .withReadConcern(ReadConcern.MAJORITY)
    ```

`MongoDatabase` and `MongoCollection` instances are immutable. Calling `.withReadConcern()` on an existing `MongoDatabase` or `MongoCollection` instance returns a new instance and does not affect the instance on which the method is called.

For example, in the following, the `collWithReadConcern` instance has an AVAILABLE read concern whereas the read concern of the `collection` is unaffected.

```scala
val collWithReadConcern = collection.withReadConcern(ReadConcern.AVAILABLE)
```

You can build `MongoClientSettings`, `MongoDatabase`, or `MongoCollection` to include a combination of read concern, read preference, and [write concern]({{<docsref "reference/write-concern">}}).

For example, the following sets all three at the collection level:

```scala
val collection = database.getCollection("restaurants")
                         .withReadPreference(ReadPreference.primary())
                         .withReadConcern(ReadConcern.MAJORITY)
                         .withWriteConcern(WriteConcern.MAJORITY)
```
