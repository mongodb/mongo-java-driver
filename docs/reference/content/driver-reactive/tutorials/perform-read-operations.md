+++
date = "2016-06-09T13:21:16-04:00"
title = "Read Operations"
[menu.main]
  parent = "Reactive Tutorials"
  identifier = "Reactive Perform Read Operations"
  weight = 15
  pre = "<i class='fa'></i>"
+++

## Find Operations

Find operations retrieve documents from a collection. You can specify a filter to select only those documents that match the filter condition.

## Prerequisites

- The example below requires a ``restaurants`` collection in the ``test`` database. To create and populate the collection, follow the directions in [github](https://github.com/mongodb/docs-assets/tree/drivers).

- Include the following import statements:

     ```java
     import com.mongodb.*;
     import com.mongodb.reactivestreams.client.MongoClients;
     import com.mongodb.reactivestreams.client.MongoClient;
     import com.mongodb.reactivestreams.client.MongoCollection;
     import com.mongodb.reactivestreams.client.MongoDatabase;
     import com.mongodb.client.model.Projections;
     import com.mongodb.client.model.Filters;
     import com.mongodb.client.model.Sorts;

     import java.util.Arrays;
     import org.bson.Document;
  
     import static com.mongodb.client.model.Filters.*;
     import static com.mongodb.client.model.Projections.*;
     ```

{{% note class="important" %}}
This guide uses the `Subscriber` implementations as covered in the [Quick Start Primer]({{< relref "driver-reactive/getting-started/quick-start-primer.md" >}}).
{{% /note %}}

## Connect to a MongoDB Deployment

Connect to a MongoDB deployment and declare and define a `MongoDatabase` instance and a `MongoCollection` instance

For example, include the following code to connect to a standalone MongoDB deployment running on localhost on port `27017` and define `database` to refer to the `test` database and `collection` to refer to the `restaurants` collection:

```java
MongoClient mongoClient = MongoClients.create();
MongoDatabase database = mongoClient.getDatabase("test");
MongoCollection<Document> collection = database.getCollection("restaurants");
```

For additional information on connecting to MongoDB, see [Connect to MongoDB]({{< ref "connect-to-mongodb.md" >}}).

## Query a Collection

To query the collection, you can use the collection's [`find()`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoCollection.html#find()" >}}) method.

You can call the method without any arguments to query all documents in a collection:

```java
collection.find().subscribe(new PrintDocumentSubscriber());
```

Or pass a filter to query for documents that match the filter criteria:

```java
collection.find(eq("name", "456 Cookies Shop"))
          .subscribe(new PrintDocumentSubscriber());
```

## Query Filters

To query for documents that match certain conditions, pass a filter document to the [`find()`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoCollection.html#find()" >}}) method.

### Empty Filter

To specify an empty filter (i.e. match all documents in a collection), use an empty [`Document`]({{< apiref "bson" "org/bson/Document.html" >}}) object.

```java
collection.find(new Document()).subscribe(new PrintDocumentSubscriber());
```
{{% note class="tip"%}}
For the [`find()`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoCollection.html#find()" >}}) method, you can also call the method without passing a filter object to match all documents in a collection.
{{% /note %}}

```java
collection.find().subscribe(new PrintDocumentSubscriber());;
```

### `Filters` Helper

To facilitate the creation of filter documents, the Java driver provides the [`Filters`]({{< apiref "mongodb-driver-core" "com/mongodb/client/model/Filters.html" >}}) class that provides filter condition helper methods.

Consider the following `find` operation which includes a filter `Document` which specifies that:

- the `stars` field is greater than or equal to 2 and less than 5, *AND*

- the `categories` field equals `"Bakery"` (or if `categories` is an array, contains the string `"Bakery"` as an element):

```java
collection.find(
    new Document("stars", new Document("$gte", 2)
          .append("$lt", 5))
          .append("categories", "Bakery")).subscribe(new PrintDocumentSubscriber());
```

The following example specifies the same filter condition using the [`Filters`]({{< apiref "mongodb-driver-core" "com/mongodb/client/model/Filters.html" >}}) helper methods:

```java
collection.find(and(gte("stars", 2), lt("stars", 5), eq("categories", "Bakery")))
          .subscribe(new PrintDocumentSubscriber());
```

For a list of MongoDB query filter operators, refer to the [MongoDB Manual]({{<docsref "reference/operator/query" >}}). For the associated `Filters` helpers, see [`Filters`]({{< apiref "mongodb-driver-core" "com/mongodb/client/model/Filters.html" >}}).
See also the  [Query Documents Tutorial]({{<docsref "tutorial/query-documents" >}}) for an overview of querying in MongoDB, including specifying filter conditions on arrays and embedded documents.

## FindPublisher

The [`find()`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoCollection.html#find()" >}}) method returns an instance of the [`FindPublisher`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/FindPublisher.html" >}}) interface. The interface provides various methods that you can chain to the `find()` method to modify the output or behavior of the query, such as [`sort()`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/FindPublisher.html#sort(org.bson.conversions.Bson)" >}})  or [`projection()`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/FindPublisher.html#projection(org.bson.conversions.Bson)" >}}), as well as for iterating the results via the `subscribe` method.

### Projections

By default, queries in MongoDB return all fields in matching documents. To specify the fields to return in the matching documents, you can specify a [projection document]({{<docsref "tutorial/project-fields-from-query-results/#projection-document" >}}).

Consider the following `find` operation which includes a projection `Document` which specifies that the matching documents return only the `name` field, `stars` field, and the `categories` field.

```java
collection.find(and(gte("stars", 2), lt("stars", 5), eq("categories", "Bakery")))
          .projection(new Document("name", 1)
              .append("stars", 1)
              .append("categories",1)
              .append("_id", 0))
          .subscribe(new PrintDocumentSubscriber());
```

To facilitate the creation of projection documents, the Java driver provides the
[`Projections`]({{< apiref "mongodb-driver-core" "com/mongodb/client/model/Projections.html" >}}) class.

```java
collection.find(and(gte("stars", 2), lt("stars", 5), eq("categories", "Bakery")))
          .projection(fields(include("name", "stars", "categories"), excludeId()))
          .subscribe(new PrintDocumentSubscriber());
```

In the projection document, you can also specify a projection expression using a [projection operator]({{<docsref "reference/operator/projection/" >}})

For an example on using the [`Projections.metaTextScore`]({{< apiref "mongodb-driver-core" "com/mongodb/client/model/Projections.html#metaTextScore(java.lang.String)" >}}),
see the [Text Search tutorial]({{<relref "driver-reactive/tutorials/text-search.md" >}}).

### Sorts

To sort documents, pass a [sort specification document]({{<docsref "reference/method/cursor.sort/#cursor.sort" >}}) to the [`FindPublisher.sort()`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/FindPublisher.html#sort(org.bson.conversions.Bson)" >}}) method.  The Java driver provides [`Sorts`]({{< relref "builders/sorts.md" >}}) helpers to facilitate the sort specification document.

```java
collection.find(and(gte("stars", 2), lt("stars", 5), eq("categories", "Bakery")))
          .sort(Sorts.ascending("name"))
          .subscribe(new PrintDocumentSubscriber());
```

### Sort with Projections

The [`FindPublisher`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/FindPublisher.html" >}}) methods themselves return `FindPublisher` objects, and as such, you can append multiple `FindPublisher` methods to the `find()` method.

```java
collection.find(and(gte("stars", 2), lt("stars", 5), eq("categories", "Bakery")))
          .sort(Sorts.ascending("name"))
          .projection(fields(include("name", "stars", "categories"), excludeId()))
          .subscribe(new PrintDocumentSubscriber());
```

### Explain

To [explain]({{< docsref "reference/command/explain/" >}}) a find operation, call the
[`FindPublisher.explain()`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/FindPublisher.html#explain()" >}}) 
method:

```java
collection.find(and(gte("stars", 2), lt("stars", 5), eq("categories", "Bakery")))
          .explain()
          .subscribe(new PrintDocumentSubscriber());
```

The driver supports explain of find operations starting with MongoDB 3.0.

## Read Preference

For read operations on [replica sets]({{<docsref "replication/" >}}) or [sharded clusters]({{<docsref "sharding/" >}}), applications can configure the [read preference]({{<docsref "reference/read-preference" >}}) at three levels:

- In a [`MongoClient()`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoClient.html" >}})

  - Via [`MongoClientSettings`]({{< apiref "mongodb-driver-core" "com/mongodb/MongoClientSettings.html" >}}):

      ```java
      MongoClient mongoClient = MongoClients.create(MongoClientSettings.builder()
                                                    .applyConnectionString(new ConnectionString("mongodb://host1,host2"))
                                                    .readPreference(ReadPreference.secondary())
                                                    .build());
      ```

  - Via [`ConnectionString`]({{< apiref "mongodb-driver-core" "com/mongodb/ConnectionString.html" >}}), as in the following example:

      ```java
      MongoClient mongoClient = MongoClients.create("mongodb://host1:27017,host2:27017/?readPreference=secondary");
      ```

- In a [`MongoDatabase`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoDatabase.html" >}}) via its [`withReadPreference`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoDatabase.html#withReadPreference(com.mongodb.ReadPreference)" >}}) method.

    ```java
    MongoDatabase database = mongoClient.getDatabase("test")
                             .withReadPreference(ReadPreference.secondary());
    ```

- In a [`MongoCollection`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoCollection.html" >}}) via its [`withReadPreference`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoCollection.html#withReadPreference(com.mongodb.ReadPreference)" >}}) method:

    ```java
    MongoCollection<Document> collection = database.getCollection("restaurants")
                .withReadPreference(ReadPreference.secondary());
    ```

`MongoDatabase` and `MongoCollection` instances are immutable. Calling `.withReadPreference()` on an existing `MongoDatabase` or `MongoCollection` instance returns a new instance and does not affect the instance on which the method is called.

For example, in the following, the `collectionWithReadPref` instance has the read preference of primaryPreferred whereas the read preference of the `collection` is unaffected.

```java
  MongoCollection<Document> collectionWithReadPref = collection.withReadPreference(ReadPreference.primaryPreferred());
```

## Read Concern

For read operations on [replica sets]({{<docsref "replication/" >}}) or [sharded clusters]({{<docsref "sharding/" >}}), applications can configure the [read concern]({{<docsref "reference/read-concern" >}}) at three levels:

- In a [`MongoClient()`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoClient.html" >}})

  - Via [`MongoClientSettings`]({{< apiref "mongodb-driver-core" "com/mongodb/MongoClientSettings.html" >}}):

      ```java
      MongoClient mongoClient = MongoClients.create(MongoClientSettings.builder()
                                                    .applyConnectionString(new ConnectionString("mongodb://host1,host2"))
                                                    .readConcern(ReadConcern.MAJORITY)
                                                    .build());
      ```

  - Via [`ConnectionString`]({{< apiref "mongodb-driver-core" "com/mongodb/ConnectionString.html" >}}), as in the following example:

      ```java
      MongoClient mongoClient = MongoClients.create("mongodb://host1:27017,host2:27017/?readConcernLevel=majority");
      ```

- In a [`MongoDatabase`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoDatabase.html" >}}) via its [`withReadConcern`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoDatabase.html#withReadConcern(com.mongodb.ReadConcern)" >}}) method, as in the following example:

    ```java
    MongoDatabase database = mongoClient.getDatabase("test")
                                        .withReadConcern(ReadConcern.MAJORITY);
    ```

- In a [`MongoCollection`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoCollection.html" >}}) via its [`withReadConcern`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoCollection.html#withReadConcern(com.mongodb.ReadConcern)" >}}) method, as in the following example:

    ```java
    MongoCollection<Document> collection = database.getCollection("restaurants")
                                              .withReadConcern(ReadConcern.MAJORITY);
    ```

`MongoDatabase` and `MongoCollection` instances are immutable. Calling `.withReadConcern()` on an existing `MongoDatabase` or `MongoCollection` instance returns a new instance and does not affect the instance on which the method is called.

For example, in the following, the `collWithReadConcern` instance has an AVAILABLE read concern whereas the read concern of the `collection` is unaffected.

```java
MongoCollection<Document> collWithReadConcern = collection.withReadConcern(ReadConcern.AVAILABLE);
```

You can build `MongoClientSettings`, `MongoDatabase`, or `MongoCollection` to include a combination of read concern, read preference, and [write concern]({{<docsref "reference/write-concern" >}}).

For example, the following sets all three at the collection level:

```java
collection = database.getCollection("restaurants")
                .withReadPreference(ReadPreference.primary())
                .withReadConcern(ReadConcern.MAJORITY)
                .withWriteConcern(WriteConcern.MAJORITY);
```
