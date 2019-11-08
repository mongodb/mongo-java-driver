+++
date = "2017-08-15T14:19:24-04:00"
title = "Change Streams"
[menu.main]
  parent = "Sync Tutorials"
  identifier = "Change Streams"
  weight = 51
  pre = "<i class='fa'></i>"
+++

## Change Streams

MongoDB 3.6 introduces a new [`$changeStream`](http://dochub.mongodb.org/core/changestreams) aggregation pipeline
operator.

Change streams provide a way to watch changes to documents in a collection. To improve the usability of this new stage, the 
`MongoCollection` API includes a new `watch` method. The `ChangeStreamIterable` sets up the change stream and automatically attempts 
to resume if it encounters a potentially recoverable error.

## Prerequisites

- The example below requires a ``restaurants`` collection in the ``test`` database. To create and populate the collection, follow the directions in [github](https://github.com/mongodb/docs-assets/tree/drivers).

- Include the following import statements:

```java
import com.mongodb.Block;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
```

- Include the following code which the examples in the tutorials will use to print the results of the change stream:

```java
Block<ChangeStreamDocument<Document>> printBlock = new Block<>() {
    @Override
    public void apply(final ChangeStreamDocument<Document> changeStreamDocument) {
        System.out.println(changeStreamDocument);
    }
};
```

## Connect to a MongoDB Deployment

Connect to a MongoDB deployment and declare and define a `MongoDatabase` and a `MongoCollection` instance.

For example, include the following code to connect to a replicaSet MongoDB deployment running on localhost on ports `27017`, `27018` and `27019`. 
It also defines `database` to refer to the `test` database and `collection` to refer to the `restaurants` collection.

```java
MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017,localhost:27018,localhost:27019");
MongoDatabase database = mongoClient.getDatabase("test");
MongoCollection<Document> collection = database.getCollection("restaurants");
```

For additional information on connecting to MongoDB, see [Connect to MongoDB]({{< ref "connect-to-mongodb.md">}}).

## Watch the collection

To create a change stream use one of the [`MongoCollection.watch()`]({{<apiref "com/mongodb/client/MongoCollection.html#watch">}}) methods.

In the following example, the change stream prints out all changes it observes.

```java
collection.watch().forEach(printBlock);
```

## Watch the database

New in the 3.8 driver and MongoDB 4.0, applications can open a single change stream to watch all non-system collections of a database. To
create such a change stream use one of the [`MongoDatabase.watch()`]({{<apiref "com/mongodb/client/MongoDatabase.html#watch">}}) methods.

In the following example, the change stream prints out all the changes it observes on the given database.

```java
database.watch().forEach(printBlock);
```

## Watch all databases

New in the 3.8 driver and MongoDB 4.0, applications can open a single change stream to watch all non-system collections of all databases 
in a MongoDB deployment. To create such a change stream use one of the 
[`MongoClient.watch()`]({{<apiref "com/mongodb/client/MongoClient.html#watch">}}) methods.

In the following example, the change stream prints out all the changes it observes on the deployment to which the `MongoClient` is
connected

```java
mongoClient.watch().forEach(printBlock);
```

## Filtering content

The `watch` method can also be passed a list of [aggregation stages]({{< docsref "meta/aggregation-quick-reference" >}}), that can modify 
the data returned by the `$changeStream` operator. Note: not all aggregation operators are supported. See the 
[`$changeStream`](http://dochub.mongodb.org/core/changestreams) documentation for more information.

In the following example the change stream prints out all changes it observes, for `insert`, `update`, `replace` and `delete` operations:

- First it uses a [`$match`]({{< docsref "reference/operator/aggregation/match/" >}}) stage to filter for documents where the `operationType` 
is either an `insert`, `update`, `replace` or `delete`.

- Then, it sets the `fullDocument` to [`FullDocument.UPDATE_LOOKUP`]({{<apiref "com/mongodb/client/model/FullDocument.html#UPDATE_LOOKUP">}}),
so that the document after the update is included in the results.

```java
collection.watch(asList(Aggregates.match(Filters.in("operationType", asList("insert", "update", "replace", "delete")))))
        .fullDocument(FullDocument.UPDATE_LOOKUP).forEach(printBlock);
```
