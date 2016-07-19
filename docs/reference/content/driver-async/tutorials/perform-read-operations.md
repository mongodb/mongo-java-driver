+++
date = "2015-03-19T12:53:30-04:00"
title = "Read Operations"
[menu.main]
  parent = "Async Tutorials"
  identifier = "Async Read Operations"
  weight = 15
  pre = "<i class='fa'></i>"
+++


## Find Operations

Find operations retrieve documents from a collection. You can specify a filter to select only those documents that match the filter condition.

## Prerequisites

- The example below requires a ``restaurants`` collection in the ``test`` database. To create and populate the collection, follow the directions in [github] (https://github.com/mongodb/docs-assets/tree/drivers).

- Include the following import statements:

```java
import com.mongodb.*;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.*;
import com.mongodb.client.model.Sorts;
import com.mongodb.connection.ClusterSettings;
import org.bson.Document;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static java.util.Arrays.asList;
```

- Include the following callback code which the examples in the tutorials will use:

```java
SingleResultCallback<Void> callbackWhenFinished = new SingleResultCallback<Void>() {
    @Override
    public void onResult(final Void result, final Throwable t) {
        System.out.println("Operation Finished!");
    }
};
```

- Include the following code which the examples will use to print the results of the find operations:

```java
Block<Document> printBlock = new Block<Document>() {
    @Override
    public void apply(final Document document) {
        System.out.println(document.toJson());
    }
};
```

## Considerations

{{% note class="important" %}}
Always check for errors in any `SingleResultCallback<T>` implementation
and handle them appropriately.

For sake of brevity, this tutorial omits the error check logic in the code examples.
{{% /note %}}


## Connect to a MongoDB Deployment

Connect to a MongoDB deployment and declare and define a `MongoDatabase` and a `MongoCollection` instances.

For example, include the following code to connect to a standalone MongoDB deployment running on localhost on port `27017` and define `database` to refer to the `test` database and `collection` to refer to the `restaurants` collection:

```java
MongoClient mongoClient = MongoClients.create();
MongoDatabase database = mongoClient.getDatabase("test");
MongoCollection<Document> collection = database.getCollection("restaurants");
```

For additional information on connecting to MongoDB, see [Connect to MongoDB]({{< relref "driver-async/tutorials/connect-to-mongodb.md" >}}).


## Query a Collection

To query the collection, you can use the [`find()`]({{< apiref "com/mongodb/async/client/MongoCollection.html#find-org.bson.conversions.Bson-">}}) method.

You can call the method without any arguments to query all documents in a collection:

```java
collection.find().forEach(printBlock, callbackWhenFinished);
```

Or pass a filter to query for documents that match the filter criteria:

```java
collection.find(eq("name", "456 Cookies Shop"))
            .forEach(printBlock, callbackWhenFinished);
```

## Query Filters

To query for documents that match certain conditions, pass a filter document to the [`find()`]({{< apiref "com/mongodb/async/client/MongoCollection.html#find-org.bson.conversions.Bson-">}}) method.

### Empty Filter

To specify an empty filter (i.e. match all documents in a collection), use an empty [`Document`]({{< apiref "org/bson/Document.html" >}}) object.

```java
collection.find(new Document()).forEach(printBlock, callbackWhenFinished);
```

{{% note class="tip"%}}
For the [`find()`]({{< apiref "com/mongodb/async/client/MongoCollection.html#find--">}}) method, you can also call the method without passing a filter object to match all documents in a collection.
{{% /note %}}

```java
collection.find().forEach(printBlock, callbackWhenFinished);
```

### `Filters` Helper

To facilitate the creation of filter documents, the Java driver provides the [`Filters`]({{< apiref "com/mongodb/client/model/Filters.html">}}) class that provides filter condition helper methods.

Consider the following `find` operation which includes a filter `Document` which specifies that:

- the `stars` field is greater than or equal to 2 and less than 5, *AND*

- the `categories` field equals `"Bakery"` (or if `categories` is an array, contains the string `"Bakery"` as an element):

```java
collection.find(
    new Document("stars", new Document("$gte", 2)
          .append("$lt", 5))
          .append("categories", "Bakery")).forEach(printBlock, callbackWhenFinished);
```

The following example specifies the same filter condition using the [`Filters`]({{< apiref "com/mongodb/client/model/Filters.html">}}) helper methods:

```java
collection.find(and(gte("stars", 2), lt("stars", 5), eq("categories", "Bakery")))
            .forEach(printBlock, callbackWhenFinished);
```

For a list of MongoDB query filter operators, refer to the [MongoDB Manual]({{<docsref "reference/operator/query">}}). For the associated `Filters` helpers, see [`Filters`]({{< apiref "com/mongodb/client/model/Filters.html">}}).
See also the  [Query Documents Tutorial]({{<docsref "tutorial/query-documents">}}) for an overview of querying in MongoDB, including specifying filter conditions on arrays and embedded documents.

## FindIterable

The [`find()`]({{< apiref "com/mongodb/async/client/MongoCollection.html#find--">}}) method returns an instance of the [`FindIterable`]({{< apiref "com/mongodb/async/client/FindIterable.html" >}}) interface. The interface provides various methods that you can chain to the `find()` method to modify the output or behavior of the query, such as [`sort()`]({{<apiref "com/mongodb/async/client/FindIterable.html#sort-org.bson.conversions.Bson-">}})  or [`projection()`]({{<apiref "com/mongodb/async/client/FindIterable.html#projection-org.bson.conversions.Bson-">}}), as well as for iterating the results, such as [`forEach()`]({{<apiref "com/mongodb/async/client/MongoIterable.html#forEach-com.mongodb.Block-com.mongodb.async.SingleResultCallback-">}}).


### Projections

By default, queries in MongoDB return all fields in matching documents. To specify the fields to return in the matching documents, you can specify a [projection document]({{<docsref "tutorial/project-fields-from-query-results/#projection-document">}}).

Consider the following `find` operation which includes a projection `Document` which specifies that the matching documents return only the `name` field, `stars` field, and the `categories` field.

```java
collection.find(and(gte("stars", 2), lt("stars", 5), eq("categories", "Bakery")))
                .projection(new Document("name", 1)
                     .append("stars", 1)
                     .append("categories",1)
                     .append("_id", 0))
                .forEach(printBlock, callbackWhenFinished);
```

To facilitate the creation of projection documents, the Java driver provides the
[`Projections`]({{<apiref "com/mongodb/client/model/Projections.html">}}) class.

```java
collection.find(and(gte("stars", 2), lt("stars", 5), eq("categories", "Bakery")))
                .projection(fields(include("name", "stars", "categories"), excludeId()))
                .forEach(printBlock, callbackWhenFinished);
```

In the projection document, you can also specify a projection expression using a [projection operator]({{<apiref "reference/operator/projection/">}})

For an example on using the [`Projections.metaTextScore`]({{<apiref "com/mongodb/client/model/Projections.html#metaTextScore-java.lang.String-">}}),
see the [Text Search tutorial]({{<relref "driver/tutorials/text-search.md">}}).

### Sorts

To sort documents, pass a [sort specification document]({{<docsref "reference/method/cursor.sort/#cursor.sort">}}) to the [`FindIterable.sort()`]({{<apiref "com/mongodb/async/client/FindIterable.html#sort-org.bson.conversions.Bson-">}}) method.  The Java driver provides [`Sorts`]({{< relref "builders/sorts.md">}}) helpers to facilitate the sort specification document.

```java
collection.find(and(gte("stars", 2), lt("stars", 5), eq("categories", "Bakery")))
                .sort(Sorts.ascending("name"))
                .forEach(printBlock, callbackWhenFinished);

```

### Sort with Projections

The [`FindIterable`]({{< apiref "com/mongodb/async/client/FindIterable.html" >}}) methods themselves return `FindIterable` objects, and as such, you can append multiple `FindIterable` methods to the `find()` method.

```java
collection.find(and(gte("stars", 2), lt("stars", 5), eq("categories", "Bakery")))
                .sort(Sorts.ascending("name"))
                .projection(fields(include("name", "stars", "categories"), excludeId()))
                .forEach(printBlock, callbackWhenFinished);
```

## MongoIterable

The [`MongoIterable`]({{< apiref "com/mongodb/async/client/FindIterable.html" >}}) interface provides helper methods to access the results of an operation:


- [`first()`]({{< apiref "com/mongodb/async/client/MongoIterable.html#first-com.mongodb.async.SingleResultCallback-" >}})

- [`forEach()`]({{< apiref "com/mongodb/async/client/MongoIterable.html#forEach-com.mongodb.Block-com.mongodb.async.SingleResultCallback-" >}})

- [`map()`]({{< apiref "com/mongodb/async/client/MongoIterable.html#map-com.mongodb.Function-" >}})

- [`into()`]({{< apiref "com/mongodb/async/client/MongoIterable.html#into-A-com.mongodb.async.SingleResultCallback-" >}})


## Read Preference

For read operations on [replica sets]({{<docsref "replication/">}}) or [sharded clusters]({{<docsref "sharding/">}}), applications can configure the [read preference]({{<docsref "reference/read-preference">}}) at three levels:

- In a [`MongoClient()`]({{< apiref "com/mongodb/async/client/MongoClient.html" >}})

  - Via [`MongoClientSettings`]({{<apiref "com/mongodb/async/client/MongoClientSettings.html">}}), as in the following example:

      ```java
      ClusterSettings clusterSettings = ClusterSettings.builder()
                                .hosts(asList(
                                new ServerAddress("host1", 27017),
                                new ServerAddress("host2", 27017))).build();
      MongoClientSettings settings = MongoClientSettings.builder()
                                        .clusterSettings(clusterSettings)
                                        .readPreference(ReadPreference.secondary())
                                        .build();
      MongoClient mongoClient = MongoClients.create(settings);
      ```                                      

  - Via a [`ConnectionString`]({{< apiref "com/mongodb/ConnectionString.html">}}) object.

      ```java
      MongoClient mongoClient = MongoClients.create(new ConnectionString(
            "mongodb://host1:27017,host2:27017/?readPreference=secondary"));
      ```

  - Via string that specifies the connection URI:

      ```java
      MongoClient mongoClient = MongoClients.create(
            "mongodb://host1:27017,host2:27017/?readPreference=secondary");
      ```

- In a [`MongoDatabase`]({{<apiref "com/mongodb/async/client/MongoDatabase.html">}}) via its [`withReadPreference`]({{<apiref
"com/mongodb/async/client/MongoDatabase.html#withReadPreference-com.mongodb.ReadPreference-">}}) method, as in the following example:


    ```java
     MongoDatabase database = mongoClient.getDatabase("test")     
            .withReadPreference(ReadPreference.secondary());
    ```

- In a [`MongoCollection`]({{<apiref "com/mongodb/async/client/MongoCollection.html">}}) via its [`withReadPreference`]({{<apiref "com/mongodb/async/client/MongoCollection.html#withReadPreference-com.mongodb.ReadPreference-">}}) method, as in the following example:

    ```java
    MongoCollection<Document> collection = database.getCollection("restaurants")
              .withReadPreference(ReadPreference.secondary());
    ```

`MongoDatabase` and `MongoCollection` instances are immutable. Calling `.withReadPreference()` on an existing `MongoDatabase` or `MongoCollection` instance returns a new instance and does not affect the instance on which the method is called.

For example, in the following, the `collectionWithReadPref` instance has the read preference of primaryPreferred whereas the read preference of the `collection` is unaffected.

```java
  MongoCollection<Document> collectionWithReadPref =
                collection.withReadPreference(ReadPreference.primaryPreferred());
```

## Read Concern

For read operations on [replica sets]({{<docsref "replication/">}}) or [sharded clusters]({{<docsref "sharding/">}}), applications can configure the [read concern]({{<docsref "reference/read-concern">}}) at three levels:

- In a [`MongoClient()`]({{< apiref "com/mongodb/async/client/MongoClient.html" >}})

  - Via [`MongoClientSettings`]({{<apiref "com/mongodb/async/client/MongoClientSettings.html">}}), as in the following example:

      ```java
      ClusterSettings clusterSettings = ClusterSettings.builder()
                                .hosts(asList(
                                new ServerAddress("host1", 27017),
                                new ServerAddress("host2", 27017))).build();
      MongoClientSettings settings = MongoClientSettings.builder()
                                        .clusterSettings(clusterSettings)
                                        .readConcern(ReadConcern.DEFAULT)
                                        .build();
      MongoClient mongoClient = MongoClients.create(settings);
      ```                                      

  - Via a [`ConnectionString`]({{< apiref "com/mongodb/ConnectionString.html">}}) object.

      ```java
      MongoClient mongoClient = MongoClients.create(new ConnectionString(
              "mongodb://host1:27017,host2:27017/?readConcernLevel=majority"));
      ```

  - Via string that specifies the connection URI:

      ```java
      MongoClient mongoClient = MongoClients.create(
              "mongodb://host1:27017,host2:27017/?readConcernLevel=majority");
      ```

- In a [`MongoDatabase`]({{<apiref "com/mongodb/async/client/MongoDatabase.html">}}) via its [`withReadConcern`]({{<apiref
"com/mongodb/client/MongoDatabase.html#withReadConcern-com.mongodb.ReadConcern-">}}) method, as in the following example:


    ```java
     MongoDatabase database = mongoClient.getDatabase("test")
                                .withReadConcern(ReadConcern.DEFAULT);
    ```

- In a [`MongoCollection`]({{<apiref "com/mongodb/async/client/MongoCollection.html">}}) via its [`withReadConcern`]({{<apiref "com/mongodb/client/MongoCollection.html#withReadConcern-com.mongodb.ReadConcern-">}}) method, as in the following example:

    ```java
    MongoCollection<Document> collection = database.getCollection("restaurants")
                                .withReadConcern(ReadConcern.DEFAULT);
    ```

`MongoDatabase` and `MongoCollection` instances are immutable. Calling `.withReadConcern()` on an existing `MongoDatabase` or `MongoCollection` instance returns a new instance and does not affect the instance on which the method is called.

For example, in the following, the `collWithReadConcern` instance has majority read concern whereas the read concern of the `collection` is unaffected.

```java
MongoCollection<Document> collWithReadConcern = collection
                                                  .withReadConcern(ReadConcern.MAJORITY);
```

You can build `MongoClientOptions`, `MongoDatabase`, or `MongoCollection` to include a combination of read concern, read preference, and [write concern]({{<docsref "reference/write-concern">}}).

For example, the following sets all three at the collection level:

```java
collection = database.getCollection("restaurants")
                .withReadPreference(ReadPreference.primary())
                .withReadConcern(ReadConcern.MAJORITY)
                .withWriteConcern(WriteConcern.MAJORITY);
```
