+++
date = "2015-03-17T15:36:56Z"
title = "Quick Start"
[menu.main]
  parent = "MongoDB Reactive Streams"
  identifier = "Reactive Quick Start"
  weight = 5
  pre = "<i class='fa'></i>"
+++

# MongoDB Reactive Streams Driver Quick Start

The following code snippets come from the [`QuickTour.java`]({{< srcref "driver-reactive-streams/src/examples/reactivestreams/tour/QuickTour.java" >}}) example code
that can be found with the reactive streams driver source on github.

{{% note %}}
See the [installation guide]({{< relref "driver-reactive/getting-started/installation.md" >}}) for instructions on how to install the MongoDB Reactive Streams Java Driver.
{{% /note %}}

{{% note class="important" %}}
This guide uses the `Subscriber` implementations as covered in the [Quick Start Primer]({{< relref "driver-reactive/getting-started/quick-start-primer.md" >}}).
{{% /note %}}


## Prerequisites

- A running MongoDB on localhost using the default port for MongoDB `27017`

- MongoDB Reactive Streams Driver.  See [Installation]({{< relref "driver-reactive/getting-started/installation.md" >}}) for instructions on how to install the MongoDB driver.

- The following import statements:

New MongoClient API (since 3.7):

```java
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.reactivestreams.client.Success;

import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Sorts.*;
import static com.mongodb.client.model.Updates.*;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
```

## Make a Connection

Use [`MongoClients.create()`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoClients.html" >}}) to make a connection to a running MongoDB instance.

The `MongoClient` instance represents a pool of connections to the database; you will only need one instance of class `MongoClient` even with concurrent operations threads.

{{% note class="important" %}}

 Typically you only create one `MongoClient` instance for a given MongoDB deployment (e.g. standalone, replica set, or a sharded cluster) and use it across your application. However, if you do create multiple instances:

 - All resource usage limits (e.g. max connections, etc.) apply per `MongoClient` instance.

 - To dispose of an instance, call `MongoClient.close()` to clean up resources.
{{% /note %}}


### Connect to a Single MongoDB instance

The following example shows several ways to connect to a single MongoDB server.

To connect to a single MongoDB instance:

- You can instantiate a MongoClient object without any parameters to connect to a MongoDB instance running on localhost on port ``27017``:

```java
MongoClient mongoClient = MongoClients.create();
```

- You can explicitly specify the hostname to connect to a MongoDB instance running on the specified host on port ``27017``:

```java
MongoClient mongoClient = MongoClients.create(
        MongoClientSettings.builder()
                .applyToClusterSettings(builder ->
                        builder.hosts(Arrays.asList(new ServerAddress("hostOne"))))
                .build());
```

- You can explicitly specify the hostname and the port:

```java
MongoClient mongoClient = MongoClients.create(
        MongoClientSettings.builder()
                .applyToClusterSettings(builder ->
                        builder.hosts(Arrays.asList(new ServerAddress("hostOne", 27018))))
                .build());
```

- You can specify the [`ConnectionString`]({{< apiref "mongodb-driver-core" "com/mongodb/ConnectionString.html" >}}):

```java
MongoClient mongoClient = MongoClients.create("mongodb://hostOne:27017");
```

## Access a Database

Once you have a ``MongoClient`` instance connected to a MongoDB deployment, use the [`MongoClient.getDatabase()`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoClient.html#getDatabase(java.lang.String)" >}}) method to access a database.

Specify the name of the database to the ``getDatabase()`` method. If a database does not exist, MongoDB creates the database when you first store data for that database.

The following example accesses the ``mydb`` database:

```java
 MongoDatabase database = mongoClient.getDatabase("mydb");
 ```

`MongoDatabase` instances are immutable.

## Access a Collection

Once you have a `MongoDatabase` instance, use its [`getCollection()`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoDatabase.html#getCollection(java.lang.String)" >}})
method to access a collection.

Specify the name of the collection to the `getCollection()` method. If a collection does not exist, MongoDB creates the collection when you first store data for that collection.

For example, using the `database` instance, the following statement accesses the collection named `test` in the `mydb` database:

```java
MongoCollection<Document> collection = database.getCollection("test");
```

`MongoCollection` instances are immutable.

## Create a Document

To create the document using the Java driver, use the [`Document`]({{< apiref "bson" "org/bson/Document.html" >}}) class.

For example, consider the following JSON document:

```javascript
  {
   "name" : "MongoDB",
   "type" : "database",
   "count" : 1,
   "versions": [ "v3.2", "v3.0", "v2.6" ],
   "info" : { x : 203, y : 102 }
  }
```

To create the document using the Java driver, instantiate a `Document` object with a field and value, and use its
 [`append()`]({{< apiref "bson" "org/bson/Document.html#append()" >}}) method to include additional fields and values to the document object. The value can be another `Document` object to specify an embedded document:

 ```java
 Document doc = new Document("name", "MongoDB")
                .append("type", "database")
                .append("count", 1)
                .append("versions", Arrays.asList("v3.2", "v3.0", "v2.6"))
                .append("info", new Document("x", 203).append("y", 102));
 ```

{{% note %}}
The BSON type of array corresponds to the Java type `java.util.List`. For a list of the BSON type and the corresponding type in Java, see the [Document]({{< relref "bson/documents.md#document" >}}) mapping.
{{% /note %}}


## Insert a Document

Once you have the `MongoCollection` object, you can insert documents into the collection.

### Insert One Document

To insert a single document into the collection, you can use the collection's [`insertOne()`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoCollection.html#insertOne(TDocument)" >}}) method.

```java
collection.insertOne(doc).subscribe(new OperationSubscriber<InsertOneResult>());
```

{{% note %}}
If no top-level `_id` field is specified in the document, MongoDB automatically adds the `_id` field to the inserted document.
{{% /note %}}

{{% note class="important" %}}
In the API all methods returning a `Publisher` are "cold" streams meaning that nothing happens until they are Subscribed to.

The example below does nothing:

```java
Publisher<InsertOneResult> publisher = collection.insertOne(doc);
```

Only when a `Publisher` is subscribed to and data requested will the operation happen:

```java
publisher.subscribe(new Subscriber<InsertOneResult>() {
    @Override
    public void onSubscribe(final Subscription s) {
        s.request(1);  // <--- Data requested and the insertion will now occur
    }

    @Override
    public void onNext(final InsertOneResult result) {
        System.out.println("Inserted: " + result);
    }

    @Override
    public void onError(final Throwable t) {
        System.out.println("Failed");
    }

    @Override
    public void onComplete() {
        System.out.println("Completed");
    }
});
```

Once the document has been inserted the `onNext` method will be called and it will
print "Inserted!" followed by the result. Finally the `onComplete` method will print "Completed".  
If there was an error for any reason the `onError` method would print "Failed".

{{% /note %}}

### Insert Multiple Documents

To add multiple documents, you can use the collection's [`insertMany()`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoCollection.html#insertMany(java.util.List)" >}}) method which takes a list of documents to insert.

The following example will add multiple documents of the form:

```javascript
{ "i" : value }
```

Create the documents in a loop and add to the `documents` list:


```java
List<Document> documents = new ArrayList<Document>();
for (int i = 0; i < 100; i++) {
    documents.add(new Document("i", i));
}
```

To insert these documents to the collection, pass the list of documents to the
[`insertMany()`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoCollection.html#insertMany(java.util.List)" >}}) method.

```java
collection.insertMany(documents).subscribe(new ObservableSubscriber<InsertManyResult>());
```

Here we block on the `Publisher` to finish so that when we call the next operation we know the data has been inserted into the database!

## Count Documents in A Collection

To count the number of documents in a collection, you can use the collection's [`countDocuments()`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoCollection#countDocuments()" >}})
method.  The following code should print `101` (the 100 inserted via `insertMany` plus the 1 inserted via the `insertOne`).

```java
collection.count()
          .subscribe(new PrintSubscriber<Long>("total # of documents after inserting "
                                              + " 100 small ones (should be 101): %s"));
```

## Query the Collection

To query the collection, you can use the collection's [`find()`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoCollection.html#find()" >}}) method. You can call the method without any arguments to query all documents in a collection or pass a filter to query for documents that match the filter criteria.

### Find the First Document in a Collection

To return the first document in the collection, use the [`find()`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoCollection.html#find()" >}}) method without any parameters and chain to `find()` method the [`first()`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/FindPublisher.html#first()" >}}) method.

{{% note class="tip" %}}
The `find().first()` construct is useful for queries that should only match a single document or if you are interested in the first document only.
{{% /note %}}

The following example prints the first document found in the collection.

```java
collection.find().first().subscribe(new PrintDocumentSubscriber());
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
`find()` method. The `find()` method returns a `FindPublisher` instance that
provides a fluent interface for chaining or controlling find operations. 
The following code retrieves all documents in the collection and prints them out
(101 documents):

```java
collection.find().subscribe(new PrintDocumentSubscriber());
```

## Specify a Query Filter

To query for documents that match certain conditions, pass a filter object to the [`find()`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoCollection.html#find()" >}}) method. To facilitate creating filter objects, the Java driver provides the [`Filters`]({{< apiref "mongodb-driver-core" "com/mongodb/client/model/Filters.html" >}}) helper.

### Get A Single Document That Matches a Filter

For example, to find the first document where the field ``i`` has the value `71`, pass an [`eq`]({{< apiref "mongodb-driver-core" "com/mongodb/client/model/Filters.html#eq(java.lang.String,TItem)" >}}) filter object to specify the equality condition:

```java
collection.find(eq("i", 71)).first().subscribe(new PrintDocumentSubscriber());
```
The example prints one document:

```json
{ "_id" : { "$oid" : "5515836e58c7b4fbc756320b" }, "i" : 71 }
```

### Get All Documents That Match a Filter

The following example returns and prints all documents where ``"i" > 50``:

```java
// now use a range query to get a larger subset
collection.find(gt("i", 50)).subscribe(new PrintDocumentSubscriber());
```
which should print the documents where `i > 50`.

To specify a filter for a range, such as ``50 < i <= 100``, you can use the [`and`]({{< apiref "mongodb-driver-core" "com/mongodb/client/model/Filters.html#and(org.bson.conversions.Bson...)" >}}) helper:

```java
collection.find(and(gt("i", 50), lte("i", 100))).subscribe(new PrintDocumentSubscriber());
```

## Update Documents

To update documents in a collection, you can use the collection's [`updateOne`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoCollection.html#updateOne(org.bson.conversions.Bson,org.bson.conversions.Bson)" >}})  and  [`updateMany`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoCollection.html#updateMany(org.bson.conversions.Bson,org.bson.conversions.Bson)" >}}) methods.

Pass to the methods:

- A filter object to determine the document or documents to update. To facilitate creating filter objects, the Java driver provides the [`Filters`]({{< apiref "mongodb-driver-core" "com/mongodb/client/model/Filters.html" >}}) helper. To specify an empty filter (i.e. match all documents in a collection), use an empty [`Document`]({{< apiref "bson" "org/bson/Document.html" >}}) object.

- An update document that specifies the modifications. For a list of the available operators, see [update operators]({{<docsref "reference/operator/update-field" >}}).

The update methods return an [`UpdateResult`]({{< apiref "mongodb-driver-core" "com/mongodb/client/result/UpdateResult.html" >}}) which provides information about the operation including the number of documents modified by the update.

### Update a Single Document

To update at most a single document, use the [`updateOne`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoCollection.html#updateOne(org.bson.conversions.Bson,org.bson.conversions.Bson)" >}})

The following example updates the first document that meets the filter ``i`` equals ``10`` and sets the value of ``i`` to ``110``:

```java
collection.updateOne(eq("i", 10), set("i", 110))
    .subscribe(new PrintSubscriber<UpdateResult>("Update Result: %s"));
```


### Update Multiple Documents

To update all documents matching the filter, use the [`updateMany`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoCollection.html#updateMany(org.bson.conversions.Bson,org.bson.conversions.Bson)" >}}) method.

The following example increments the value of ``i`` by ``100`` for all documents where  =``i`` is less than ``100``:

```java
UpdateResult updateResult = collection.updateMany(lt("i", 100), inc("i", 100));
    .subscribe(new PrintSubscriber<UpdateResult>("Update Result: %s"));
```

## Delete Documents

To delete documents from a collection, you can use the collection's [`deleteOne`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoCollection.html#deleteOne(org.bson.conversions.Bson)" >}}) and [`deleteMany`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoCollection.html#deleteMany(org.bson.conversions.Bson)" >}}) methods.

Pass to the methods a filter object to determine the document or documents to delete. To facilitate creating filter objects, the Java driver provides the [`Filters`]({{< apiref "mongodb-driver-core" "com/mongodb/client/model/Filters.html" >}}) helper. To specify an empty filter (i.e. match all documents in a collection), use an empty [`Document`]({{< apiref "bson" "org/bson/Document.html" >}}) object.

The delete methods return a [`DeleteResult`]({{< apiref "mongodb-driver-core" "com/mongodb/client/result/DeleteResult.html" >}})
which provides information about the operation including the number of documents deleted.

### Delete a Single Document That Match a Filter

To delete at most a single document that match the filter, use the [`deleteOne`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoCollection.html#deleteOne(org.bson.conversions.Bson)" >}}) method:

The following example deletes at most one document that meets the filter ``i`` equals ``110``:

```java
collection.deleteOne(eq("i", 110))
          .subscribe(new PrintSubscriber<DeleteResult>("Delete Result: %s"));
```

### Delete All Documents That Match a Filter

To delete all documents matching the filter use the [`deleteMany`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoCollection.html#deleteMany(org.bson.conversions.Bson)" >}}) method.

The following example deletes all documents where ``i`` is greater or equal to ``100``:

```java
DeleteResult deleteResult = collection.deleteMany(gte("i", 100))
          .subscribe(new PrintSubscriber<DeleteResult>("Delete Result: %s"));
```

## Create Indexes

To create an index on a field or fields, pass an index specification document to the [`createIndex()`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoCollection.html#createIndex(org.bson.conversions.Bson)" >}}) method. An index key specification document contains the fields to index and the index type for each field:

```java
new Document(<field1>, <type1>).append(<field2>, <type2>) ...
```

- For an ascending index type, specify ``1`` for ``<type>``.
- For a descending index type, specify ``-1`` for ``<type>``.

The following example creates an ascending index on the ``i`` field:

```java
collection.createIndex(new Document("i", 1))
          .subscribe(new PrintSubscriber<String>("Create Index Result: %s"));
```

For a list of other index types, see [Create Indexes]({{< ref "driver-reactive/tutorials/indexes.md" >}})

### Additional Information

For additional tutorials about using MongoDB with Pojos, see the [Pojos Quick Start]({{< ref "driver-reactive/getting-started/quick-start-pojo.md" >}}).

For additional tutorials (such as to use the aggregation framework, specify write concern, etc.), see [Java Driver Tutorials]({{< ref "driver-reactive/tutorials/index.md" >}})
