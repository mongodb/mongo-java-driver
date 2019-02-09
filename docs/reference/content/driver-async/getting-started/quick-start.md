+++
date = "2015-03-17T15:36:56Z"
title = "Quick Start"
[menu.main]
  parent = "MongoDB Async Driver"
  identifier = "Async Quick Start"
  weight = 10
  pre = "<i class='fa'></i>"
+++

# MongoDB Async Driver Quick Start

The following code snippets come from the [`QuickTour.java`]({{< srcref "driver-async/src/examples/tour/QuickTour.java">}}) example code
that can be found with the async driver source on github.

{{% note %}}
The callback-based Async Java Driver has been deprecated in favor of the 
[MongoDB Reactive Streams Java Driver](http://mongodb.github.io/mongo-java-driver-reactivestreams/).
{{% /note %}}

## SingleResultCallback

The MongoDB Async driver provides an asynchronous API that can leverage either Netty or `AsynchronousSocketChannel` for fast and non-blocking I/O.

The MongoDB Asynchronous Driver API mirrors the new Synchronous MongoDB Driver API, but asynchronous methods that make network I/O operations take a [`SingleResultCallback<T>`]({{< apiref "com/mongodb/async/SingleResultCallback.html">}}) and return immediately. The `SingleResultCallback<T>` interface requires the implementation of a single method `onResult(T result, Throwable t)` which is called upon the completion of the operation.  Upon successful operation, the `result` parameter contains the result of the operation. If the operation failed for any reason, then the `t` contains the reason for the failure.

{{% note class="important" %}}
Always check for errors in any `SingleResultCallback<T>` implementation
and handle them appropriately.

For sake of brevity, this tutorial omits the error check logic in the code examples.
{{% /note %}}

To use a callback more than once, you can either create a class that implements the callback or assign the callback to a variable.

## Prerequisites

- A running MongoDB on localhost using the default port for MongoDB `27017`

- MongoDB Async Driver.  See [Installation]({{< relref "driver-async/getting-started/installation.md" >}}) for instructions on how to install the MongoDB driver.

- The following import statements:

```java
import com.mongodb.Block;
import com.mongodb.ServerAddress;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.ConnectionString;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;
import static java.util.Arrays.asList;
```

- The following callback code which the examples in the tutorials will use:

```java
SingleResultCallback<Document> callbackPrintDocuments = new SingleResultCallback<Document>() {
   @Override
   public void onResult(final Document document, final Throwable t) {
       System.out.println(document.toJson());
   }
};

SingleResultCallback<Void> callbackWhenFinished = new SingleResultCallback<Void>() {
    @Override
    public void onResult(final Void result, final Throwable t) {
        System.out.println("Operation Finished!");
    }
};

```

- The following `Block` code which the examples will use to print the results of the find operations:

```java
Block<Document> printDocumentBlock = new Block<Document>() {
    @Override
    public void apply(final Document document) {
        System.out.println(document.toJson());
    }
};
```

## Make a Connection

To make a connection to a running MongoDB instance, use [`MongoClients.create`]({{< apiref "com/mongodb/async/client/MongoClients.html#create()" >}}) to create a new [`MongoClient`]({{< apiref "com/mongodb/async/client/MongoClient.html">}}) instance. A `MongoClient` instance actually represents a pool of connections
to the database; you will only need one instance of class
`MongoClient` even with multiple concurrently executing asynchronous operations.

{{% note class="important" %}}
Typically you only create one `MongoClient` instance for a given MongoDB
deployment (e.g. standalone, replica set, or a sharded cluster) and use it across your application. However, if you do create multiple instances:

-  All resource usage limits (max connections, etc.) apply per `MongoClient` instance.
-  To dispose of an instance, call `MongoClient.close()` to clean up resources.
{{% /note %}}

### Connect to a Standalone MongoDB Instance

The following example shows various ways to connect to a standalone MongoDB instance running on the local machine.

To connect to a standalone MongoDB instance:

- You can call [`MongoClients.create()`]({{< apiref "com/mongodb/async/client/MongoClients.html#create()" >}}) without any parameters to connect to a MongoDB instance running on localhost on port ``27017``:

```java
MongoClient mongoClient = MongoClients.create();
```

- You can call [`MongoClients.create()`]({{< apiref "com/mongodb/async/client/MongoClients.html#create(java.lang.String)" >}}) with a string that specifies the connection string:

```java
MongoClient mongoClient = MongoClients.create("mongodb://localhost");
```

The connection string mostly follows [RFC 3986](http://tools.ietf.org/html/rfc3986), with the exception of the domain name. For MongoDB, it is possible to list multiple domain names separated by a comma. For more information on the connection string, see [connection string]({{< docsref "reference/connection-string" >}}).

- You can call [`MongoClients.create()`]({{< apiref "com/mongodb/async/client/MongoClients.html#create(com.mongodb.ConnectionString)" >}}) with a [`ConnectionString`]({{< apiref "com/mongodb/ConnectionString.html">}}) object:

```java
MongoClient mongoClient = MongoClients.create(new ConnectionString("mongodb://localhost"));
```

- You can call [`MongoClients.create()`]({{< apiref "com/mongodb/async/client/MongoClients.html#create(com.mongodb.MongoClientSettings)" >}}) with a [`MongoClientSettings`]({{< apiref "com/mongodb/MongoClientSettings.html">}}) object:

```java
ClusterSettings clusterSettings = ClusterSettings.builder()
                                  .hosts(asList(new ServerAddress("localhost"))).build();
MongoClientSettings settings = MongoClientSettings.builder()
                                  .clusterSettings(clusterSettings).build();
MongoClient mongoClient = MongoClients.create(settings);
```

## Access a Database

Once you have a [`MongoClient`]({{< apiref "com/mongodb/async/client/MongoClient.html">}}) instance connected to a MongoDB deployment, use its [`getDatabase()`]({{<apiref "com/mongodb/async/client/MongoClient.html#getDatabase(java.lang.String)">}}) method to access a database.

Specify the name of the database to the `getDatabase()` method. The `getDatabase()` method does not require a callback since there is no network I/O required.  If a database does not exist, MongoDB creates the database when you first store data for that database.

The following example accesses the `mydb` database:

```java
MongoDatabase database = mongoClient.getDatabase("mydb");
```

## Get a Collection

Once you have a [`MongoDatabase`]({{< apiref "com/mongodb/async/client/MongoDatabase.html" >}}) instance, use its [`getCollection()`]({{< apiref "com/mongodb/async/client/MongoDatabase.html#getCollection(java.lang.String)">}})
method to access a collection.

Specify the name of the collection to the `getCollection()` method. If a collection does not exist, MongoDB creates the collection when you first store data for that collection.

For example, using the `database` instance, the following statement accesses the collection named `test` in the `mydb` database:

```java
MongoCollection<Document> collection = database.getCollection("test");
```

## Create a Document

To create the document using the Java async driver, use the [`Document`]({{< apiref "org/bson/Document.html" >}}) class.

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

To create the document using the Java async driver, instantiate a `Document` object with a field and value, and use its
 [`append()`]({{< apiref "org/bson/Document.html#append" >}}) method to include additional fields and values to the document object. The value can be another `Document` object to specify an embedded document:

 ```java
 Document doc = new Document("name", "MongoDB")
                .append("type", "database")
                .append("count", 1)
                .append("versions", Arrays.asList("v3.2", "v3.0", "v2.6"))
                .append("info", new Document("x", 203).append("y", 102));
 ```

{{% note %}}
The BSON type of array corresponds to the Java type `java.util.List`. For a list of the BSON type and the corresponding type in Java, see [BSON  Documents reference]({{<relref "bson/documents.md" >}}).
{{% /note %}}

## Insert a Document

Once you have the [`MongoCollection`]({{< apiref "com/mongodb/async/client/MongoCollection.html">}}) object, you can insert documents into the collection.

### Insert One Document

To insert the document into the collection, use the [`insertOne()`]({{<apiref "com/mongodb/async/client/MongoCollection.html#insertOne(TDocument,com.mongodb.async.SingleResultCallback)">}}) method.

```java
collection.insertOne(doc, new SingleResultCallback<Void>() {
    @Override
    public void onResult(final Void result, final Throwable t) {
        System.out.println("Inserted!");
    }
});
```

{{% note %}}
If no top-level `_id` field is specified in the document, the driver automatically adds the `_id` field to the inserted document.
{{% /note %}}

`SingleResultCallback<T>` is a [functional interface](https://docs.oracle.com/javase/specs/jls/se8/html/jls-9.html#jls-9.8):

```java
collection.insertOne(doc, (Void result, final Throwable t) -> System.out.println("Inserted!"));
```

### Insert Multiple Documents

To add multiple documents, you can use the [`insertMany()`]({{< apiref "com/mongodb/async/client/MongoCollection.html#insertMany(java.util.List,com.mongodb.async.SingleResultCallback)">}}) method which takes a list of documents to insert.

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
[`insertMany()`]({{< apiref "com/mongodb/async/client/MongoCollection.html#insertMany(java.util.List,com.mongodb.async.SingleResultCallback)">}}) method.

```java
collection.insertMany(documents, new SingleResultCallback<Void>() {
    @Override
    public void onResult(final Void result, final Throwable t) {
        System.out.println("Documents inserted!");
    }
});
```

{{% note %}}
If no top-level `_id` field is specified in the document, the driver automatically adds the `_id` field to the inserted document.
{{% /note %}}

## Count Documents in A Collection

To count the number of documents in a collection, you can use the collection's [`count()`]({{< apiref "com/mongodb/async/client/MongoCollection.html#count(com.mongodb.async.SingleResultCallback)">}})
method.  The following code should print `101` (the 100 inserted via `insertMany` plus the 1 inserted via the `insertOne`).

```java
collection.countDocuments(
  new SingleResultCallback<Long>() {
      @Override
      public void onResult(final Long count, final Throwable t) {
          System.out.println(count);
      }
  });
```

## Query the Collection

To query the collection, you can use the collection's [`find()`]({{< apiref "com/mongodb/async/client/MongoCollection.html#find()">}}) method. You can call the method without any arguments to query all documents in a collection or pass a filter to query for documents that match the filter criteria.

The [`find()`]({{< apiref "com/mongodb/async/client/MongoCollection.html#find()">}}) method returns a [`FindIterable()`]({{< apiref "com/mongodb/async/client/FindIterable.html" >}}) instance that provides a fluent interface for chaining other methods.

### Find the First Document in a Collection

To return the first document in the collection, use the [find()]({{< apiref "com/mongodb/async/client/MongoCollection.html#find()">}}) method without any parameters and chain to [find()]({{< apiref "com/mongodb/async/client/MongoCollection.html#find()">}}) method the [`first()`] ({{< apiref "com/mongodb/async/client/MongoIterable.html#first(com.mongodb.async.SingleResultCallback)">}}) method.

If the collection is empty, the operation returns null.

{{% note class="tip" %}}
The `find().first()` construct is useful for queries that should only match a single document or if you are interested in the first document only.
{{% /note %}}

The following example prints the first document found in the collection, using the `printDocument` callback declared earlier in the tutorial:

```java
collection.find().first(callbackPrintDocuments);
```

The example will print the following document:

```json
{ "_id" : { "$oid" : "579f5278b9c1d14ae2a31c27" }, "name" : "MongoDB", "type" : "database", "count" : 1, "versions" : ["v3.2", "v3.0", "v2.6"], "info" : { "x" : 203, "y" : 102 } }
```

{{% note %}}
The `_id` element has been added automatically by the Java async driver to your
document and your value will differ from that shown. MongoDB reserves field
names that start with `"_"` and `"$"` for internal use.
{{% /note %}}

### Find All Documents in a Collection

To retrieve all the documents in the collection, we will use the
[`find()`]({{< apiref "com/mongodb/async/client/MongoCollection.html#find()">}}) method without any parameters.

You can chain the
[`forEach()`]({{< apiref "com/mongodb/async/client/MongoIterable.html#forEach(com.mongodb.Block,com.mongodb.async.SingleResultCallback)" >}}) method to the `find()` method to iterate through the results and apply a [`Block`]({{< apiref "com/mongodb/Block.html" >}}) to each document in the results.  The [`forEach()`]({{< apiref "com/mongodb/async/client/MongoIterable.html#forEach(com.mongodb.Block,com.mongodb.async.SingleResultCallback)" >}}) method also takes a callback that is run once the iteration has finished.

The following code retrieves all documents in the collection and prints the returned documents (101 documents). The example uses the `callbackWhenFinished` and the `printDocumentBlock` defined earlier in the tutorial:

```java
collection.find().forEach(printDocumentBlock, callbackWhenFinished);
```

## Specify a Query Filter

To query for documents that match certain conditions, pass a filter object to the [find()]({{< apiref "com/mongodb/async/client/MongoCollection.html#find()">}}) method. To facilitate creating filter objects, the driver provides the [`Filters`]({{< apiref "com/mongodb/client/model/Filters.html">}}) helper.

### Get a Single Document That Matches a Filter

For example, to find the first document where the field ``i`` has the value `71`, pass an [`eq`]({{<apiref  "com/mongodb/client/model/Filters.html#eq(java.lang.String,TItem)">}}) filter object to specify the equality condition. The example uses the `callbackPrintDocuments` defined earlier in the tutorial:


```java
collection.find(eq("i", 71)).first(callbackPrintDocuments);
```

### Get All Documents That Match a Filter

The following example returns and prints all documents where `"i" > 50`. The example uses the `printDocumentBlock` code and `callbackWhenFinished`  defined earlier in the tutorial:

```java
collection.find(gt("i", 50)).forEach(printDocumentBlock, callbackWhenFinished);
```

To specify a filter for a range, such as ``50 < i <= 100``, you can use the [`and`]({{<apiref "com/mongodb/client/model/Filters.html#and(org.bson.conversions.Bson...)">}}) helper:


```java
collection.find(and(gt("i", 50), lte("i", 100))).forEach(printDocumentBlock, callbackWhenFinished);
```

## Updating documents

To update documents in a collection, you can use the collection's [`updateOne`]({{< apiref "com/mongodb/async/client/MongoCollection.html#updateMany(org.bson.conversions.Bson,org.bson.conversions.Bson,com.mongodb.async.SingleResultCallback)">}})  and  [`updateMany`]({{< apiref "com/mongodb/async/client/MongoCollection.html#updateMany(org.bson.conversions.Bson,org.bson.conversions.Bson,com.mongodb.async.SingleResultCallback)">}}) methods.

Pass to the methods:

- A filter object to determine the document or documents to update. To facilitate creating filter objects, Java async driver provides the [`Filters`]({{< apiref "com/mongodb/client/model/Filters.html">}}) helper. To specify an empty filter (i.e. match all documents in a collection), use an empty [`Document`]({{< apiref "org/bson/Document.html" >}}) object.

- An update document that specifies the modifications. For a list of the available operators, see [update operators]({{<docsref "reference/operator/update-field">}}).

- A callback.

The update methods return an [`UpdateResult`]({{<apiref "com/mongodb/client/result/UpdateResult.html">}}), which provides information about the operation including the number of documents modified by the update.

### Update a Single Document

To update at most a single document, use the [`updateOne`]({{< apiref "com/mongodb/async/client/MongoCollection.html#updateMany(org.bson.conversions.Bson,org.bson.conversions.Bson,com.mongodb.async.SingleResultCallback)">}}).

The following example updates the first document that meets the filter ``i`` equals ``10`` and sets the value of ``i`` to ``110``:

```java
collection.updateOne(eq("i", 10), set("i", 110),
    new SingleResultCallback<UpdateResult>() {
        @Override
        public void onResult(final UpdateResult result, final Throwable t) {
            System.out.println(result.getModifiedCount());
        }
    });
```

### Update Multiple Documents

To update all documents matching the filter, use the [`updateMany`]({{< apiref "com/mongodb/async/client/MongoCollection.html#updateMany(org.bson.conversions.Bson,org.bson.conversions.Bson)">}}) method.

The following example increments the value of ``i`` by ``100`` for all documents where ``i`` is less than ``100``:

```java
collection.updateMany(lt("i", 100), inc("i", 100),
    new SingleResultCallback<UpdateResult>() {
        @Override
        public void onResult(final UpdateResult result, final Throwable t) {
            System.out.println(result.getModifiedCount());
        }
    });
```

## Delete Documents

To delete documents from a collection, you can use the collection's [`deleteOne`]({{< apiref "com/mongodb/async/client/MongoCollection.html#deleteOne(org.bson.conversions.Bson,com.mongodb.async.SingleResultCallback)">}}) and [`deleteMany`]({{< apiref "com/mongodb/async/client/MongoCollection.html#deleteMany(org.bson.conversions.Bson,com.mongodb.async.SingleResultCallback)">}}) methods.

Pass to the methods:

- A filter object to determine the document or documents to delete. To facilitate creating filter objects, Java driver provides the [`Filters`]({{< apiref "com/mongodb/client/model/Filters.html">}}) helper. To specify an empty filter (i.e. match all documents in a collection), use an empty [`Document`]({{< apiref "org/bson/Document.html" >}}) object.

- A callback.

The delete methods return a [`DeleteResult`]({{< apiref "com/mongodb/client/result/DeleteResult.html">}})
which provides information about the operation including the number of documents deleted.

### Delete a Single Document That Match a Filter

To delete at most a single document that match the filter, use the [`deleteOne`]({{< apiref "com/mongodb/async/client/MongoCollection.html#deleteOne(org.bson.conversions.Bson,com.mongodb.async.SingleResultCallback)">}}) method:

The following example deletes at most one document that meets the filter ``i`` equals ``110``:

```java
collection.deleteOne(eq("i", 110), new SingleResultCallback<DeleteResult>() {
    @Override
    public void onResult(final DeleteResult result, final Throwable t) {
        System.out.println(result.getDeletedCount());
    }
});
```

### Delete All Documents That Match a Filter

To delete all documents matching the filter use the [`deleteMany`]({{< apiref "com/mongodb/async/client/MongoCollection.html#deleteMany(org.bson.conversions.Bson,com.mongodb.async.SingleResultCallback)">}}) method.

The following example deletes all documents where ``i`` is greater or equal to ``100``:

```java
collection.deleteMany(gte("i", 100), new SingleResultCallback<DeleteResult>() {
    @Override
    public void onResult(final DeleteResult result, final Throwable t) {
        System.out.println(result.getDeletedCount());
    }
});
```

### Additional Information

For additional tutorials about using MongoDB with POJOs, see the [POJOs Quick Start]({{< ref "driver-async/getting-started/quick-start-pojo.md" >}}).

For additional tutorials (such as to use the aggregation framework, specify write concern, etc.), see the [Java Async Driver Tutorials]({{< ref "driver-async/tutorials/index.md" >}}).
