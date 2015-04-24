+++
date = "2015-03-17T15:36:56Z"
title = "Quick Tour"
[menu.main]
  parent = "Async Getting Started"
  identifier = "Async Quick Tour"
  weight = 30
  pre = "<i class='fa'></i>"
+++

# MongoDB Async Driver Quick Tour

The following code snippets come from the `QuickTour.java` example code
that can be found with the [async driver
source]({{< srcref "driver-async/src/examples/tour/QuickTour.java">}}).

{{% note %}}
See the [installation guide]({{< relref "driver-async/getting-started/installation-guide.md#mongodb-async-driver" >}})
for instructions on how to install the MongoDB Async Driver.
{{% /note %}}

## Going Async with Callbacks

The MongoDB Async driver provides an asynchronous API that can leverage either Netty or Java 7's `AsynchronousSocketChannel` for fast and non-blocking IO.

The API mirrors the new Synchronous API from the MongoDB Driver, but any methods that cause network IO take a `SingleResponseCallback<T>` and return immediately, where `T` is the type of response for the document.

The `SingleResponseCallback<T>` interface requires the implementation of a single method `onResult(T result, Throwable t)` which is called when the operation has completed.  The `result` parameter contains the result of the operation, if successful. If the operation failed for any reason then the `t` contains the `Throwable` reason for the failure.

{{% note class="important" %}}
It's important to always check for errors in any `SingleResponseCallback<T>` implementation
and handle them appropriately! Below the error checks are left out only for the sake of brevity.
{{% /note %}}

## Make a Connection

The following example shows multiple ways to connect to the database `mydb` on the local machine, using the 
[`MongoClients.create`]({{< apiref "com/mongodb/async/client/MongoClients.html#create-com.mongodb.ConnectionString-" >}}) helper.


```java
// To directly connect to the default server localhost on port 27017
MongoClient mongoClient = MongoClients.create();

// Use a Connection String
MongoClient mongoClient = MongoClients.create("mongodb://localhost");

// or a Connection String
MongoClient mongoClient = MongoClients.create(new ConnectionString("mongodb://localhost"));

// or provide custom MongoClientSettings
ClusterSettings clusterSettings = ClusterSettings.builder().hosts(asList(new ServerAddress("localhost"))).build();
MongoClientSettings settings = MongoClientSettings.builder().clusterSettings(clusterSettings).build();
MongoClient mongoClient = MongoClients.create(settings);

MongoDatabase database = mongoClient.getDatabase("mydb");
```

At this point, the `database` object will be a connection to a MongoDB
server for the specified database.

{{% note %}}
There is no callback required for `getDatabase("mydb")` as there is no network IO required.
A `MongoDatabase` instance provides methods to interact with a database
but the database might not actually exist and will only be created on the
insertion of data via some means; e.g. the creation of a collection or the insertion of documents
which do require callbacks as they require network IO.
{{% /note %}}

### MongoClient

The `MongoClient` instance actually represents a pool of connections
to the database; you will only need one instance of class
`MongoClient` even with multiple concurrently executing asynchronous operations.

{{% note class="important" %}}
Typically you only create one `MongoClient` instance for a given database
cluster and use it across your application. When creating multiple instances:

-   All resource usage limits (max connections, etc) apply per
    `MongoClient` instance
-   To dispose of an instance, make sure you call `MongoClient.close()`
    to clean up resources
{{% /note %}}

## Get a Collection

To get a collection to operate upon, specify the name of the collection to
the [`getCollection(String collectionName)`]({{< apiref "com/mongodb/async/client/MongoDatabase.html#getCollection-java.lang.String-">}})
method:

The following example gets the collection `test`:

```java
MongoCollection<Document> collection = database.getCollection("test");
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

To create the document using the Java driver, use the
[Document]({{< apiref "org/bson/Document.html">}}) class. You
can use this class to create the embedded document as well.

```java
Document doc = new Document("name", "MongoDB")
               .append("type", "database")
               .append("count", 1)
               .append("info", new Document("x", 203).append("y", 102));
```

To insert the document into the collection, use the `insertOne()` method.

```java
collection.insertOne(doc, new SingleResultCallback<Void>() {
    @Override
    public void onResult(final Void result, final Throwable t) {
        System.out.println("Inserted!");
    }
});
```

As `SingleResponseCallback<T>` is a [functional interface](https://docs.oracle.com/javase/specs/jls/se8/html/jls-9.html#jls-9.8) and it can be
implemented as a lambda for users on Java 8:

```java
collection.insertOne(doc, (Void result, final Throwable t) -> System.out.println('Inserted!'));
```

Once the document has been inserted the `onResult` callback will be called and it will
print "Inserted!".  Remember, in a normal application you would always check for the presence of
errors in the `t` variable.

## Add Multiple Documents

To add multiple documents, you can use the `insertMany()` method.

The following example will add multiple documents of the form:

```javascript
{ "i" : value }
```

Create the documents in a loop.

```java
List<Document> documents = new ArrayList<Document>();
for (int i = 0; i < 100; i++) {
    documents.add(new Document("i", i));
}
```

To insert these documents to the collection, pass the list of documents to the
`insertMany()` method.

```java
collection.insertMany(documents, new SingleResultCallback<Void>() {
    @Override
    public void onResult(final Void result, final Throwable t) {
        System.out.println("Documents inserted!");
    }
});
```

## Count Documents in A Collection

Now that we've inserted 101 documents (the 100 we did in the loop, plus
the first one), we can check to see if we have them all using the
[count()]({{< apiref "com/mongodb/async/client/MongoCollection#count--">}})
method. The following code should print `101`.

```java
collection.count(
  new SingleResultCallback<Long>() {
      @Override
      public void onResult(final Long count, final Throwable t) {
          System.out.println(count);
      }
  });
```

## Query the Collection

Use the
[find()]({{< apiref "com/mongodb/async/client/MongoCollection.html#find--">}})
method to query the collection.

### Find the First Document in a Collection

To get the first document in the collection, call the
[first()]({{< apiref "com/mongodb/async/client/MongoIterable.html#first--">}})
method on the [find()]({{< apiref "com/mongodb/async/client/MongoCollection.html#find--">}})
operation. `collection.find().first()` returns the first document or null rather than a cursor.
This is useful for queries that should only match a single document, or if you are
interested in the first document only.

{{% note %}}
Sometimes you will need the same or similar callbacks more than once.  In these situations
it makes sense to DRY (Do not Repeat Yourself) up your code and save the callback either
as a concrete class or assign to a variable as below:

```java
SingleResultCallback<Document> printDocument = new SingleResultCallback<Document>() {
    @Override
    public void onResult(final Document document, final Throwable t) {
        System.out.println(document.toJson());
    }
};
```
{{% /note %}}

The following example passes the `printDocument` callback  to the `first` method:

```java
collection.find().first(printDocument);
```

The example will print the following document:

```json
{ "_id" : { "$oid" : "551582c558c7b4fbacf16735" },
  "name" : "MongoDB", "type" : "database", "count" : 1,
  "info" : { "x" : 203, "y" : 102 } }
```

{{% note %}}
The `_id` element has been added automatically by MongoDB to your
document and your value will differ from that shown. MongoDB reserves field
names that start with
"_" and "$" for internal use.
{{% /note %}}

### Find All Documents in a Collection

To retrieve all the documents in the collection, we will use the
`find()` method. The `find()` method returns a `FindIterable` instance that
provides a fluent interface for chaining or controlling find operations. Use the
`forEach()` method to provide a `Block` to apply to each document and a callback that
is run once the iteration has finished.
The following code retrieves all documents in the collection and prints them out
(101 documents) and then finally prints out "Operation Finished!":

```java
Block<Document> printDocumentBlock = new Block<Document>() {
    @Override
    public void apply(final Document document) {
        System.out.println(document.toJson());
    }
};
SingleResultCallback<Void> callbackWhenFinished = new SingleResultCallback<Void>() {
    @Override
    public void onResult(final Void result, final Throwable t) {
        System.out.println("Operation Finished!");
    }
};

collection.find().forEach(printDocumentBlock, callbackWhenFinished);
```

## Get A Single Document with a Query Filter

We can create a filter to pass to the find() method to get a subset of
the documents in our collection. For example, if we wanted to find the
document for which the value of the "i" field is 71, we would do the
following (reusing the `printDocument` callback):

```java
import static com.mongodb.client.model.Filters.*;

collection.find(eq("i", 71)).first(printDocument);
```

will eventually print just one document:

```json
{ "_id" : { "$oid" : "5515836e58c7b4fbc756320b" }, "i" : 71 }
```


{{% note %}}
Use the [Filters]({{< apiref "com/mongodb/client/model/Filters">}}), [Sorts]({{< apiref "com/mongodb/client/model/Sorts">}}) and [Projections]({{< apiref "com/mongodb/client/model/Projections">}})
helpers for simple and concise ways of building up queries.
{{% /note %}}

## Get a Set of Documents with a Query

We can use the query to get a set of documents from our collection. For
example, if we wanted to get all documents where `"i" > 50`, we could
write (reusing `printDocumentBlock` block and `callbackWhenFinished` callback):

```java
// now use a range query to get a larger subset
collection.find(gt("i", 50)).forEach(printDocumentBlock, callbackWhenFinished);
```
which should print the documents where `i > 50`.

We could also get a range, say `50 < i <= 100`:

```java
collection.find(and(gt("i", 50), lte("i", 100))).forEach(printDocumentBlock, callbackWhenFinished);
```

## Sorting documents

We can also use the [Sorts]({{< apiref "com/mongodb/client/model/Sorts">}}) helpers to sort documents.
We add a sort to a find query by calling the `sort()` method on a `FindIterable`.  Below we use the [`exists()`]({{ < apiref "com/mongodb/client/model/Filters.html#exists-java.lang.String-">}}) helper and sort
[`descending("i")`]({{ < apiref "com/mongodb/client/model/Sorts.html#exists-java.lang.String-">}}) helper to
sort our documents:

```java
collection.find(exists("i")).sort(descending("i")).first(printDocument);
```

## Projecting fields

Sometimes we don't need all the data contained in a document. The [Projections]({{< apiref "com/mongodb/client/model/Projections">}}) 
helpers can be used to build the projection parameter for the find operation and limit the fields returned.  
Below we'll sort the collection, exclude the `_id` field and output the first matching document:

```java
collection.find().projection(excludeId()).first(printDocument);
```

## Updating documents

There are numerous [update operators](http://docs.mongodb.org/manual/reference/operator/update-field/)
supported by MongoDB.

To update at most a single document (may be 0 if none match the filter), use the [`updateOne`]({{< apiref "com/mongodb/async/client/MongoCollection.html#updateOne-org.bson.conversions.Bson-org.bson.conversions.Bson-">}})
method to specify the filter and the update document.  Here we update the first document that meets the filter `i` equals `10` and set the value of `i` to `110`:

```java
collection.updateOne(eq("i", 10), new Document("$set", new Document("i", 110)),
    new SingleResultCallback<UpdateResult>() {
        @Override
        public void onResult(final UpdateResult result, final Throwable t) {
            System.out.println(result.getModifiedCount());
        }
    });
```

To update all documents matching the filter use the [`updateMany`]({{< apiref "com/mongodb/async/client/MongoCollection.html#updateMany-org.bson.conversions.Bson-org.bson.conversions.Bson-">}})
method.  Here we increment the value of `i` by `100` where `i`
is less than `100`.

```java
collection.updateMany(lt("i", 100), new Document("$inc", new Document("i", 100)),
    new SingleResultCallback<UpdateResult>() {
        @Override
        public void onResult(final UpdateResult result, final Throwable t) {
            System.out.println(result.getModifiedCount());
        }
    });
```

The update methods return an [`UpdateResult`]({{< apiref "com/mongodb/client/result/UpdateResult.html">}}),
which provides information about the operation including the number of documents modified by the update.

## Deleting documents

To delete at most a single document (may be 0 if none match the filter) use the [`deleteOne`]({{< apiref "com/mongodb/async/client/MongoCollection.html#deleteOne-org.bson.conversions.Bson-">}})
method:

```java
collection.deleteOne(eq("i", 110), new SingleResultCallback<DeleteResult>() {
    @Override
    public void onResult(final DeleteResult result, final Throwable t) {
        System.out.println(result.getDeletedCount());
    }
});
```

To delete all documents matching the filter use the 
[`deleteMany`]({{< apiref "com/mongodb/async/client/MongoCollection.html#deleteMany-org.bson.conversions.Bson-">}}) method. Here we delete 
all documents where `i` is greater or equal to `100`:

```java
collection.deleteMany(gte("i", 100), new SingleResultCallback<DeleteResult>() {
    @Override
    public void onResult(final DeleteResult result, final Throwable t) {
        System.out.println(result.getDeletedCount());
    }
});
```

The delete methods return a [`DeleteResult`]({{< apiref "com/mongodb/client/result/DeleteResult.html">}}),
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

```java
SingleResultCallback<BulkWriteResult> printBatchResult = new SingleResultCallback<BulkWriteResult>() {
    @Override
    public void onResult(final BulkWriteResult result, final Throwable t) {
        System.out.println(result);
    }
};

// 2. Ordered bulk operation - order is guaranteed
collection.bulkWrite(
  Arrays.asList(new InsertOneModel<>(new Document("_id", 4)),
                new InsertOneModel<>(new Document("_id", 5)),
                new InsertOneModel<>(new Document("_id", 6)),
                new UpdateOneModel<>(new Document("_id", 1),
                                     new Document("$set", new Document("x", 2))),
                new DeleteOneModel<>(new Document("_id", 2)),
                new ReplaceOneModel<>(new Document("_id", 3),
                                      new Document("_id", 3).append("x", 4))),
  printBatchResult
);


 // 2. Unordered bulk operation - no guarantee of order of operation
collection.bulkWrite(
  Arrays.asList(new InsertOneModel<>(new Document("_id", 4)),
                new InsertOneModel<>(new Document("_id", 5)),
                new InsertOneModel<>(new Document("_id", 6)),
                new UpdateOneModel<>(new Document("_id", 1),
                                     new Document("$set", new Document("x", 2))),
                new DeleteOneModel<>(new Document("_id", 2)),
                new ReplaceOneModel<>(new Document("_id", 3),
                                      new Document("_id", 3).append("x", 4))),
  new BulkWriteOptions().ordered(false),
  printBatchResult
);
```

{{% note class="important" %}}
Use of the bulkWrite methods is not recommended when connected to pre-2.6 MongoDB servers, as this was the first server version to support bulk write commands for insert, update, and delete in a way that allows the driver to implement the correct semantics for BulkWriteResult and BulkWriteException. The methods will still work for pre-2.6 servers, but performance will suffer, as each write operation has to be executed one at a time.
{{% /note %}}
