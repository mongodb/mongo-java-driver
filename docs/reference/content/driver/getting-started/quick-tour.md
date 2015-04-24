+++
date = "2015-03-17T15:36:56Z"
title = "Quick Tour"
[menu.main]
  parent = "Sync Getting Started"
  identifier = "Sync Quick Tour"
  weight = 10
  pre = "<i class='fa'></i>"
+++

# MongoDB Driver Quick Tour

The following code snippets come from the `QuickTour.java` example code
that can be found with the [driver source]({{< srcref "driver/src/examples/tour/QuickTour.java">}}).

{{% note %}}
See the [installation guide]({{< relref "driver/getting-started/installation-guide.md" >}})
for instructions on how to install the MongoDB Driver.
{{% /note %}}

## Make a Connection

The following example shows five ways to connect to the
database `mydb` on the local machine. If the database does not exist, MongoDB
will create it for you.

```java
// To directly connect to a single MongoDB server
// (this will not auto-discover the primary even if it's a member of a replica set)
MongoClient mongoClient = new MongoClient();

// or
MongoClient mongoClient = new MongoClient( "localhost" );

// or
MongoClient mongoClient = new MongoClient( "localhost" , 27017 );

// or, to connect to a replica set, with auto-discovery of the primary, supply a seed list of members
MongoClient mongoClient = new MongoClient(
  Arrays.asList(new ServerAddress("localhost", 27017),
                new ServerAddress("localhost", 27018),
                new ServerAddress("localhost", 27019)));

// or use a connection string
MongoClientURI connectionString = new MongoClientURI("mongodb://localhost:27017,localhost:27018,localhost:27019");
MongoClient mongoClient = new MongoClient(connectionString);

MongoDatabase database = mongoClient.getDatabase("mydb");
```

At this point, the `database` object will be a connection to a MongoDB
server for the specified database.

### MongoClient

The `MongoClient` instance actually represents a pool of connections
to the database; you will only need one instance of class
`MongoClient` even with multiple threads.

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
the [`getCollection()`]({{< apiref "com/mongodb/client/MongoDatabase.html#getCollection-java.lang.String-">}})
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
[`Document`]({{< apiref "org/bson/Document.html">}}) class. You
can use this class to create the embedded document as well.

```java
Document doc = new Document("name", "MongoDB")
               .append("type", "database")
               .append("count", 1)
               .append("info", new Document("x", 203).append("y", 102));
```

To insert the document into the collection, use the `insertOne()` method.

```java
collection.insertOne(doc);
```

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
collection.insertMany(documents);
```

## Count Documents in A Collection

Now that we've inserted 101 documents (the 100 we did in the loop, plus
the first one), we can check to see if we have them all using the
[`count()`]({{< apiref "com/mongodb/client/MongoCollection#count--">}})
method. The following code should print `101`.

```java
System.out.println(collection.count());
```

## Query the Collection

Use the
[`find()`]({{< apiref "com/mongodb/client/MongoCollection.html#find--">}})
method to query the collection.

### Find the First Document in a Collection

To get the first document in the collection, call the
[`first()`]({{< apiref "com/mongodb/client/MongoIterable.html#first--">}})
method on the [`find()`]({{< apiref "com/mongodb/client/MongoCollection.html#find--">}})
operation. `collection.find().first()` returns the first document or null rather than a cursor.
This is useful for queries that should only match a single document, or if you are
interested in the first document only.

The following example prints the first document found in the collection.

```java
Document myDoc = collection.find().first();
System.out.println(myDoc.toJson());
```

The example should print the following document:

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
`iterator()` method to get an iterator over the set of documents that matched the
query and iterate. The following code retrieves all documents in the collection
and prints them out (101 documents):

```java
MongoCursor<Document> cursor = collection.find().iterator();
try {
    while (cursor.hasNext()) {
        System.out.println(cursor.next().toJson());
    }
} finally {
    cursor.close();
}
```

Although the following idiom is permissible, its use is discouraged as the
application can leak a cursor if the loop
terminates early:

```java
for (Document cur : collection.find()) {
    System.out.println(cur.toJson());
}
```

## Get A Single Document with a Query Filter

We can create a filter to pass to the find() method to get a subset of
the documents in our collection. For example, if we wanted to find the
document for which the value of the "i" field is 71, we would do the
following:

```java
import static com.mongodb.client.model.Filters.*;

myDoc = collection.find(eq("i", 71)).first();
System.out.println(myDoc.toJson());
```

and it should just print just one document

```json
{ "_id" : { "$oid" : "5515836e58c7b4fbc756320b" }, "i" : 71 }
```


{{% note %}}
Use the [`Filters`]({{< apiref "com/mongodb/client/model/Filters">}}), [`Sorts`]({{< apiref "com/mongodb/client/model/Sorts">}}) and
[`Projections`]({{< apiref "com/mongodb/client/model/Projections">}})
helpers for simple and concise ways of building up queries.
{{% /note %}}

## Get a Set of Documents with a Query

We can use the query to get a set of documents from our collection. For
example, if we wanted to get all documents where `"i" > 50`, we could
write:

```java
// now use a range query to get a larger subset
Block<Document> printBlock = new Block<Document>() {
     @Override
     public void apply(final Document document) {
         System.out.println(document.toJson());
     }
};
collection.find(gt("i", 50)).forEach(printBlock);
```

Notice we use the `forEach` method on `FindIterable` which applies a block to each
document and we print all documents where `i > 50`.

We could also get a range, say `50 < i <= 100`:

```java
collection.find(and(gt("i", 50), lte("i", 100))).forEach(printBlock);
```

## Sorting documents

We can also use the [`Sorts`]({{< apiref "com/mongodb/client/model/Sorts">}}) helpers to sort documents.
We add a sort to a find query by calling the `sort()` method on a `FindIterable`.  Below we use the
[`exists()`]({{ < apiref "com/mongodb/client/model/Filters.html#exists-java.lang.String-">}}) helper and sort
[`descending("i")`]({{ < apiref "com/mongodb/client/model/Sorts.html#exists-java.lang.String-">}}) helper to
sort our documents:

```java
myDoc = collection.find(exists("i")).sort(descending("i")).first();
System.out.println(myDoc.toJson());
```

## Projecting fields

Sometimes we don't need all the data contained in a document, the [`Projections`]({{< apiref "com/mongodb/client/model/Projections">}})
helpers help build the projection parameter for the
find operation.  Below we'll sort the collection, exclude the `_id` field and output the first
matching document:

```java
myDoc = collection.find().projection(excludeId()).first();
System.out.println(myDoc.toJson());
```

## Updating documents

There are numerous [update operators](http://docs.mongodb.org/manual/reference/operator/update-field/)
supported by MongoDB.

To update at most a single document (may be 0 if none match the filter), use the [`updateOne`]({{< apiref "com/mongodb/client/MongoCollection.html#updateOne-org.bson.conversions.Bson-org.bson.conversions.Bson-">}})
method to specify the filter and the update document.  Here we update the first document that meets the filter `i` equals `10` and set the value of `i` to `110`:

```java
collection.updateOne(eq("i", 10), new Document("$set", new Document("i", 110)));
```

To update all documents matching the filter use the [`updateMany`]({{< apiref "com/mongodb/async/client/MongoCollection.html#updateMany-org.bson.conversions.Bson-org.bson.conversions.Bson-">}})
method.  Here we increment the value of `i` by `100` where `i` is less than `100`.

```java
UpdateResult updateResult = collection.updateMany(lt("i", 100),
          new Document("$inc", new Document("i", 100)));
System.out.println(updateResult.getModifiedCount());
```

The update methods return an [`UpdateResult`]({{< apiref "com/mongodb/client/result/UpdateResult.html">}})
which provides information about the operation including the number of documents modified by the update.

## Deleting documents

To delete at most a single document (may be 0 if none match the filter) use the [`deleteOne`]({{< apiref "com/mongodb/client/MongoCollection.html#deleteOne-org.bson.conversions.Bson-">}})
method:

```java
collection.deleteOne(eq("i", 110));
```

To delete all documents matching the filter use the [`deleteMany`]({{< apiref "com/mongodb/client/MongoCollection.html#deleteMany-org.bson.conversions.Bson-">}}) method.  
Here we delete all documents where `i` is greater or equal to `100`:

```java
DeleteResult deleteResult = collection.deleteMany(gte("i", 100));
System.out.println(deleteResult.getDeletedCount());
```

The delete methods return a [`DeleteResult`]({{< apiref "com/mongodb/client/result/DeleteResult.html">}})
which provides information about the operation including the number of documents deleted.

## Bulk operations

These new commands allow for the execution of bulk
insert/update/delete operations. There are two types of bulk operations:

1.  Ordered bulk operations.

      Executes all the operation in order and error out on the first write error.

2.   Unordered bulk operations.

      Executes all the operations and reports any the errors.

      Unordered bulk operations do not guarantee order of execution.

Let's look at two simple examples using ordered and unordered
operations:

```java
// 2. Ordered bulk operation - order is guarenteed
collection.bulkWrite(
  Arrays.asList(new InsertOneModel<>(new Document("_id", 4)),
                new InsertOneModel<>(new Document("_id", 5)),
                new InsertOneModel<>(new Document("_id", 6)),
                new UpdateOneModel<>(new Document("_id", 1),
                                     new Document("$set", new Document("x", 2))),
                new DeleteOneModel<>(new Document("_id", 2)),
                new ReplaceOneModel<>(new Document("_id", 3),
                                      new Document("_id", 3).append("x", 4))));


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
  new BulkWriteOptions().ordered(false));
```

{{% note class="important" %}}
Use of the bulkWrite methods is not recommended when connected to pre-2.6 MongoDB servers, as this was the first server version to support bulk write commands for insert, update, and delete in a way that allows the driver to implement the correct semantics for BulkWriteResult and BulkWriteException. The methods will still work for pre-2.6 servers, but performance will suffer, as each write operation has to be executed one at a time.
{{% /note %}}
