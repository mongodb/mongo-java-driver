+++
date = "2015-03-17T15:36:56Z"
draft = true
title = "CRUD Operations"
[menu.main]
  parent = "Getting Started"
  weight = 10
  pre = "<i class='fa'></i>"
+++

# Getting Started

## Introduction

This page is a brief overview of working with the 3.0 MongoDB Java
Driver.

For more information about the Java API, please refer to the [online API
Documentation for 3.0 Java Driver](http://api.mongodb.org/java/3.0/index.html).

# A Quick Tour

See the [Binaries section of the
README](https://github.com/mongodb/mongo-java-driver/tree/master#binaries)
for instructions on how to include the driver in your project.


{{% note %}}
The following code snippets come from the QuickTour.java example code that can
be found with the [driver source](https://github.com/mongodb/mongo-java-driver/blob/3.0.x/driver/src/examples/tour/QuickTour.java).
{{% /note %}}

### Making a Connection

To make a connection to a MongoDB, you need to have at the minimum, the
name of a database to connect to. The database doesn't have to exist -if
it doesn't, MongoDB will create it for you.

Additionally, you can specify the server address and port when
connecting. The following example shows three ways to connect to the
database `mydb` on the local machine :

```java
// To directly connect to a single MongoDB server (note that this will not auto-discover the primary even
// if it's a member of a replica set:
MongoClient mongoClient = new MongoClient();
// or
MongoClient mongoClient = new MongoClient( "localhost" );
// or
MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
// or, to connect to a replica set, with auto-discovery of the primary, supply a seed list of members
MongoClient mongoClient = new MongoClient(Arrays.asList(new ServerAddress("localhost", 27017),
                                      new ServerAddress("localhost", 27018),
                                      new ServerAddress("localhost", 27019)));
// or use a connection string
MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017,localhost:27018,localhost:27019"));

MongoDatabase database = mongoClient.getDatabase("mydb");
```

At this point, the `database` object will be a connection to a MongoDB
server for the specified database. With it, you can do further
operations.

{{% note %}}
The `MongoClient` instance actually represents a pool of connections to the database; you will only need one instance of class `MongoClient` even with multiple threads. See the concurrency \<java-driver-concurrency\> doc page for more information.
{{% /note %}}

The `MongoClient` class is designed to be thread safe and shared among
threads. Typically you create only 1 instance for a given database
cluster and use it across your application. If for some reason you
decide to create many `MongoClient` instances, note that:

-   all resource usage limits (max connections, etc) apply per
    `MongoClient` instance
-   to dispose of an instance, make sure you call `MongoClient.close()`
    to clean up resources

### Getting a Collection

To get a collection to use, just specify the name of the collection to
the [getCollection(String
collectionName)](http://api.mongodb.org/java/3.0/com/mongodb/client/MongoDatabase.html#getCollection-java.lang.String-)
method:

```java
MongoCollection<Document> collection = database.getCollection("test");
```

Once you have this collection object, you can now do things like insert
data, query for data, etc.

### Inserting a Document

Once you have the collection object, you can insert documents into the
collection. For example, lets make a little document that in JSON would
be represented as

``` {.sourceCode .javascript}
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

Notice that the above has an "inner" document embedded within it. To do
this, we can use the
[Document](http://api.mongodb.org/java/3.0/org/bson/Document.html) class
to create the document (including the inner document), and then just
simply insert it into the collection using the `insertOne()` method.

```java
Document doc = new Document("name", "MongoDB")
               .append("type", "database")
               .append("count", 1)
               .append("info", new Document("x", 203).append("y", 102));
collection.insertOne(doc);
```

### Finding the First Document in a Collection Using `find()`

To show that the document we inserted in the previous step is there, we
can do a simple
[find()](http://api.mongodb.org/java/3.0/com/mongodb/client/MongoCollection.html#find--)
operation followed by a call to
[first()](http://api.mongodb.org/java/3.0/com/mongodb/client/MongoIterable.html#first--)
to get the first document in the collection. This method returns a
single document, and it's useful for queries which should only match a
single document, or you are only interested in the first. You don't have
to deal with the cursor.

```java
Document myDoc = collection.find().first();
System.out.println(myDoc);
```

and you should see

```java
Document{{_id=54b5594843bb7b25f1c9da72, name=MongoDB, type=database, count=1, info=Document{{x=203, y=102}}}}
```

{{% note %}}
The `_id` element has been added automatically by MongoDB to your
document. Remember, MongoDB reserves element names that start with
"\_" and "\$" for internal use.
{{% /note %}}

### Adding Multiple Documents

In order to do more interesting things with queries, let's add multiple
simple documents to the collection. These documents will just be

```javascript
{
   "i" : value
}
```

and we can do this fairly efficiently in a loop

```java
List<Document> documents = new ArrayList<Document>();
for (int i = 0; i < 100; i++) {
    documents.add(new Document("i", i));
}
collection.insertMany(documents);
```

### Counting Documents in A Collection

Now that we've inserted 101 documents (the 100 we did in the loop, plus
the first one), we can check to see if we have them all using the
[count()](http://api.mongodb.org/java/3.0/com/mongodb/client/MongoCollection.html#count--)
method.

```java
System.out.println(collection.count());
```

and it should print `101`.

### Using a Cursor to Get All the Documents

In order to get all the documents in the collection, we will use the
`find()` method. The `find()` method returns a `MongoIterable` instance
which allows us to iterate over the set of documents that matched our
query. So to query all of the documents and print them out :

```java
MongoCursor<Document> cursor = collection.find().iterator();
try {
    while (cursor.hasNext()) {
        System.out.println(cursor.next());
    }
} finally {
    cursor.close();
}
```

and that should print all 101 documents in the collection.

Note that while this idiom is permissible:

```java
for (Document cur : collection.find()) {
    System.out.println(cur);
}
```

its use is discouraged as the application can leak a cursor if the loop
terminates early.

### Getting A Single Document with A Query Filter

We can create a filter to pass to the find() method to get a subset of
the documents in our collection. For example, if we wanted to find the
document for which the value of the "i" field is 71, we would do the
following ;

```java
myDoc = collection.find(eq("i", 71)).first();
System.out.println(myDoc);
```

and it should just print just one document

```javascript
Document{{_id=54b5629643bb7b2a52e19ea3, i=71}}
```

Note that this usage relies on a static import of the Filters.eq method:

```java
import static com.mongodb.client.model.Filters.*;
```

### Getting A Set of Documents With a Query

We can use the query to get a set of documents from our collection. For
example, if we wanted to get all documents where `"i" > 50`, we could
write:

```java
// now use a range query to get a larger subset
cursor = collection.find(gt("i", 50)).iterator();

try {
    while (cursor.hasNext()) {
        System.out.println(cursor.next());
    }
} finally {
    cursor.close();
}
```

which should print the documents where `i > 50`.

We could also get a range, say `50 < i <= 100`:

```java
cursor = collection.find(and(gt("i", 50), lte("i", 100))).iterator();

try {
    while (cursor.hasNext()) {
        System.out.println(cursor.next());
    }
} finally {
    cursor.close();
}
```

### MaxTime

MongoDB 2.6 introduced the ability to timeout individual queries:

```java
collection.find().maxTime(1, TimeUnit.SECONDS).first();
```

In the example above the maxTime is set to one second and the query will
be aborted after the full second is up.

### Bulk operations

Under the covers MongoDB is moving away from the combination of a write
operation followed by get last error (GLE) and towards a write commands
API. These new commands allow for the execution of bulk
insert/update/delete operations. There are two types of bulk operations:

1.  

    Ordered bulk operations.

    :   Executes all the operation in order and error out on the first
        write error.

2.  

    Unordered bulk operations.

    :   These operations execute all the operations in parallel and
        aggregates up all the errors. Unordered bulk operations do not
        guarantee order of execution.

Let's look at two simple examples using ordered and unordered
operations:

```java
// 2. Ordered bulk operation - order is guarenteed
collection.bulkWrite(Arrays.asList(new InsertOneModel<>(new Document("_id", 4)),
                                   new InsertOneModel<>(new Document("_id", 5)),
                                   new InsertOneModel<>(new Document("_id", 6)),
                                   new UpdateOneModel<>(new Document("_id", 1),
                                                        new Document("$set", new Document("x", 2))),
                                   new DeleteOneModel<>(new Document("_id", 2)),
                                   new ReplaceOneModel<>(new Document("_id", 3),
                                                         new Document("_id", 3).append("x", 4))));


 // 2. Unordered bulk operation - no guarantee of order of operation
collection.bulkWrite(Arrays.asList(new InsertOneModel<>(new Document("_id", 4)),
                                   new InsertOneModel<>(new Document("_id", 5)),
                                   new InsertOneModel<>(new Document("_id", 6)),
                                   new UpdateOneModel<>(new Document("_id", 1),
                                                        new Document("$set", new Document("x", 2))),
                                   new DeleteOneModel<>(new Document("_id", 2)),
                                   new ReplaceOneModel<>(new Document("_id", 3),
                                                         new Document("_id", 3).append("x", 4)))),
                     new BulkWriteOptions().ordered(false));
```

{{% note %}}
For servers older than 2.6 the API will down convert the operations,
and support the correct semantics for BulkWriteResult and
BulkWriteException each write operation has to be done one at a time.
It's not possible to down convert 100% so there might be slight edge
cases where it cannot correctly report the right numbers.
{{% /note %}}

Quick Tour of the Administrative Functions
------------------------------------------

### Getting A List of Databases

You can get a list of the available databases:

```java
for (String name: mongoClient.listDatabaseNames()) {
    System.out.println(name);
}
```

Calling the `getDatabase()` on `MongoClient` does not create a database.
Only when a database is written to will a database be created. Examples
would be creating an index or collection or inserting a document into a
collection.

### Dropping A Database

You can drop a database by name using a `MongoClient` instance:

```java
mongoClient.dropDatabase("databaseToBeDropped");
```

### Creating A Collection

There are two ways to create a collection. Inserting a document will
create the collection if it doesn't exist or calling the
[createCollection](http://docs.mongodb.org/manual/reference/method/db.createCollection)
command.

An example of creating a capped collection\_ sized to 1 megabyte:

```java
database.createCollection("cappedCollection", new CreateCollectionOptions().capped(true).sizeInBytes(0x100000));
```

### Getting A List of Collections

You can get a list of the available collections in a database:

```java
for (String name : database.listCollectionNames()) {
    System.out.println(name);
}
```

### Dropping A Collection

You can drop a collection by using the drop() method:

```java
collection.dropCollection();
```

And you should notice that the collection no longer exists.

### Creating An Index

MongoDB supports indexes, and they are very easy to add on a collection.
To create an index, you just specify the field that should be indexed,
and specify if you want the index to be ascending (`1`) or descending
(`-1`). The following creates an ascending index on the `i` field :

```java
// create an ascending index on the "i" field
 collection.createIndex(new Document("i", 1));
```

### Getting a List of Indexes on a Collection

You can get a list of the indexes on a collection:

```java
for (final Document index : collection.listIndexes()) {
    System.out.println(index);
}
```

and you should see something like

```javascript
Document{{v=1, key=Document{{_id=1}}, name=_id_, ns=mydb.test}}
Document{{v=1, key=Document{{i=1}}, name=i_1, ns=mydb.test}}
```

### Text indexes

MongoDB also provides text indexes to support text search of string
content. Text indexes can include any field whose value is a string or
an array of string elements. To create a text index specify the string
literal "text" in the index document:

```java
// create a text index on the "content" field
coll.createIndex(new BasicDBObject("content", "text"));
```

As of MongoDB 2.6 text indexes are now integrated into the main query
language and enabled by default:

```java
// Insert some documents
collection.insertOne(new Document("_id", 0).append("content", "textual content"));
collection.insertOne(new Document("_id", 1).append("content", "additional content"));
collection.insertOne(new Document("_id", 2).append("content", "irrelevant content"));

// Find using the text index
Document search = new Document("$search", "textual content -irrelevant");
Document textSearch = new Document("$text", search);
long matchCount = collection.count(textSearch);
System.out.println("Text search matches: "+ matchCount);

// Find using the $language operator
textSearch = new Document("$text", search.append("$language", "english"));
matchCount = collection.count(textSearch);
System.out.println("Text search matches (english): "+ matchCount);

// Find the highest scoring match
Document projection = new Document("score", new Document("$meta", "textScore"));
myDoc = collection.find(textSearch).projection(projection).first();
System.out.println("Highest scoring document: "+ myDoc);
```

and it should print:

```javascript
Text search matches: 2
Text search matches (english): 2
Highest scoring document: Document{{_id=1, content=additional content, score=0.75}}
```

For more information about text search see the
text index \</core/index-text\> and
\$text query operator \</reference/operator/query/text\> documentation.
