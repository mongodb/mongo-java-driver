+++
date = "2015-03-17T15:36:56Z"
title = "Quick Tour"
[menu.main]
  parent = "Getting Started"
  weight = 10
  pre = "<i class='fa'></i>"
+++

# MongoDB Driver Quick Tour

The following code snippets come from the `QuickTour.java` example code
that can be found with the [driver source]({{< srcref "src/examples/example/QuickTour.java">}}).

{{% note %}}
See the [installation guide]({{< relref "getting-started/installation-guide.md" >}})
for instructions on how to install the MongoDB Driver.
{{% /note %}}

## Making a Connection

To make a connection to a MongoDB, you need to have at the minimum, the
name of a database to connect to. The database doesn't have to exist -if
it doesn't, MongoDB will create it for you.

Additionally, you can specify the server address and port when
connecting. The following example shows four ways to connect to the
database `mydb` on the local machine :

```java
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ParallelScanOptions;
import com.mongodb.ServerAddress;

import java.util.List;
import java.util.Set;

import static java.util.concurrent.TimeUnit.SECONDS;

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

DB db = mongoClient.getDB( "mydb" );
```

At this point, the `db` object will be a connection to a MongoDB server
for the specified database. With it, you can do further operations.

The `MongoClient` class is designed to be thread safe and shared among
threads. Typically you create only 1 instance for a given database
cluster and use it across your application.

{{% note class="important" %}}
When creating many `MongoClient` instances:

-   All resource usage limits (max connections, etc) apply per
    `MongoClient` instance
-   To dispose of an instance, make sure you call `MongoClient.close()`
    to clean up resources
{{% /note %}}

## Authentication (Optional)

MongoDB can be run in a secure mode where access to databases is
controlled via authentication. When run in this mode, any client
application must provide a list of credentials which will be used to
authenticate against. In the Java driver, you simply provide the
credentials when creating a `MongoClient` instance:

```java
MongoCredential credential = MongoCredential.createMongoCRCredential(userName, database, password);
MongoClient mongoClient = new MongoClient(new ServerAddress(), Arrays.asList(credential));
```

MongoDB supports various different authentication mechanisms see the
[access control tutorials](http://docs.mongodb.org/manual/administration/security-access-control)
for more information.

## Getting a Collection

To get a collection to use, just specify the name of the collection to
the [getCollection(String
collectionName)](http://api.mongodb.org/java/2.13/com/mongodb/DB.html#getCollection%28java.lang.String%29)
method:

```java
DBCollection coll = db.getCollection("testCollection");
```

Once you have this collection object, you can now do things like insert
data, query for data, etc

## Setting Write Concern

As of version 2.10.0, the default write concern is
[WriteConcern.ACKNOWLEDGED](http://api.mongodb.org/java/2.13/com/mongodb/WriteConcern.html#ACKNOWLEDGED),
but it can be easily changed:

```java
mongoClient.setWriteConcern(WriteConcern.JOURNALED);
```

There are many options for write concern. Additionally, the default
write concern can be overridden on the database, collection, and
individual update operations. Please consult the [API
Documentation](http://api.mongodb.org/java/2.13/index.html) for
details.

## Inserting a Document

Once you have the collection object, you can insert documents into the
collection. For example, lets make a little document that in JSON would
be represented as

```json
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
[BasicDBObject](http://api.mongodb.org/java/2.13/com/mongodb/BasicDBObject.html)
class to create the document (including the inner document), and then
just simply insert it into the collection using the `insert()` method.

```java
BasicDBObject doc = new BasicDBObject("name", "MongoDB")
        .append("type", "database")
        .append("count", 1)
        .append("info", new BasicDBObject("x", 203).append("y", 102));
coll.insert(doc);
```

## Finding the First Document in a Collection Using `findOne()`

To show that the document we inserted in the previous step is there, we
can do a simple
[findOne()](http://api.mongodb.org/java/2.13/com/mongodb/DBCollection.html#findOne%28java.lang.Object%29)
operation to get the first document in the collection. This method
returns a single document (rather than the [DBCursor](http://api.mongodb.org/java/2.13/com/mongodb/DBCursor.html)
that the [find()](http://api.mongodb.org/java/2.13/com/mongodb/DBCollection.html#find()')
operation returns), and it's useful for things where there only is one
document, or you are only interested in the first. You don't have to
deal with the cursor.

```java
DBObject myDoc = coll.findOne();
System.out.println(myDoc);
```

and you should see

```json
{ "_id" : "49902cde5162504500b45c2c" ,
  "name" : "MongoDB" ,
  "type" : "database" ,
  "count" : 1 ,
  "info" : { "x" : 203 , "y" : 102}}
```

{{% note %}}
The `_id` element has been added automatically by MongoDB to your
document and your value will differ from that shown. MongoDB reserves field
names that start with "_" and "$" for internal use.
{{% /note %}}

## Adding Multiple Documents

In order to do more interesting things with queries, let's add multiple
simple documents to the collection. These documents will just be

```json
{
   "i" : value
}
```

and we can do this fairly efficiently in a loop

```java
for (int i=0; i < 100; i++) {
    coll.insert(new BasicDBObject("i", i));
}
```

Notice that we can insert documents of different "shapes" into the same
collection. This aspect is what we mean when we say that MongoDB is
"schema-free"

## Counting Documents in A Collection

Now that we've inserted 101 documents (the 100 we did in the loop, plus
the first one), we can check to see if we have them all using the
`getCount()` method.

```java
System.out.println(coll.getCount());
```

and it should print `101`.

## Using a Cursor to Get All the Documents

In order to get all the documents in the collection, we will use the
`find()` method. The `find()` method returns a `DBCursor` object which
allows us to iterate over the set of documents that matched our query.
So to query all of the documents and print them out :

```java
DBCursor cursor = coll.find();
try {
   while(cursor.hasNext()) {
       System.out.println(cursor.next());
   }
} finally {
   cursor.close();
}
```

and that should print all 101 documents in the collection.

## Getting A Single Document with A Query

We can create a query to pass to the find() method to get a subset of
the documents in our collection. For example, if we wanted to find the
document for which the value of the "i" field is 71, we would do the
following ;

```java
BasicDBObject query = new BasicDBObject("i", 71);

cursor = coll.find(query);

try {
   while(cursor.hasNext()) {
       System.out.println(cursor.next());
   }
} finally {
   cursor.close();
}
```

and it should just print just one document

```json
{ "_id" : "49903677516250c1008d624e" , "i" : 71 }
```

You may commonly see examples and documentation in MongoDB which use `$` Operators, such as this:

```json
db.things.find({j: {$ne: 3}, k: {$gt: 10} });
```

These are represented as regular `String` keys in the Java driver, using
embedded `DBObjects`:

```java
query = new BasicDBObject("j", new BasicDBObject("$ne", 3))
        .append("k", new BasicDBObject("$gt", 10));

cursor = coll.find(query);

try {
    while(cursor.hasNext()) {
        System.out.println(cursor.next());
    }
} finally {
    cursor.close();
}
```

## Getting A Set of Documents With a Query

We can use the query to get a set of documents from our collection. For
example, if we wanted to get all documents where `"i" > 50`, we could
write:

```java
// find all where i > 50
query = new BasicDBObject("i", new BasicDBObject("$gt", 50));

cursor = coll.find(query);
try {
    while (cursor.hasNext()) {
        System.out.println(cursor.next());
    }
} finally {
    cursor.close();
}
```

which should print the documents where `i > 50`.

We could also get a range, say `20 < i <= 30`:

```java
query = new BasicDBObject("i", new BasicDBObject("$gt", 20).append("$lte", 30));
cursor = coll.find(query);

try {
    while (cursor.hasNext()) {
        System.out.println(cursor.next());
    }
} finally {
    cursor.close();
}
```

## MaxTime

MongoDB 2.6 introduced the ability to timeout individual queries:

```java
coll.find().maxTime(1, SECONDS).count();
```

In the example above the maxTime is set to one second and the query will
be aborted after the full second is up.

## Bulk operations

Under the covers MongoDB is moving away from the combination of a write
operation followed by get last error (GLE) and towards a write commands
API. These new commands allow for the execution of bulk
insert/update/remove operations. There are two types of bulk operations:

1.  Ordered bulk operations.

      Executes all the operation in order and error out on the first write error.

2.   Unordered bulk operations.

      Executes all the operations and reports any the errors.

      Unordered bulk operations do not guarantee order of execution.

Let's look at two simple examples using ordered and unordered
operations:

```java
// 1. Ordered bulk operation
BulkWriteOperation builder = coll.initializeOrderedBulkOperation();
builder.insert(new BasicDBObject("_id", 1));
builder.insert(new BasicDBObject("_id", 2));
builder.insert(new BasicDBObject("_id", 3));

builder.find(new BasicDBObject("_id", 1)).updateOne(new BasicDBObject("$set", new BasicDBObject("x", 2)));
builder.find(new BasicDBObject("_id", 2)).removeOne();
builder.find(new BasicDBObject("_id", 3)).replaceOne(new BasicDBObject("_id", 3).append("x", 4));

BulkWriteResult result = builder.execute();

// 2. Unordered bulk operation - no guarantee of order of operation
builder = coll.initializeUnorderedBulkOperation();
builder.find(new BasicDBObject("_id", 1)).removeOne();
builder.find(new BasicDBObject("_id", 2)).removeOne();

result = builder.execute();
```

{{% note class="important" %}}
Use of the bulkWrite methods is not recommended when connected to pre-2.6 MongoDB servers, as this was the first server version to support bulk write commands for insert, update, and delete in a way that allows the driver to implement the correct semantics for BulkWriteResult and BulkWriteException. The methods will still work for pre-2.6 servers, but performance will suffer, as each write operation has to be executed one at a time.
{{% /note %}}
