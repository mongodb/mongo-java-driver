+++
date = "2015-03-17T15:36:56Z"
title = "Admin Quick Tour"
[menu.main]
  parent = "Async Getting Started"
  identifier = "Async Admin Quick Tour"
  weight = 50
  pre = "<i class='fa'></i>"
+++

# MongoDB Driver Admin Quick Tour

This is the second part of the MongoDB driver quick tour. In the
[quick tour]({{< relref "driver-async/getting-started/quick-tour.md" >}}) we looked at how to
use the Async Java driver to execute basic CRUD operations.  In this section we'll look at some of the
administrative features available in the driver.

The following code snippets come from the `QuickTourAdmin.java` example code
that can be found with the [driver
source]({{< srcref "driver-async/src/examples/tour/QuickTourAdmin.java">}}).

{{% note %}}
See the [installation guide]({{< relref "driver-async/getting-started/installation-guide.md" >}})
for instructions on how to install the MongoDB Driver.
{{% /note %}}

## Setup

To get started we'll quickly connect and create a `mongoClient`, `database` and `collection`
variable for use in the examples below:

```java
MongoClient mongoClient = new MongoClient(new ConnectionString("mongodb://localhost"));
MongoDatabase database = mongoClient.getDatabase("mydb");
MongoCollection<Document> collection = database.getCollection("test");
```

{{% note %}}
Calling the `getDatabase()` on `MongoClient` does not create a database.
Only when a database is written to will a database be created.  Examples include the creation of an index or the insertion of a document 
into a previously non-existent collection.
{{% /note %}}

{{% note %}}
Sometimes you will need the same or similar callbacks more than once.  In these situations
it makes sense to DRY (Do not Repeat Yourself) up your code and save the callback either
as a concrete class or assign to a variable as below:

```java
SingleResultCallback<Void> callbackWhenFinished = new SingleResultCallback<Void>() {
    @Override
    public void onResult(final Void result, final Throwable t) {
        System.out.println("Operation Finished!");
    }
};
```
{{% /note %}}


## Get A List of Databases

You can get a list of the available databases:

```java
mongoClient.listDatabaseNames().forEach(new Block<String>() {
    @Override
    public void apply(final String s) {
        System.out.println(s);
    }
}, callbackWhenFinished);
```

## Drop A Database

You can drop a database by name using a `MongoClient` instance:

```java
mongoClient.getDatabase("databaseToBeDropped").drop(callbackWhenFinished);
```

## Create A Collection

Collections in MongoDB are created automatically simply by inserted a document into it. Using the 
[`createCollection`]({{< apiref "com/mongodb/async/client/MongoDatabase.html#createCollection-java.lang.String-com.mongodb.async.SingleResultCallback-">}}) 
method, you can also create a collection explicitly in order to customize its configuration. For example, to create a capped collection sized to 1 megabyte:

```java
database.createCollection("cappedCollection",
  new CreateCollectionOptions().capped(true).sizeInBytes(0x100000),
  callbackWhenFinished);
```

## Get A List of Collections

You can get a list of the available collections in a database:

```java
database.listCollectionNames().forEach(new Block<String>() {
    @Override
    public void apply(final String databaseName) {
        System.out.println(databaseName);
    }
}, callbackWhenFinished);
```

## Drop A Collection

You can drop a collection by using the drop() method:

```java
collection.drop(callbackWhenFinished);
```

## Create An Index

MongoDB supports secondary indexes. To create an index, you just
specify the field or combination of fields, and for each field specify the direction of the index for that field.
We can use the [`Indexes`]({{< relref "builders/indexes.md">}}) helpers to create index keys:

```java
// create an ascending index on the "i" field
 collection.createIndex(Indexes.ascending("i"), callbackWhenFinished);
```

## Get a List of Indexes on a Collection

Use the `listIndexes()` method to get a list of indexes. The following creates a
`printDocumentBlock` Block that prints out the Json version of a document and then passes
that block to the `forEach` method on a
[`mongoIterable`]({{< apiref "com/mongodb/async/client/MongoIterable.html">}})
so that it will printout all the indexes on the collection `test`:

```java
Block<Document> printDocumentBlock = new Block<Document>() {
    @Override
    public void apply(final Document document) {
        System.out.println(document.toJson());
    }
};

collection.listIndexes().forEach(printDocumentBlock, callbackWhenFinished);
```

The example should print the following indexes:

```json
{ "v" : 1, "key" : { "_id" : 1 }, "name" : "_id_", "ns" : "mydb.test" }
{ "v" : 1, "key" : { "i" : 1 }, "name" : "i_1", "ns" : "mydb.test" }
Operation Finished!
```

## Text indexes

MongoDB also provides text indexes to support text search of string
content. Text indexes can include any field whose value is a string or
an array of string elements. To create a text index use the [`Indexes.text`]({{< relref "builders/indexes.md#text-index">}})
static helper:

```java
// create a text index on the "content" field
coll.createIndex(Indexes.text("content"), callbackWhenFinished);
```

As of MongoDB 2.6, text indexes are now integrated into the main query
language and enabled by default (here we use the [`Filters.text`]({{< relref "builders/filters.md#evaluation">}}) helper):

```java
// Insert some documents
collection.insertOne(new Document("_id", 0).append("content", "textual content"), callbackWhenFinished);
collection.insertOne(new Document("_id", 1).append("content", "additional content"), callbackWhenFinished);
collection.insertOne(new Document("_id", 2).append("content", "irrelevant content"), callbackWhenFinished);

// Find using the text index
long matchCount = collection.count(text("textual content -irrelevant"));
System.out.println("Text search matches: " + matchCount);

// Find using the $language operator
Bson textSearch = Filters.text("textual content -irrelevant", new TextSearchOptions().language("english"));
matchCount = collection.count(textSearch);
System.out.println("Text search matches (english): " + matchCount);

// Find the highest scoring match
// Find using the text index
collection.count(text("textual content -irrelevant"), new SingleResultCallback<Long>() {
    @Override
    public void onResult(final Long matchCount, final Throwable t) {
        System.out.println("Text search matches: " + matchCount);
    }
});


// Find using the $language operator
Bson textSearch = text("textual content -irrelevant", "english");
collection.count(textSearch, new SingleResultCallback<Long>() {
    @Override
    public void onResult(final Long matchCount, final Throwable t) {
        System.out.println("Text search matches (english): " + matchCount);
    }
});

// Find the highest scoring match
Document projection = new Document("score", new Document("$meta", "textScore"));
collection.find(textSearch).projection(projection).first(new SingleResultCallback<Document>() {
    @Override
    public void onResult(final Document highest, final Throwable t) {
        System.out.println("Highest scoring document: " + highest.toJson());

    }
});
```

and it should print:

```json
Text search matches: 2
Text search matches (english): 2
Highest scoring document: { "_id" : 1, "content" : "additional content", "score" : 0.75 }
```

For more information about text search see the [text index]({{< docsref "/core/index-text" >}}) and
[$text query operator]({{< docsref "/reference/operator/query/text">}}) documentation.

## Running a command

While not all commands have a specific helper, however you can run any [command]({{< docsref "/reference/command">}})
by using the [`runCommand()`]({{< apiref "com/mongodb/async/client/MongoDatabase.html#runCommand-org.bson.conversions.Bson-com.mongodb.ReadPreference-com.mongodb.async.SingleResultCallback-">}})
method.  Here we call the [buildInfo]({{ docsref "reference/command/buildInfo" }}) command:

```java
database.runCommand(new Document("buildInfo", 1), new SingleResultCallback<Document>() {
    @Override
    public void onResult(final Document buildInfo, final Throwable t) {
        System.out.println(buildInfo);
    }
});
```
