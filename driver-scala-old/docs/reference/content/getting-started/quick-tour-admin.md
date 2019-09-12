+++
date = "2015-03-17T15:36:56Z"
title = "Admin Quick Tour"
[menu.main]
  parent = "Getting Started"
  identifier = "Admin Quick Tour"
  weight = 50
  pre = "<i class='fa'></i>"
+++

# Admin Quick Tour

This is the second part of the MongoDB driver quick tour. In the
[quick tour]({{< relref "getting-started/quick-tour.md" >}}) we looked at how to
use the MongoDB Scala driver to execute basic CRUD operations.  In this section we'll look at some of the
administrative features available in the driver.

The following code snippets come from the `QuickTourAdmin.scala` example code
that can be found with the [driver source]({{< srcref "examples/src/test/scala/tour/QuickTourAdmin.scala">}}). 

{{% note %}}
See the [installation guide]({{< relref "getting-started/installation-guide.md" >}}) for instructions on how to install the MongoDB Driver.

This guide uses the `Helper` implicits as covered in the [Quick Tour Primer]({{< relref "getting-started/quick-tour-primer.md" >}}).
{{% /note %}}

## Setup

To get started we'll quickly connect and create a `mongoClient`, `database` and `collection`
variable for use in the examples below:

```scala
val mongoClient: MongoClient = MongoClient()
val database: MongoDatabase = mongoClient.getDatabase("mydb")
val collection: MongoCollection[Document] = database.getCollection("test")
```

{{% note %}}
Calling the `getDatabase()` on `MongoClient` does not create a database.
Only when a database is written to will a database be created.  Examples include the creation of an index or the insertion of a document 
into a previously non-existent collection.
{{% /note %}}

## Get A List of Databases

You can get a list of the available databases by calling the `listDatabaseNames` method.  Here we use the implicit 
`printResults` helper so that we can print the list of database names:

```scala
mongoClient.listDatabaseNames().printResults()
```

## Drop A Database

You can drop a database by name using a `MongoClient` instance. Here we block for the `Observable` to complete before continuing.

```scala
mongoClient.getDatabase("databaseToBeDropped").drop().headResult()
```

## Create A Collection

Collections in MongoDB are created automatically simply by inserted a document into it. Using the 
[`createCollection`]({{< apiref "org/mongodb/scala/MongoDatabase.html#createCollection(collectionName:String):org.mongodb.scala.Observable[org.mongodb.scala.Completed]">}}) method, 
you can also create a collection explicitly in order to customize its configuration. For example, to create a capped collection sized to 1 megabyte:

```scala
database.createCollection("cappedCollection",
  CreateCollectionOptions().capped(true).sizeInBytes(0x100000)
).printHeadResult("Collection Created! ")
```

## Get A List of Collections

You can get a list of the available collections in a database:

```scala
database.listCollectionNames().printResults("Collection Names: ")
```

## Drop A Collection

You can drop a collection by using the drop() method:

```scala
collection.drop().headResult()
```

## Create An Index

MongoDB supports secondary indexes. To create an index, you just
specify the field or combination of fields, and for each field specify the direction of the index for that field.
For `1` ascending  or `-1` for descending. 
We can use the [`Indexes`]({{< relref "builders/indexes.md">}}) helpers to create index keys:

```scala
collection.createIndex(ascending("i")).printResults("Created an index named: ")
```

## Get a List of Indexes on a Collection

Use the [`listIndexes()`]({{< apiref "org/mongodb/scala/MongoCollection.html#listIndexes[C]()(implicite:org.mongodb.scala.Helpers.DefaultsTo[C,org.mongodb.scala.collection.immutable.Document],implicitct:scala.reflect.ClassTag[C]):org.mongodb.scala.ListIndexesObservable[C]">}}) method to get a list of indexes.

```scala
collection.listIndexes().printResults()
```

The example should print the following indexes:

```json
{ "v" : 1, "key" : { "_id" : 1 }, "name" : "_id_", "ns" : "mydb.test" }
{ "v" : 1, "key" : { "i" : 1 }, "name" : "i_1", "ns" : "mydb.test" }
```

## Text indexes

MongoDB also provides text indexes to support text search of string
content. Text indexes can include any field whose value is a string or
an array of string elements. To create a text index use the [`Indexes.text`]({{< relref "builders/indexes.md#text-index">}})
static helper:

The following example creates a text index by specifying the string literal "text" in the index document, then insert some sample documents.
Using a for comprehension we can combine the two operations:

```scala
val indexAndInsert = for {
  indexResults <- collection.createIndex(Document("content" -> "text"))
  insertResults <- collection.insertMany(List(
    Document("_id" -> 0, "content" -> "textual content"),
    Document("_id" -> 1, "content" -> "additional content"),
    Document("_id" -> 2, "content" -> "irrelevant content"))
  )
} yield insertResults

indexAndInsert.results()
```

As of MongoDB 2.6, text indexes are now integrated into the main query
language and enabled by default (here we use the [`Filters.text`]({{< relref "builders/filters.md#evaluation">}}) helper):

```scala
// Find using the text index
collection.countDocuments(text("textual content -irrelevant")).printResults("Text search matches: ")

// Find using the $language operator
val textSearch: Bson = text("textual content -irrelevant", TextSearchOptions().language("english"))
collection.countDocuments(textSearch).printResults("Text search matches (english): ")

// Find the highest scoring match
collection.find(textSearch)
  .projection(metaTextScore("score"))
  .first()
  .printHeadResult("Highest scoring document: ")
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
by using the [`runCommand()`]({{< apiref "org/mongodb/scala/MongoDatabase.html#runCommand[TResult](command:org.bson.conversions.Bson)(implicite:org.mongodb.scala.Helpers.DefaultsTo[TResult,org.mongodb.scala.collection.immutable.Document],implicitct:scala.reflect.ClassTag[TResult]):org.mongodb.scala.Observable[TResult]">}}) 
method.  Here we call the [buildInfo]({{ docsref "reference/command/buildInfo" }}) command:

```scala
database.runCommand(Document("buildInfo" -> 1)).printHeadResult()
```
{{% note class="important" %}}
If no [`readPreference`]({{< apiref "org/mongodb/scala/package.html#ReadPreference=com.mongodb.ReadPreference">}}) is passed 
to `runCommand` then the command will be run on the primary node.
{{% /note %}}
