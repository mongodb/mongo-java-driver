+++
date = "2016-06-07T23:28:50-04:00"
title = "Text Search"
[menu.main]
parent = "Scala Tutorials"
identifier = "Scala Text Search"
weight = 80
pre = "<i class='fa'></i>"
+++

## Text Search

MongoDB supports query operations that perform a [text search]({{<docsref "text-search">}}) of string content. To perform text search, MongoDB uses a [text index]({{<docsref "core/index-text">}}) and the [`$text` query operator]({{<docsref "reference/operator/query/text">}}).

The Scala driver provides the [`Filters.text()`]({{<scapiref "org/mongodb/scala/model/Filters$.html#text(search:String):org.mongodb.scala.bson.conversions.Bson">}}) helper to facilitate the creation of text search query filters.

## Prerequisites

- The example below requires a ``restaurants`` collection in the ``test`` database. To create and populate the collection, follow the directions in [github](https://github.com/mongodb/docs-assets/tree/drivers).

- Include the following import statements:

     ```scala
     import org.mongodb.scala._
     import org.mongodb.scala.model._
     ```

{{% note class="important" %}}
This guide uses the `Observable` implicits as covered in the [Quick Start Primer]({{< relref "driver-scala/getting-started/quick-start-primer.md" >}}).
{{% /note %}}

## Connect to a MongoDB Deployment

Connect to a MongoDB deployment and declare and define a `MongoDatabase` instance.

For example, include the following code to connect to a standalone MongoDB deployment running on localhost on port `27017` and define `database` to refer to the `test` database:

```scala
val mongoClient: MongoClient = MongoClient()
val database: MongoDatabase = mongoClient.getDatabase("test")
```

For additional information on connecting to MongoDB, see [Connect to MongoDB]({{< ref "connect-to-mongodb.md">}}).

## Create the `text` Index

To create a [text index]({{<docsref "core/index-text">}}), use the [`Indexes.text`]({{< relref "builders/indexes.md#text-index">}})
static helper to create a specification for a text index and pass to [`MongoCollection.createIndex()`]({{<scapiref "org/mongodb/scala/MongoCollection.html#createIndex-org.bson.conversions.Bson-">}}) method.

The following example creates a text index on the `name` field for the `restaurants` collection.

```scala
val collection: MongoCollection[Document] = database.getCollection("restaurants")
collection.createIndex(Indexes.text("name")).printResults()
```

## Perform Text Search

To perform text search, use the [`Filters.text()`]({{<scapiref "org/mongodb/scala/model/Filters$.html#text(search:String,textSearchOptions:org.mongodb.scala.model.TextSearchOptions):org.mongodb.scala.bson.conversions.Bson">}}) helper to specify the text search query filter.

For example, the following code performs a text search on the `name` field for the word `"bakery"` or `"coffee"`.

```scala
collection.countDocuments(Filters.text("bakery coffee")).printResults("Text search matches: ")
```

The example should print the following output:

```json
Text search matches: [2]
```

For more information on the text search, see [`$text` operator]({{<docsref "reference/operator/query/text">}}).

### Text Score

For each matching document, text search assigns a score, representing the relevance of a document to the specified text search query filter. To return and sort by score, use the [`$meta`]({{<docsref "reference/operator/query/text/#sort-by-text-search-score">}}) operator in the projection document and the sort expression.


```scala
collection.find(Filters.text("bakery cafe"))
                       .projection(Projections.metaTextScore("score"))
                       .sort(Sorts.metaTextScore("score"))
                       .printResults()
```

### Specify a Text Search Option

The  [`Filters.text()`]({{<scapiref "org/mongodb/scala/model/Filters$.html#text(search:String,textSearchOptions:org.mongodb.scala.model.TextSearchOptions):org.mongodb.scala.bson.conversions.Bson">}}) helper can accept various [text search options]({{<docsref "reference/operator/query/text">}}). The Scala driver provides [`TextSearchOptions`]({{<scapiref "org/mongodb/scala/model/package$$TextSearchOptions$.html">}}) class to specify these options.

For example, the following text search specifies the [text search language]({{<docsref "reference/text-search-languages">}}) option when performing text search for the word `cafe`:

```scala
collection.countDocuments(Filters.text("cafe", TextSearchOptions().language("english")))
                                 .printResults("Text search matches (english): ")
```

The example should print the following output:

```json
Text search matches (english): [1]
```

For more information about text search see the following sections in the MongoDB Server Manual:

- [`$text` query operator]({{< docsref "reference/operator/query/text">}})

- [`text` index]({{< docsref "core/index-text" >}})

- [Text Search Languages]({{<docsref "reference/text-search-languages">}})
