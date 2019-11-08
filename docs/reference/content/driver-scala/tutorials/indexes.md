+++
date = "2016-05-31T10:10:30-04:00"
title = "Create Indexes"
[menu.main]
parent = "Scala Tutorials"
identifier = "Scala Create Indexes"
weight = 15
pre = "<i class='fa'></i>"
+++

## Create Indexes

[Indexes]({{<docsref "indexes">}}) support the efficient execution of queries in MongoDB. To create an [index]({{<docsref "indexes">}}) on a field or fields, pass an index specification document to the [`MongoCollection.createIndex()`]({{<apiref "org/mongodb/scala/MongoCollection.html#createIndex(key:org.mongodb.scala.bson.conversions.Bson,options:org.mongodb.scala.model.IndexOptions):org.mongodb.scala.SingleObservable[String]">}}) method.

The MongoDB Scala Driver provides
the [`Indexes`]({{<apiref "org/mongodb/scala/model/Indexes.html">}}) class that
includes helper methods to create index specification
documents for the various MongoDB Index key types.

{{% note %}}
MongoDB only creates an index if an index of the same specification does not already exist.
{{% /note %}}

## Prerequisites

- Include the following import statements:

     ```scala
     import org.mongodb.scala._

     import org.mongodb.Indexes
     import org.mongodb.IndexOptions
     import org.mongodb.Filters
     ```

{{% note class="important" %}}
This guide uses the `Observable` implicits as covered in the [Quick Start Primer]({{< relref "driver-scala/getting-started/quick-start-primer.md" >}}).
{{% /note %}}

## Connect to a MongoDB Deployment

Connect to a MongoDB deployment and declare and define a `MongoDatabase` and a `MongoCollection` instances.

For example, include the following code to connect to a standalone MongoDB deployment running on localhost on port `27017` and define `database` to refer to the `test` database and `collection` to refer to the `restaurants` collection:

```scala
val mongoClient: MongoClient = MongoClient()
val database: MongoDatabase = mongoClient.getDatabase("test")
val collection: MongoCollection[Document] = database.getCollection("restaurants")
```

For additional information on connecting to MongoDB, see [Connect to MongoDB]({{< ref "connect-to-mongodb.md">}}).

## Ascending Index

To create a specification for an ascending index, use the [`Indexes.ascending`]({{<apiref "org/mongodb/scala/model/Indexes$.html#ascending(fieldNames:String*):org.mongodb.scala.bson.conversions.Bson">}}) helper methods.

### Single Ascending Index

The following example creates an ascending index on the `name` field:

```scala
collection.createIndex(Indexes.ascending("name"))
          .printResults()
```

### Compound Ascending Index

The following example creates an ascending [compound index]({{<docsref "core/index-compound">}})  on the `stars` field and the `name`
 field:

```scala
collection.createIndex(Indexes.ascending("stars", "name"))
          .printResults()
```

For an alternative way to create a compound index, see [Compound Indexes](#compound-indexes).

## Descending Index

To create a specification of a descending index, use the [`Indexes.descending`]({{<apiref "org/mongodb/scala/model/Indexes$.html#descending(fieldNames:String*):org.mongodb.scala.bson.conversions.Bson">}}) helper methods.

### Single Descending Key Index

The following example creates a descending index on the `stars` field:

```scala
collection.createIndex(Indexes.descending("stars"))
          .printResults()
```

### Compound Descending Key Index

The following example creates a descending [compound index]({{<docsref "core/index-compound">}}) on the `stars` field and the `name` field:

```scala
collection.createIndex(Indexes.descending("stars", "name"))
          .printResults()
```

For an alternative way to create a compound index, see [Compound Indexes](#compound-indexes).

## Compound Indexes

To create a specification for a [compound index]({{<docsref "core/index-compound">}}), use the [`Indexes.compoundIndex`]({{<apiref "org/mongodb/scala/model/Indexes$.html#compoundIndex(indexes:org.mongodb.scala.bson.conversions.Bson*):org.mongodb.scala.bson.conversions.Bson">}}) helper methods.

{{% note %}}
To create a specification for a compound index where all the keys are ascending, you can use the [`ascending()`](#compound-ascending-key-index) method. To create a specification for a compound index where all the keys are descending, you can use the [`descending()`](##compound-descending-key-index) method.
{{% /note %}}

The following example creates a compound index with the `stars` field in descending order and the `name` field in ascending order:

```scala
collection.createIndex(
              Indexes.compoundIndex(Indexes.descending("stars"), 
                                    Indexes.ascending("name")))
          .printResults()
```

## Text Indexes

MongoDB provides [text indexes]({{<docsref "core/index-text">}}) to support text search of string content. Text indexes can include any field whose value is a string or an array of string elements. To create a specification for a text index, use the
[`Indexes.text`]({{<apiref "org/mongodb/scala/model/Indexes$.html#text(fieldName:String):org.mongodb.scala.bson.conversions.Bson">}}) helper method.

The following example creates a text index on the `name` field:

```scala
collection.createIndex(Indexes.text("name"))
          .printResults()
```

## Hashed Index

To create a specification for a [hashed index]({{<docsref "core/index-hashed">}}) index, use the [`Indexes.hashed`]({{<apiref "org/mongodb/scala/model/Indexes$.html#hashed(fieldName:String):org.mongodb.scala.bson.conversions.Bson">}}) helper method.

The following example creates a hashed index on the `_id` field:

```scala
collection.createIndex(Indexes.hashed("_id"))
          .printResults()
```

## Geospatial Indexes

To support geospatial queries, MongoDB supports various
[geospatial indexes]({{<docsref "applications/geospatial-indexes">}}).

### `2dsphere`

To create a specification for a [`2dsphere` index]({{<docsref "core/2dsphere">}}), use the [`Indexes.geo2dsphere`]({{<apiref "org/mongodb/scala/model/Indexes$.html#geo2dsphere(fieldNames:String*):org.mongodb.scala.bson.conversions.Bson">}}) static helper methods.

The following example creates a `2dsphere` index on the `"contact.location"` field:

```scala
collection.createIndex(Indexes.geo2dsphere("contact.location"))
          .printResults()
```

### geoHaystack

To create a specification for a [`geoHaystack` index]({{<docsref "core/geohaystack/">}}), use the [`Indexes.geoHaystack`]({{<apiref "org/mongodb/scala/model/Indexes$.html#geoHaystack(fieldName:String,additional:org.mongodb.scala.bson.conversions.Bson):org.mongodb.scala.bson.conversions.Bson">}}) method. `geoHaystack` indexes can improve performance on queries that use flat geometries.

The following example creates a `geoHaystack` index on the `contact.location` field and an ascending index on the `stars` field:

```scala
val haystackOption = IndexOptions().bucketSize(1.0)
collection.createIndex(
            Indexes.geoHaystack("contact.location", Indexes.ascending("stars")),
            haystackOption)
        .printResults()
```


To query a haystack index, use the [`geoSearch`]({{<docsref "reference/command/geoSearch">}}) command.

## IndexOptions

```scala
import org.mongodb.scala.model.IndexOptions
```

In addition to the index specification document, the
[`createIndex()`]({{<apiref "org/mongodb/scala/MongoCollection.html#createIndex(key:org.mongodb.scala.bson.conversions.Bson,options:org.mongodb.scala.model.IndexOptions):org.mongodb.scala.SingleObservable[String]">}}) method can take an index options document, such as to create unique indexes or partial indexes.

The Scala driver provides the [IndexOptions]({{<apiref "org/mongodb/scala/model/index.html#IndexOptions=com.mongodb.client.model.IndexOptions">}}) class to specify various index options.

### Unique Index

The following specifies a [`unique(true)`]({{<apiref "org/mongodb/scala/model/index.html#IndexOptions=com.mongodb.client.model.IndexOptions">}}) option to create a [unique index]({{<docsref "core/index-unique">}}) on the `name` and `stars` fields:

```scala
val indexOptions = IndexOptions().unique(true)
collection.createIndex(Indexes.ascending("name", "stars"), indexOptions)
          .printResults()
```

For more information on unique indexes, see [Unique Indexes]({{<docsref "core/index-unique">}}).

### Partial Index

To create a [partial index]({{<docsref "core/index-partial/">}}), include a [partialFilterExpression]({{<apiref "org/mongodb/scala/model/index.html#IndexOptions=com.mongodb.client.model.IndexOptions">}}) as an index option.

The following example creates a partial index on documents that have `status` field equal to `"A"`.

```scala
val partialFilterIndexOptions = IndexOptions()
             .partialFilterExpression(Filters.exists("contact.email"))
collection.createIndex(
                Indexes.descending("name", "stars"), partialFilterIndexOptions)
          .printResults()
```

For more information on partial indexes, see [Partial Indexes]({{<docsref "core/index-partial/">}}).

## Get a List of Indexes on a Collection

Use the `listIndexes()` method to get a list of indexes. The following lists the indexes on the collection:

```scala
collection.listIndexes().printResults()
```

For other index options, see [MongoDB Manual]({{<docsref "core/index-properties">}}).
