+++
date = "2016-06-12T17:26:54-04:00"
title = "Geospatial Search"
[menu.main]
parent = "Scala Tutorials"
identifier = "Scala Geospatial Search"
weight = 85
pre = "<i class='fa'></i>"
+++

## Geospatial Search

To support geospatial queries, MongoDB provides various geospatial indexes as well as [geospatial query operators]({{<docsref "reference/operator/query-geospatial/" >}}).

## Prerequisites

- The example below requires a ``restaurants`` collection in the ``test`` database. To create and populate the collection, follow the directions in [github](https://github.com/mongodb/docs-assets/tree/drivers).

- Include the following import statements:

     ```scala
     import org.mongodb.scala._
     import org.mongodb.scala.model.geojson._
     import org.mongodb.scala.model.Indexes
     import org.mongodb.scala.model.Filters
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

For additional information on connecting to MongoDB, see [Connect to MongoDB]({{< ref "connect-to-mongodb.md" >}}).

## Create the `2dsphere` Index

To create a [`2dsphere` index]({{<docsref "core/2dsphere" >}}), use the [`Indexes.geo2dsphere`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/model/Indexes$.html#geo2dsphere(fieldNames:String*):org.mongodb.scala.bson.conversions.Bson" >}})
helper to create a specification for the `2dsphere` index and pass to [`MongoCollection.createIndex()`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html#createIndex(key:org.mongodb.scala.bson.conversions.Bson,options:org.mongodb.scala.model.IndexOptions):org.mongodb.scala.SingleObservable[String]" >}}) method.

The following example creates a `2dsphere` index on the `"contact.location"` field for the `restaurants` collection.

```scala
val collection = database.getCollection("restaurants")
collection.createIndex(Indexes.geo2dsphere("contact.location")).printResults()
```

## Query for Locations Near a GeoJSON Point

MongoDB provides various [geospatial query operators]({{<docsref "reference/operator/query-geospatial" >}}). To facilitate the creation of geospatial queries filters, the Scala driver provides the [`Filters`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/model/Filters$.html" >}}) class and the ``org.mongodb.scala.model.geojson`` package.

The following example returns documents that are at least 1000 meters from and at most 5000 meters from the specified GeoJSON point ``org.mongodb.scala.model.geojson.Point``, sorted from nearest to farthest:

```scala
val refPoint = Point(Position(-73.9667, 40.78))
collection.find(Filters.near("contact.location", refPoint, 5000.0, 1000.0)).printResults()
```
