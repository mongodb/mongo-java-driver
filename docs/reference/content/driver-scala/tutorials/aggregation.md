+++
date = "2016-06-08T14:19:24-04:00"
title = "Aggregation"
[menu.main]
parent = "Scala Tutorials"
identifier = "Scala Aggregation"
weight = 50
pre = "<i class='fa'></i>"
+++

## Aggregation Framework

The [aggregation pipeline]({{<docsref "core/aggregation-pipeline" >}}) is a framework for data aggregation, modeled on the concept of data processing pipelines.

## Prerequisites

- The example below requires a ``restaurants`` collection in the ``test`` database. To create and populate the collection, follow the directions in [github](https://github.com/mongodb/docs-assets/tree/drivers).

- Include the following import statements:

     ```scala
     import org.mongodb.scala._

     import org.mongodb.scala.model.Aggregates._
     import org.mongodb.scala.model.Accumulators._
     import org.mongodb.scala.model.Filters._
     import org.mongodb.scala.model.Projections._
     ```

{{% note class="important" %}}
This guide uses the `Observable` implicits as covered in the [Quick Start Primer]({{< relref "driver-scala/getting-started/quick-start-primer.md" >}}).
{{% /note %}}

## Connect to a MongoDB Deployment

Connect to a MongoDB deployment and declare and define a `MongoDatabase` and a `MongoCollection` instances.

For example, include the following code to connect to a standalone MongoDB deployment running on localhost on port `27017` and define `database` to refer to the `test` database and `collection` to refer to the `restaurants` collection.

```scala
val mongoClient: MongoClient = MongoClient()
val database: MongoDatabase = mongoClient.getDatabase("test")
val collection: MongoCollection[Document] = database.getCollection("restaurants")
```

For additional information on connecting to MongoDB, see [Connect to MongoDB]({{< ref "connect-to-mongodb.md" >}}).

## Perform Aggregation

To perform aggregation, pass a list of [aggregation stages]({{< docsref "meta/aggregation-quick-reference" >}}) to the [`MongoCollection.aggregate()`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html#aggregate[C](pipeline:Seq[org.mongodb.scala.bson.conversions.Bson])(implicite:org.mongodb.scala.bson.DefaultHelper.DefaultsTo[C,TResult],implicitct:scala.reflect.ClassTag[C]):org.mongodb.scala.AggregateObservable[C]" >}}) method.
The Scala driver provides the [`Aggregates`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/model/Aggregates$.html" >}}) helper class that contains builders for aggregation stages.

In the following example, the aggregation pipeline

- First uses a [`$match`]({{< docsref "reference/operator/aggregation/match/" >}}) stage to filter for documents whose `categories` array field contains the element `Bakery`. The example uses [`Aggregates.filter`]({{< relref "builders/aggregation.md#match" >}}) to build the `$match` stage.

- Then, uses  a [`$group`]({{< docsref "reference/operator/aggregation/group/" >}}) stage to group the matching documents by the `stars` field, accumulating a count of documents for each distinct value of `stars`. The example uses [`Aggregates.group`]({{< relref "builders/aggregation.md#group" >}}) to build the `$group` stage and [`Accumulators.sum`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/model/Accumulators$.html#sum[TExpression](fieldName:String,expression:TExpression):org.mongodb.scala.model.BsonField" >}}) to build the [accumulator expression]({{< docsref "reference/operator/aggregation/group/#accumulator-operator" >}}).  For the [accumulator expressions]({{< docsref "reference/operator/aggregation-group/" >}}) for use within the [`$group`]({{< docsref "reference/operator/aggregation/group/" >}}) stage, the Scala driver provides [`Accumulators`]({{< apiref "mongodb-driver-core" "com/mongodb/client/model/Accumulators.html" >}}) helper class.
```scala
collection.aggregate(Seq(
  Aggregates.filter(Filters.equal("categories", "Bakery")),
  Aggregates.group("$stars", Accumulators.sum("count", 1))
)).printResults()
```

### Use Aggregation Expressions

For [$group accumulator expressions]({{< docsref "reference/operator/aggregation-group/" >}}), the Scala driver provides [`Accumulators`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/model/Accumulators$.html" >}}) helper class. For other [aggregation expressions]({{< docsref "meta/aggregation-quick-reference/#aggregation-expressions" >}}), manually build the expression `Document`.

In the following example, the aggregation pipeline uses a [`$project`]({{< docsref "reference/operator/aggregation/project/" >}}) stage to return only the `name` field and the calculated field `firstCategory` whose value is the first element in the `categories` array. The example uses [`Aggregates.project`]({{< relref "builders/aggregation.md#project" >}}) and various
[`Projections`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/model/Projections$.html" >}}) methods to build the `$project` stage.


```scala
collection.aggregate(
  Seq(
    Aggregates.project(
      Projections.fields(
        Projections.excludeId(),
        Projections.include("name"),
        Projections.computed(
          "firstCategory",
          Document("$arrayElemAt"-> Seq("$categories", 0))
        )
      )
    )
  )
).printResults()
```
