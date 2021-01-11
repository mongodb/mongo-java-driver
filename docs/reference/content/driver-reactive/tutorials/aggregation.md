+++
date = "2016-06-08T14:19:24-04:00"
title = "Aggregation"
[menu.main]
parent = "Reactive Tutorials"
identifier = "Reactive Aggregation"
weight = 50
pre = "<i class='fa'></i>"
+++

## Aggregation Framework

The [aggregation pipeline]({{<docsref "core/aggregation-pipeline" >}}) is a framework for data aggregation, modeled on the concept of data processing pipelines.

## Prerequisites

- The example below requires a ``restaurants`` collection in the ``test`` database. To create and populate the collection, follow the directions in [github](https://github.com/mongodb/docs-assets/tree/drivers).

- Include the following import statements:

     ```java
     import com.mongodb.reactivestreams.client.MongoClients;
     import com.mongodb.reactivestreams.client.MongoClient;
     import com.mongodb.reactivestreams.client.MongoCollection;
     import com.mongodb.reactivestreams.client.MongoDatabase;
     import com.mongodb.client.model.Aggregates;
     import com.mongodb.client.model.Accumulators;
     import com.mongodb.client.model.Projections;
     import com.mongodb.client.model.Filters;
     
     import org.bson.Document;
     ```

{{% note class="important" %}}
This guide uses the `Subscriber` implementations as covered in the [Quick Start Primer]({{< relref "driver-reactive/getting-started/quick-start-primer.md" >}}).
{{% /note %}}

## Connect to a MongoDB Deployment

Connect to a MongoDB deployment and declare and define a `MongoDatabase` and a `MongoCollection` instances.

For example, include the following code to connect to a standalone MongoDB deployment running on localhost on port `27017` and define `database` to refer to the `test` database and `collection` to refer to the `restaurants` collection.

```java
MongoClient mongoClient = MongoClients.create();
MongoDatabase database = mongoClient.getDatabase("test");
MongoCollection<Document> collection = database.getCollection("restaurants");
```

For additional information on connecting to MongoDB, see [Connect to MongoDB]({{< ref "connect-to-mongodb.md" >}}).

## Perform Aggregation

To perform aggregation, pass a list of [aggregation stages]({{< docsref "meta/aggregation-quick-reference" >}}) to the [`MongoCollection.aggregate()`]({{< apiref "mongodb-driver-sync" "com/mongodb/client/MongoCollection.html#aggregate(java.util.List)" >}}) method.
The Java driver provides the [`Aggregates`]({{< apiref "mongodb-driver-core" "com/mongodb/client/model/Aggregates.html" >}}) helper class that contains builders for aggregation stages.

In the following example, the aggregation pipeline

- First uses a [`$match`]({{< docsref "reference/operator/aggregation/match/" >}}) stage to filter for documents whose `categories` array field contains the element `Bakery`. The example uses [`Aggregates.match`]({{< relref "builders/aggregation.md#match" >}}) to build the `$match` stage.

- Then, uses  a [`$group`]({{< docsref "reference/operator/aggregation/group/" >}}) stage to group the matching documents by the `stars` field, accumulating a count of documents for each distinct value of `stars`. The example uses [`Aggregates.group`]({{< relref "builders/aggregation.md#group" >}}) to build the `$group` stage and [`Accumulators.sum`]({{< apiref "mongodb-driver-core" "com/mongodb/client/model/Accumulators#sum(java.lang.String,TExpression)" >}}) to build the [accumulator expression]({{< docsref "reference/operator/aggregation/group/#accumulator-operator" >}}).  For the [accumulator expressions]({{< docsref "reference/operator/aggregation-group/" >}}) for use within the [`$group`]({{< docsref "reference/operator/aggregation/group/" >}}) stage, the Java driver provides [`Accumulators`]({{< apiref "mongodb-driver-core" "com/mongodb/client/model/Accumulators.html" >}}) helper class.

```java
collection.aggregate(
      Arrays.asList(
              Aggregates.match(Filters.eq("categories", "Bakery")),
              Aggregates.group("$stars", Accumulators.sum("count", 1))
      )
).subscribe(new PrintDocumentSubscriber());
```

### Use Aggregation Expressions

For [$group accumulator expressions]({{< docsref "reference/operator/aggregation-group/" >}}), the Java driver provides [`Accumulators`]({{< apiref "mongodb-driver-core" "com/mongodb/client/model/Accumulators.html" >}}) helper class. For other [aggregation expressions]({{< docsref "meta/aggregation-quick-reference/#aggregation-expressions" >}}), manually build the expression `Document`.

In the following example, the aggregation pipeline uses a [`$project`]({{< docsref "reference/operator/aggregation/project/" >}}) stage to return only the `name` field and the calculated field `firstCategory` whose value is the first element in the `categories` array. The example uses [`Aggregates.project`]({{< relref "builders/aggregation.md#project" >}}) and various
[`Projections`]({{< apiref "mongodb-driver-core" "com/mongodb/client/model/Projections.html" >}}) methods to build the `$project` stage.


```java
collection.aggregate(
      Arrays.asList(
          Aggregates.project(
              Projections.fields(
                    Projections.excludeId(),
                    Projections.include("name"),
                    Projections.computed(
                            "firstCategory",
                            new Document("$arrayElemAt", Arrays.asList("$categories", 0))
                    )
              )
          )
      )
).subscribe(new PrintDocumentSubscriber());
```

### Explain an Aggregation

To [explain]({{< docsref "reference/command/explain/" >}}) an aggregation pipeline, call the
[`AggregatePublisher.explain()`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/AggregatePublisher.html#explain()" 
> >}}) 
method:

```java
collection.aggregate(
      Arrays.asList(
              Aggregates.match(Filters.eq("categories", "Bakery")),
              Aggregates.group("$stars", Accumulators.sum("count", 1))))
      .explain()
      .subscribe(new PrintDocumentSubscriber());
```

The driver supports explain of aggregation pipelines starting with MongoDB 3.6.
