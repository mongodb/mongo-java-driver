
+++
date = "2016-06-12T17:26:54-04:00"
title = "Geospatial Search"
[menu.main]
  parent = "Async Tutorials"
  identifier = "Async Geospatial Search"
  weight = 85
  pre = "<i class='fa'></i>"
+++

## Geospatial Search

To support geospatial queries, MongoDB provides various geospatial indexes as well as [geospatial query operators]({{<docsref "reference/operator/query-geospatial/">}}).

## Prerequisites

- The example below requires a ``restaurants`` collection in the ``test`` database. To create and populate the collection, follow the directions in [github] (https://github.com/mongodb/docs-assets/tree/drivers).

- Include the following import statements:

```java
import com.mongodb.Block;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Position;
import org.bson.Document;
```


- Include the following callback code which the examples in the tutorials will use:

```java
SingleResultCallback<Void> callbackWhenFinished = new SingleResultCallback<Void>() {
    @Override
    public void onResult(final Void result, final Throwable t) {
        System.out.println("Operation Finished!");
    }
};
```

- Include the following code which the examples in the tutorials will use to print the results of the aggregation:

```java
Block<Document> printBlock = new Block<Document>() {
    @Override
    public void apply(final Document document) {
        System.out.println(document.toJson());
    }
};
```

## Connect to a MongoDB Deployment

Connect to a MongoDB deployment and declare and define a `MongoDatabase` and a `MongoCollection` instances.

For example, include the following code to connect to a standalone MongoDB deployment running on localhost on port `27017` and define `database` to refer to the `test` database and `collection` to refer to the `restaurants` collection:

```java
MongoClient mongoClient = MongoClients.create();
MongoDatabase database = mongoClient.getDatabase("test");
MongoCollection<Document> collection = database.getCollection("restaurants");
```

For additional information on connecting to MongoDB, see [Connect to MongoDB]({{< relref "driver-async/tutorials/connect-to-mongodb.md" >}}).

## Create the `2dsphere` Index

To create a [`2dsphere` index]({{<docsref "core/2dsphere">}}), use the [`Indexes.geo2dsphere`]({{<apiref "com/mongodb/client/model/Indexes.html#geo2dspherejava.lang.String...)">}})
helper to create a specification for the `2dsphere` index and pass to [`MongoCollection.createIndex()`]({{<apiref "com/mongodb/client/MongoCollection.html#createIndex(org.bson.conversions.Bson)">}}) method.

The following example creates a `2dsphere` index on the `"contact.location"` field for the `restaurants` collection.

```java
collection.createIndex(Indexes.geo2dsphere("contact.location"), new SingleResultCallback<String>() {
    @Override
    public void onResult(final String result, final Throwable t) {
        System.out.println("Operation Finished!");
    }
});
```

## Query for Locations Near a GeoJSON Point

MongoDB provides various [geospatial query operators]({{<apiref "reference/operator/query-geospatial">}}). To facilitate the creation of geospatial queries filters, the Java driver provides the [`Filters`]({{< apiref "com/mongodb/client/model/Filters.html">}}) class and the ``com.mongodb.client.model.geojson`` package.

The following example returns documents that are at least 1000 meters from and at most 5000 meters from the specified GeoJSON point ``com.mongodb.client.model.geojson.Point``, sorted from nearest to farthest:

```java
Point refPoint = new Point(new Position(-73.9667, 40.78));
collection.find(Filters.near("contact.location", refPoint, 5000.0, 1000.0))
          .forEach(printBlock, callbackWhenFinished);
```
