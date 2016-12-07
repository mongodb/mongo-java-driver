+++
date = "2016-06-12T17:26:54-04:00"
title = "Geospatial Search"
[menu.main]
parent = "Sync Tutorials"
identifier = "Geospatial Search"
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
     import com.mongodb.MongoClient;
     import com.mongodb.client.MongoCollection;
     import com.mongodb.client.MongoDatabase;
     import com.mongodb.client.MongoCursor;
     import com.mongodb.client.model.geojson.*;
     import com.mongodb.client.model.Indexes;
     import com.mongodb.client.model.Filters;
     import org.bson.Document;
     ```

- Include the following code which the examples in the tutorials will use to print the results of the geospatial search:

     ```java
     Block<Document> printBlock = new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document.toJson());
            }
        };
     ```

## Connect to a MongoDB Deployment

Connect to a MongoDB deployment and declare and define a `MongoDatabase` instance.

For example, include the following code to connect to a standalone MongoDB deployment running on localhost on port `27017` and define `database` to refer to the `test` database:

```java
MongoClient mongoClient = new MongoClient();
MongoDatabase database = mongoClient.getDatabase("test");
```

For additional information on connecting to MongoDB, see [Connect to MongoDB]({{< ref "connect-to-mongodb.md">}}).

## Create the `2dsphere` Index

To create a [`2dsphere` index]({{<docsref "core/2dsphere">}}), use the [`Indexes.geo2dsphere`]({{<apiref "com/mongodb/client/model/Indexes.html#geo2dsphere-java.lang.String...-">}})
helper to create a specification for the `2dsphere` index and pass to [`MongoCollection.createIndex()`]({{<apiref "com/mongodb/client/MongoCollection.html#createIndex-org.bson.conversions.Bson-">}}) method.

The following example creates a `2dsphere` index on the `"contact.location"` field for the `restaurants` collection.

```java
MongoCollection<Document> collection = database.getCollection("restaurants");
collection.createIndex(Indexes.geo2dsphere("contact.location"));
```

## Query for Locations Near a GeoJSON Point

MongoDB provides various [geospatial query operators]({{<apiref "reference/operator/query-geospatial">}}). To facilitate the creation of geospatial queries filters, the Java driver provides the [`Filters`]({{< apiref "com/mongodb/client/model/Filters.html">}}) class and the ``com.mongodb.client.model.geojson`` package.

The following example returns documents that are at least 1000 meters from and at most 5000 meters from the specified GeoJSON point ``com.mongodb.client.model.geojson.Point``, sorted from nearest to farthest:

```java
Point refPoint = new Point(new Position(-73.9667, 40.78));
collection.find(Filters.near("contact.location", refPoint, 5000.0, 1000.0)).forEach(printBlock);
```
