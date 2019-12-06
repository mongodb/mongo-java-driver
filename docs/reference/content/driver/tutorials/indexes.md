+++
date = "2016-05-31T10:10:30-04:00"
title = "Create Indexes"
[menu.main]
parent = "Sync Tutorials"
identifier = "Create Indexes"
weight = 15
pre = "<i class='fa'></i>"
+++

## Create Indexes

[Indexes]({{<docsref "indexes">}}) support the efficient execution of queries in MongoDB. To create an [index]({{<docsref "indexes">}}) on a field or fields, pass an index specification document to the [`MongoCollection.createIndex()`]({{<apiref "com/mongodb/client/MongoCollection.html#createIndex(org.bson.conversions.Bson)">}}) method.

The MongoDB Java Driver provides
the [`Indexes`]({{<apiref "com/mongodb/client/model/Indexes.html">}}) class that
includes static factory methods to create index specification
documents for the various MongoDB Index key types.

{{% note %}}
MongoDB only creates an index if an index of the same specification does not already exist.
{{% /note %}}

## Prerequisites

- Include the following import statements:

     ```java
     import com.mongodb.client.MongoClient;
     import com.mongodb.client.MongoClients;
     import com.mongodb.client.MongoDatabase;
     import com.mongodb.client.MongoCollection;
     import org.bson.Document;

     import com.mongodb.client.model.Indexes;
     import com.mongodb.client.model.IndexOptions;
     import com.mongodb.client.model.Filters;
     ```

## Connect to a MongoDB Deployment

Connect to a MongoDB deployment and declare and define a `MongoDatabase` and a `MongoCollection` instances.

For example, include the following code to connect to a standalone MongoDB deployment running on localhost on port `27017` and define `database` to refer to the `test` database and `collection` to refer to the `restaurants` collection:

```java
MongoClient mongoClient = MongoClients.create();
MongoDatabase database = mongoClient.getDatabase("test");
MongoCollection<Document> collection = database.getCollection("restaurants");
```

For additional information on connecting to MongoDB, see [Connect to MongoDB]({{< ref "connect-to-mongodb.md">}}).

## Ascending Index

To create a specification for an ascending index, use the [`Indexes.ascending`]({{<apiref "com/mongodb/client/model/Indexes.html">}}) static helper methods.

### Single Ascending Index

The following example creates an ascending index on the
`name` field:

```java
collection.createIndex(Indexes.ascending("name"));
```

### Compound Ascending Index

The following example creates an ascending [compound index]({{<docsref "core/index-compound">}})  on the `stars` field and the `name`
 field:

```java
collection.createIndex(Indexes.ascending("stars", "name"));
```

For an alternative way to create a compound index, see [Compound Indexes](#compound-indexes).

## Descending Index

To create a specification of a descending index, use the [`Indexes.descending`]({{<apiref "com/mongodb/client/model/Indexes.html">}}) static helper methods.

### Single Descending Key Index

The following example creates a descending index on the `stars` field:

```java
collection.createIndex(Indexes.descending("stars"));
```

### Compound Descending Key Index

The following example creates a descending [compound index]({{<docsref "core/index-compound">}}) on the `stars` field and the `name` field:

```java
collection.createIndex(Indexes.descending("stars", "name"));
```

For an alternative way to create a compound index, see [Compound Indexes](#compound-indexes).

## Compound Indexes

To create a specification for a [compound index]({{<docsref "core/index-compound">}}), use the [`Indexes.compoundIndex`]({{<apiref "com/mongodb/client/model/Indexes.html">}}) static helper methods.

{{% note %}}
To create a specification for a compound index where all the keys are ascending, you can use the [`ascending()`](#compound-ascending-key-index) method. To create a specification for a compound index where all the keys are descending, you can use the [`descending()`](##compound-descending-key-index) method.
{{% /note %}}

The following example creates a compound index with the `stars` field in descending order and the `name` field in ascending order:

```java
collection.createIndex(Indexes.compoundIndex(Indexes.descending("stars"), Indexes.ascending("name")));
```

## Text Indexes

MongoDB provides [text indexes]({{<docsref "core/index-text">}}) to support text search of string content. Text indexes can include any field whose value is a string or an array of string elements. To create a specification for a text index, use the
[`Indexes.text`]({{<apiref "com/mongodb/client/model/Indexes.html#text(java.lang.String)">}}) static helper method.

The following example creates a text index on the `name` field:

```java
collection.createIndex(Indexes.text("name"));
```

## Hashed Index

To create a specification for a [hashed index]({{<docsref "core/index-hashed">}}) index, use the [`Indexes.hashed`]({{<apiref "com/mongodb/client/model/Indexes.html#hashed(java.lang.String)">}}) static helper method.

The following example creates a hashed index on the `_id` field:

```java
collection.createIndex(Indexes.hashed("_id"));
```

## Geospatial Indexes

To support geospatial queries, MongoDB supports various
[geospatial indexes]({{<docsref "applications/geospatial-indexes">}}).

### `2dsphere`

To create a specification for a [`2dsphere` index]({{<docsref "core/2dsphere">}}), use the [`Indexes.geo2dsphere`]({{<apiref "com/mongodb/client/model/Indexes.html#geo2dsphere(java.lang.String...)">}}) static helper methods.

The following example creates a `2dsphere` index on the `"contact.location"` field:

```java
collection.createIndex(Indexes.geo2dsphere("contact.location"));
```

### `2d`

To create a specification for a [`2d` index]({{<docsref "core/2d/">}}) index, use the [`Indexes.geo2d`]({{<apiref "com/mongodb/client/model/Indexes.html#geo2d(java.lang.String)">}})
static helper method.

{{% note class="important" %}}
A 2d index is for data stored as points on a two-dimensional plane
and is intended for legacy coordinate pairs used in MongoDB 2.2 and
earlier.
{{% /note %}}

The following example creates a `2d` index on the `"contact.location"` field:

```java
collection.createIndex(Indexes.geo2d("contact.location"));
```

### geoHaystack

To create a specification for a [`geoHaystack` index]({{<docsref "core/geohaystack/">}}), use the [`Indexes.geoHaystack`]({{<apiref "com/mongodb/client/model/Indexes.html#geoHaystack(java.lang.String,org.bson.conversions.Bson)">}}) method. `geoHaystack` indexes can improve performance on queries that use flat geometries.

The following example creates a `geoHaystack` index on the `contact.location` field and an ascending index on the `stars` field:

```java
IndexOptions haystackOption = new IndexOptions().bucketSize(1.0);
collection.createIndex(
         Indexes.geoHaystack("contact.location", Indexes.ascending("stars")),
         haystackOption);
```


To query a haystack index, use the [`geoSearch`]({{<docsref "reference/command/geoSearch">}}) command.

## IndexOptions

```java
import com.mongodb.client.model.IndexOptions;
```

In addition to the index specification document, the
[`createIndex()`]({{<apiref "com/mongodb/client/MongoCollection.html#createIndex(org.bson.conversions.Bson)">}}) method can take an index options document, such as to create unique indexes or partial indexes.

The Java Driver provides the [IndexOptions]({{<apiref "com/mongodb/client/model/IndexOptions.html">}}) class to specify various index options.

### Unique Index

The following specifies a [`unique(true)`]({{<apiref "com/mongodb/client/model/IndexOptions.html#unique(boolean)">}}) option to create a [unique index]({{<docsref "core/index-unique">}}) on the `name` and `stars` fields:

```java
IndexOptions indexOptions = new IndexOptions().unique(true);
collection.createIndex(Indexes.ascending("name", "stars"), indexOptions);
```

For more information on unique indexes, see [Unique Indexes]({{<docsref "core/index-unique">}}).

### Partial Index

To create a [partial index]({{<docsref "core/index-partial/">}}), include a [partialFilterExpression]({{<apiref "com/mongodb/client/model/IndexOptions.html#partialFilterExpression(org.bson.conversions.Bson)">}}) as an index option.

The following example creates a partial index on documents that have `status` field equal to `"A"`.

```java
IndexOptions partialFilterIndexOptions = new IndexOptions()
                .partialFilterExpression(Filters.exists("contact.email"));
collection.createIndex(
                Indexes.descending("name", "stars"), partialFilterIndexOptions);
```

For more information on partial indexes, see [Partial Indexes]({{<docsref "core/index-partial/">}}).

## Get a List of Indexes on a Collection

Use the `listIndexes()` method to get a list of indexes. The following lists the indexes on the collection:

```java
for (Document index : collection.listIndexes()) {
    System.out.println(index.toJson());
}
```

For other index options, see [MongoDB Manual]({{<docsref "core/index-properties">}}) .
