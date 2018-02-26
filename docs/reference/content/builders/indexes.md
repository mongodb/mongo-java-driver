+++
date = "2015-07-16T14:27:51-04:00"
title = "Indexes"
[menu.main]
  parent = "Builders"
  weight = 60
  pre = "<i class='fa'></i>"
+++

## Indexes

The [`Indexes`]({{< apiref "com/mongodb/client/model/Indexes" >}}) class provides static factory methods for all the MongoDB Index key types.  
Each method returns an instance of the [`Bson`]({{< relref "bson/documents.md#bson" >}}) type, which can in turn be used with the `createIndex`
methods.

For brevity, you may choose to import the methods of the `Indexes` class statically:

```java
import static com.mongodb.client.model.Indexes.*;
```

All the examples below assume this static import.

### Ascending

To specify an ascending index key, use one of the [`ascending`]({{< apiref "com/mongodb/client/model/Indexes" >}}) methods.

This example specifies an ascending index key for the `quantity` field:

```java
ascending("quantity")
```

This example specifies a compound index key composed of the `quantity` field sorted in ascending order and the `totalAmount` field
sorted in ascending order:

```java
ascending("quantity", "totalAmount")
```

### Descending

To specify a descending index key, use one of the `descending` methods.

This example specifies a descending index key on the `quantity` field:

```java
descending("quantity")
```

This example specifies a compound index key composed of the `quantity` field sorted in descending order and the `totalAmount` field
sorted in descending order:


```java
descending("quantity", "totalAmount")
```

### Compound indexes

To specify a compound index, use the `compoundIndex` method.

This example specifies a compound index key composed of the `quantity` field sorted in ascending order, followed by the `totalAmount` field
sorted in ascending order, followed by the `orderDate` field sorted in descending order:

```java
compoundIndex(ascending("quantity", "totalAmount"), descending("orderDate"))
```

### Text Index

To specify a [text]({{< docsref "core/index-text" >}}) index key, use the `text` method.

This example specifies a text index key for the `description` field:

```java
text("description")
```

This example specifies a text index key for every field that contains string data:

```java
text()
```

### Hashed Index

To specify a [hashed]({{< docsref "core/index-hashed" >}}) index key, use the `hashed` method.

This example specifies a hashed index key for the `timestamp` field:

```java
hashed("timestamp")
```

### Geospatial Indexes

There are also helpers for creating the index keys for the various [geospatial indexes]({{< docsref "applications/geospatial-indexes" >}})
supported by mongodb.

#### 2dsphere

To specify a [2dsphere]({{< docsref "core/2dsphere/" >}}) index key, use one of the `geo2dsphere` methods.


This example specifies a 2dsphere index on the `location` field:

```java
geo2dsphere("location")
```

#### 2d

To specify a [2d]({{< docsref "core/2d/" >}}) index key, use the `geo2d` method.

{{% note class="important"%}}
A 2d index is for data stored as points on a two-dimensional plane and is intended for legacy coordinate pairs used in MongoDB 2.2 and earlier.
{{% /note %}}

This example specifies a 2d index on the `points` field:

```java
geo2d("points")
```


#### geoHaystack

To specify a [geoHaystack]({{< docsref "core/geohaystack/" >}}) index key, use the `geoHaystack` method.

{{% note class="important"%}}
For queries that use spherical geometry, a 2dsphere index is a better option than a haystack index. 2dsphere indexes allow field reordering;
geoHaystack indexes require the first field to be the location field. Also, geoHaystack indexes are only usable via commands and so always
return all results at once.
{{% /note %}}

This example specifies a geoHaystack index on the `position` field and an ascending index on the `type` field:

```java
geoHaystack("position", ascending("type"))
```
