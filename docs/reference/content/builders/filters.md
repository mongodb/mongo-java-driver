+++
date = "2015-03-19T14:27:51-04:00"
title = "Filters"
[menu.main]
  parent = "Builders"
  weight = 10
  pre = "<i class='fa'></i>"
+++

## Filters

The [`Filters`]({{< apiref "mongodb-driver-core" "com/mongodb/client/model/Filters" >}}) class provides static factory methods for all the MongoDB query 
operators.  Each method returns an instance of the [`Bson`]({{< relref "bson/documents.md#bson" >}}) type, which can in turn be passed to
any method that expects a query filter.

For brevity, you may choose to import the methods of the `Filters` class statically:

```java
import static com.mongodb.client.model.Filters.*;
```
  
All the examples below assume this static import.
  
### Comparison

The comparison operator methods include:

- `eq`: Matches values that are equal to a specified value.  
- `gt`: Matches values that are greater than a specified value.
- `gte`: Matches values that are greater than or equal to a specified value.
- `lt`: Matches values that are less than a specified value.
- `lte`: Matches values that are less than or equal to a specified value.
- `ne`: Matches all values that are not equal to a specified value.
- `in`: Matches any of the values specified in an array.  
- `nin`: Matches none of the values specified in an array.
- `empty`: Matches all the documents. 

#### Examples

This example creates a filter that selects all documents where the value of the `qty` field equals `20`:

```java
eq("qty", 20)
```

which will render as:

```json
{  
   "qty" : 20
}
```

This example creates a filter that selects all documents where the value of the `qty` field is either `5` or `20`:

```java
in("qty", 5, 20)
```

This example creates a filter that selects all documents because the predicate is empty:

```java
empty()
```

It will render as:

```json
{}
```

### Logical

The logical operator methods include:

- `and`: Joins filters with a logical AND and selects all documents that match the conditions of both filters.    
- `or`: Joins filters with a logical OR and selects all documents that match the conditions of either filters. 
- `not`: Inverts the effect of a query expression and selects documents that do not match the filter. 
- `nor`: Joins filters with a logical NOR and selects all documents that fail to match both filters.
 
#### Examples

This example creates a filter that selects all documents where the value of the `qty` field is greater than `20` and the value of the 
`user` field equals `"jdoe"`:
 
```java
and(gt("qty", 20), eq("user", "jdoe"))
```

The `and` method generates a `$and` operator at all times, so the result would render:

```json
{
  "$and": [
    { "qty" : { "$gt" : 20 }},
    { "user" : "jdoe" }
  ]
}
```

This example creates a filter that selects all documents where the `price` field value equals `0.99` or `1.99`; and the `sale` field value 
is equal to `true` or the `qty` field value is less than `20`:
  
```java
and(
    or(
        eq("price", 0.99), 
        eq("price", 1.99)
    ),
    or(
        eq("sale", true), 
        lt("qty", 20)
    )
)
```

This query cannot be constructed using an implicit and operation, because it uses the `$or` operator more than once.  So it will render as:

```json
{
 "$and" : 
    [
      { "$or" : [ { "price" : 0.99 }, { "price" : 1.99 } ] },
      { "$or" : [ { "sale" : true }, { "qty" : { "$lt" : 20 } } ] }
    ]
}
```

### Arrays

The array operator methods include:

- `all`: Matches arrays that contain all elements specified in the query 
- `elemMatch`: Selects documents if element in the array field matches all the specified $elemMatch conditions
- `size`: Selects documents if the array field is a specified size

#### Examples

This example selects documents with a `tags` array containing both `"ssl"` and `"security"`:

```java
all("tags", Arrays.asList("ssl", "security"))
```

### Elements

The elements operator methods include:

- `exists`: Selects documents that have the specified field.
- `type`: Selects documents if a field is of the specified type.

#### Examples

This example selects documents that have a `qty` field and its value does not equal `5` or `15`:

```java
and(exists("qty"), nin("qty", 5, 15))
```

This example selects documents that have a `qty` field with the type of `BsonInt32`:

```java
type("qty", BsonType.INT32)
```

Available with MongoDB 3.2, this example selects any documents that have a `qty` field with any "number" bson type:

```java
type("qty", "number")
```

### Evaluation

The evaluation operator methods include:

- `mod`: Performs a modulo operation on the value of a field and selects documents with a specified result.
- `regex`: Selects documents where values match a specified regular expression.
- `text`: Selects documemts matching a full-text search expression.
- `where`: Matches documents that satisfy a JavaScript expression.

#### Examples

This example assumes a collection that has a text index in the field `abstract`.  It selects documents that have a `abstract` field 
containing the term `coffee`:

```java
text("coffee")
```

Available with MongoDB 3.2, a version 3 text index allows case-sensitive searches. This example selects documents that have an 
`abstract` field containing the exact term `coffee`:

```java
text("coffee", new TextSearchOptions().caseSensitive(true))
```

Available with MongoDB 3.2, a version 3 text index allows diacritic-sensitive searches. This example selects documents that have an 
`abstract` field containing the exact term `café`:

```java
text("café", new TextSearchOptions().diacriticSensitive(true))
```

### Bitwise

The bitwise query operators, available with MongoDB 3.2 include:

- `bitsAllSet`: Selects documents where the all the specified bits of a field are set (i.e. 1).
- `bitsAllClear`: Selects documents where the all the specified bits of a field are clear (i.e. 0).
- `bitsAnySet`: Selects documents where at least one of the specified bits of a field are set (i.e. 1).
- `bitsAnyClear`: Selects documents where at least one of the specified bits of a field are clear (i.e. 0)


#### Examples

The example selects documents that have a `bitField` field with bits set at positions of the corresponding bitmask `50` (i.e. `00110010`):

```java
bitsAllSet("bitField", 50)
```

### Geospatial

The geospatial operator methods include:

- `geoWithin`: Selects all documents containing a field whose value is a GeoJSON geometry that falls within within a bounding GeoJSON 
geometry.
- `geoWithinBox`: Selects all documents containing a field with grid coordinates data that exist entirely within the specified box.
- `geoWithinPolygon`: Selects all documents containing a field with grid coordinates data that exist entirely within the specified polygon.
- `geoWithinCenter`: Selects all documents containing a field with grid coordinates data that exist entirely within the specified circle.
- `geoWithinCenterSphere`: Selects geometries containing a field with geospatial data (GeoJSON or legacy coordinate pairs) that exist 
entirely within the specified circle, using spherical geometry. 
- `geoIntersects`: Selects geometries that intersect with a GeoJSON geometry. The 2dsphere index supports $geoIntersects.
- `near`: Returns geospatial objects in proximity to a point. Requires a geospatial index. The 2dsphere and 2d indexes support $near.
- `nearSphere`: Returns geospatial objects in proximity to a point on a sphere. Requires a geospatial index. The 2dsphere and 2d 
indexes support $nearSphere. 

To make it easier to construct GeoJSON-based filters, the driver also include a full GeoJSON class hierarchy:

- [`Point`]({{< apiref "mongodb-driver-core" "com/mongodb/client/model/geojson/Point" >}}): A representation of a GeoJSON Point.
- [`MultiPoint`]({{< apiref "mongodb-driver-core" "com/mongodb/client/model/geojson/MultiPoint" >}}): A representation of a GeoJSON MultiPoint.
- [`LineString`]({{< apiref "mongodb-driver-core" "com/mongodb/client/model/geojson/LineString" >}}): A representation of a GeoJSON LineString.
- [`MultiLineString`]({{< apiref "mongodb-driver-core" "com/mongodb/client/model/geojson/MultiLineString" >}}): A representation of a GeoJSON MultiLineString.
- [`Polygon`]({{< apiref "mongodb-driver-core" "com/mongodb/client/model/geojson/Polygon" >}}): A representation of a GeoJSON Polygon.
- [`MultiPolygon`]({{< apiref "mongodb-driver-core" "com/mongodb/client/model/geojson/MultiPolygon" >}}): A representation of a GeoJSON MultiPolygon.
- [`GeometryCollection`]({{< apiref "mongodb-driver-core" "com/mongodb/client/model/geojson/GeometryCollection" >}}): A representation of a GeoJSON 
GeometryCollection.


#### Examples

This example creates a filter that selects all documents where the `geo` field contains a GeoJSON Geometry object that falls within the 
given polygon:

```java
Polygon polygon = new Polygon(Arrays.asList(new Position(0, 0), 
                                            new Position(4, 0), 
                                            new Position(4, 4), 
                                            new Position(0, 4),
                                            new Position(0, 0)));
geoWithin("geo", polygon))
```

Similarly, this example creates a filter that selects all documents where the `geo` field contains a GeoJSON Geometry object that 
intersects the given Point:

```java
geoIntersects("geo", new Point(new Position(4, 0)))
```


