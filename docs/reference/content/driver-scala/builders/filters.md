+++
date = "2015-03-19T14:27:51-04:00"
title = "Filters"
[menu.main]
  parent = "Scala Builders"
  identifier = "Scala Filters"
  weight = 10
  pre = "<i class='fa'></i>"
+++

## Filters

The [`Filters`]({{< scapiref "org/mongodb/scala/model/Filters$" >}}) class provides static factory methods for all the MongoDB query 
operators.  Each method returns an instance of the [`Bson`]({{< relref "bson/documents.md#bson" >}}) type, which can in turn be passed to
any method that expects a query filter.

For brevity, you may choose to import the methods of the `Filters` class statically:

```scala
import org.mongodb.scala.model.Filters._
```
  
All the examples below assume this static import.
  
### Comparison

The comparison operator methods include:

- `eq`: Matches values that are equal to a specified value. Aliased to `equal` as `eq` is a reserved word. 
- `gt`: Matches values that are greater than a specified value.
- `gte`: Matches values that are greater than or equal to a specified value.
- `lt`: Matches values that are less than a specified value.
- `lte`: Matches values that are less than or equal to a specified value.
- `ne`: Matches all values that are not equal to a specified value. Aliased to `notEqual` as `neq` is a reserved word. 
- `in`: Matches any of the values specified in an array.  
- `nin`: Matches none of the values specified in an array. 

#### Examples

This example creates a filter that selects all documents where the value of the `qty` field equals `20`:

```scala
`eq`("qty", 20)
equal("qty", 20)
```

which will render as:

```json
{  
   "qty" : 20
}
```

This example creates a filter that selects all documents where the value of the `qty` field is either `5` or `15`:

```scala
in("qty", 5, 15)
```

### Logical

The logical operator methods include:

- `and`: Joins filters with a logical AND and selects all documents that match the conditions of both filters.    
- `or`: Joins filters with a logical OR and selects all documents that match the conditions of either filters. 
- `not`: Inverts the effect of a query expression and selects documents that do not match the filter. 
- `nor`: Joins filters with a logical NOR and selects all documents that fail to match both filters.
 
#### Examples

This example creates a filter that selects all documents where ther value of the `qty` field is greater than `20` and the value of the 
`user` field equals `"jdoe"`:
 
```scala
and(gt("qty", 20), equal("user", "jdoe"))
```

The `and` method generates a `$and` operator only if necessary, as the query language implicity ands together all the elements in a 
filter. So the above example will render as: 

```json
{ 
   "qty" : { "$gt" : 20 },
   "user" : "jdoe"
}
```

This example creates a filter that selects all documents where the `price` field value equals `0.99` or `1.99`; and the `sale` field value 
is equal to `true` or the `qty` field value is less than `20`:
  
```scala
and(or(equal("price", 0.99), equal("price", 1.99)
    or(equal("sale", true), lt("qty", 20)))
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

```scala
all("tags", "ssl", "security")
```

### Elements

The elements operator methods include:

- `exists`: Selects documents that have the specified field.
- `type`: Selects documents if a field is of the specified type. Aliased to `bsonType` as `type` is a reserved word.

#### Examples

This example selects documents that have a `qty` field and its value does not equal `5` or `15`:

```scala
and(exists("qty"), nin("qty", 5, 15))
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

```scala
text("coffee")
```

Available with MongoDB 3.2, a version 3 text index allows case-sensitive searches. This example selects documents that have an 
`abstract` field containing the exact term `coffee`:

```scala
text("coffee", TextSearchOptions().caseSensitive(true))
```

Available with MongoDB 3.2, a version 3 text index allows diacritic-sensitive searches. This example selects documents that have an 
`abstract` field containing the exact term `café`:

```scala
text("café", TextSearchOptions().diacriticSensitive(true))
```

### Bitwise

The bitwise query operators, available with MongoDB 3.2 include:

- `bitsAllSet`: Selects documents where the all the specified bits of a field are set (i.e. 1).
- `bitsAllClear`: Selects documents where the all the specified bits of a field are clear (i.e. 0).
- `bitsAnySet`: Selects documents where at least one of the specified bits of a field are set (i.e. 1).
- `bitsAnyClear`: Selects documents where at least one of the specified bits of a field are clear (i.e. 0)


#### Examples

The example selects documents that have a `bitField` field with bits set at positions of the corresponding bitmask `50` (i.e. `00110010`):

```scala
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

- [`Point`]({{< scapiref "org/mongodb/scala/model/geojson/package$$Point$" >}}): A representation of a GeoJSON Point.
- [`MultiPoint`]({{< scapiref "org/mongodb/scala/model/geojson/package$$MultiPoint$" >}}): A representation of a GeoJSON MultiPoint.
- [`LineString`]({{< scapiref "org/mongodb/scala/model/geojson/package$$LineString$" >}}): A representation of a GeoJSON LineString.
- [`MultiLineString`]({{< scapiref "org/mongodb/scala/model/geojson/package$$MultiLineString$" >}}): A representation of a GeoJSON MultiLineString.
- [`Polygon`]({{< scapiref "org/mongodb/scala/model/geojson/package$$Polygon$" >}}): A representation of a GeoJSON Polygon.
- [`MultiPolygon`]({{< scapiref "org/mongodb/scala/model/geojson/package$$MultiPolygon$" >}}): A representation of a GeoJSON MultiPolygon.
- [`GeometryCollection`]({{< scapiref "org/mongodb/scala/model/geojson/package$$GeometryCollection$" >}}): A representation of a GeoJSON 
GeometryCollection.


#### Examples

This example creates a filter that selects all documents where the `geo` field contains a GeoJSON Geometry object that falls within the 
given polygon:

```scala
    val polygon: Polygon = Polygon(Seq(Position(0, 0), Position(4, 0),
                                       Position(4, 4), Position(0, 4),
                                       Position(0, 0)))
    geoWithin("geo", polygon)
```

Similarly, this example creates a filter that selects all documents where the `geo` field contains a GeoJSON Geometry object that 
intersects the given Point:

```scala
geoIntersects("geo", Point(Position(4, 0)))
```


