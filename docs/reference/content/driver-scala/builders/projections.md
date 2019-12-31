+++
date = "2015-03-19T14:27:51-04:00"
title = "Projections"
[menu.main]
  parent = "Scala Builders"
  identifier = "Scala Projections"
  weight = 20
  pre = "<i class='fa'></i>"
+++

## Projections

The [`Projections`]({{< scapiref "org/mongodb/scala/model/Projections$" >}}) class provides static factory methods for all the MongoDB 
projection opererators.  Each method returns an instance of the [`Bson`]({{< relref "bson/documents.md#bson" >}}) type, which can in turn
be passed to any method that expects a projection.

For brevity, you may choose to import the methods of the `Projections` class statically:

```scala
import org.mongodb.scala.model.Projections._
```
  
All the examples below assume this static import.

### Inclusion

By default, all fields of each document are projected.  To specify the inclusion of one or more fields (which implicitly excludes all 
other fields except `_id`), use the `include` method.  

This example includes the `quantity` field and (implicitly) the `_id` field:

```scala
include("quantity")
```

This example includes the `quantity` and `totalAmount` fields and (implicitly) the `_id` field:

```scala
include("quantity", "totalAmount")
```

### Exclusion

To specify the exclusion of one or more fields (which implicitly includes all other fields), use the `exclude` method.

This example excludes the `quantity` field:

```scala
exclude("quantity")
```

This example excludes the `quantity` and `totalAmount` fields:

```scala
exclude("quantity", "totalAmount")
```

### Exclusion of _id

To specify the exclusion of the `_id` field, use the `excludeId` method:
 
```scala
excludeId()
```

which is just shorthand for:

```scala
exclude("_id")
```

### Array Element Match with a Supplied Filter

To specify a projection that includes only the first element of an array that matches a supplied query filter (the 
[elemMatch]({{< docsref "reference/operator/projection/elemMatch" >}}) operator), use the `elemMatch` method that takes a 
field name and a filter. 

This example projects the first element of the `orders` array where the `quantity` field is greater that `3`:
  
```scala
elemMatch("orders", Filters.gt("quantity", 3))
```

### Array Element Match with an Implicit Filter

To specify a projection that includes only the first element of an array that matches the filter supplied as part of the query (the 
[positional $ operator]({{< docsref "reference/operator/projection/positional/#projection" >}})), use the `elemMatch` method that takes 
just a field name.

This example projects the first element of the `orders` array that matches the query filter:

```scala
elemMatch("orders")
```
     
### Slice

To project [a slice of an array]({{< docsref "reference/operator/projection/slice" >}}), use one of the `slice` methods. 

This example projects the first `7` elements of the `tags` array:

```scala
slice("tags", 7)
```

This example skips the first `2` elements of the `tags` array and projects the next `5`:

```scala
slice("tags", 2, 5)
```

### Text Score

To specify a projection of [the score of a `$text` query]({{< docsref "reference/operator/query/text/#return-the-text-search-score" >}}),
use the `metaTextScore` method to specify the name of the projected field.

This example projects the text score as the value of the `score` field:

```scala
metaTextScore("score")
```


### Combining Projections

To combine multiple projections, use the `fields` method.

This example includes the `quantity` and `totalAmount` fields and excludes the `_id` field:

```scala
fields(include("quantity", "totalAmount"), excludeId()) 
```


