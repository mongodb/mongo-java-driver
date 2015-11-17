+++
date = "2015-03-19T14:27:51-04:00"
title = "Aggregation"
[menu.main]
  parent = "Builders"
  weight = 40
  pre = "<i class='fa'></i>"
+++

## Aggregation

The [`Aggregates`]({{< apiref "com/mongodb/client/model/Aggregates" >}}) class provides static factory methods that build [aggregation 
pipeline operators]({{< docsref "reference/operator/aggregation/" >}}).  Each method returns an instance of the 
[`Bson`]({{< relref "bson/documents.md#bson" >}}) type, which can in turn be passed to the `aggregate` method of `MongoCollection`.

For brevity, you may choose to import the methods of the `Aggregates` class statically:

```java
import com.mongodb.client.model.Aggregates.*;
```
  
All the examples below assume this static import.
  
### Match
  
The [`$match`]({{< docsref "reference/operator/aggregation/match/" >}}) pipeline stage passes all documents matching the 
specified filter to the next stage.  Though the filter can be an instance of any class that implements `Bson`, it's convenient to 
combine with use of the [`Filters`]({{< apiref "com/mongodb/client/model/Filters" >}}) class.  In the example below, it's assumed that the 
`eq` method of the `Filters` class has been statically imported.
  
This example creates a pipeline stage that matches all documents where the `author` field is equal to `"Dave"`:
 
```java
match(eq("author", "Dave"))
```

### Project
  
The [`$project`]({{< docsref "reference/operator/aggregation/project/" >}}) pipeline stage passes the projected fields of all 
documents to the next stage.  Though the projection can be an instance of any class that implements `Bson`, it's convenient to combine 
with use of the [`Projections`]({{< apiref "com/mongodb/client/model/Projections" >}}) class.  In the example below, it's assumed that the 
`include`, `excludeId`, and `fields` methods of the `Projections` class have been statically imported. 
  
This example creates a pipeline stage that excludes the `_id` field but includes the `title` and `author` fields:
 
```java
project(fields(include("title", "author"), excludeId()))
```

#### Projecting Computed Fields

The `$project` stage can project computed fields as well.

This example simply projects the `qty` field into a new field called `quantity`.  In other words, it renames the field:
 
```java
project(computed("quantity", "$qty"))
```

### Sample
The [`$sample`]({{< docsref "reference/operator/aggregation/sample/" >}}) pipeline stage randomly select N documents from its input.
This example creates a pipeline stage that randomly selects 5 documents from the collection:

```java
sample(5)
```

### Sort
  
The [`$sort`]({{< docsref "reference/operator/aggregation/sort/" >}}) pipeline stage passes all documents to the next stage, 
sorted by the specified sort criteria. Though the sort criteria can be an instance of any class that implements `Bson`, it's convenient to 
combine with use of the [`Sorts`]({{< apiref "com/mongodb/client/model/Sorts" >}}) class.  In the example below, it's assumed that the 
`descending`, `ascending`, and `orderBy` methods of the `Sorts` class have been statically imported.
  
This example creates a pipeline stage that sorts in descending order according to the value of the `age` field and then in ascending order 
according to the value of the `posts` field:
 
```java
sort(orderBy(descending("age"), ascending("posts")))
```

### Skip

The [`$skip`]({{< docsref "reference/operator/aggregation/skip/" >}}) pipeline stage skips over the specified number of 
documents that pass into the stage and passes the remaining documents to the next stage.

This example skips the first `5` documents:

```java
skip(5)
```

### Limit

The [`$limit`]({{< docsref "reference/operator/aggregation/limit/" >}}) pipeline stage limits the number of documents passed
to the next stage.
  
This example limits the number of documents to `10`:

```java
limit(10)
```

### Lookup

Starting in 3.2, MongoDB provides a new [`$lookup`]({{< docsref "reference/operator/aggregation/lookup/" >}}) pipeline stage 
that performs a left outer join with another collection to filter in documents from the joined collection for processing.

This example performs a left outer join on the `fromCollection` collection, joining the `local` field to the `from` field and outputted in 
the `joinedOutput` field:

```java
lookup("fromCollection", "local", "from", "joinedOutput")
```

### Group

The [`$group`]({{< docsref "reference/operator/aggregation/group/" >}}) pipeline stage groups documents by some specified 
expression and outputs to the next stage a document for each distinct grouping.  A group consists of an `_id` which specifies the 
expression on which to group, and zero or more 
[accumulators]({{< docsref "reference/operator/aggregation/group/#accumulator-operator" >}}) which are evaluated for each 
grouping.  To simplify the expression of accumulators, the driver includes an 
[`Accumulators`]({{< apiref "com/mongodb/client/model/Accumulators" >}}) class with static factory methods for each of the supported 
accumulators. In the example below, it's assumed that the `sum` and `avg` methods of the `Accumulators` class have been statically 
imported. 
 
This example groups documents by the value of the `customerId` field, and for each group accumulates the sum and average of the values of 
the `quantity` field into the `totalQuantity` and `averageQuantity` fields, respectively. 

```java
group("$customerId", sum("totalQuantity", "$quantity"), avg("averageQuantity", "$quantity"))
```   

### Unwind

The [`$unwind`]({{< docsref "reference/operator/aggregation/unwind/" >}}) pipeline stage deconstructs an array field from the 
input documents to output a document for each element.

This example outputs, for each document, a document for each element in the `sizes` array:

```java
unwind("$sizes")
```

Available with MongoDB 3.2, this example also includes any documents that have missing or `null` values for the `$sizes` field or where 
the `$sizes` list is empty:

```java
unwind("$sizes", new UnwindOptions().preserveNullAndEmptyArrays(true))
```

Available with MongoDB 3.2, this example unwinds the `sizes` array and also outputs the array index into the `$position` field:

```java
unwind("$sizes", new UnwindOptions().includeArrayIndex("$position"))
```

### Out

The [`$out`]({{< docsref "reference/operator/aggregation/out/" >}}) pipeline stage outputs all documents to the specified 
collection.  It must be the last stage in any aggregate pipeline:

This example writes the pipeline to the `authors` collection:
     
```java
out("authors")
```

### Creating a Pipeline

The above pipeline operators are typically combined into a list and passed to the `aggregate` method of a `MongoCollection`.  For instance:

```java
collection.aggregate(Arrays.asList(match(eq("author", "Dave")),
                                   group("$customerId", sum("totalQuantity", "$quantity"),
                                                        avg("averageQuantity", "$quantity"))
                                   out("authors")));
```










