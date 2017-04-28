+++
date = "2015-03-19T14:27:51-04:00"
title = "Aggregates"
[menu.main]
  parent = "Builders"
  identifier = "Aggregates"
  weight = 40
  pre = "<i class='fa'></i>"
+++

## Aggregates

The [`Aggregates`]({{< apiref "com/mongodb/client/model/Aggregates" >}}) class provides static factory methods that build [aggregation
pipeline operators]({{< docsref "reference/operator/aggregation/" >}}).  Each method returns an instance of the
[`Bson`]({{< relref "bson/documents.md#bson" >}}) type, which can in turn be passed to the `aggregate` method of `MongoCollection`.

For brevity, you may choose to import the methods of the `Aggregates` class statically:

```java
import static com.mongodb.client.model.Aggregates.*;
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

### GraphLookup

The [`$graphLookup`]({{< docsref "reference/operator/aggregation/graphLookup/" >}}) pipeline stage performs a recursive search on a specified collection to match field A of one document to some field B of the other documents. For the matching documents, the stage repeats the search to match field A from the matching documents to the field B of the remaining documents until no new documents are encountered or until a specified depth. To each output document, `$graphLookup` adds a new array field that contains the traversal results of the search for that document.

The following example computes the social network graph for users in the `contacts` collection, recursively matching the value in the `friends` field to the `name` field, up to recursive depth of 1.

```java
graphLookup("contacts", "$friends", "friends", "name", "socialNetwork",
	new GraphLookupOptions().maxDepth(1))
```

Using `GraphLookupOptions`, the output can be tailored to restrict the depth of the recursion as well to inject a field containing the depth of the recursion at which a document was included.

### SortByCount

The [`$sortByCount`]({{< docsref "reference/operator/aggregation/sortByCount/" >}}) stage groups documents by a given expression and then sorts these groups by count in descending order. The `sortByCount` outputs documents that contains an `_id` field, which contains the discrete values of the given expression, and the `count` field that contains the number of documents that fall into that group.

The following example groups documents by the truncated value of the field `x` and computes the count for each distinct value of `x`. 

```java
sortByCount(new Document("$floor", "$x"))
```

### ReplaceRoot

The [`$replaceRoot`]({{< docsref "reference/operator/aggregation/replaceRoot/" >}}) pipeline stage replaces each input document to the stage with the specified document. All existing fields, including the `_id` field, are replaced. 

If each input document to the `replaceRoot` stage has a field `a1` that contains a field `b` whose value is a document, the following operation replaces each input document with the document in the `b` field.

```java
replaceRoot("$a1.b")
```

### AddFields

The [`$addFields`]({{< docsref "reference/operator/aggregation/addFields/" >}}) pipeline stage adds new fields to documents. The stage outputs documents that contain all existing fields from the input documents and the newly added fields.

This example adds two new fields, `myNewField` and `z` to the input documents; `myNewField` has the value `{c: 3, d: 4}`, `z` has the value 5.

```java
addFields(new Field("myNewField", new Document("c", 3).append("d", 4)),
	new Field("z", 5))
```

These new fields do not need be statically defined.  The following example shows how to add a new field which is a function of the current document's values.  In this case, a new field `alt3` is added with a value of `true` if the current value of the field `a` is less than 3.  Otherwise, `alt3` will be `false` in the new field.

```java
addFields(new Field("alt3", new Document("$lt", asList("$a", 3))))
```

### Count

The [`$count`]({{< docsref "reference/operator/aggregation/count/" >}}) pipeline stage specifies the name of the field that will contain the number of documents that enter this stage.  The `$count` stage is syntactic sugar for: `{$group:{_id:null, count:{$sum:1}}}`

There are two ways to invoke this stage.  The first way is to explicitly name the resulting field as in the two following examples:

```java
count("count")
```

```java
count("total")
```

These two invocations will put the count in the `count` and `total` fields respectively.  If `count` is the field name to be used, this can be shortened with the following convenience method:

```java
count()
```

This invocation defaults the field name to `count`.


### Bucket

The [`$bucket`]({{< docsref "reference/operator/aggregation/bucket/" >}}) pipeline stage automates the bucketing of data around predefined boundary values.

The following example shows a basic `$bucket` stage:

```java
bucket("$screenSize", [0, 24, 32, 50, 70, 200])
```

This will result in output that looks like this:

```json
[_id:0, count:1]
[_id:24, count:2]
[_id:32, count:1]
[_id:50, count:1]
[_id:70, count:2]
```

The default output is simply the lower bound as the `_id` and a single field containing the size of that bucket.  This output can be modified using the `BucketOptions` class.  The above example can be expanded to look like this:

```java
bucket("$screenSize", [0, 24, 32, 50, 70], new BucketOptions()
                .defaultBucket("monster")
                .output(sum("count", 1), push("matches", "$screenSize")))
```

The optional value `defaultBucket` defines the name of the bucket for values that fall outside defined bucket boundaries.  If `defaultBucket` is undefined and values exist outside of the defined bucket boundaries, the stage will produce an error.  The other value is the `output` field which defines the shape of the document output for each bucket.  The output of this stage looks something like this:

```json
[[_id: 0, count: 1, matches: [22]],
 [_id: 24, count: 2, matches: [24, 30]],
 [_id: 32, count: 1, matches: [42]],
 [_id: 50, count: 1, matches: [55]],
 [_id: monster, count: 2, matches: [75, 155]]]
```

This output contains not only the size of the bucket but also the values in the bucket.  Notice the enormous screen sizes are found in the synthetic bucket named `monster` reflecting the outrageously large screen sizes.

### BucketAuto

The [`$bucketAuto`]({{< docsref "reference/operator/aggregation/bucketAuto/" >}}) pipeline stage automatically determines the boundaries of each bucket in its attempt to distribute the documents evenly into a specified number of buckets. Depending on the input documents, the number of buckets may be less than the specified number of buckets.

For example, this stage creates 10 buckets:

```java
bucketAuto("$price", 10)
```

This results in output that looks something like this:

```json
[[_id: [min: 2, max: 30], count: 14],
 [_id: [min: 30, max: 58], count: 14],
 [_id: [min: 58, max: 86], count: 14],
 [_id: [min: 86, max: 114], count: 14],
 [_id: [min: 114, max: 142], count: 14],
 [_id: [min: 142, max: 170], count: 14],
 [_id: [min: 170, max: 198], count: 14],
 [_id: [min: 198, max: 226], count: 14],
 [_id: [min: 226, max: 254], count: 14],
 [_id: [min: 254, max: 274], count: 11]]
```

Note the uniformity of bucket sizes except for the last bucket.  For a more precise scheme of bucket definition, the `BucketAutoOptions` class exposes the opportunity to use a [preferred number](https://en.wikipedia.org/wiki/Preferred_number) based scheme to determine those boundary values.  As with `BucketOptions`, the output document shape can be defined using the `output` value on `BucketAutoOptions`.  An example of these options is shown below:

```java
bucketAuto("$price", 10, new BucketAutoOptions()
            .granularity(BucketGranularity.POWERSOF2)
            .output(sum("count", 1), avg("avgPrice", "$price")))
```

### Facet 

The [`$facet`]({{< docsref "reference/operator/aggregation/facet/" >}}) pipeline stage allows for the definition of a faceted search.  The stage is defined with a set of names and nested aggregation pipelines which define each particular facet.  For example, to return to the example of the television screen size search, the following `$facet` will return a document that groups televisions by size and manufacturer:

```java
facet(
	new Facet("Screen Sizes",
		unwind("$attributes"),
		bucketAuto("$attributes.screen_size", 5, new BucketAutoOptions()
			.output(sum("count", 1)))),
	new Facet("Manufacturer",
		sortByCount("$attributes.manufacturer"),
		limit(5))
)
```

This stage returns a document that looks like this:

```json
{
	"Manufacturer": [
		{"_id": "Vizio", "count": 17},
		{"_id": "Samsung", "count": 17},
		{"_id": "Sony", "count": 17}
	],
	"Screen Sizes": [
		{"_id": {"min": 35, "max": 45}, "count": 10},
		{"_id": {"min": 45, "max": 55}, "count": 10},
		{"_id": {"min": 55, "max": 65}, "count": 10},
		{"_id": {"min": 65, "max": 75}, "count": 10},
		{"_id": {"min": 75, "max": 85}, "count": 11}
	]
}

```

### Creating a Pipeline

The above pipeline operators are typically combined into a list and passed to the `aggregate` method of a `MongoCollection`.  For instance:

```java
collection.aggregate(Arrays.asList(match(eq("author", "Dave")),
                                   group("$customerId", sum("totalQuantity", "$quantity"),
                                                        avg("averageQuantity", "$quantity"))
                                   out("authors")));
```
