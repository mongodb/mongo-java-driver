+++
date = "2016-05-29T12:47:54-04:00"
title = "Bulk Writes"
[menu.main]
  parent = "Scala Perform Write Operations"
  identifier = "Scala Bulk Writes"
  weight = 10
  pre = "<i class='fa'></i>"
+++

## Bulk operations

Starting in version 2.6, MongoDB servers support bulk write commands for insert, update, and delete in a way that allows the driver to implement the correct semantics for BulkWriteResult and BulkWriteException.

There are two types of bulk operations, ordered and unordered bulk operations.

1. Ordered bulk operations execute all the operation in order and error out on the first write error.

2. Unordered bulk operations execute all the operations and report any the errors. Unordered bulk operations do not guarantee order of execution.

The following code provide examples using ordered and unordered operations:

{{% note class="important" %}}
This guide uses the `Observable` implicits as covered in the [Quick Start Primer]({{< relref "driver-scala/getting-started/quick-start-primer.md" >}}).
{{% /note %}}

```scala
import org.mongodb.scala._
import org.mongodb.scala.model._

// 1. Ordered bulk operation - order is guaranteed
collection.bulkWrite(
  List(InsertOneModel(Document("_id" -> 4)),
    InsertOneModel(Document("_id" -> 5)),
    InsertOneModel(Document("_id" -> 6)),
    UpdateOneModel(Document("_id" -> 1), Document("$set", Document("x" -> 2))),
    DeleteOneModel(Document("_id" -> 2)),
    ReplaceOneModel(Document("_id"-> 3), Document("_id" -> 3, "x" -> 4)))
).printResults()


 // 2. Unordered bulk operation - no guarantee of order of operation
collection.bulkWrite(
  List(InsertOneModel(Document("_id" -> 4)),
    InsertOneModel(Document("_id" -> 5)),
    InsertOneModel(Document("_id" -> 6)),
    UpdateOneModel(Document("_id" -> 1), Document("$set", Document("x" -> 2))),
    DeleteOneModel(Document("_id" -> 2)),
    ReplaceOneModel(Document("_id"-> 3), Document("_id" -> 3, "x" -> 4))),
  BulkWriteOptions().ordered(false)
).printResults()
```
