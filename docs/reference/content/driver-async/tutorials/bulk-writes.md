+++
date = "2016-05-29T12:47:54-04:00"
title = "Bulk Writes"
[menu.main]
  parent = "Async Write Operations"
  identifier = "Async Bulk Writes"
  weight = 10
  pre = "<i class='fa'></i>"
+++

## Bulk operations

Starting in version 2.6, MongoDB servers support bulk write commands for insert, update, and delete in a way that allows the driver to implement the correct semantics for BulkWriteResult and BulkWriteException.

There are two types of bulk operations, ordered and unordered bulk operations.

1. Ordered bulk operations execute all the operation in order and error out on the first write error.

2. Unordered bulk operations execute all the operations and report any the errors. Unordered bulk operations do not guarantee order of execution.

The following code provide examples using ordered and unordered
operations:

```java
// Callback
SingleResultCallback<BulkWriteResult> printBatchResult = new SingleResultCallback<BulkWriteResult>() {
    @Override
    public void onResult(final BulkWriteResult result, final Throwable t) {
        System.out.println(result);
    }
};

// 1. Ordered bulk operation - order is guaranteed
collection.bulkWrite(
  Arrays.asList(new InsertOneModel<>(new Document("_id", 4)),
                new InsertOneModel<>(new Document("_id", 5)),
                new InsertOneModel<>(new Document("_id", 6)),
                new UpdateOneModel<>(new Document("_id", 1),
                                     new Document("$set", new Document("x", 2))),
                new DeleteOneModel<>(new Document("_id", 2)),
                new ReplaceOneModel<>(new Document("_id", 3),
                                      new Document("_id", 3).append("x", 4))),
  printBatchResult
);


 // 2. Unordered bulk operation - no guarantee of order of operation
collection.bulkWrite(
  Arrays.asList(new InsertOneModel<>(new Document("_id", 4)),
                new InsertOneModel<>(new Document("_id", 5)),
                new InsertOneModel<>(new Document("_id", 6)),
                new UpdateOneModel<>(new Document("_id", 1),
                                     new Document("$set", new Document("x", 2))),
                new DeleteOneModel<>(new Document("_id", 2)),
                new ReplaceOneModel<>(new Document("_id", 3),
                                      new Document("_id", 3).append("x", 4))),
  new BulkWriteOptions().ordered(false),
  printBatchResult
);
```

{{% note class="important" %}}
Use of the bulkWrite methods is not recommended when connected to pre-2.6 MongoDB servers, as this was the first server version to support bulk write commands for insert, update, and delete in a way that allows the driver to implement the correct semantics for BulkWriteResult and BulkWriteException. The methods will still work for pre-2.6 servers, but performance will suffer, as each write operation has to be executed one at a time.
{{% /note %}}
