+++
date = "2015-03-19T14:27:51-04:00"
title = "Sort Criteria  "
[menu.main]
  parent = "Builders"
  weight = 30
  pre = "<i class='fa'></i>"
+++

## Sorts

The [`Sorts`]({{< apiref "mongodb-driver-core" "com/mongodb/client/model/Sorts" >}}) class provides static factory methods for all the MongoDB sort criteria 
operators.  Each method returns an instance of the [`Bson`]({{< relref "bson/documents.md#bson" >}}) type, which can in turn be passed to
any method that expects sort criteria.

For brevity, you may choose to import the methods of the `Sorts` class statically:

```java
import static com.mongodb.client.model.Sorts.*;
```
  
All the examples below assume this static import.

### Ascending

To specify an ascending sort, use one of the `ascending` methods.

This example specifies an ascending sort on the `quantity` field:

```java
ascending("quantity")
```

This example specifies an ascending sort on the `quantity` field, followed by an ascending sort on the `totalAmount` field:

```java
ascending("quantity", "totalAmount") 
```

### Descending

To specify a descending sort, use one of the `descending` methods.

This example specifies a descending sort on the `quantity` field:

```java
descending("quantity")
```

This example specifies a descending sort on the `quantity` field, followed by a descending sort on the `totalAmount` field:


```java
descending("quantity", "totalAmount") 
```

### Text Score

To specify a sort by [the score of a `$text` query]({{< docsref "reference/operator/query/text/#sort-by-text-search-score" >}}), use the 
`metaTextScore` method to specify the name of the projected field.

This example specifies a sort on the score of a `$text` query that will be projected into the `scoreValue` field in a projection on the 
same query:

```java
metaTextScore("scoreValue")
```

### Combining sort criteria

To specify the combination of multiple sort criteria, use the `orderBy` method.

This example specifies an ascending sort on the `quantity` field, followed by an ascending sort on the `totalAmount` field, followed by a 
descending sort on the `orderDate` field:

```java
orderBy(ascending("quantity", "totalAmount"), descending("orderDate"))
```

