+++
date = "2015-03-19T14:27:51-04:00"
title = "Updates"
[menu.main]
  parent = "Builders"
  weight = 50
  pre = "<i class='fa'></i>"
+++

## Updates

The [`Updates`]({{< apiref "mongodb-driver-core" "com/mongodb/client/model/Updates" >}}) class provides static factory methods for all the MongoDB update 
operators.  Each method returns an instance of the [`Bson`]({{< relref "bson/documents.md#bson" >}}) type, which can in turn be passed to
any method that expects an update.

For brevity, you may choose to import the methods of the `Updates` class statically:

```java
import static com.mongodb.client.model.Updates.*;
```
  
All the examples below assume this static import.

### Field Updates

This section describes update operators that apply to the value of an entire field.

#### Set

The [`$set`]({{< docsref "reference/operator/update/set/" >}}) update operator sets the value of a field to the specified value.

This example sets the value of the `quantity` field to `11`:

```java
set("quantity", 11)
```

#### Unset

The [`$unset`]({{< docsref "reference/operator/update/unset/" >}}) update operator deletes the field with the given name.

This example deletes the `quantity` field:

```java
unset("quantity")
```

#### Set On Insert

The [`$setOnInsert`]({{< docsref "reference/operator/update/setOnInsert/" >}}) update operator sets the value of a field to the given 
value, but only if the update is an 
[upsert]({{< docsref "tutorial/modify-documents/#specify-upsert-true-for-the-update-specific-fields-operation" >}}) that results in an 
insert of a document.

This example sets the value of the `defaultQuantity` field to `10` if an upsert resulted in the insert of a document:

```java
setOnInsert("defaultQuantity", 10)
```
  
#### Increment

The [`$inc`]({{< docsref "reference/operator/update/inc/" >}}) update operator increments the value of a numeric field by a specified 
value.

This example increments the value of the `quantity` field by `5`:

```java
inc("quantity", 5)
```

#### Multiply

The [`$mul`]({{< docsref "reference/operator/update/mul/" >}}) update operator multiplies the value of a numeric field by a specified value.

This example multiplies the value of the `price` field by `1.2`:

```java
mul("price", 1.2)
```

#### Rename

The [`$rename`]({{< docsref "reference/operator/update/rename/" >}}) update operator renames a field.

This example renames the `qty` field to `quantity`:

```java
rename("qty", "quantity")
```

#### Min

The [`$min`]({{< docsref "reference/operator/update/min/" >}}) update operator updates the value of the field to a specified value *if* the
specified value is less than the current value of the field .

This example sets the value of the `lowScore` field to the minimum of its current value and 150:

```java
min("lowScore", 150)
```

#### Max

The [`$max`]({{< docsref "reference/operator/update/max/" >}}) update operator updates the value of the field to a specified value *if* 
the specified value is greater than the current value of the field .

This example sets the value of the `highScore` field to the maximum of its current value and 900:
                                                                               
```java
max("highScore", 900)
```

#### Current Date

The [`$currentDate`]({{< docsref "reference/operator/update/currentDate/" >}}) update operator sets the value of the field with the 
specified name to the current date, either as a BSON [date]({{< docsref "reference/bson-types/#document-bson-type-date" >}}) or as a BSON 
[timestamp]({{< docsref "reference/bson-types/#document-bson-type-timestamp" >}}).

This example sets the value of the `lastModified` field to the current date as a BSON date type:

```java
currentDate("lastModified")
```

This example sets the value of the `lastModified` field to the current date as a BSON timestamp type:

```java
currentTimestamp("lastModified")
```

#### Bit

The [`$bit`]({{< docsref "reference/operator/update/bit/" >}}) update operator performs a bitwise update of the integral value of a field.

This example performs a bitwise AND between the number `10` and the integral value of the `mask` field:

```java
bitwiseAnd("mask", 10)
```

This example performs a bitwise OR between the number `10` and the integral value of the `mask` field:

```java
bitwiseOr("mask", 10)
```

This example performs a bitwise XOR between the number `10` and the integral value of the `mask` field:

```java
bitwiseXor("mask", 10)
```

### Array Updates

This section describes update operators that apply to the contents of the array value of a field.

#### Add to Set

The [`$addToSet`]({{< docsref "reference/operator/update/addToSet/" >}}) update operator adds a value to an array unless the value is 
already present, in which case $addToSet does nothing to that array. 

This example adds the value `"a"` to the array value of the `letters' field:

```java
addToSet("letters", "a")
```

This example adds each of the values `"a"`, `"b"`, and `"c"` to the array value of the `letters' field:

```java
addEachToSet("letters", Arrays.asList("a", "b", "c"))
```

#### Pop

The [`$pop`]({{< docsref "reference/operator/update/pop/" >}}) update operator removes the first or last element of an array. 

This example pops the first element off of the array value of the `scores` field:

```java
popFirst("scores")
```

This example pops the last element off of the array value of the `scores` field:

```java
popLast("scores")
```

#### Pull All

The [`$pullAll`]({{< docsref "reference/operator/update/pullAll/" >}}) update operator removes all instances of the specified values from
an existing array. 

This example removes the scores `0` and `5` from the `scores` array:

```java
pullAll("scores", Arrays.asList(0, 5))
```

#### Pull 

The [`$pull`]({{< docsref "reference/operator/update/pull/" >}}) update operator removes from an existing array all instances of a value 
or values that match a specified query.

This example removes the value `0` from the `scores` array:

```java
pull("scores", 0)
```

This example removes all elements from the `votes` array that are greater than or equal to `6`:

```java
pullByFilter(Filters.gte("votes", 6))
```

#### Push

The [`$push`]({{< docsref "reference/operator/update/push/" >}}) update operator appends a specified value to an array.

This examples pushes the value `89` to the `scores` array:

```java
push("scores", 89)
```

This examples pushes each of the values `89`, `90`, and `92` to the `scores` array:

```java
pushEach("scores", Arrays.asList(89, 90, 92))
```

This example pushes each of the values `89`, `90`, and `92` to the start of the `scores` array:

```java
pushEach("scores", Arrays.asList(89, 90, 92), new PushOptions().position(0))
```

This example pushes each of the values `89`, `90`, and `92` to the `scores` array, sorts the array in descending order, and removes all 
but the first 5 elements of the array:

```java
pushEach("scores", Arrays.asList(89, 90, 92), new PushOptions().sort(-1).slice(5))
```

This example pushes each of the documents `{ wk: 5, score: 8 }`, `{ wk: 6, score: 7 }`, and `{ wk: 7, score: 6 }` to the `quizzes` array, 
sorts the array in descending order by `score`, and removes all but the last 3 elements of the array:

```java
pushEach("quizzes",
                 Arrays.asList(new Document("week", 5).append("score", 8),
                               new Document("week", 6).append("score", 7),
                               new Document("week", 7).append("score", 6)),
                 new PushOptions().sortDocument(Sorts.descending("score")).slice(-3))
```


### Combining Multiple Update Operators

Often, an application will need to atomically update multiple fields of a single document by combine two or more of the update operators 
described above.  

This example sets the value of the `quantity` field to 11, the value of the `total` field to `30.40`, and pushes each of the values 
`4.99`, `5.99`, and `10.99` to the array value of the `prices` field:

```java
combine(set("quantity", 11), 
        set("total", 30.40),
        pushEach("prices", Arrays.asList(4.99, 5.99, 10.99)))
```











