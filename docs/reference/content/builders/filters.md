+++
date = "2015-03-19T14:27:51-04:00"
title = "Filters"
[menu.main]
  parent = "Builders"
  weight = 10
  pre = "<i class='fa'></i>"
+++

## Filters

The [`Filters`]({{< apiref "com/mongodb/client/model/Filters" >}}) class provides static factory methods for all the MongoDB query 
operators.  Each method returns an instance of the [`Bson`]({{< relref "bson/documents.md#bson" >}}) type, which can in turn be passed to
any method that expects a query filter.

For brevity, you may choose to import the methods of the `Filters` class statically:

```java
import com.mongodb.client.model.Filters.*;
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
 
```java
and(gt("qty", 20), eq("user", "jdoe"))
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
  
```java
and(or(eq("price", 0.99), eq("price", 1.99)
    or(eq("sale", true), lt("qty", 20)))
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
text("abstract", "coffee")
```

