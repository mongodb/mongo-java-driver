+++
date = "2016-06-09T12:47:43-04:00"
title = "What's New"
[menu.main]
  identifier = "Release notes"
  weight = 15
  pre = "<i class='fa fa-level-up'></i>"
+++

# What's New

This release includes full support for the upcoming MongoDB 3.4 server release.  Key new features include:

### Support for Decimal128 Format

``` java
import org.bson.types.Decimal128;
```

The [Decimal128]({{<docsref "release-notes/3.3-dev-series/#decimal-type">}}) format supports numbers with up to 34 decimal digits
(i.e. significant digits) and an exponent range of −6143 to +6144.

To create a `Decimal128` number, you can use

- [`Decimal128.parse()`] ({{<apiref "org/bson/types/Decimal128.html">}}) with a string:

      ```java
      Decimal128.parse("9.9900");
      ```

- [`new Decimal128()`] ({{<apiref "org/bson/types/Decimal128.html">}}) with a long:


      ```java
      new Decimal128(10L);
      ```

- [`new Decimal128()`] ({{<apiref "org/bson/types/Decimal128.html">}}) with a `java.math.BigDecimal`:

      ```java
      new Decimal128(new BigDecimal("4.350000"));
      ```

### Support for Collation

```java
import com.mongodb.client.model.Collation;
```

[Collation]({{<docsref "release-notes/3.3-dev-series-collation/">}}) allows users to specify language-specific rules for string
comparison. 
Use the [`Collation.builder()`] ({{<apiref "com/mongodb/client/model/Collation.html">}}) 
to create the `Collation` object. For example, the following example creates a `Collation` object with Primary level of comparison and [locale]({{<docsref "release-notes/3.3-dev-series-collation/#supported-languages-and-locales">}}) ``fr``.

```java
Collation.builder().collationStrength(CollationStrength.PRIMARY).locale("fr").build()));
```

You can specify collation at the collection level, at an index level, or at a collation-supported operation level:

#### Collection Level

To specify collation at the collection level, pass a `Collation` object as an option to the `createCollection()` method. To specify options to the `createCollection` method, use the [`CreateCollectionOptions`]({{<apiref "com/mongodb/client/model/CreateCollectionOptions.html">}}) class. 

```java
database.createCollection("myColl", new CreateCollectionOptions().collation(
                              Collation.builder()
                                    .collationStrength(CollationStrength.PRIMARY)
                                    .locale("fr").build()));
```

#### Index Level

To specify collation for an index, pass a `Collation` object as an option to the `createIndex()` method. To specify index options, use the [IndexOptions]({{<apiref "com/mongodb/client/model/IndexOptions.html">}}) class. 

```java
IndexOptions collationIndexOptions = new IndexOptions().name("collation-fr")
                                       .collation(Collation.builder()
                                       .collationStrength(CollationStrength.SECONDARY)
                                       .locale("fr").build());

collection.createIndex(
        Indexes.ascending("name"), collationIndexOptions);
```

#### Operation Level

The following operations support collation by specifying the
`Collation` object to their respective Iterable object.

- `MongoCollection.aggregate()`
   
- `MongoCollection.distinct()`
   
- `MongoCollection.find()`

- `MongoCollection.mapReduce()`

For example:

```java
Collation collation = Collation.builder()
                                 .collationStrength(CollationStrength.SECONDARY)
                                 .locale("fr").build();

collection.find(Filters.eq("category", "cafe")).collation(collation);

collection.aggregate(Arrays.asList(
                         Aggregates.group("$category", Accumulators.sum("count", 1))))
          .collation(collation);

```

The following operations support collation by specifying the
`Collation` object as an option using the corresponding option class
for the method:

- `MongoCollection.count()`
- `MongoCollection.deleteOne()`
- `MongoCollection.deleteMany()`
- `MongoCollection.findOneAndDelete()` 
- `MongoCollection.findOneAndReplace()`
- `MongoCollection.findOneAndUpdate()`
- `MongoCollection.updateOne()`
- `MongoCollection.updateMany()`
- `MongoCollection.bulkWrite()` for:

   - `DeleteManyModel` and `DeleteOneModel` using `DeleteOptions` to specify the collation
   - `ReplaceOneModel`, `UpdateManyModel`, and `UpdateOneModel` using `UpdateOptions` to specify the collation.

For example:

```java
Collation collation = Collation.builder()
                                 .collationStrength(CollationStrength.SECONDARY)
                                 .locale("fr").build();

collection.count(Filters.eq("category", "cafe"), new CountOptions().collation(collation));

collection.updateOne(Filters.eq("category", "cafe"), set("stars", 1), 
                     new UpdateOptions().collation(collation));

collection.bulkWrite(Arrays.asList(
                new UpdateOneModel<>(new Document("category", "cafe"),
                                     new Document("$set", new Document("x", 0)), 
                                     new UpdateOptions().collation(collation)),
                new DeleteOneModel<>(new Document("category", "cafe"), 
                                     new DeleteOptions().collation(collation))));
```
  

For more information on collation, including the supported locales, refer to the
[manual]({{<docsref "release-notes/3.3-dev-series-collation/">}}).

### Other MongoDB 3.4 features

* Support for specification of
[maximum staleness for secondary reads](https://github.com/mongodb/specifications/blob/master/source/max-staleness/max-staleness.rst)
* Support for the
[MongoDB handshake protocol](https://github.com/mongodb/specifications/blob/master/source/mongodb-handshake/handshake.rst).
* Builders for [eight new aggregation pipeline stages]({{<docsref "release-notes/3.4/#aggregation">}})
* Helpers for creating [read-only views]({{<docsref "release-notes/3.4/#views">}})
* Support for the [linearizable read concern](https://docs.mongodb.com/master/release-notes/3.4/#linearizable-read-concern)

### Support for JNDI

The synchronous driver now includes a [JNDI]({{<ref "driver/tutorials/jndi.md">}}) ObjectFactory implementation.


## Upgrading

See the [upgrading guide]({{<ref "upgrading.md">}}) on how to upgrade to 3.4.
