+++
date = "2016-06-09T12:47:43-04:00"
title = "What's New"
[menu.main]
  identifier = "Release notes"
  weight = 15
  pre = "<i class='fa fa-level-up'></i>"
+++

## What's New in 3.6

Key new features of the 3.6 Java driver release:

### Change Stream support

The 3.6 release adds support for [change streams](http://dochub.mongodb.org/core/changestreams).

* [Change Stream Quick Start]({{<ref "driver/tutorials/change-streams.md">}}) 
* [Change Stream Quick Start (Async)]({{<ref "driver-async/tutorials/change-streams.md">}})

### Retryable writes

The 3.6 release adds support for retryable writes using the `retryWrites` option in 
[`MongoClientOptions`]({{<apiref "com/mongodb/MongoClientOptions">}}).

### Compression

The 3.6 release adds support for compression of messages to and from appropriately configured MongoDB servers:

* [Compression Tutorial]({{<ref "driver/tutorials/compression.md">}})
* [Compression Tutorial (Async)]({{<ref "driver-async/tutorials/compression.md">}})

### Causal consistency
              
The 3.6 release adds support for [causally consistency](http://dochub.mongodb.org/core/causal-consistency) via the new
[`ClientSession`]({{<apiref "com/mongodb/session/ClientSession">}}) API. 

### Application-configured server selection

The 3.6 release adds support for application-configured control over server selection, using the `serverSelector` option in
[`MongoClientOptions`]({{<apiref "com/mongodb/MongoClientOptions">}}).

## What's New in 3.5

Key new features of the 3.5 Java driver release:

### Native POJO support

The 3.5 release adds support for [POJO](https://en.wikipedia.org/wiki/Plain_old_Java_object) serialization at the BSON layer, and can be
used by the synchronous and asynchronous drivers.  See the POJO Quick start pages for details.

* [POJO Quick Start]({{<ref "driver/getting-started/quick-start-pojo.md">}}) 
* [POJO Quick Start (Async)]({{<ref "driver-async/getting-started/quick-start-pojo.md">}})
* [POJO Reference]({{<ref "bson/pojos.md">}}) 

### Improved JSON support

The 3.5 release improves support for JSON parsing and generation.

* Implements the new [Extended JSON specification](https://github.com/mongodb/specifications/blob/master/source/extended-json.rst)
* Implements custom JSON converters to give applications full control over JSON generation for each BSON type

See the [JSON reference]({{<ref "bson/extended-json.md">}}) for details. 

### Connection pool monitoring

The 3.5 release adds support for monitoring connection pool-related events.

* [Connection pool monitoring in the driver]({{<ref "driver/reference/monitoring.md">}})
* [Connection pool monitoring in the async driver]({{<ref "driver-async/reference/monitoring.md">}})

### SSLContext configuration

The 3.5 release supports overriding the default `javax.net.ssl.SSLContext` used for SSL connections to MongoDB.

* [SSL configuration in the driver]({{<ref "driver/tutorials/ssl.md">}})
* [SSL configuration in the async driver]({{<ref "driver-async/tutorials/ssl.md">}})

### KeepAlive configuration deprecated

The 3.5 release deprecated socket keep-alive settings, also socket keep-alive checks are now on by default.
It is *strongly recommended* that system keep-alive settings should be configured with shorter timeouts. 

See the 
['does TCP keep-alive time affect MongoDB deployments?']({{<docsref "/faq/diagnostics/#does-tcp-keepalive-time-affect-mongodb-deployments">}}) 
documentation for more information.


## What's New in 3.4

The 3.4 release includes full support for the MongoDB 3.4 server release.  Key new features include:

### Support for Decimal128 Format

``` java
import org.bson.types.Decimal128;
```

The [Decimal128]({{<docsref "release-notes/3.4/#decimal-type">}}) format supports numbers with up to 34 decimal digits
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

[Collation]({{<docsref "reference/collation/">}}) allows users to specify language-specific rules for string
comparison. 
Use the [`Collation.builder()`] ({{<apiref "com/mongodb/client/model/Collation.html">}}) 
to create the `Collation` object. For example, the following example creates a `Collation` object with Primary level of comparison and [locale]({{<docsref "reference/collation-locales-defaults/#supported-languages-and-locales">}}) ``fr``.

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
[manual]({{<docsref "reference/collation/">}}).

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

See the [upgrading guide]({{<ref "upgrading.md">}}) on how to upgrade to 3.5.
