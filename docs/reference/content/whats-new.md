+++
date = "2016-06-09T12:47:43-04:00"
title = "What's New"
[menu.main]
  identifier = "Release notes"
  weight = 15
  pre = "<i class='fa fa-level-up'></i>"
+++

# What's new in 4.3

This release fully supports all MongoDB releases from versions 2.6 to 4.4. It also supports some features of the next release of MongoDB.

New features of the 4.3 Java driver release include:

* Added support for the MongoDB Versioned API.  See the 
  [`ServerApi`]({{< apiref "mongodb-driver-core" "com/mongodb/ServerApi.html" >}}) API documentation for details.
* Removed most restrictions on allowed characters in field names of documents being inserted or replaced.  This is a behavioral change 
  for any application that is relying on client-side enforcement of these restrictions. In particular: 
  * Restrictions on field names containing the "." character have been removed. This affects all insert and replace operations.
  * Restrictions on field names starting with the "$" character have been removed for all insert operations.
  * Restrictions in nested documents on field names starting with the "$" character have been removed for all replace operations.  
  * Restrictions in the top-level document on field names starting with the "$" character remain for all replace operations. This is 
    primarily to prevent accidental use of a replace operation when the intention was to use an update operation.
  * Note that unacknowledged writes using dollar-prefixed or dotted keys may be silently rejected by pre-5.0 servers, where some 
    restrictions on field names are still enforced in the server.
* Added support for setting
  [Netty](https://netty.io/) [`io.netty.handler.ssl.SslContext`]({{< nettyapiref "io/netty/handler/ssl/SslContext.html" >}}),
  which may be used as a convenient way to utilize [OpenSSL](https://www.openssl.org/) as an alternative
  to the TLS/SSL protocol implementation in a JDK.
* Added [builders]({{< apiref "mongodb-driver-core" "com/mongodb/client/model/Aggregates.html#setWindowFields(TExpression,org.bson.conversions.Bson,java.util.List)" >}})
  for the new [`$setWindowFields`](https://dochub.mongodb.org/core/window-functions-set-window-fields)
  pipeline stage of an aggregation pipeline.

# What's new in 4.2

This release fully supports all MongoDB releases from versions 2.6 to 4.4. New features of the 4.2 Java driver release include:

* The Reactive Streams driver now utilizes [Project Reactor](https://projectreactor.io/) internally. Project reactor follows 
  the Reactive Streams specification and has greatly simplified the implementation of the driver.
* Added support for Azure and GCP key stores to client-side field level encryption.
* Added support for caching Kerberos tickets so that they can be re-used for multiple authentication requests.
* Added support for constructing legacy `com.mongodb.MongoClient` instances with `MongoClientSettings` or `ConnectionString` as 
  the configuration, instead of `MongoClientOptions` or `MongoClientURI`.
* Added support for `explain` of `find` and `aggregate` commands.
* Added a `JsonObject` class to make encoding from and decoding to JSON more efficient by avoiding an intermediate `Map` representation.
* Added a `BsonRepresentation` annotation that allows `ObjectId` BSON values to be represented as `String`s in POJO classes. 
* Added support for the `BsonIgnore` annotation in Scala case classes.
* Added a `Filters.empty()` method.

# What's new in 4.1

This release fully supports all MongoDB releases from versions 2.6 to 4.4. Key new features of the 4.1 Java driver release include:

* Significant reduction in client-perceived failover times during planned maintenance events
* Update and delete operations support index hinting.
* The find operation supports allowDiskUse for sorts that require too much memory to execute in RAM.
* Authentication supports the MONGODB-AWS mechanism using Amazon Web Services (AWS) Identity and Access Management (IAM) credentials.
* Authentication requires fewer round trips to the server, resulting in faster connection setup.

# What's new in 4.0

This release adds no new features but, as a major release, contains breaking changes that may affect your application. Please consult the 
[Upgrading Guide]({{<ref "upgrading.md" >}}) for an enumeration of the breaking changes.

# What's new in 3.12

This release fully supports all MongoDB releases from versions 2.6 to 4.2. Key new features of the 3.12 Java driver release include:

### Security improvements

Client-side field level encryption is supported. Automatic encryption and decryption is available for users of
[MongoDB Enterprise Advanced](https://www.mongodb.com/products/mongodb-enterprise-advanced), while explicit encryption and decryption is
available for users of MongoDB Community.

See [Client-side Encryption]({{<ref "driver/tutorials/client-side-encryption.md" >}}) for further details.

### Improved interoperability when using the native UUID type

The driver now supports setting the BSON binary representation of `java.util.UUID` instances via a new `UuidRepresentation` property on
`MongoClientSettings`.  The default representation has not changed, but it will in the upcoming 4.0 major release of the driver. 
Applications that store UUID values in MongoDB can use this setting to easily control the representation in MongoDB without having to
register a `Codec<Uuid>` in the `CodecRegistry`.  

See [`MongoClientSettings.getUuidRepresentation`]({{< apiref "mongodb-driver-core" "com/mongodb/MongoClientSettings.html#getUuidRepresentation()" >}}) for details. 

## What's new in 3.11

This release fully supports all MongoDB releases from versions 2.6 to 4.2. Key new features of the 3.11 Java driver release include:

### Improved transactions support

* The transactions API supports MongoDB 4.2 distributed transactions for use with sharded clusters. Distributed transactions use the same
API as replica set transactions.
* The sessions API supports the
  [`ClientSession.withTransaction()`]({{< apiref "mongodb-driver-sync" "com/mongodb/client/ClientSession.html#withTransaction(com.mongodb.client.TransactionBody) " >}})
  method to conveniently run a transaction with automatic retries and at-most-once semantics.
* The transactions API supports the
 [`maxCommitTime`]({{< apiref "mongodb-driver-core" "com/mongodb/TransactionOptions.html#getMaxCommitTime(java.util.concurrent.TimeUnit)" >}}) option to control the
 maximum amount of time to wait for a transaction to commit.

### Reliability improvements

* Most read operations are by default
  [automatically retried](https://github.com/mongodb/specifications/blob/master/source/retryable-reads/retryable-reads.rst). Supported read
  operations that fail with a retryable error are retried automatically and transparently.
* [Retryable writes](https://docs.mongodb.com/manual/core/retryable-writes/) are now enabled by default. Supported write
  operations that fail with a retryable error are retried automatically and transparently, with at-most-once update semantics.
* DNS [SRV](https://en.wikipedia.org/wiki/SRV_record) records are periodically polled in order to update the mongos proxy list without
  having to change client configuration or even restart the client application. This feature is particularly useful when used with a sharded
  cluster on [MongoDB Atlas](https://www.mongodb.com/cloud/atlas), which dynamically updates SRV records whenever you resize your Atlas
  sharded cluster.
* Connections to the replica set primary are no longer closed after a step-down, allowing in progress read operations to complete.

### Security improvements

Client-side encryption is supported. Automatic encryption and decryption is available for users of
[MongoDB Enterprise Advanced](https://www.mongodb.com/products/mongodb-enterprise-advanced), while explicit encryption and decryption is
available for users of MongoDB Community.

See [Client-side Encryption]({{<ref "driver/tutorials/client-side-encryption.md" >}}) for further details.

### General improvements

* New [`aggregate`]({{< apiref "mongodb-driver-sync" "com/mongodb/client/MongoDatabase.html##aggregate(java.util.List)" >}}) helper methods support running
database-level aggregations.
* Aggregate helper methods now support the `$merge` pipeline stage, and
[`Aggregates.merge()`]({{< apiref "mongodb-driver-core" "com/mongodb/client/model/Aggregates.html#merge(java.lang.String)" >}}) builder methods support creation of
the new pipeline stage.
* [Zstandard](https://facebook.github.io/zstd/) for wire protocol compression is supported in addition to Snappy and Zlib.
* Change stream helpers now support the `startAfter` option.
* Index creation helpers now support wildcard indexes.

### Full list of changes

* [New Features](https://jira.mongodb.org/issues/?jql=project%20%3D%20JAVA%20AND%20issuetype%20%3D%20%22New%20Feature%22%20AND%20resolution%20%3D%20Fixed%20AND%20fixVersion%20%3D%204.0.0%20ORDER%20BY%20component%20DESC%2C%20key%20ASC)
* [Improvements](https://jira.mongodb.org/issues/?jql=project%20%3D%20JAVA%20AND%20issuetype%20%3D%20Improvement%20AND%20resolution%20%3D%20Fixed%20AND%20fixVersion%20%3D%204.0.0%20ORDER%20BY%20component%20DESC%2C%20key%20ASC)
* [Bug Fixes](https://jira.mongodb.org/issues/?jql=project%20%3D%20JAVA%20AND%20issuetype%20%3D%20Bug%20AND%20resolution%20%3D%20Fixed%20AND%20fixVersion%20%3D%204.0.0%20ORDER%20BY%20component%20DESC%2C%20key%20ASC)

## What's new in 3.10

Key new features of the 3.10 Java driver release:

### Native asynchronous TLS/SSL support

Previously, any use of our asynchronous drivers with TLS/SSl required the use of Netty.  In 3.10, the driver supports asynchronous
TLS/SSL natively, so no third-party dependency is required. To accomplish this, the driver contains code vendored from
[https://github.com/marianobarrios/tls-channel](https://github.com/marianobarrios/tls-channel).

### Operational improvements

When connecting with the `mongodb+srv` protocol, the driver now polls DNS for changes to the SRV records when connected to a sharded 
cluster.  See 
[Polling SRV Records for Mongos Discovery](https://github.com/mongodb/specifications/blob/master/source/polling-srv-records-for-mongos-discovery/polling-srv-records-for-mongos-discovery.rst) 
for more details.

When retryable writes are enabled, the driver now outputs log messages at DEBUG level when a retryable write is attempted.  See
[JAVA-2964](https://jira.mongodb.org/browse/JAVA-2964) for more details.

### Codec Improvements

The driver now mutates the id property of a POJO, when inserting a document via a POJO with a `null` id property of type `ObjectId`. See
[JAVA-2674](https://jira.mongodb.org/browse/JAVA-2674) for more details.

### JNDI

The driver now offers a JNDI `ObjectFactory` implementation for creating instances of `com.mongodb.client.MongoClient`, the interface
introduced in version 3.7 of the driver. See [JAVA-2883](https://jira.mongodb.org/browse/JAVA-2883) for more details.

### Full list of changes

* [New Features](https://jira.mongodb.org/issues/?jql=project%20%3D%20JAVA%20AND%20issuetype%20%3D%20%22New%20Feature%22%20AND%20fixVersion%20%3D%203.10.0%20ORDER%20BY%20component%20DESC%2C%20key%20ASC)
* [Improvements](https://jira.mongodb.org/issues/?jql=project%20%3D%20JAVA%20AND%20issuetype%20%3D%20Improvement%20AND%20fixVersion%20%3D%203.10.0%20ORDER%20BY%20component%20DESC%2C%20key%20ASC)
* [Bug Fixes](https://jira.mongodb.org/issues/?jql=project%20%3D%20JAVA%20AND%20issuetype%20%3D%20Bug%20AND%20fixVersion%20%3D%203.10.0%20ORDER%20BY%20component%20DESC%2C%20key%20ASC)

## What's new in 3.9

Key new features of the 3.9 Java driver release:

### Android support

The `mongodb-driver-embedded-android` module supports interaction with a MongoDB server running on an Android device.
See [MongoDB Mobile](https://www.mongodb.com/products/mobile) for more details.

### Deprecations

Numerous classes and methods have been deprecated in the 3.9 release in preparation for a major 4.0 release.  See the 
[Upgrading Guide]({{<ref "upgrading.md" >}}) for more information.

## What's New in 3.8

Key new features of the 3.8 Java driver release:

### Transactions

The Java driver now provides support for executing CRUD operations within a transaction (requires MongoDB 4.0).  See the 
[Transactions and MongoDB Drivers](https://docs.mongodb.com/master/core/transactions/#transactions-and-mongodb-drivers) section
of the documentation and select the `Java (Sync)` tab.

### Change Stream enhancements

The Java driver now provides support for opening a change stream against an entire database, via new 
[`MongoDatabase.watch`]({{< apiref "mongodb-driver-sync" "com/mongodb/client/MongoDatabase.html" >}}) methods, or an 
entire deployment, via new [`MongoClient.watch`]({{< apiref "mongodb-driver-sync" "com/mongodb/client/MongoClient.html" >}}) methods. See 
[Change Streams]({{<ref "driver/tutorials/change-streams.md" >}}) for further details.

### SCRAM-256 Authentication Mechanism

The Java driver now provides support for the SCRAM-256 authentication mechanism (requires MongoDB 4.0).


## What's New in 3.7

Key new features of the 3.7 Java driver release:

### Java 9 support

#### Modules

The Java driver now provides a set of JAR files that are compliant with the Java 9 
[module specification](http://cr.openjdk.java.net/~mr/jigsaw/spec/), and `Automatic-Module-Name` declarations have been added 
to the manifests of those JAR files. See the [Installation Guide]({{<ref "driver/getting-started/installation.md" >}}) 
for information on which JAR files are now Java 9-compliant modules as well as what each of their module names is.  

Note that it was not possible to modularize all the existing JAR files due to the fact that, for some of them, packages are split amongst 
multiple JAR files, and this violates a core rule of the Java 9 module system which states that at most one module contains classes for any 
given package. For instance, the `mongodb-driver` and `mongodb-driver-core` JAR files both contain classes in the `com.mongodb` package, 
and thus it's not possible to make both `mongodb-driver` and `mongodb-driver-core` Java 9 modules. Also so-called 
"uber jars" like `mongo-java-driver` are not appropriate for Java 9 modularization, as they can conflict with their non-uber brethren, and 
thus have not been given module names. 

Note that none of the modular JAR files contain `module-info` class files yet.  Addition of these classes will be considered in a future 
release.

#### New Entry Point

So that the driver can offer a modular option, a new entry point has been added to the `com.mongodb.client` package. 
Static methods in this entry point, `com.mongodb.client.MongoClients`, returns instances of a new `com.mongodb.client.MongoClient` 
interface.  This interface, while similar to the existing `com.mongodb.MongoClient` class in that it is a factory for 
`com.mongodb.client.MongoDatabase` instances, does not support the legacy `com.mongodb.DBCollection`-based API, and thus does not suffer 
from the aforementioned package-splitting issue that prevents Java 9 modularization. This new entry point is encapsulated in the new 
`mongodb-driver-sync` JAR file, which is also a Java 9-compliant module.

The new entry point also moves the driver further in the direction of effective deprecation of the legacy API, which is now only available
only via the `mongo-java-driver` and `mongodb-driver` uber-jars, which are not Java 9 modules. At this point there are no plans to offer 
the legacy API as a Java 9 module.

See [Connect To MongoDB]({{<ref "driver/tutorials/connect-to-mongodb.md" >}}) for details on the new `com.mongodb.client.MongoClients`
and how it compares to the existing `com.mongodb.MongoClient` class.   

### Unix domain socket support

The 3.7 driver adds support for Unix domain sockets via the [`jnr.unixsocket`](http://https://github.com/jnr/jnr-unixsocket) library.
Connecting to Unix domain sockets is done via the [`ConnectionString`]({{< apiref "mongodb-driver-core" "com/mongodb/ConnectionString" >}}) or via
[`UnixServerAddress`]({{< apiref "mongodb-driver-core" "com/mongodb/UnixServerAddress.html" >}}).

### PojoCodec improvements

The 3.7 release brings support for `Map<String, Object>` to the POJO `Codec`.

### JSR-310 Instant, LocalDate & LocalDateTime support

Support for `Instant`, `LocalDate` and `LocalDateTime` has been added to the driver. The MongoDB Java drivers team would like to thank
[Cezary Bartosiak](https://github.com/cbartosiak) for their excellent contribution to the driver. Users needing alternative data structures
and / or more flexibility regarding JSR-310 dates should check out the alternative JSR-310 codecs provider by Cezary:
[bson-codecs-jsr310](https://github.com/cbartosiak/bson-codecs-jsr310).

### JSR-305 NonNull annotations

The public API is now annotated with JSR-305 compatible `@NonNull` and `@Nullable` annotations.  This will allow programmers
to rely on tools like FindBugs/SpotBugs, IDEs like IntelliJ IDEA, and compilers like the Kotlin compiler to find errors in the use of the 
driver via static analysis rather than via runtime failures.

### Improved logging of commands

When the log level is set to DEBUG for the `org.mongodb.driver.protocol.command` logger, the driver now logs additional information to aid
in debugging:

* Before sending the command, it logs the full command (up to 1000 characters), and the request id.
* After receive a response to the command, it logs the request id and elapsed time in milliseconds.

Here's an example

```
10:37:29.099 [cluster-ClusterId {value='5a466138741fc252712a6d71', description='null'}-127.0.0.1:27017] DEBUG org.mongodb.driver.protocol.command - 
Sending command '{ "hello" : 1, "$db" : "admin" } ...' with request id 4 to database admin on connection [connectionId{localValue:1, serverValue:1958}] to server 127.0.0.1:27017
10:37:29.104 [cluster-ClusterId{value='5a466138741fc252712a6d71', description='null'}-127.0.0.1:27017] DEBUG org.mongodb.driver.protocol.command - 
Execution of command with request id 4 completed successfully in 22.44 ms on connection [connectionId {localValue:1, serverValue:1958}] to server 127.0.0.1:27017
```
 
### Improved support for "raw" documents

When working with "raw" BSON for improved performance via the [`RawBsonDocument`]({{< apiref "bson" "org/bson/RawBsonDocument" >}}), the efficiency
of accessing embedded documents and arrays has been drastically improved by returning raw slices of the containing document or array.  For
instance

```java
RawBsonDocument doc = new RawBsonDocument(bytes);

// returns a RawBsonDocument that is a slice of the bytes from the containing doc
BsonDocument embeddedDoc = doc.getDocument("embeddedDoc");

// returns a RawBsonArray that is a slice of the bytes from the containing doc
BsonArray embeddedArray = doc.getArray("embeddedArray");

// returns a RawBsonDocument that is a slice of the bytes from the containing array 
BsonDocument embeddedDoc2 = (BsonDocument) embeddedArray.get(0); 
``` 

## What's New in 3.6

Key new features of the 3.6 Java driver release:

### Change Stream support

The 3.6 release adds support for [change streams](http://dochub.mongodb.org/core/changestreams).

* [Change Stream Quick Start]({{<ref "driver/tutorials/change-streams.md" >}}) 
* [Change Stream Quick Start (Async)]({{<ref "driver-reactive/tutorials/change-streams.md" >}})

### Retryable writes

The 3.6 release adds support for retryable writes using the `retryWrites` option in 
[`MongoClientOptions`]({{< apiref "mongodb-driver-legacy" "com/mongodb/MongoClientOptions" >}}).

### Compression

The 3.6 release adds support for compression of messages to and from appropriately configured MongoDB servers:

* [Compression Tutorial]({{<ref "driver/tutorials/compression.md" >}})
* [Compression Tutorial (Async)]({{<ref "driver-reactive/tutorials/compression.md" >}})

### Causal consistency
              
The 3.6 release adds support for [causally consistency](http://dochub.mongodb.org/core/causal-consistency) via the new
[`ClientSession`]({{< apiref "mongodb-driver-core" "com/mongodb/session/ClientSession" >}}) API. 

### Application-configured server selection

The 3.6 release adds support for application-configured control over server selection, using the `serverSelector` option in
[`MongoClientOptions`]({{< apiref "mongodb-driver-legacy" "com/mongodb/MongoClientOptions" >}}).

### POJO Codec improvements

The 3.6 release brings new improvements to the POJO `Codec`:

  * Improved sub-class and discriminator support.
  * Support for custom Collection and Map implementations.
  * Improvements to the `BsonCreator` annotation, which now supports `@BsonId` and `@BsonProperty` with values that represent the read name of the property.
  * A new [`PropertyCodecProvider`]({{< apiref "bson" "org/bson/codecs/pojo/PropertyCodecProvider" >}}) API, allowing for easy and type-safe handling of container types.
  * Added the [`SET_PRIVATE_FIELDS_CONVENTION`]({{< apiref "bson" "org/bson/codecs/pojo/Conventions.html#SET_PRIVATE_FIELDS_CONVENTION" >}}) convention.
  * Added the [`USE_GETTERS_FOR_SETTERS`]({{< apiref "bson" "org/bson/codecs/pojo/Conventions.html#USE_GETTERS_FOR_SETTERS" >}}) convention.

The MongoDB Java drivers team would like to thank both [Joseph Florencio](https://github.com/jflorencio) and [Qi Liu](https://github.com/visualage)
for their excellent contributions to the PojoCodec.

## What's New in 3.5

Key new features of the 3.5 Java driver release:

### Native POJO support

The 3.5 release adds support for [POJO](https://en.wikipedia.org/wiki/Plain_old_Java_object) serialization at the BSON layer, and can be
used by the synchronous and asynchronous drivers.  See the POJO Quick start pages for details.

* [POJO Quick Start]({{<ref "driver/getting-started/quick-start-pojo.md" >}}) 
* [POJO Quick Start (Async)]({{<ref "driver-reactive/getting-started/quick-start-pojo.md" >}})
* [POJO Reference]({{<ref "bson/pojos.md" >}}) 

### Improved JSON support

The 3.5 release improves support for JSON parsing and generation.

* Implements the new [Extended JSON specification](https://github.com/mongodb/specifications/blob/master/source/extended-json.rst)
* Implements custom JSON converters to give applications full control over JSON generation for each BSON type

See the [JSON reference]({{<ref "bson/extended-json.md" >}}) for details. 

### Connection pool monitoring

The 3.5 release adds support for monitoring connection pool-related events.

* [Connection pool monitoring in the driver]({{<ref "driver/reference/monitoring.md" >}})
* [Connection pool monitoring in the async driver]({{<ref "driver-reactive/reference/monitoring.md" >}})

### SSLContext configuration

The 3.5 release supports overriding the default `javax.net.ssl.SSLContext` used for SSL connections to MongoDB.

* [SSL configuration in the driver]({{<ref "driver/tutorials/ssl.md" >}})
* [SSL configuration in the async driver]({{<ref "driver-reactive/tutorials/ssl.md" >}})

### KeepAlive configuration deprecated

The 3.5 release deprecated socket keep-alive settings, also socket keep-alive checks are now on by default.
It is *strongly recommended* that system keep-alive settings should be configured with shorter timeouts. 

See the 
['does TCP keep-alive time affect MongoDB deployments?']({{<docsref "/faq/diagnostics/#does-tcp-keepalive-time-affect-mongodb-deployments" >}}) 
documentation for more information.


## What's New in 3.4

The 3.4 release includes full support for the MongoDB 3.4 server release.  Key new features include:

### Support for Decimal128 Format

``` java
import org.bson.types.Decimal128;
```

The [Decimal128]({{<docsref "release-notes/3.4/#decimal-type" >}}) format supports numbers with up to 34 decimal digits
(i.e. significant digits) and an exponent range of −6143 to +6144.

To create a `Decimal128` number, you can use

- [`Decimal128.parse()`] ({{< apiref "bson" "org/bson/types/Decimal128.html" >}}) with a string:

      ```java
      Decimal128.parse("9.9900");
      ```

- [`new Decimal128()`] ({{< apiref "bson" "org/bson/types/Decimal128.html" >}}) with a long:


      ```java
      new Decimal128(10L);
      ```

- [`new Decimal128()`] ({{< apiref "bson" "org/bson/types/Decimal128.html" >}}) with a `java.math.BigDecimal`:

      ```java
      new Decimal128(new BigDecimal("4.350000"));
      ```

### Support for Collation

```java
import com.mongodb.client.model.Collation;
```

[Collation]({{<docsref "reference/collation/" >}}) allows users to specify language-specific rules for string
comparison. 
Use the [`Collation.builder()`] ({{< apiref "mongodb-driver-core" "com/mongodb/client/model/Collation.html" >}}) 
to create the `Collation` object. For example, the following example creates a `Collation` object with Primary level of comparison and [locale]({{<docsref "reference/collation-locales-defaults/#supported-languages-and-locales" >}}) ``fr``.

```java
Collation.builder().collationStrength(CollationStrength.PRIMARY).locale("fr").build()));
```

You can specify collation at the collection level, at an index level, or at a collation-supported operation level:

#### Collection Level

To specify collation at the collection level, pass a `Collation` object as an option to the `createCollection()` method. To specify options to the `createCollection` method, use the [`CreateCollectionOptions`]({{< apiref "mongodb-driver-core" "com/mongodb/client/model/CreateCollectionOptions.html" >}}) class. 

```java
database.createCollection("myColl", new CreateCollectionOptions().collation(
                              Collation.builder()
                                    .collationStrength(CollationStrength.PRIMARY)
                                    .locale("fr").build()));
```

#### Index Level

To specify collation for an index, pass a `Collation` object as an option to the `createIndex()` method. To specify index options, use the [IndexOptions]({{< apiref "mongodb-driver-core" "com/mongodb/client/model/IndexOptions.html" >}}) class. 

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
[manual]({{<docsref "reference/collation/" >}}).

### Other MongoDB 3.4 features

* Support for specification of
[maximum staleness for secondary reads](https://github.com/mongodb/specifications/blob/master/source/max-staleness/max-staleness.rst)
* Support for the
[MongoDB handshake protocol](https://github.com/mongodb/specifications/blob/master/source/mongodb-handshake/handshake.rst).
* Builders for [eight new aggregation pipeline stages]({{<docsref "release-notes/3.4/#aggregation" >}})
* Helpers for creating [read-only views]({{<docsref "release-notes/3.4/#views" >}})
* Support for the [linearizable read concern](https://docs.mongodb.com/master/release-notes/3.4/#linearizable-read-concern)

### Support for JNDI

The synchronous driver now includes a [JNDI]({{<ref "driver/tutorials/jndi.md" >}}) ObjectFactory implementation.


## Upgrading

See the [upgrading guide]({{<ref "upgrading.md" >}}) on how to upgrade to 3.5.
