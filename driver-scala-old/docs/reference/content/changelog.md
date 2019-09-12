+++
date = "2015-11-18T09:56:14Z"
title = "Changelog"
[menu.main]
  weight = 90
  pre = "<i class='fa fa-cog'></i>"
+++

## Changelog

Changes between released versions

### 2.7.0
  * Updated MongoDB Driver Async to 3.11.0
  * Added support for Scala 2.13 [SCALA-506](https://jira.mongodb.org/browse/SCALA-506)
  * Updated DEFAULT_CODEC_REGISTRY order, as upstream BsonCodec captures all Bson instances and priority needs to be lower.
  * Support $merge pipeline stage in aggregations [SCALA-473](https://jira.mongodb.org/browse/SCALA-473)
  * Support $replaceWith pipeline stage in aggregations
  * Added the ability to specify a pipeline to an update command [SCALA-513](https://jira.mongodb.org/browse/SCALA-513)
  * Added client side encryption support [SCALA-468](https://jira.mongodb.org/browse/SCALA-468)
  * Added batchSize support for MongoIterable based Observables [SCALA-552](https://jira.mongodb.org/browse/SCALA-552)
  * Added support for sealed traits [SCALA-554](https://jira.mongodb.org/browse/SCALA-554)
  * Added caseclass support for Sets, Vectors and Streams [SCALA-346](https://jira.mongodb.org/browse/SCALA-346)
  * Improved support for BsonPropery annotations in ADTs [SCALA-485](https://jira.mongodb.org/browse/SCALA-485)
  * Deprecated BsonArray.apply(Iterable[BsonValue]) added BsonArray.fromIterable [SCALA-531](https://jira.mongodb.org/browse/SCALA-531)
  * Fix `UninitializedFieldError` in MacroCodecs under `-Xcheckinit` [SCALA-542](https://jira.mongodb.org/browse/SCALA-542)

### 2.6.0
  * Updated MongoDB Driver Async to 3.10.0
  * Support Skipping in GridFS [SCALA-477](https://jira.mongodb.org/browse/SCALA-477)
  * Support running commands as aggregation	[SCALA-481](https://jira.mongodb.org/browse/SCALA-481)
  * The driver now natively supports TLS/SSL without netty [JAVA-3100](https://jira.mongodb.org/browse/JAVA-3100)

### 2.5.0
  * Updated MongoDB Driver Async to 3.9.0
  * Fixed Filter and Zip observables requesting the correct number of results [SCALA-457](https://jira.mongodb.org/browse/SCALA-457)
  * Updated the Aggregates.lookup helper to match the Java API [SCALA-446](https://jira.mongodb.org/browse/SCALA-446)
  * Added support for the available read concern [SCALA-440](https://jira.mongodb.org/browse/SCALA-440)

### 2.4.2
  * Updated MongoDB Driver Async to 3.8.2 [SCALA-449](https://jira.mongodb.org/browse/SCALA-449)
  * Allow value classes with codecs to be supported by the macro codecs [SCALA-447](https://jira.mongodb.org/browse/SCALA-447)

### 2.4.1
  * Updated MongoDB Driver Async to 3.8.1 [SCALA-441](https://jira.mongodb.org/browse/SCALA-441)
  * Map ObservableImplicits.head execution context to the executing thread [SCALA-430](https://jira.mongodb.org/browse/SCALA-430)
  * Support tagged types in codec generator macro for case classes [SCALA-414](https://jira.mongodb.org/browse/SCALA-414)
  * Fix MongoCollection.createIndexes signature [SCALA-431](https://jira.mongodb.org/browse/SCALA-431)

### 2.4.0
  * Updated MongoDB Driver Async to 3.8.0.
  * Added type aliases for builders, makes imports simpler when using MongoClientSettings [SCALA-421](https://jira.mongodb.org/browse/SCALA-421)
  * Deprecated `MongoCollection.count` and added `MongoCollection.countDocuments` and `MongoCollection.estimatedDocumentCount` [SCALA-422](https://jira.mongodb.org/browse/SCALA-422)
  * Added method to disable computing MD5 checksums when uploading files [SCALA-373](https://jira.mongodb.org/browse/SCALA-373)
  * Added cluster and database wide change stream support [SCALA-405](https://jira.mongodb.org/browse/SCALA-405)
  * Added transaction support. [SCALA-388](https://jira.mongodb.org/browse/SCALA-388)
  * Added `MongoCredential.createScramSha256Credential`. [SCALA-375](https://jira.mongodb.org/browse/SCALA-375)
  * Updated CaseClassCodec error catching for unsupported types. [SCALA-343](https://jira.mongodb.org/browse/SCALA-343)
  * Fixed `ExecutionContextObservable` race condition regarding ordering of Observer calls. [SCALA-405](https://jira.mongodb.org/browse/SCALA-405]
  * `FindObservable.maxScan` deprecated. [SCALA-385](https://jira.mongodb.org/browse/SCALA-385)
  * `FindObservable.snapshot` deprecated. [SCALA-386](https://jira.mongodb.org/browse/SCALA-386)
  * `MongoCredential.createMongoCRCredential` deprecated. [SCALA-371](https://jira.mongodb.org/browse/SCALA-371)

### 2.3.0

  * Updated MongoDB Driver Async to 3.7.0. [SCALA-398](https://jira.mongodb.org/browse/SCALA-398)
  * Updated MongoClientSettings to use the new central `com.mongodb.MongoClientSettings`. [SCALA-394](https://jira.mongodb.org/browse/SCALA-394)
  * Added Aggregates.Variable $lookup helper. [SCALA-399](https://jira.mongodb.org/browse/SCALA-399)
  * Added ReplaceOptions. [SCALA-360](https://jira.mongodb.org/browse/SCALA-360)

### 2.2.1
  * Updated MongoDB Driver Async to 3.6.3, fixes implicit session leak. [SCALA-378](https://jira.mongodb.org/browse/SCALA-378)

### 2.2.0

  * Updated MongoDB Driver Async to 3.6.0
  * MongoDB 3.6 support [SCALA-336](https://jira.mongodb.org/browse/SCALA-336)
    See the [what's new in 3.6 guide](http://mongodb.github.io/mongo-java-driver/3.6/whats-new/)
  * Fixed exception handling in Macro Codecs [SCALA-319](https://jira.mongodb.org/browse/SCALA-319)
  * Added implicit headOption method [SCALA-334](https://jira.mongodb.org/browse/SCALA-334)
  * Added BsonProperty annotation [SCALA-321](https://jira.mongodb.org/browse/SCALA-321)
  * Updated Mongodb Driver Async dependency to [3.5.0](https://jira.mongodb.org/browse/SCALA-335)
  * CaseClassCodec - Added support for internal vals. [SCALA-314](https://jira.mongodb.org/browse/SCALA-314)
  * CaseClassCodec - Added handling of extra values in the document. [SCALA-307](https://jira.mongodb.org/browse/SCALA-307) [SCALA-323](https://jira.mongodb.org/browse/SCALA-323)
  * Added support for custom Map implementations that don't include type information. [SCALA-311](https://jira.mongodb.org/browse/SCALA-311)

### 2.1.0

  * Added support for type aliases in the CaseClassCodec. [SCALA-305](https://jira.mongodb.org/browse/SCALA-305)
  * Added the ability to ignore `None` values when encoding `Option` fields. [SCALA-300](https://jira.mongodb.org/browse/SCALA-300)
  * Added the ability to handle missing values for `Option` fields. [SCALA-299](https://jira.mongodb.org/browse/SCALA-299)
  * Improved the CaseClassCodec handling of `null` values. [SCALA-301](https://jira.mongodb.org/browse/SCALA-301)

### 2.0.0

  * Added Case class support. [SCALA-168](https://jira.mongodb.org/browse/SCALA-168)
  * Added `observeOn(context: ExecutionContext)` so alternative execution contexts can be used with `Observables`. [SCALA-242](https://jira.mongodb.org/browse/SCALA-242)
  * Improved error message when actioning unsubscribed to Observables. [SCALA-248](https://jira.mongodb.org/browse/SCALA-248) 
  * Fixed FoldLeftObservable, ensuring that only one request for data is actioned and that all the data is requested. [SCALA-289](https://jira.mongodb.org/browse/SCALA-289)
  * Added SingleObservable trait and implicits for easy conversion and identification of Observables that return a single result. [SCALA-234](https://jira.mongodb.org/browse/SCALA-234)
  * MongoCollection methods now default to the collection type rather than Document. [SCALA-250](https://jira.mongodb.org/browse/SCALA-250)

### 1.2.1

  * Removed erroneous scala-reflect dependency. [SCALA-288](https://jira.mongodb.org/browse/SCALA-288) 

### 1.2.0

  * Added support for maxStaleness for secondary reads. [SCALA-251](https://jira.mongodb.org/browse/SCALA-251) [SCALA-280](https://jira.mongodb.org/browse/SCALA-280)
  * Added support for MONGODB-X509 auth without username. [SCALA-279](https://jira.mongodb.org/browse/SCALA-279)
  * Added support for library authors to extend the handshake metadata. [SCALA-252](https://jira.mongodb.org/browse/SCALA-252)
  * Added support for the new Aggregation stages in 3.4 [SCALA-258](https://jira.mongodb.org/browse/SCALA-258)
  * Added support for views [SCALA-255](https://jira.mongodb.org/browse/SCALA-255)
  * Added Collation support [SCALA-249](https://jira.mongodb.org/browse/SCALA-249)
  * Added support for BsonDecimal128 [SCALA-241](https://jira.mongodb.org/browse/SCALA-241)
  * Added support for ReadConcern.LINEARIZABLE [SCALA-247](https://jira.mongodb.org/browse/SCALA-247)
  * Fixed bug where some connection string options were not applied [SCALA-253](https://jira.mongodb.org/browse/SCALA-253)
  * Added GridFS Support [SCALA-154](https://jira.mongodb.org/browse/SCALA-154)

### 1.1.1
  * Updated Mongodb Driver Async dependency to [3.2.2](https://jira.mongodb.org/browse/SCALA-237)
  * Ensure Observables can be subscribed to multiple times [SCALA-239](https://jira.mongodb.org/browse/SCALA-239)

### 1.1

  * Updated to support MongoDB 3.2.
    * Added support for [Document Validation](https://docs.mongodb.org/manual/release-notes/3.2/#document-validation).
    * Added support for [ReadConcern](https://docs.mongodb.org/manual/release-notes/3.2/#readconcern).
    * Added support for [partialIndexes](https://docs.mongodb.org/manual/release-notes/3.2/#partial-indexes).
    * Added new helpers for [Aggregation](https://docs.mongodb.org/manual/release-notes/3.2/#aggregation-framework-enhancements).
    * Added new helpers for [bitwise filters](https://docs.mongodb.org/manual/release-notes/3.2/#bit-test-query-operators).
    * Added support for version 3 [text indexes](https://docs.mongodb.org/manual/release-notes/3.2/#text-search-enhancements).
  * Updated Mongodb Driver Async dependency to [3.2.0](https://jira.mongodb.org/browse/SCALA-222)

[Full issue list](https://jira.mongodb.org/issues/?jql=fixVersion%20%3D%201.1%20AND%20project%20%3D%20SCALA).

### 1.0.1

  * Fixed missing scala codec registry issue when using custom MongoSettings
  * Removed unnecessary scala dependency 

[Full issue list](https://jira.mongodb.org/issues/?jql=fixVersion%20%3D%201.0.1%20AND%20project%20%3D%20SCALA).

### 1.0 

  * Initial release

