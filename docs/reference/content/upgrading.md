+++
date = "2015-03-19T12:53:39-04:00"
title = "Upgrade Considerations"
[menu.main]
  identifier = "Upgrading"
  weight = 80
  pre = "<i class='fa fa-level-up'></i>"
+++

# Upgrading to the 4.3 Driver

The 4.3 release is a minor release as per the definition of [semantic versioning](https://semver.org). As such, there are no breaking
changes in this release for users upgrading from the 4.2 release.

# Upgrading to the 4.2 Driver

The 4.2 release is a minor release as per the definition of [semantic versioning](https://semver.org). As such, there are no breaking
changes in this release for users upgrading from the 4.1 release.

Note that in this release the Reactive Streams driver has taken an implementation dependency on 
[Project Reactor](https://projectreactor.io/).  The minimum supported Project Reactor version is *Californium-SR23*.

# Upgrading to the 4.1 Driver

The 4.1 release is a minor release as per the definition of [semantic versioning](https://semver.org). As such, there are no breaking
changes in this release for users upgrading from the 4.0 release.  Please see the upgrading guide for the 4.0 driver (just below) if you
plan to upgrade directly from a 3.x release of the driver. 

# Upgrading to the 4.0 Driver

## Upgrading from the 3.12 Java driver

The 4.0 release is a major release as per the definition of [semantic versioning](https://semver.org). As such, users
that upgrade to this release should expect breaking changes. That said, we have attempted to ensure that the upgrade 
process is as seamless as possible.  Breaking changes are as follows:

  * Numerous classes and methods have been removed from the driver. All of these API elements are annotated as deprecated in the 
    3.12 release, so if you compile your application with 3.12 and enable deprecation warnings in the compiler, you will be
    able to locate all uses of these API elements and follow the recommendations contained in the Javadoc for each API element
    to remove usage from your application. Most of these API elements are unlikely to be used by normal applications, but one bears
    mentioning explicitly: the entire callback-based asynchronous driver has been removed. Applications relying on this driver must either
    port their application to the Reactive Streams driver, or else must remain on a 3.x driver release.
  * While the `com.mongodb.MongoClient#getDB` method is deprecated in 3.x drivers, it has not been removed in this release. It will remain 
    at least until the next major release cycle.
  * The insert helper methods now return an insert result object instead of void.
  * The various `toJson` methods on `BsonDocument`, `Document`, and `DBObject` now return "relaxed" JSON instead of "strict" JSON.  This
    creates more readable JSON documents at the cost of a loss of some BSON type information (e.g., differentiating between 32 and 64 bit
    integers).
  * The default BSON representation of `java.util.UUID` values has changed from `JAVA_LEGACY` to `UNSPECIFIED`.  Applications that
    store or retrieve UUID values must explicitly specify which representation to use, via the `uuidRepresentation` property of
    `MongoClientSettings`.
  * The connection pool no longer enforces any restrictions on the size of the wait queue of threads or asynchronous tasks that
    require a connection to MongoDB.  It is up to the application to throttle requests sufficiently rather than rely on the driver to
    throw a `MongoWaitQueueFullException`.
  * The driver no longer logs via JUL (`java.util.logging`).  The only supported logging framework is SLF4J.
  * The embedded and Android drivers have been removed.  Applications relying on these drivers must remain on a 3.x driver release.
  * The `mongo-java-driver` and `mongodb-driver` "uber-jars" are no longer published.  Applications that reference either of these artifacts
    must switch to either `mongodb-driver-sync` or `mongodb-driver-legacy`, depending on which API is being used. Care should be taken to
    ensure that neither of the uber-jars are included via a transitive dependency, as that could introduce versioning conflicts.
  * Java 8 is now the minimum supported version. Applications using older versions of Java must remain on a 3.x driver release.
  * Several binary compatibility breaks were introduced (in particular, the change to the signature of the insert helper methods), so any
    classes that link to the driver must be recompiled in order to work with this release.
        
## Upgrading from the 1.12 Reactive Streams driver

The main change to the MongoDB Reactive Streams Java Driver 1.12 driver is the removal of the `Success` type.

Breaking changes are as follows:

  * `Publisher<Success>` has been migrated to `Publisher<Void>`. 
    Please note that `onNext` will not be called just `onComplete` if the operation is successful or `onError` if there is an error.
  * Removal of deprecated methods

## Upgrading from the 2.8 Scala driver

As the mongodb-driver-async package was deprecated in 3.x. The 4.0 version of the MongoDB Scala Driver is now built upon the
mongo-java-driver-reactivestreams 4.0 driver. One major benefit is now the Scala driver is also a reactive streams driver.

Breaking changes are as follows:

  * `Observable` is now a reactive streams `Publisher` implementations
    `Observable` implicits extend any `Publisher` implementation and can be imported from `org.mongodb.scala._`
  * Completed type has now been removed. `Observable[Completed]` has been migrated to `Observable[Void]`. 
    Please note that `onNext` will not be called just `onComplete` if the operation is successful or `onError` if there is an error.
  * Removal of deprecated methods


# System Requirements

The minimum JVM is Java 8.

# Compatibility

The following table specifies the compatibility of the MongoDB Java driver for use with a specific version of MongoDB.

|Java Driver Version|MongoDB 3.0 |MongoDB 3.2|MongoDB 3.4|MongoDB 3.6|MongoDB 4.0|MongoDB 4.2|MongoDB 4.4|
|-------------------|------------|-----------|-----------|-----------|-----------|-----------|-----------|
|Version 4.3        |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |
|Version 4.2        |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |
|Version 4.1        |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |
|Version 4.0        |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |     |
|Version 3.12       |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓* |
|Version 3.11       |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |     |
|Version 3.10       |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |     |     |
|Version 3.9        |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |     |     |
|Version 3.9        |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |     |     |
|Version 3.8        |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |     |     |
|Version 3.7        |  ✓  |  ✓  |  ✓  |  ✓  |     |     |     |
|Version 3.6        |  ✓  |  ✓  |  ✓  |  ✓  |     |     |     |
|Version 3.5        |  ✓  |  ✓  |  ✓  |     |     |     |     |
|Version 3.4        |  ✓  |  ✓  |  ✓  |     |     |     |     |
|Version 3.3        |  ✓  |  ✓  |     |     |     |     |     |
|Version 3.2        |  ✓  |  ✓  |     |     |     |     |     |
|Version 3.1        |  ✓  |     |     |     |     |     |     |
|Version 3.0        |  ✓  |     |     |     |     |     |     |

\* The 3.12 driver is tested against MongoDB 4.4 but does not support all the new features
