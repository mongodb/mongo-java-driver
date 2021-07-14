+++
date = "2015-03-19T12:53:39-04:00"
title = "Upgrade Considerations"
[menu.main]
  identifier = "Upgrading to 3.9"
  weight = 80
  pre = "<i class='fa fa-level-up'></i>"
+++

{{% note class="important" %}}
Please note that the 3.x series of releases (which includes support through MongoDB 4.2) will be the last releases that are compatible
with *Java 6 or Java 7*.  The 4.0 Java driver will require a minimum of Java 8. 

Please note that 3.12.x is tested against MongoDB 5.0, however support for all the new features is only available via a 4.x driver.
{{% /note %}}

## Upgrading from 3.11.x

In the upcoming 4.0 release, all deprecated API elements except those documented as "not currently scheduled for removal" will be removed.
Currently the only deprecated API elements _not_ scheduled for removal are:

* [`Mongo.getDB`]({{<apiref "com/mongodb/Mongo.html#getDB(java.lang.String)">}})
* [`JsonMode.STRICT`]({{<apiref "org/bson/json/JsonMode.html#STRICT">}})

To prepare for the 4.0 release, please compile with deprecation warnings enabled and replace all usage of deprecated API elements with their
recommended replacements.

The 3.12 release is binary and source compatible with the 3.11 release, except for methods that have been added to interfaces that
have been marked as unstable, and changes to classes or interfaces that have been marked as internal or annotated as Beta.

## Upgrading from 3.10.x

Please note that the 3.11 driver enables both retryable reads and retryable writes by default, so users upgrading to the 3.11 driver may
wish to adjust any custom retry logic in order to prevent applications from retrying for too long.

In the upcoming 4.0 release, all deprecated API elements except those documented as "not currently scheduled for removal" will be removed.
Currently the only deprecated API elements _not_ scheduled for removal are:

* [`Mongo.getDB`]({{<apiref "com/mongodb/Mongo.html#getDB(java.lang.String)">}})
* [`JsonMode.STRICT`]({{<apiref "org/bson/json/JsonMode.html#STRICT">}})

To prepare for the 4.0 release, please compile with deprecation warnings enabled and replace all usage of deprecated API elements with their
recommended replacements.

The 3.11 release is binary and source compatible with the 3.10 release, except for methods that have been added to interfaces that
have been marked as unstable, and changes to classes or interfaces that have been marked as internal or annotated as Beta.

## Upgrading from 3.9.x

Please note that two large portions of the current driver have been deprecated as of the 3.9.0 release:

* The callback-based asynchronous driver has been deprecated in favor of the 
[Reactive Streams Java Driver](http://mongodb.github.io/mongo-java-driver-reactivestreams/).
* Much of the "core" driver on which the both the asynchronous and synchronous drivers are built has been deprecated.  In 3.0 we made most
of this "core" driver a public API, but the benefit to users has turned out to be small and the maintenance costs of maintaining backwards 
compatibility has turned out to be high.  For that reason we've decided to remove it from the public API (though it will still continue 
to exist as an implementation artifact).

In the upcoming 4.0 release, all deprecated API elements except those documented as "not currently scheduled for removal" will be removed. 
Currently the only deprecated API elements _not_ scheduled for removal are:

* [`Mongo.getDB`]({{<apiref "com/mongodb/Mongo.html#getDB(java.lang.String)">}})
* [`JsonMode.STRICT`]({{<apiref "org/bson/json/JsonMode.html#STRICT">}}) 

To prepare for the 4.0 release, please compile with deprecation warnings enabled and replace all usage of deprecated API elements with their
recommended replacements.

Also, note that the 3.11 release (which will include support for MongoDB 4.2) will be the last release that is compatible with *Java 6
or Java 7*.  The 4.0 Java driver will require a minimum of Java 8. The 3.11 release will also be the last non-patch release in the 3.x 
line. In particular, support for MongoDB 4.4 will only be made available via a 4.x driver release.

The 3.10 release is binary and source compatible with the 3.9 release, except for methods that have been added to interfaces that
have been marked as unstable, and changes to classes or interfaces that have been marked as internal or annotated as Beta.

## Upgrading from 2.x

See the Upgrade guide in the 3.0 driver reference documentation for breaking changes in 3.0.

## System Requirements

The minimum JVM is Java 6. However, specific features require more recent versions:

- [Atlas M0 (Free Tier)](https://docs.atlas.mongodb.com/getting-started/) support requires Java 8 or greater due to 
its reliance on TLS Server Name Indication (SNI).
- SSL support requires Java 7 or greater in order to perform host name verification, which is enabled by default.  See
[SSL]({{< relref "driver/tutorials/ssl.md" >}}) for details on how to disable host name verification.
- The asynchronous API requires Java 7 when TLS/SSL is disabled, as by default it relies on
[`AsynchronousSocketChannel`](http://docs.oracle.com/javase/7/docs/api/java/nio/channels/AsynchronousSocketChannel.html) for
its implementation.  See [Async]({{< ref "driver-async/index.md" >}}) for details on configuring the driver to use 
[Netty](http://netty.io/) instead.
- The asynchronous API requires Java 8 when TLS/SSL is enabled. See [Async]({{< ref "driver-async/index.md" >}}) for details on configuring 
the driver to use [Netty](http://netty.io/) instead.

## Compatibility

The following table specifies the compatibility of the MongoDB Java driver for use with a specific version of MongoDB.

|Java Driver Version|MongoDB 3.0 |MongoDB 3.2|MongoDB 3.4|MongoDB 3.6|MongoDB 4.0|MongoDB 4.2|MongoDB 4.4|
|-------------------|------------|-----------|-----------|-----------|-----------|-----------|-----------|
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