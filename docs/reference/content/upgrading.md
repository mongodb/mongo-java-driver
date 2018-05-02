+++
date = "2015-03-19T12:53:39-04:00"
title = "Upgrade Considerations"
[menu.main]
  identifier = "Upgrading to 3.7"
  weight = 80
  pre = "<i class='fa fa-level-up'></i>"
+++

## Upgrading from 3.6.x

In order to modularize the driver for Java 9, the ```MongoDriverInformation`` class had to be moved from the ``com.mongodb.client`` package
to the ``com.mongodb`` package.  Generally the only consumers who rely on this class directly are other drivers that wrap the Java driver,
so it's not expected that this change will affect many applications, so long as they also upgrade any wrapping drivers they they rely on
(e .g. the Reactive Streams driver or the Scala driver) at the same time.

Otherwise, the 3.7 release is binary and source compatible with the 3.6 release, except for methods that have been added to interfaces that
have been marked as unstable, and changes to classes or interfaces that have been marked as internal or annotated as Beta.

## Upgrading from 2.x

See the Upgrade guide in the 3.0 driver reference documentation for breaking changes in 3.0.

### System Requirements

The minimum JVM is now Java 6: however, specific features require Java 7:

- SSL support requires Java 7 in order to perform host name verification, which is enabled by default.  See
[SSL]({{< relref "driver/tutorials/ssl.md" >}}) for details on how to disable host name verification.
- The asynchronous API requires Java 7, as by default it relies on
[`AsynchronousSocketChannel`](http://docs.oracle.com/javase/7/docs/api/java/nio/channels/AsynchronousSocketChannel.html) for
its implementation.  See [Async]({{< ref "driver-async/index.md" >}}) for details on configuring the driver to use [Netty](http://netty.io/) instead.

## Compatibility

The following table specifies the compatibility of the MongoDB Java driver for use with a specific version of MongoDB.

|Java Driver Version|MongoDB 2.6|MongoDB 3.0 |MongoDB 3.2|MongoDB 3.4|MongoDB 3.6|
|-------------------|-----------|------------|-----------|-----------|-----------|
|Version 3.7        |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |
|Version 3.6        |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |
|Version 3.5        |  ✓  |  ✓  |  ✓  |  ✓  |     |
|Version 3.4        |  ✓  |  ✓  |  ✓  |  ✓  |     |
|Version 3.3        |  ✓  |  ✓  |  ✓  |     |     |
|Version 3.2        |  ✓  |  ✓  |  ✓  |     |     |
|Version 3.1        |  ✓  |  ✓  |     |     |     |
|Version 3.0        |  ✓  |  ✓  |     |     |     |

The following table specifies the compatibility of the MongoDB Java driver for use with a specific version of Java.

|Java Driver Version|Java 5 | Java 6 | Java 7 | Java 8 |
|-------------------|-------|--------|--------|--------|
|Version 3.7        |     | ✓ | ✓ | ✓ |
|Version 3.6        |     | ✓ | ✓ | ✓ |
|Version 3.5        |     | ✓ | ✓ | ✓ |
|Version 3.4        |     | ✓ | ✓ | ✓ |
|Version 3.3        |     | ✓ | ✓ | ✓ |
|Version 3.2        |     | ✓ | ✓ | ✓ |
|Version 3.1        |     | ✓ | ✓ | ✓ |
|Version 3.0        |     | ✓ | ✓ | ✓ |
