+++
date = "2015-03-19T12:53:39-04:00"
title = "Upgrading to 3.0"
[menu.main]
  parent = "Whats New"
  identifier = "Upgrading to 3.0"
  weight = 40
  pre = "<i class='fa fa-wrench'></i>"
+++

# Upgrading to 3.0

The 3.0 release is **incompatible** with the 2.x release series. However, where possible, the 2.13.0 driver has deprecated classes or 
methods that have been removed in 3.0.

Before upgrading to 3.0, compile against 2.13.0 with deprecation warnings enabled and remove use of any deprecated methods or classes.

## System Requirements

The minimum JVM is now Java 6: however, specific features require Java 7:

- SSL support requires Java 7 in order to perform host name verification, which is enabled by default.  See below and on
[SSL]({{< relref "driver/reference/connecting/ssl.md" >}}) for details on how to disable host name verification.
- The asynchronous API requires Java 7, as by default it relies on
[`AsynchronousSocketChannel`](http://docs.oracle.com/javase/7/docs/api/java/nio/channels/AsynchronousSocketChannel.html) for
its implementation.  See [Async]({{< ref "driver-async/index.md" >}}) for details on configuring the driver to use [Netty](http://netty.io/) instead.

## Incompatibilities

The following lists the most significant backwards-breaking changes, along with the recommended solutions.

### General

The driver now enables host name verification by default for SSL connections.  If you are knowingly using an invalid certificate, or are 
using Java 6 (see above), set the 
[`sslInvalidHostNameAllowed`](http://api.mongodb.org/java/3.0/com/mongodb/MongoClientOptions.html#isSslInvalidHostNameAllowed--)
property to true.

### MongoClientOptions

Providing little practical value, 
the [`autoConnectRetry`](https://api.mongodb.org/java/2.13/com/mongodb/MongoClientOptions .html#isAutoConnectRetry--) and
[`maxAutoConnectRetryTime`](https://api.mongodb.org/java/2.13/com/mongodb/MongoClientOptions.html#getMaxAutoConnectRetryTime--) 
properties in
[`MongoClientOptions`]({{< apiref "com/mongodb/MongoClientOptions" >}}) have been removed from the Java driver to be consistent with other 
MongoDB-supported drivers .

#### MongoClient

The [`MongoClient`]({{< apiref "com/mongodb/MongoClient" >}}) (and
[`ServerAddress`]({{< apiref "com/mongodb/ServerAddress" >}})) constructors no longer throw
[`UnknownHostException`](http://docs.oracle.com/javase/8/docs/api/java/net/UnknownHostException.html): This breaks source but not binary
compatibility, so re-compilation with 3.0 will only succeed after removing any reference to this exception in catch blocks or method
throws declarations.

### DB

The [`requestStart`](https://api.mongodb.org/java/2.13/com/mongodb/DB.html#requestStart--) and
[`requestDone`](https://api.mongodb.org/java/2.13/com/mongodb/DB.html#requestDone--) methods in
[`DB`]({{< apiref "com/mongodb/DB" >}}) have been removed: These methods have been removed in accordance with the
[server selection specification](https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection.rst#what-happened-to-pinning).

The [`authenticate`](https://api.mongodb.org/java/2.13/com/mongodb/DB.html#authenticate-java.lang.String-char:A-) method in
[`DB`]({{< apiref "com/mongodb/DB" >}}) has been replaced with
[`MongoClient`](http://api.mongodb.org/java/3.0/com/mongodb/MongoClient.html#MongoClient-java.util.List-java.util.List-) constructors that
take [`MongoCredential`]({{< apiref "com/mongodb/MongoCredential" >}}) instances.

### DBCollection

The [`ensureIndex`](https://api.mongodb.org/java/2.13/com/mongodb/DBCollection.html#ensureIndex-com.mongodb.DBObject-) methods in
[`DBCollection`]({{< apiref "com/mongodb/DBCollection" >}}) have been removed:
replace with the corresponding
[`createIndex`](https://api.mongodb.org/java/2.13/com/mongodb/DBCollection.html#createIndex-com.mongodb.DBObject-) method.

### WriteResult

The [`getLastError`](http://api.mongodb.org/java/2.13/com/mongodb/WriteResult.html#getLastError--) method in
[`WriteResult`]({{< apiref "com/mongodb/WriteResult" >}}) has been removed: this method does not work reliably in
the 2.x series and there is no way to make work reliably, so it has been removed.  Replace with use of an acknowledged 
[`WriteConcern`]({{< apiref "com/mongodb/WriteConcern" >}}) when executing the write operation.

### DBRef

The [`fetch`](https://api.mongodb.org/java/2.13/com/mongodb/DBRefBase.html#fetch--) method in
[`DBRef`]({{< apiref "com/mongodb/DBRef" >}}) has been removed: use the
[`findOne`](https://api.mongodb.org/java/3.0/com/mongodb/DBCollection.html#findOne-java.lang.Object-) method instead.
