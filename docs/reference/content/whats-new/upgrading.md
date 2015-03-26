+++
date = "2015-03-19T12:53:39-04:00"
title = "Upgrading to 3.0"
[menu.main]
  weight = 40
  parent = "What's New"
  pre = "<i class='fa fa-wrench'></i>"
+++

# Upgrading to 3.0

The 3.0 release is **incompatible** with the 2.x release series. However, where possible, the 2.13.0 driver has deprecated classes or 
methods that have been removed in 3.0.0.

Before upgrading to 3.0.0, compile against 2.13.0 with deprecation warnings enabled and remove use of any deprecated methods or classes.

## System Requirements

The minimum JVM is now Java 6: however, specific features require Java 7:

- SSL support requires Java 7 in order to perform host name verification, which is enabled by default.  See
[SSL]({{< relref "reference/connecting/ssl.md" >}}) for details on how to disable host name verification.
- The asynchronous API requires Java 7, as by default it relies on
[AsynchronousSocketChannel](http://docs.oracle.com/javase/7/docs/api/java/nio/channels/AsynchronousSocketChannel.html) for
its implementation.  See [Async]({{< ref "async" >}}) for details on configuring the driver to use [Netty](http://netty.io/) instead.

## Incompatiblities

The following lists the most significant backwards-breaking changes, along with the recommended solutions:

* SSL host name verification has been enabled by default: the driver now enables host name verification by default for SSL connections.  If
you are using an invalid certificate or are using Java 6 (with which the driver does not support host name verification), set the
[sslInvalidHostNameAllowed](http://api.mongodb.org/java/3.0/com/mongodb/MongoClientOptions.html#isSslInvalidHostNameAllowed--)
property to true.
* The [requestStart](https://api.mongodb.org/java/2.13/com/mongodb/DB.html#requestStart--) and
[requestDone](https://api.mongodb.org/java/2.13/com/mongodb/DB.html#requestDone--) methods in
[DB](https://api.mongodb.org/java/2.13/com/mongodb/DB.html) have been removed: These methods have been removed in accordance with the
[server selection specification](https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection.rst#what-happened-to-pinning).
* [ServerAddress](http://api.mongodb.org/java/2.13/com/mongodb/ServerAddress.html) and
[MongoClient](http://api.mongodb.org/java/2.13/com/mongodb/MongoClient.html) constructors no longer throw
[UnknownHostException](http://docs.oracle.com/javase/8/docs/api/java/net/UnknownHostException.html): This breaks source but not binary
compatibility, so re-compilation with 3.0 will only succeed after removing any reference to this exception in catch blocks or method
throws declarations.
* The [getLastError](http://api.mongodb.org/java/2.13/com/mongodb/WriteResult.html#getLastError--) method in
[WriteResult](http://api.mongodb.org/java/2.13/com/mongodb/WriteResult.html) has been removed: this method does not work reliably in
the 2.x series and there is no way to make work reliably, so it has been removed.
* The [autoConnectRetry](https://api.mongodb.org/java/2.13/com/mongodb/MongoClientOptions.html#isAutoConnectRetry--) and
[maxAutoConnectRetryTime](https://api.mongodb.org/java/2.13/com/mongodb/MongoClientOptions.html#getMaxAutoConnectRetryTime--) properties in
[MongoClientOptions](http://api.mongodb.org/java/2.13/com/mongodb/MongoClientOptions.html) have been removed: these options turned out to
 have little practical value, and as no other MongoDB-support driver provides these options, they have been removed from the Java driver. 
* The [authenticate](https://api.mongodb.org/java/2.13/com/mongodb/DB.html#authenticate-java.lang.String-char:A-) method in the
[DB](https://api.mongodb.org/java/2.13/com/mongodb/DB.html) class has been removed: replace with use of
[MongoClient constructors](http://api.mongodb.org/java/2.13/com/mongodb/MongoClient.html#MongoClient-java.util.List-java.util.List-) that
take [MongoCredential](https://api.mongodb.org/java/2.13/com/mongodb/MongoCredential.html) instances.
* The [ensureIndex](https://api.mongodb.org/java/2.13/com/mongodb/DBCollection.html#ensureIndex-com.mongodb.DBObject-) methods in
[DBCollection](https://api.mongodb.org/java/2.13/com/mongodb/DBCollection.html) have been removed:
replace with the corresponding
[createIndex](https://api.mongodb.org/java/2.13/com/mongodb/DBCollection.html#createIndex-com.mongodb.DBObject-) method.
* The [fetch](https://api.mongodb.org/java/2.13/com/mongodb/DBRefBase.html#fetch--) method in
[DBRef](https://api.mongodb.org/java/2.13/com/mongodb/DBRef.html) has been removed: use the
[findOne](https://api.mongodb.org/java/2.13/com/mongodb/DBCollection.html#findOne-java.lang.Object-) method instead.
