+++
date = "2015-03-19T12:53:26-04:00"
title = "SSL"
[menu.main]
  parent = "Connecting"
  identifier = "SSL"
  weight = 10
  pre = "<i class='fa'></i>"
+++

## SSL

The Scala driver supports SSL connections to MongoDB servers using the underlying support for SSL provided by
[Netty](http://netty .io/). You can configure the driver to use SSL with `MongoClientSettings` by setting the sslEnabled property to true 
and the stream factory to [`NettyStreamFactoryFactory`]({{< apiref "org.mongodb.connection.NettyStreamFactoryFactory" >}}), as in:

```scala
import org.mongodb.scala.connection.NettyStreamFactoryFactory

MongoClientSettings.builder()
                   .streamFactoryFactory(NettyStreamFactoryFactory())
                   .applyToSslSettings(b => b.enabled(true))
                   .build()
```

See [Netty Configuration]({{< relref "reference/connecting/connection-settings.md#netty-configuration" >}}) for details on 
configuring Netty.

### Host name verification

By default, the driver ensures that the host name included in the server's SSL certificate(s) matches the host name(s) provided when 
constructing a `MongoClient`.  However, this host name verification requires a Java 7 JVM, as it relies on additions to the 
`javax.net.SSLParameters` class that were introduced in Java 7.  If your application must run on Java 6, or for some other reason you need
 to disable host name verification, you must expicitly indicate this in `SslSettings` using the `invalidHostNameAllowed` property:
   
```scala
MongoClientSettings.builder()
                   .streamFactoryFactory(NettyStreamFactoryFactory())
                   .applyToSslSettings(b => b.enabled(true).invalidHostNameAllowed(true))
                   .build()
``` 

### JVM system properties

A typical application will need to set several JVM system properties to ensure that the client is able to validate the SSL certificate 
presented by the server:

- `javax.net.ssl.trustStore`: the path to a trust store containing the certificate of the signing authority
- `javax.net.ssl.trustStorePassword`: the password to access this trust store 

The trust store is typically created with the [keytool](http://docs.oracle.com/javase/8/docs/technotes/tools/unix/keytool.html) 
command line program provided as part of the JDK.  For example:

```bash
    keytool -importcert -trustcacerts -file <path to certificate authority file> 
        -keystore <path to trust store> -storepass <password>
```

A typical application will also need to set several JVM system properties to ensure that the client presents an SSL certificate to the 
MongoDB server:

- `javax.net.ssl.keyStore`: the path to a key store containing the client's SSL certificates
- `javax.net.ssl.keyStorePassword`: the password to access this key store
 
The key store is typically created with the [keytool](http://docs.oracle.com/javase/8/docs/technotes/tools/unix/keytool.html) or the
[openssl](https://www.openssl.org/docs/apps/openssl.html) command line program.

For more information on configuring a Java application for SSL, please refer to the  
[JSSE Reference Guide](http://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/JSSERefGuide.html).


## JVM Support for TLS v1.1 and newer

Industry best practices recommend, and some regulations require, the use of TLS 1.1 or newer. Though no application changes are required
for the driver to make use of the newest TLS protocols, Java runtime environments prior to Java 8 started to enable TLS 1.1 only in later
updates:

* Java 7
  - Starting with
    [Update 131](http://www.oracle.com/technetwork/java/javaseproducts/documentation/javase7supportreleasenotes-1601161.html#R170_131),
    released October 8, 2016, TSL 1.1 and TLS 1.2 are enabled by default.
  - Starting with
    [Update 95](http://www.oracle.com/technetwork/java/javaseproducts/documentation/javase7supportreleasenotes-1601161.html#R170_95),
    released January 19, 2016, TLS 1.1 and TLS 1.2 can be enabled by applications via the `jdk.tls.client.protocols` system property.

* Java 6
  - Starting with
    [Update 141](http://www.oracle.com/technetwork/java/javase/documentation/overview-156328.html#R160_141), released on January 17, 2017,
    TSL 1.1 and TLS 1.2 are enabled by default.
  - Starting with
    [Update 115 b32](http://www.oracle.com/technetwork/java/javase/documentation/overview-156328.html#6u115-b32), released July 19, 2016,
    TLS 1.1 and TLS 1.2 can be enabled by applications via the `jdk.tls.client.protocols` system property.

Note that these updates are only available from Oracle via its Java SE commercial support program.  Java 7 Update 131
is available via [OpenJDK](http://openjdk.java.net/install/).
