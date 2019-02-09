+++
date = "2015-03-19T12:53:26-04:00"
title = "SSL"
[menu.main]
  parent = "Async Connection Settings"
  identifier = "Async SSL"
  weight = 10
  pre = "<i class='fa'></i>"
+++

## TLS/SSL

The asynchronous Java driver supports TLS/SSL connections to MongoDB servers using the underlying support 
for TLS/SSL provided by the JDK.

## Specify TLS/SSL and Netty Configuration

{{% note %}}
If your application requires Netty, it must explicitly add a dependency to
Netty artifacts.  The driver is currently tested against Netty 4.1.
{{% /note %}}

### Via Connection String

To configure the driver to use TLS/SSL, include the `ssl=true` option in the connection string, as in:

```java
MongoClient client = MongoClients.create("mongodb://localhost/?ssl=true");
```

To configure the driver to use use TLS/SSL with Netty, include the `ssl=true` and `streamType=netty` options in the connection string, as
in:

```java
MongoClient client = MongoClients.create("mongodb://localhost/?streamType=netty&ssl=true");
```

{{% note %}}
The streamType connection string query parameter is deprecated as of the 3.10 release and will be removed in the next major release.
{{% /note %}}

### Via `MongoClientSettings`

To specify TLS/SSL with [`MongoClientSettings`]({{< apiref "com/mongodb/MongoClientSettings.Builder.html#streamFactoryFactory(com.mongodb.connection.StreamFactoryFactory)">}}),
set the ``sslEnabled`` property to ``true`, as in

```java
MongoClient client = MongoClients.create(MongoClientSettings.builder()
        .applyToClusterSettings(builder -> builder.hosts(Arrays.asList(new ServerAddress())))
        .applyToSslSettings(builder -> builder.enabled(true))
        .build());
```

To specify TLS/SSL using Netty, set the ``sslEnabled`` property to ``true``, and the stream factory to 
[`NettyStreamFactoryFactory`]({{< apiref "com/mongodb/connection/netty/NettyStreamFactoryFactory" >}}), as in

```java
EventLoopGroup eventLoopGroup = new NioEventLoopGroup();  // make sure application shuts this down

MongoClient client = MongoClients.create(MongoClientSettings.builder()
        .applyToClusterSettings(builder -> builder.hosts(Arrays.asList(new ServerAddress())))
        .applyToSslSettings(builder -> builder.enabled(true))
        .streamFactoryFactory(NettyStreamFactoryFactory.builder()
                .eventLoopGroup(eventLoopGroup).build())
        .build());
```

By default, the Netty-based streams will use the [NioEventLoopGroup](http://netty.io/4.0/api/io/netty/channel/nio/NioEventLoopGroup.html)
and Netty's [default `ByteBufAllocator`](http://netty.io/4.0/api/io/netty/buffer/ByteBufAllocator.html#DEFAULT), but these are
configurable via the [`NettyStreamFactoryFactory`]({{< apiref "com/mongodb/connection/netty/NettyStreamFactoryFactory" >}}) constructor.   

To override the default [`javax.net.ssl.SSLContext`](https://docs.oracle.com/javase/8/docs/api/javax/net/ssl/SSLContext.html) used for SSL
connections, set the `sslContext` property on the `SslSettings`, as in:

```java
MongoClient client = MongoClients.create(MongoClientSettings.builder()
        .applyToClusterSettings(builder -> builder.hosts(Arrays.asList(new ServerAddress())))
        .applyToSslSettings(builder ->
                builder.enabled(true)
                       .context(sslContext))
        .streamFactoryFactory(NettyStreamFactoryFactory.builder()
                .eventLoopGroup(eventLoopGroup).build())
        .build());
```


## Disable Hostname Verification


By default, the driver ensures that the hostname included in the
server's SSL certificate(s) matches the hostname(s) provided when
creating a `MongoClient`.

If your application needs to disable host name verification, you must explicitly indicate this using the `invalidHostNameAllowed` property:

```java
MongoClient client = MongoClients.create(MongoClientSettings.builder()
        .applyToClusterSettings(builder -> builder.hosts(Arrays.asList(new ServerAddress())))
        .applyToSslSettings(builder -> 
                builder.enabled(true)
                       .invalidHostNameAllowed(true))
        .build());
```

Or via the connection string:

```java
MongoClient client = MongoClients.create("mongodb://localhost/?ssl=true&sslInvalidHostNameAllowed=true");
```

{{% note %}}
The streamType connection string query parameter is deprecated as of the 3.10 release and will be removed in the next major release.
{{% /note %}}

## JVM System Properties for TLS/SSL

A typical application will need to set several JVM system properties to
ensure that the client is able to validate the TLS/SSL certificate
presented by the server:

-  `javax.net.ssl.trustStore`:
      The path to a trust store containing the certificate of the
      signing authority

-  `javax.net.ssl.trustStorePassword`:
      The password to access this trust store

The trust store is typically created with the
[`keytool`](http://docs.oracle.com/javase/8/docs/technotes/tools/unix/keytool.html)
command line program provided as part of the JDK. For example:

```bash
    keytool -importcert -trustcacerts -file <path to certificate authority file>
        -keystore <path to trust store> -storepass <password>
```

A typical application will also need to set several JVM system
properties to ensure that the client presents an TLS/SSL certificate to the
MongoDB server:

- `javax.net.ssl.keyStore`
      The path to a key store containing the client's TLS/SSL certificates

- `javax.net.ssl.keyStorePassword`
      The password to access this key store

The key store is typically created with the
[`keytool`](http://docs.oracle.com/javase/8/docs/technotes/tools/unix/keytool.html)
or the [`openssl`](https://www.openssl.org/docs/apps/openssl.html)
command line program.

For more information on configuring a Java application for TLS/SSL, please
refer to the [`JSSE Reference Guide`](http://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/JSS
ERefGuide.html).
