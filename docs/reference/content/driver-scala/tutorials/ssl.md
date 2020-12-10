+++
date = "2016-05-29T22:05:03-04:00"
title = "TLS/SSL"
[menu.main]
  parent = "Scala Connect to MongoDB"
  identifier = "Scala SSL"
  weight = 10
  pre = "<i class='fa'></i>"
+++

## TLS/SSL

The Java driver supports TLS/SSL connections to MongoDB servers using
the underlying support for TLS/SSL provided by the JDK. 
You can configure the driver to use TLS/SSL either with [`ConnectionString`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/package$$ConnectionString$.html" >}}) or with
[`MongoClientSettings`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoClientSettings$.html" >}}).

## MongoClient API (since 3.7)

### Specify TLS/SSL via `ConnectionString`

```scala
import org.mongodb.scala._
```

To specify TLS/SSL with [`ConnectionString`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/package$$ConnectionString$.html" >}}), specify `ssl=true` as part of the connection
string, as in:

```scala
val mongoClient: MongoClient = MongoClient("mongodb://localhost/?ssl=true")
```

### Specify TLS/SSL via `MongoClientSettings`

To specify TLS/SSL with with [`MongoClientSettings`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoClientSettings$.html" >}}), set the `enabled` property to 
`true`, as in:

```scala
val settings = MongoClientSettings.builder()
    .applyToSslSettings((builder: SslSettings.Builder) => builder.enabled(true))
    .build()
val client = MongoClients.create(settings)
```

### Specify `SSLContext` via `MongoClientSettings`

```scala
import javax.net.ssl.SSLContext
```

To specify the [`javax.net.ssl.SSLContext`](https://docs.oracle.com/javase/8/docs/api/javax/net/ssl/SSLContext.html) with 
[`MongoClientSettings`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoClientSettings$.html" >}}), set the `sslContext` property, as in:

```scala
val sslContext: SSLContext = ???
val settings = MongoClientSettings.builder()
    .applyToSslSettings((builder: SslSettings.Builder) => {
        builder.enabled(true)
        builder.context(sslContext)
    })
    .build()
val client = MongoClients.create(settings)
```

## Disable Hostname Verification

By default, the driver ensures that the hostname included in the
server's SSL certificate(s) matches the hostname(s) provided when
constructing a [`MongoClient()`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoClient$.html" >}}).

If your application needs to disable hostname verification, you must explicitly indicate
this in `MongoClientSettings`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoClientSettings$.html" >}}) 

```scala
val settings = MongoClientSettings.builder()
    .applyToSslSettings((builder: SslSettings.Builder) => {
        builder.enabled(true)
        builder.invalidHostNameAllowed(true)
    })
    .build()
```

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
