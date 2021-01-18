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

To specify the [`javax.net.ssl.SSLContext`]({{< javaseref "api/javax/net/ssl/SSLContext.html" >}}) with 
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
this in [`MongoClientSettings`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoClientSettings$.html" >}}) 

```scala
val settings = MongoClientSettings.builder()
    .applyToSslSettings((builder: SslSettings.Builder) => {
        builder.enabled(true)
        builder.invalidHostNameAllowed(true)
    })
    .build()
```

## Common TLS/SSL Configuration Tasks
See [Java Driver Common TLS/SSL Configuration Tasks]({{< relref "driver/tutorials/ssl.md#common-tls-ssl-configuration-tasks" >}}).
