+++
date = "2016-05-29T22:05:03-04:00"
title = "TLS/SSL"
[menu.main]
  parent = "Reactive Connect to MongoDB"
  identifier = "Reactive SSL"
  weight = 10
  pre = "<i class='fa'></i>"
+++

## TLS/SSL

The Java driver supports TLS/SSL connections to MongoDB servers using
the underlying support for TLS/SSL provided by the JDK. 
You can configure the driver to use TLS/SSL either with [`ConnectionString`]({{< apiref "mongodb-driver-core" "com/mongodb/ConnectionString" >}}) or with
[`MongoClientSettings`]({{< apiref "mongodb-driver-core" "com/mongodb/MongoClientSettings" >}}).

## MongoClient API (since 3.7)

### Specify TLS/SSL via `ConnectionString`

```java
com.mongodb.reactivestreams.client.MongoClients;
com.mongodb.reactivestreams.client.MongoClient;
```

To specify TLS/SSL with [`ConnectionString`]({{< apiref "mongodb-driver-core" "com/mongodb/ConnectionString" >}}), specify `ssl=true` as part of the connection
string, as in:

```java
MongoClient mongoClient = MongoClients.create("mongodb://localhost/?ssl=true");
```

### Specify TLS/SSL via `MongoClientSettings`

```java
import com.mongodb.MongoClientSettings;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoClient;
```

To specify TLS/SSL with with [`MongoClientSettings`]({{< apiref "mongodb-driver-core" "com/mongodb/MongoClientSettings" >}}), set the `enabled` property to 
`true`, as in:

```java
MongoClientSettings settings = MongoClientSettings.builder()
        .applyToSslSettings(builder -> builder.enabled(true))
        .build();
MongoClient client = MongoClients.create(settings);
```

### Specify `SSLContext` via `MongoClientSettings`

```java
import javax.net.ssl.SSLContext;
import com.mongodb.MongoClientSettings;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoClient;
```

To specify the [`javax.net.ssl.SSLContext`](https://docs.oracle.com/javase/8/docs/api/javax/net/ssl/SSLContext.html) with 
[`MongoClientSettings`]({{< apiref "mongodb-driver-core" "com/mongodb/MongoClientSettings" >}}), set the `sslContext` property, as in:

```java
SSLContext sslContext = ...
MongoClientSettings settings = MongoClientSettings.builder()
        .applyToSslSettings(builder -> {
                    builder.enabled(true);
                    builder.context(sslContext);
                })
        .build();
MongoClient client = MongoClients.create(settings);
```

## Disable Hostname Verification

By default, the driver ensures that the hostname included in the
server's SSL certificate(s) matches the hostname(s) provided when
constructing a [`MongoClient()`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoClient.html" >}}).

If your application needs to disable hostname verification, you must explicitly indicate
this in `MongoClientSettings`]({{< apiref "mongodb-driver-core" "com/mongodb/MongoClientSettings" >}}) 

```java
MongoClientSettings settings = MongoClientSettings.builder()
        .applyToSslSettings(builder -> {
                    builder.enabled(true);
                    builder.invalidHostNameAllowed(true);
                })
        .build();
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
