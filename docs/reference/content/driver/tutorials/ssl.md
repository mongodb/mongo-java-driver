+++
date = "2016-05-29T22:05:03-04:00"
title = "TLS/SSL"
[menu.main]
  parent = "Connect to MongoDB"
  identifier = "SSL"
  weight = 10
  pre = "<i class='fa'></i>"
+++

## TLS/SSL

The Java driver supports TLS/SSL connections to MongoDB servers using
the underlying support for TLS/SSL provided by the JDK. You can
configure the driver to use TLS/SSL either with [`MongoClientURI`]({{<apiref "com/mongodb/MongoClientURI">}}) or with
[`MongoClientOptions`]({{<apiref "com/mongodb/MongoClientOptions">}}).

## Specify TLS/SSL via `MongoClientURI`

```java
import com.mongodb.MongoClientURI;
```

To specify TLS/SSL with [`MongoClientURI`]({{<apiref "com/mongodb/MongoClientURI">}}), specify `ssl=true` as part of the connection
string, as in:

```java
MongoClientURI uri = new MongoClientURI("mongodb://localhost/?ssl=true");
MongoClient mongoClient = new MongoClient(uri);
```

## Specify TLS/SSL via `MongoClientOptions`

```java
import com.mongodb.MongoClientOptions;
```

To specify TLS/SSL with with [`MongoClientOptions`]({{<apiref "com/mongodb/MongoClientOptions">}}), set the `sslEnabled` property to `true`, as in:

```java
 MongoClientOptions options = MongoClientOptions.builder().sslEnabled(true).build();
 MongoClient client = new MongoClient("localhost", options);
```

## Specify `SSLContext` via `MongoClientOptions`

```java
import javax.net.ssl.SSLContext;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClient;
```

To specify the [`javax.net.ssl.SSLContext`](https://docs.oracle.com/javase/8/docs/api/javax/net/ssl/SSLContext.html) with 
[`MongoClientOptions`]({{<apiref "com/mongodb/MongoClientOptions">}}), set the `sslContext` property, as in:

```java
 SSLContext sslContext = ...
 MongoClientOptions options = MongoClientOptions.builder()
                                                .sslEnabled(true)
                                                .sslContext(sslContext)
                                                .build();
 MongoClient client = new MongoClient("localhost", options);
```

## Disable Hostname Verification

By default, the driver ensures that the hostname included in the
server's SSL certificate(s) matches the hostname(s) provided when
constructing a [`MongoClient()`]({{< apiref "com/mongodb/MongoClient.html">}}). However, the hostname verification
requires a Java 7 JVM, as it relies on additions introduced in Java 7
to the `javax.net.SSLParameters` class.

If your application must run on Java 6, or for some other reason you
need to disable hostname verification, you must explicitly indicate
this in [`MongoClientOptions`]({{<apiref "com/mongodb/MongoClientOptions">}}) using the `sslInvalidHostNameAllowed`
property:

```java
MongoClientOptions.builder().sslEnabled(true).sslInvalidHostNameAllowed(true).build();
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
