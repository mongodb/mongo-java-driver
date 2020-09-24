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
the underlying support for TLS/SSL provided by the JDK. 
You can configure the driver to use TLS/SSL either with [`ConnectionString`]({{< apiref "mongodb-driver-core" "com/mongodb/ConnectionString" >}}) or with
[`MongoClientSettings`]({{< apiref "mongodb-driver-core" "com/mongodb/MongoClientSettings" >}}).
With the legacy MongoClient API you can use either [`MongoClientURI`]({{< apiref "mongodb-driver-core" "com/mongodb/MongoClientURI" >}}) or 
[`MongoClientOptions`]({{< apiref "mongodb-driver-core" "com/mongodb/MongoClientOptions" >}}).

## MongoClient API (since 3.7)

### Specify TLS/SSL via `ConnectionString`

```java
com.mongodb.client.MongoClients;
com.mongodb.client.MongoClient;
```

To specify TLS/SSL with [`ConnectionString`]({{< apiref "mongodb-driver-core" "com/mongodb/ConnectionString" >}}), specify `ssl=true` as part of the connection
string, as in:

```java
MongoClient mongoClient = MongoClients.create("mongodb://localhost/?ssl=true");
```

### Specify TLS/SSL via `MongoClientSettings`

```java
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
```

To specify TLS/SSL with with [`MongoClientSettings`]({{< apiref "mongodb-driver-core" "com/mongodb/MongoClientSettings" >}}), set the `enabled` property to 
`true`, as in:

```java
MongoClientSettings settings = MongoClientSettings.builder()
        .applyToSslSettings(builder -> 
            builder.enabled(true))
        .build();
MongoClient client = MongoClients.create(settings);
```

### Specify `SSLContext` via `MongoClientSettings`

```java
import javax.net.ssl.SSLContext;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
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

## Legacy MongoClient API

### Specify TLS/SSL via `MongoClientURI`

```java
import com.mongodb.MongoClientURI;
import com.mongodb.MongoClient;
```

To specify TLS/SSL with [`MongoClientURI`]({{< apiref "mongodb-driver-core" "com/mongodb/MongoClientURI" >}}), specify `ssl=true` as part of the connection
string, as in:

```java
MongoClientURI uri = new MongoClientURI("mongodb://localhost/?ssl=true");
MongoClient mongoClient = new MongoClient(uri);
```

### Specify TLS/SSL via `MongoClientOptions`

```java
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClient;
```

To specify TLS/SSL with with [`MongoClientOptions`]({{< apiref "mongodb-driver-core" "com/mongodb/MongoClientOptions" >}}), set the `sslEnabled` property to `true`, as in:

```java
MongoClientOptions options = MongoClientOptions.builder()
        .sslEnabled(true)
        .build();
MongoClient client = new MongoClient("localhost", options);
```

### Specify `SSLContext` via `MongoClientOptions`

```java
import javax.net.ssl.SSLContext;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClient;
```

To specify the [`javax.net.ssl.SSLContext`](https://docs.oracle.com/javase/8/docs/api/javax/net/ssl/SSLContext.html) with 
[`MongoClientOptions`]({{< apiref "mongodb-driver-core" "com/mongodb/MongoClientOptions" >}}), set the `sslContext` property, as in:

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
constructing a [`MongoClient()`]({{< apiref "mongodb-driver-sync" "com/mongodb/client/MongoClient.html" >}}).

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

or, with the legacy `MongoClientOptions`]({{< apiref "mongodb-driver-core" "com/mongodb/MongoClientOptions" >}}), using the `sslInvalidHostNameAllowed` property:

```java
MongoClientOptions.builder()
        .sslEnabled(true)
        .sslInvalidHostNameAllowed(true)
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


### Forcing TLS 1.2

Some applications may want to force only the TLS 1.2 protocol. To do this, set the `jdk.tls.client.protocols` system property to "TLSv1.2".

Java runtime environments prior to Java 8 started to enable the TLS 1.2 protocol only in later updates, as shown in the previous section. For the driver to force the use of the TLS 1.2 protocol with a Java runtime environment prior to Java 8, ensure that the update has TLS 1.2 enabled.


## OCSP

{{% note %}}
The Java driver cannot enable OCSP by default on a per MongoClient basis.
{{% /note %}}

### Client-driven OCSP

An application will need to set JVM system and security properties to ensure that client-driven OCSP is enabled:

-  `com.sun.net.ssl.checkRevocation`:
      When set to `true`, this system property enables revocation checking.

-  `ocsp.enable`:
      When set to `true`, this security property enables client-driven OCSP.
      
To configure an application to use client-driven OCSP, the application must already be set up to connect to a server using TLS. Setting these system properties is required to enable client-driven OCSP.

{{% note %}}
The support for TLS provided by the JDK utilizes “hard fail” behavior in the case of an unavailable OCSP responder in contrast to the mongo shell and drivers that utilize “soft fail” behavior.
{{% /note %}}

### OCSP Stapling

{{% note class="important" %}}
The following exception may occur when using OCSP stapling with Java runtime environments that use the TLS 1.3 protocol (Java 11 and higher use TLS 1.3 by default):

`javax.net.ssl.SSLHandshakeException: extension (5) should not be presented in certificate_request`

The exception is due to a known issue with TLS 1.3 in Java 11 and higher. To avoid this exception when using a Java runtime environments using the TLS 1.3 protocol, you can force the application to use the TLS 1.2 protocol. To do this, set the `jdk.tls.client.protocols` system property to "TLSv1.2".
{{% /note %}}

An application will need to set several JVM system properties to set up OCSP stapling:

-  `jdk.tls.client.enableStatusRequestExtension`:
      When set to `true` (its default value), this enables OCSP stapling.

-  `com.sun.net.ssl.checkRevocation`:
      When set to `true`, this enables revocation checking. If this property is not set to `true`, then the connection will be allowed to proceed regardless of the presence or status of the revocation information.

To configure an application to use OCSP stapling, the application must already be set up to connect to a server using TLS, and the server must be set up to staple an OCSP response to the certificate it returns as part of the the TLS handshake.

For more information on configuring a Java application to use OCSP, please
refer to the "Client-driven OCSP and OCSP Stapling" section in the [`JSSE Reference Guide`](https://docs.oracle.com/javase/9/security/java-secure-socket-extension-jsse-reference-guide.htm).
