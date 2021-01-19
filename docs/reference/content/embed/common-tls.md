<p></p>
### Configure Trust Store and Key Store
One may either configure trust stores and key stores specific to the client via
[`javax.net.ssl.SSLContext.init(KeyManager[] km, TrustManager[] tm, SecureRandom random)`]
({{< javaseref "api/javax/net/ssl/SSLContext.html#init-javax.net.ssl.KeyManager:A-javax.net.ssl.TrustManager:A-java.security.SecureRandom-" >}}),
or set the JVM default ones.

#### Set the Default Trust Store

A typical application will need to set several JVM system properties to
ensure that the client is able to *validate* the TLS/SSL certificate
presented by the server:

-  `javax.net.ssl.trustStore`:
   The path to a trust store containing the certificate of the
   signing authority
   (see `<path to trust store>` below)

-  `javax.net.ssl.trustStorePassword`:
   The password to access this trust store
   (see `<trust store password>` below)

The trust store is typically created with the
[`keytool`]({{< javaseref "technotes/tools/unix/keytool.html" >}})
command line program provided as part of the JDK. For example:

```bash
keytool -importcert -trustcacerts -file <path to certificate authority file>
            -keystore <path to trust store> -storepass <trust store password>
```

#### Set the Default Key Store

A typical application will also need to set several JVM system
properties to ensure that the client *presents* an TLS/SSL [client certificate](https://docs.mongodb.com/manual/tutorial/configure-ssl/#set-up-mongod-and-mongos-with-client-certificate-validation) to the
MongoDB server:

- `javax.net.ssl.keyStore`
  The path to a key store containing the client's TLS/SSL certificates
  (see `<path to key store>` below)

- `javax.net.ssl.keyStorePassword`
  The password to access this key store
  (see `<trust store password>` below)

The key store is typically created with the
[`keytool`]({{< javaseref "technotes/tools/unix/keytool.html" >}})
or the [`openssl`](https://www.openssl.org/docs/apps/openssl.html)
command line program. For example, if you have a file with the client certificate and its private key
(may be in the PEM format)
and want to create a key store in the [PKCS #12](https://www.rfc-editor.org/rfc/rfc7292) format,
you can do the following:

```sh
openssl pkcs12 -export -in <path to client certificate & private key file>
            -out <path to key store> -passout pass:<trust store password>
``` 

For more information on configuring a Java application for TLS/SSL, please
refer to the [`JSSE Reference Guide`]({{< javaseref "technotes/guides/security/jsse/JSSERefGuide.html" >}}).


### Forcing TLS 1.2

Some applications may want to force only the TLS 1.2 protocol. To do this, set the `jdk.tls.client.protocols` system property to "TLSv1.2".

Java runtime environments prior to Java 8 started to enable the TLS 1.2 protocol only in later updates, as shown in the previous section. For the driver to force the use of the TLS 1.2 protocol with a Java runtime environment prior to Java 8, ensure that the update has TLS 1.2 enabled.


### OCSP

{{% note %}}
The Java driver cannot enable OCSP by default on a per MongoClient basis.
{{% /note %}}

#### Client-driven OCSP

An application will need to set JVM system and security properties to ensure that client-driven OCSP is enabled:

-  `com.sun.net.ssl.checkRevocation`:
   When set to `true`, this system property enables revocation checking.

-  `ocsp.enable`:
   When set to `true`, this security property enables client-driven OCSP.

To configure an application to use client-driven OCSP, the application must already be set up to connect to a server using TLS. Setting these system properties is required to enable client-driven OCSP.

{{% note %}}
The support for TLS provided by the JDK utilizes “hard fail” behavior in the case of an unavailable OCSP responder in contrast to the mongo shell and drivers that utilize “soft fail” behavior.
{{% /note %}}

#### OCSP Stapling

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
refer to the [`Client-Driven OCSP and OCSP Stapling`]({{< javaseref "technotes/guides/security/jsse/ocsp.html" >}}).