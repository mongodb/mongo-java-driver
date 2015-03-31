+++
date = "2015-03-19T12:53:26-04:00"
title = "SSL"
[menu.main]
  parent = "Async Connecting"
  identifier = "Async SSL"
  weight = 10
  pre = "<i class='fa'></i>"
+++

## SSL

The Java driver supports SSL connections to MongoDB servers using the underlying support for SSL provided by the JDK. You can configure 
the driver to use SSL either with `ConnectionString` or with `MongoClientSettings`.  

With `ConnectionString`, specify `ssl=true as a query parameter, as in:

```java
new ConnectionString("mongodb://localhost/?ssl=true")
```

With `MongoClientSettings`, set the sslEnabled property to true, as in:

```java
MongoClientSettings.builder()                                                  
                   .sslSettings(SslSettings.builder()
                                           .enabled(true)
                                           .build())   
                   .build()                                                    
```

### Host name verification

By default, the driver ensures that the host name included in the server's SSL certificate(s) matches the host name(s) provided when 
constructing a `MongoClient`.  However, this host name verification requires a Java 7 JVM, as it relies on additions to the 
`javax.net.SSLParameters` class that were introduced in Java 7.  If your application must run on Java 6, or for some other reason you need
 to disable host name verification, you must expicitly indicate this in `SslSettings` using the `invalidHostNameAllowed` property:
   
```java
MongoClientSettings.builder()                                             
                   .sslSettings(SslSettings.builder()                     
                                           .enabled(true)                 
                                           .invalidHostNameAllowed(true)  
                                           .build())                      
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






   
