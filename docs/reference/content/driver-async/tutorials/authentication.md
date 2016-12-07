+++
date = "2015-03-19T14:27:51-04:00"
title = "Authentication"
[menu.main]
  parent = "Async Connection Settings"
  identifier = "Async Authentication"
  weight = 20
  pre = "<i class='fa'></i>"
+++

## Authentication

The Java driver supports all MongoDB [authentication mechanisms](http://docs.mongodb.org/manual/core/authentication/), including those
only available in the MongoDB [Enterprise Edition](http://docs.mongodb.org/manual/administration/install-enterprise/).

## `MongoCredential`

```java
import com.mongodb.MongoCredential;
```

An authentication credential is represented as an instance of the
[`MongoCredential`]({{< apiref "com/mongodb/MongoCredential" >}}) class. The [`MongoCredential`]({{<apiref "com/mongodb/MongoCredential.html">}}) class includes static
factory methods for each of the supported authentication mechanisms.  

To specify a list of these instances, use the [`MongoClientSettings`]({{< apiref "com/mongodb/async/client/MongoClientSettings" >}}) and pass as a parameter to the
[`MongoClients.create()`]({{<apiref "com/mongodb/async/client/MongoClients.html#create-com.mongodb.async.client.MongoClientSettings-">}}) method.

To specify a single `MongoCredential` instance, you can also use the [`ConnectionString`]({{< apiref "com/mongodb/ConnectionString" >}}) and pass to a
[`MongoClients.create()`]({{< apiref "com/mongodb/async/client/MongoClients.html#create-com.mongodb.ConnectionString-" >}}) method.

{{% note %}}

- You can also specify the credential with a string that specifies the connection URI and pass the string to the [`MongoClients.create()`]({{< apiref "com/mongodb/async/client/MongoClients.html#create-java.lang.String-" >}}) method that takes the connection string as a parameter.  For brevity, the tutorial omits the examples using the string.

- Given the flexibility of role-based access control in MongoDB, it is usually sufficient to authenticate with a single user, but, for completeness, the driver accepts a list of credentials.
{{% /note %}}

## Default Authentication Mechanism

Starting in MongoDB 3.0, MongoDB changed the default authentication
mechanism from [`MONGODB-CR`]({{<docsref "/core/security-mongodb-cr">}}) to
[`SCRAM-SHA-1`]({{<docsref "core/security-scram-sha-1">}}).

To create a credential that will authenticate using the default
authentication mechanism regardless of server version, create a
credential using the [`createCredential`]({{<apiref "com/mongodb/MongoCredential.html#createCredential-java.lang.String-java.lang.String-char:A-">}})
static factory method:

```java
String user; // the user name
String database; // the name of the database in which the user is defined
char[] password; // the password as a character array
// ...


MongoCredential credential = MongoCredential.createCredential(user, database, password);

ClusterSettings clusterSettings = ClusterSettings.builder()
                                  .hosts(asList(new ServerAddress("localhost"))).build();
MongoClientSettings settings = MongoClientSettings.builder()
                                  .clusterSettings(clusterSettings)
                                  .credentialList(Arrays.asList(credential))
                                  .build();
MongoClient mongoClient = MongoClients.create(settings);
```

Or use a `ConnectionString` instance that does not  specify the
authentication mechanism:

```java
MongoClient mongoClient = MongoClients.create(
            new ConnectionString("mongodb://user1:pwd1@host1/?authSource=db1"));
```

For challenge and response mechanisms, using the default authentication
mechanism is the recommended approach as the approach will make
upgrading from MongoDB 2.6 to MongoDB 3.0 seamless, even after
[upgrading the authentication schema]({{<docsref "release-notes/3.0-scram/">}}).

## SCRAM-SHA-1

To explicitly create a credential of type [`SCRAM-SHA-1`]({{<docsref "core/security-scram-sha-1/">}}), use the [`createScramSha1Credential`]({{<apiref "com/mongodb/MongoCredential.html#createScramSha1Credential-java.lang.String-java.lang.String-char:A-">}}) method:


```java
MongoCredential credential = MongoCredential.createScramSha1Credential(user,
                                                                       database,
                                                                       password);

ClusterSettings clusterSettings = ClusterSettings.builder()
                                  .hosts(asList(new ServerAddress("localhost"))).build();
MongoClientSettings settings = MongoClientSettings.builder()
                                  .clusterSettings(clusterSettings)
                                  .credentialList(Arrays.asList(credential))
                                  .build();
MongoClient mongoClient = MongoClients.create(settings);

```

Or use a `ConnectionString` instance that explicitly specifies the
`authMechanism=SCRAM-SHA-1`:


```java
MongoClient mongoClient = MongoClients.create(new ConnectionString(
            "mongodb://user1:pwd1@host1/?authSource=db1&authMechanism=SCRAM-SHA-1"));

```

## MONGODB-CR

To explicitly create a credential of type [`MONGODB-CR`]({{<docsref "core/security-mongodb-cr">}}) use the [`createMongCRCredential`]({{<apiref "com/mongodb/MongoCredential.html#createMongoCRCredential-java.lang.String-java.lang.String-char:A-">}})
static factory method:

```java
MongoCredential credential = MongoCredential.createMongoCRCredential(user,
                                                                     database,
                                                                     password);
ClusterSettings clusterSettings = ClusterSettings.builder()
                                  .hosts(asList(new ServerAddress("localhost"))).build();
MongoClientSettings settings = MongoClientSettings.builder()
                                  .clusterSettings(clusterSettings)
                                  .credentialList(Arrays.asList(credential))
                                  .build();
MongoClient mongoClient = MongoClients.create(settings);
```

Or use a  `ConnectionString` instance that explicitly specifies the
`authMechanism=MONGODB-CR`:

```java
MongoClient mongoClient = MongoClients.create(new ConnectionString(
            "mongodb://user1:pwd1@host1/?authSource=db1&authMechanism=MONGODB-CR"));
```

{{% note %}}
After the [authentication schema upgrade]({{<docsref "release-notes/3.0-scram/">}}) from MONGODB-CR to SCRAM-SHA-1,
MONGODB-CR credentials will fail to authenticate.
{{% /note %}}

## X.509

With [X.509]({{<docsref "core/security-x.509">}}) mechanism, MongoDB uses the
X.509 certificate presented during SSL negotiation to
authenticate a user whose name is derived from the distinguished name
of the X.509 certificate.

X.509 authentication requires the use of SSL connections with
certificate validation and is available in MongoDB 2.6 and later. To
create a credential of this type use the
[`createMongoX509Credential`]({{<apiref "com/mongodb/MongoCredential.html#createMongoX509Credential-java.lang.String-">}}) static factory method:

```java
String user;     // The x.509 certificate derived user name, e.g. "CN=user,OU=OrgUnit,O=myOrg,..."
MongoCredential credential = MongoCredential.createMongoX509Credential(user);
ClusterSettings clusterSettings = ClusterSettings.builder()
                                  .hosts(asList(new ServerAddress("localhost"))).build();

EventLoopGroup eventLoopGroup = new NioEventLoopGroup();  // make sure application shuts this down

MongoClientSettings settings = MongoClientSettings.builder()
                .clusterSettings(clusterSettings)
                .credentialList(Arrays.asList(credential))
                .streamFactoryFactory(NettyStreamFactoryFactory.builder().eventLoopGroup(eventLoopGroup).build())
                .sslSettings(SslSettings.builder().enabled(true).build())
                .build();
MongoClient mongoClient = MongoClients.create(settings);
```

Or use a `ConnectionString` instance that explicitly specifies the
`authMechanism=MONGODB-X509`:

```java
MongoClient mongoClient = MongoClients.create(new ConnectionString(
            "mongodb://subjectName@host1/?authMechanism=MONGODB-X509&streamType=netty&ssl=true"));
```

See the MongoDB server
[x.509 tutorial](http://docs.mongodb.org/manual/tutorial/configure-x509-client-authentication/#add-x-509-certificate-subject-as-a-user) for
more information about determining the subject name from the certificate.

## Kerberos (GSSAPI)

[MongoDB Enterprise](http://www.mongodb.com/products/mongodb-enterprise) supports proxy
authentication through Kerberos service. To create a credential of type
[Kerberos (GSSAPI)]({{<apiref "core/authentication/#kerberos-authentication">}}), use the
[`createGSSAPICredential`]({{<apiref "com/mongodb/MongoCredential.html#createGSSAPICredential-java.lang.String-">}})
static factory method:

```java
String user;   // The Kerberos user name, including the realm, e.g. "user1@MYREALM.ME"
// ...
MongoCredential credential = MongoCredential.createGSSAPICredential(user);
ClusterSettings clusterSettings = ClusterSettings.builder()
                                  .hosts(asList(new ServerAddress("localhost"))).build();
MongoClientSettings settings = MongoClientSettings.builder()
                                  .clusterSettings(clusterSettings)
                                  .credentialList(Arrays.asList(credential))
                                  .build();
MongoClient mongoClient = MongoClients.create(settings);

```

Or use a `ConnectionString` that explicitly specifies the
`authMechanism=GSSAPI`:

```java
MongoClient mongoClient = MongoClients.create(new ConnectionString(
            "mongodb://username%40MYREALM.ME@host1/?authMechanism=GSSAPI"));
```

{{% note %}}
The method refers to the `GSSAPI` authentication mechanism instead of `Kerberos` because technically the driver is authenticating via the
[GSSAPI](https://tools.ietf.org/html/rfc4752) SASL mechanism.

The `GSSAPI` authentication mechanism is supported only in the following environments:

* Linux: Java 6 and above
* Windows: Java 7 and above with [SSPI](https://msdn.microsoft.com/en-us/library/windows/desktop/aa380493)
* OS X: Java 7 and above
{{% /note %}}

To successfully authenticate via Kerberos, the application typically
must specify several system properties so that the underlying GSSAPI
Java libraries can acquire a Kerberos ticket:

```java
java.security.krb5.realm=MYREALM.ME
java.security.krb5.kdc=mykdc.myrealm.me
```

Depending on the Kerberos setup, additional property specifications may be required, either via the application code or, in some cases, the [withMechanismProperty()]({{<apiref "com/mongodb/MongoCredential.html#withMechanismProperty-java.lang.String-T-">}}) method of the `MongoCredential` instance:

- **[`SERVICE_NAME`]({{< apiref "com/mongodb/MongoCredential.html#SERVICE_NAME_KEY" >}})**


- **[`CANONICALIZE_HOST_NAME`]({{< apiref "com/mongodb/MongoCredential.html#CANONICALIZE_HOST_NAME_KEY" >}})**


- **[`JAVA_SUBJECT`]({{< apiref "com/mongodb/MongoCredential.html#JAVA_SUBJECT_KEY" >}})**

- **[`JAVA_SASL_CLIENT_PROPERTIES`]({{< apiref "com/mongodb/MongoCredential.html#JAVA_SASL_CLIENT_PROPERTIES_KEY" >}})**

For example, to specify the `SERVICE_NAME` property via the `MongoCredential` object:


```java
credential = credential.withMechanismProperty(MongoCredential.SERVICE_NAME_KEY, "othername");
```

Or via the `ConnectionString`:

```
mongodb://username%40MYREALM.com@myserver/?authMechanism=GSSAPI&authMechanismProperties=SERVICE_NAME:othername
```


## LDAP (PLAIN)

[MongoDB Enterprise](http://www.mongodb.com/products/mongodb-enterprise) supports proxy authentication through a Lightweight Directory Access Protocol (LDAP) service. To create a credential of type [LDAP]({{<apiref "core/authentication/#ldap-proxy-authority-authentication">}}) use the
[`createPlainCredential`]({{<apiref "com/mongodb/MongoCredential.html#createPlainCredential-java.lang.String-java.lang.String-char:A-">}}) static factory method:

```java
String user;          // The LDAP user name
char[] password;      // The LDAP password
// ...
MongoCredential credential = MongoCredential.createPlainCredential(user, "$external", password);
ClusterSettings clusterSettings = ClusterSettings.builder()
                                  .hosts(asList(new ServerAddress("localhost"))).build();
MongoClientSettings settings = MongoClientSettings.builder()
                                  .clusterSettings(clusterSettings)
                                  .credentialList(Arrays.asList(credential))
                                  .build();
MongoClient mongoClient = MongoClients.create(settings);
```

or with a connection string:

```java
MongoClient mongoClient = MongoClients.create(new ConnectionString(
          "mongodb://user1@host1/?authSource=$external&authMechanism=PLAIN"));
```

{{% note %}}
The method refers to the `plain` authentication mechanism instead of `LDAP` because technically the driver is authenticating via the [PLAIN](https://www.ietf.org/rfc/rfc4616.txt) SASL mechanism.
{{% /note %}}
