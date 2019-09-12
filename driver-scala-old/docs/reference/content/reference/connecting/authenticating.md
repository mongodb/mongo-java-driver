+++
date = "2015-03-19T14:27:51-04:00"
title = "Authenticating"
[menu.main]
  parent = "Connecting"
  identifier = "Authenticating"
  weight = 20
  pre = "<i class='fa'></i>"
+++

# Authentication

The Scala driver supports all MongoDB [authentication mechanisms](http://docs.mongodb.org/manual/core/authentication/), including those
only available in the MongoDB [Enterprise Edition](http://docs.mongodb.org/manual/administration/install-enterprise/).

An authentication credential is represented as an instance of the
[`MongoCredential`]({{< apiref "org/mongodb/scala/MongoCredential$" >}}) class, which includes static factory methods for
each of the supported authentication mechanisms.  A list of these instances must be passed to the driver via a
[`MongoClient`]({{< apiref "org/mongodb/scala/MongoClient$" >}}) static factory method that takes a 
[`MongoClientSettings`]({{< apiref "org/mongodb/scala/MongoClientSettings$" >}}) parameter.  Alternatively, a single 
`MongoCredential` can be created implicity via a 
[`ConnectionString`]({{< apiref "org/mongodb/scala/ConnectionString$" >}}) and passed to a `MongoClient` static factory method that 
takes a `ConnectionString` parameter. 

{{% note %}}
Given the flexibility of role-based access control in MongoDB, it is usually sufficient to authenticate with a single user, but, for 
completeness, the driver accepts a list of credentials.
{{% /note %}}

## Default authentication mechanism

In MongoDB 3.0, MongoDB changed the default authentication mechanism from [`MONGODB-CR`]({{<docsref "core/security-mongodb-cr">}}) to
[`SCRAM-SHA-1`]({{<docsref "core/security-scram">}}). In MongoDB 4.0 support for the deprecated
[`MONGODB-CR`]({{<docsref "core/security-mongodb-cr">}}) mechanism was removed and
[`SCRAM-SHA-256`]({{<docsref "core/security-scram">}}) support was added. 

To create a credential that will authenticate properly regardless of server version, create a credential using the following static 
factory method:

 ```scala
import com.mongodb.MongoCredential._

// ...

val user: String = "userName"                       // the user name
val source: String = "databaseName"                 // the source where the user is defined
val password: Array[Char] = "password".toCharArray  // the password as a character array
// ...
val credential: MongoCredential = createCredential(user, source, password)

val settings: MongoClientSettings = MongoClientSettings.builder()
    .applyToClusterSettings(b => b.hosts(List(new ServerAddress("host1")).asJava)
    .credential(credential)
    .build()
val mongoClient: MongoClient = MongoClient(settings)

```

or with a connection string:

```scala
val connectionString: String = "mongodb://user1:pwd1@host1/?authSource=db1"

val mongoClient: MongoClient = MongoClient(connectionString)
```

For challenge and response mechanisms, using the default authentication mechanism is the recommended approach as it will make upgrading
from MongoDB 2.6 to MongoDB 3.0 seamless, even after [upgrading the authentication schema]({{<docsref "release-notes/3.0-scram/">}}).
For MongoDB 4.0 users it is also recommended as the supported authentication mechanisms are checked and the correct hashing algorithm is
used.

## SCRAM

Salted Challenge Response Authentication Mechanism (`SCRAM`) has been the default authentication mechanism for MongoDB since 3.0. `SCRAM` is
based on the [IETF RFC 5802](https://tools.ietf.org/html/rfc5802) standard that defines best practices for implementation of
challenge-response mechanisms for authenticating users with passwords.

MongoDB 3.0 introduced support for `SCRAM-SHA-1` which uses the `SHA-1` hashing function. MongoDB 4.0 introduced support for `SCRAM-SHA-256`
which uses the `SHA-256` hashing function.

### SCRAM-SHA-256

Requires MongoDB 4.0 and `featureCompatibilityVersion` to be set to 4.0.

To explicitly create a credential of type [`SCRAM-SHA-256`]({{<docsref "core/security-scram/">}}) use the following static factory method:

```scala
val credential: MongoCredential = createScramSha256Credential(user, source, password)
```

or with a connection string:

```scala
val connectionString: String = "mongodb://user1:pwd1@host1/?authSource=db1&authMechanism=SCRAM-SHA-256"
```

### SCRAM-SHA-1

To explicitly create a credential of type [`SCRAM-SHA-1`]({{<docsref "core/security-scram/">}}) use the following static factory method:

```scala
val credential: MongoCredential = createScramSha1Credential(user, source, password)
```

or with a connection string:

```scala
val connectionString: String = "mongodb://user1:pwd1@host1/?authSource=db1&authMechanism=SCRAM-SHA-1"
```

## MONGODB-CR

{{% note class="important" %}}
Starting in version 4.0, MongoDB removes support for the deprecated MongoDB Challenge-Response (`MONGODB-CR`) authentication mechanism.

If your deployment has user credentials stored in `MONGODB-CR` schema, you must upgrade to `SCRAM` before you upgrade to version 4.0.
For information on upgrading to `SCRAM`, see Upgrade to [SCRAM]({{<docsref "release-notes/3.0-scram/">}}).
{{% /note %}}

To explicitly create a credential of type [MONGODB-CR](http://docs.mongodb.org/manual/core/authentication/#mongodb-cr-authentication)
use the following static factory method:

```scala
val credential: MongoCredential = createMongoCRCredential(user, database, password)
```

or with a connection string:

```scala
val connectionString: String = "mongodb://user1:pwd1@host1/?authSource=db1&authMechanism=MONGODB-CR"
```

Note that this is not recommended as a credential created in this way will fail to authenticate after an authentication schema upgrade
from MONGODB-CR to SCRAM-SHA-1.

## x.509

The [x.509](http://docs.mongodb.org/manual/core/authentication/#x-509-certificate-authentication) mechanism authenticates a user
whose name is derived from the distinguished subject name of the X.509 certificate presented by the driver during SSL negotiation. This
authentication method requires the use of SSL connections with certificate validation and is available in MongoDB 2.6 and newer. To
create a credential of this type use the following static factory method:

```scala
val user: String = "..."     // The x.509 certificate derived user name, e.g. "CN=user,OU=OrgUnit,O=myOrg,..."
val credential: MongoCredential = createMongoX509Credential(user)
```

or with a connection string:

```scala
val connectionString: String = "mongodb://subjectName@host1/?authMechanism=MONGODB-X509"
```

See the MongoDB server
[x.509 tutorial](http://docs.mongodb.org/manual/tutorial/configure-x509-client-authentication/#add-x-509-certificate-subject-as-a-user) for
more information about determining the subject name from the certificate.

## Kerberos (GSSAPI)

[MongoDB Enterprise](http://www.mongodb.com/products/mongodb-enterprise) supports proxy authentication through a Kerberos service.  To
create a credential of type [Kerberos (GSSAPI)](http://docs.mongodb.org/manual/core/authentication/#kerberos-authentication) use the
following static factory method:

```scala
val user: String = "..."   // The Kerberos user name, including the realm, e.g. "user1@MYREALM.ME"
// ...
val credential: MongoCredential = createGSSAPICredential(user)
```

or with a connection string:

```scala
val connectionString: String = "mongodb://username%40REALM.com@host1/?authMechanism=GSSAPI"
```

{{% note %}}
The method refers to the `GSSAPI` authentication mechanism instead of `Kerberos` because technically the driver is authenticating via the 
[GSSAPI](https://tools.ietf.org/html/rfc4752) SASL mechanism.
{{% /note %}}

To successfully authenticate via Kerberos, the application typically must specify several system properties so that the underlying GSSAPI
Java libraries can acquire a Kerberos ticket:

    java.security.krb5.realm=MYREALM.ME
    java.security.krb5.kdc=mykdc.myrealm.me

{{% note %}}
The `GSSAPI` authentication mechanism is supported only in the following environments:

* Linux: Java 6 and above 
* Windows: Java 7 and above with [SSPI](https://msdn.microsoft.com/en-us/library/windows/desktop/aa380493)
* OS X: Java 7 and above
{{% /note %}}

## LDAP (PLAIN)

[MongoDB Enterprise](http://www.mongodb.com/products/mongodb-enterprise) supports proxy authentication through a Lightweight Directory
Access Protocol (LDAP) service.  To create a credential of type [LDAP](http://docs.mongodb
.org/manual/core/authentication/#ldap-proxy-authority-authentication) use the following static factory method:

```scala
val user: String = "..."                        // The LDAP user name
val password: Array[Char] = "...".toCharArray   // The LDAP password

// ...
val credential: MongoCredential = createPlainCredential(user, "$external", password)
```

or with a connection string:

```scala
val connectionString: String = "mongodb://user1@host1/?authSource=$external&authMechanism=PLAIN"
```

{{% note %}}
The method refers to the `plain` authentication mechanism instead of `LDAP` because technically the driver is authenticating via the 
[PLAIN](https://www.ietf.org/rfc/rfc4616.txt) SASL mechanism.
{{% /note %}}
