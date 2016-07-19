+++
date = "2016-05-29T23:27:26-04:00"
title = "Authentication"
[menu.main]
  parent = "Connect to MongoDB"
  identifier = "Authentication"
  weight = 20
  pre = "<i class='fa'></i>"
+++

## Authentication

The Java driver supports all [MongoDB authentication mechanisms]({{<docsref "core/authentication/">}}), including those only available in the
[MongoDB Enterprise Edition]({{<docsref "administration/install-enterprise/">}}).

## `MongoCredential`

```java
import com.mongodb.MongoCredential;
```

An authentication credential is represented as an instance of the
[`MongoCredential`]({{<apiref "com/mongodb/MongoCredential.html">}}) class. The [`MongoCredential`]({{<apiref "com/mongodb/MongoCredential.html">}}) class includes static
factory methods for each of the supported authentication mechanisms.

To specify a list of these instances, use one of
several [`MongoClient()`]({{< apiref "com/mongodb/MongoClient.html">}}) constructors that take a parameter of type
`List <MongoCredential>`.

To specify a single `MongoCredential`, you can also use a [`MongoClientURI`]({{< apiref "/com/mongodb/MongoClientURI.html">}}) and pass it to a [`MongoClient()`]({{< apiref "com/mongodb/MongoClient.html">}}) constructor that takes a `MongoClientURI` parameter.

{{% note %}}
Given the flexibility of role-based access control in MongoDB, it is
usually sufficient to authenticate with a single user, but, for
completeness, the driver accepts a list of credentials.
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
MongoClient mongoClient = new MongoClient(new ServerAddress("host1", 27017),
                                         Arrays.asList(credential));
```
Or use a connection string without explicitly specifying the
authentication mechanism:

```java
MongoClientURI uri = new MongoClientURI("mongodb://user1:pwd1@host1/?authSource=db1");
MongoClient mongoClient = new MongoClient(uri);
```

For challenge and response mechanisms, using the default authentication
mechanism is the recommended approach as the approach will make
upgrading from MongoDB 2.6 to MongoDB 3.0 seamless, even after
[upgrading the authentication schema]({{<docsref "release-notes/3.0-scram/">}}).

## SCRAM-SHA-1

To explicitly create a credential of type [`SCRAM-SHA-1`]({{<docsref "core/security-scram-sha-1/">}}), use the [`createScramSha1Credential`]({{<apiref "com/mongodb/MongoCredential.html#createScramSha1Credential-java.lang.String-java.lang.String-char:A-">}}) method:

```java

String user; // the user name
String database; // the name of the database in which the user is defined
char[] password; // the password as a character array
// ...
MongoCredential credential = MongoCredential.createScramSha1Credential(user,
                                                                      database,
                                                                      password);
MongoClient mongoClient = new MongoClient(new ServerAddress("host1", 27017),
                                             Arrays.asList(credential));
```

Or use a connection string  that explicitly specifies the
`authMechanism=SCRAM-SHA-1`:

```java
MongoClientURI uri = new MongoClientURI("mongodb://user1:pwd1@host1/?authSource=db1&authMechanism=SCRAM-SHA-1");
MongoClient mongoClient = new MongoClient(uri);
```
## MONGODB-CR

To explicitly create a credential of type [`MONGODB-CR`]({{<docsref "core/security-mongodb-cr">}}) use the [`createMongCRCredential`]({{<apiref "com/mongodb/MongoCredential.html#createMongoCRCredential-java.lang.String-java.lang.String-char:A-">}})
static factory method:

```java
String user; // the user name
String database; // the name of the database in which the user is defined
char[] password; // the password as a character array
// ...
MongoCredential credential = MongoCredential.createMongoCRCredential(user,
                                                                    database,
                                                                    password);
MongoClient mongoClient = new MongoClient(new ServerAddress("host1", 27017),
                                         Arrays.asList(credential));
```
Or use a connection string that explicitly specifies the
`authMechanism=MONGODB-CR`:

```java
MongoClientURI uri = new MongoClientURI("mongodb://user1:pwd1@host1/?authSource=db1&authMechanism=MONGODB-CR");
MongoClient mongoClient = new MongoClient(uri);
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
String user;     // The X.509 certificate derived user name, e.g. "CN=user,OU=OrgUnit,O=myOrg,..."
// ...
MongoCredential credential = MongoCredential.createMongoX509Credential(user);

MongoClient mongoClient = new MongoClient(new ServerAddress("host1", 27017),
                                         Arrays.asList(credential));
```

Or use a connection string that explicitly specifies the
`authMechanism=MONGODB-X509`:

```java
MongoClientURI uri = new MongoClientURI("mongodb://subjectName@host1/?authMechanism=MONGODB-X509");
MongoClient mongoClient = new MongoClient(uri);
```

See the MongoDB server [x.509 tutorial]({{<apiref "tutorial/configure-x509-client-authentication/#add-x-509-certificate-subject-as-a-user">}})
for more information about determining the subject
name from the certificate.

## Kerberos (GSSAPI) {#gssapi}

[MongoDB Enterprise](http://www.mongodb.com/products/mongodb-enterprise) supports proxy
authentication through Kerberos service. To create a credential of type
[Kerberos (GSSAPI)]({{<apiref "core/authentication/#kerberos-authentication">}}), use the
[`createGSSAPICredential`]({{<apiref "com/mongodb/MongoCredential.html#createGSSAPICredential-java.lang.String-">}})
static factory method:

```java
String user;   // The Kerberos user name, including the realm, e.g. "user1@MYREALM.ME"
// ...
MongoCredential credential = MongoCredential.createGSSAPICredential(user);
```
Or use a connection string that explicitly specifies the
`authMechanism=GSSAPI`:

```java
MongoClientURI uri = new MongoClientURI("mongodb://username%40REALM.com@host1/?authMechanism=GSSAPI");
```
{{%note%}}

The method refers to the `GSSAPI` authentication mechanism instead
of `Kerberos` because technically the driver authenticates via
the [`GSSAPI`](https://tools.ietf.org/html/rfc4752) SASL mechanism.

{{%/note%}}

To successfully authenticate via Kerberos, the application typically
must specify several system properties so that the underlying GSSAPI
Java libraries can acquire a Kerberos ticket:

```java
java.security.krb5.realm=MYREALM.ME
java.security.krb5.kdc=mykdc.myrealm.me
```

## LDAP (PLAIN)

[MongoDB Enterprise](http://www.mongodb.com/products/mongodb-enterprise) supports proxy authentication through a Lightweight Directory Access Protocol (LDAP) service. To create a credential of type [LDAP]({{<apiref "core/authentication/#ldap-proxy-authority-authentication">}}) use the
[`createPlainCredential`]({{<apiref "com/mongodb/MongoCredential.html#createPlainCredential-java.lang.String-java.lang.String-char:A-">}}) static factory method:

```java
String user;          // The LDAP user name
char[] password;      // The LDAP password
// ...
MongoCredential credential = MongoCredential.createPlainCredential(user, "$external", password);
```
Or use a connection string that explicitly specifies the
`authMechanism=PLAIN`:

```java
MongoClientURI uri = new MongoClientURI("mongodb://user1@host1/?authSource=$external&authMechanism=PLAIN");
```

{{%note%}}
The method refers to the `plain` authentication mechanism instead
of `LDAP` because technically the driver authenticates via the
[`PLAIN`](https://www.ietf.org/rfc/rfc4616.txt) SASL mechanism.
{{%/note%}}
