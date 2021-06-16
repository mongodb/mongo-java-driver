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

The Java driver supports all [MongoDB authentication mechanisms]({{<docsref "core/authentication/" >}}), including those only available in the
[MongoDB Enterprise Edition]({{<docsref "administration/install-enterprise/" >}}).

## `MongoCredential`

```java
import com.mongodb.MongoCredential;
```

New MongoClient API (since 3.7):

```java
import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
```

Legacy MongoClient API:

```java
import com.mongodb.MongoClient;
import com.mongodb.ConnectionString;
```

An authentication credential is represented as an instance of the
[`MongoCredential`]({{< apiref "mongodb-driver-core" "com/mongodb/MongoCredential.html" >}}) class. The [`MongoCredential`]({{< apiref "mongodb-driver-core" "com/mongodb/MongoCredential.html" >}}) class includes static
factory methods for each of the supported authentication mechanisms.

You can also use a [`ConnectionString`]({{< apiref "mongodb-driver-core" "com/mongodb/ConnectionString.html" >}}) and pass it to a
[`MongoClient()`]({{< apiref "mongodb-driver-legacy" "com/mongodb/MongoClient.html" >}}) constructor that takes a `ConnectionString` 
parameter.

## Default Authentication Mechanism

In MongoDB 3.0, MongoDB changed the default authentication mechanism from [`MONGODB-CR`]({{<docsref "core/security-mongodb-cr" >}}) to
[`SCRAM-SHA-1`]({{<docsref "core/security-scram" >}}). In MongoDB 4.0 support for the deprecated
[`MONGODB-CR`]({{<docsref "core/security-mongodb-cr" >}}) mechanism was removed and
[`SCRAM-SHA-256`]({{<docsref "core/security-scram" >}}) support was added.


To create a credential that will authenticate using the default
authentication mechanism regardless of server version, create a
credential using the [`createCredential`]({{< apiref "mongodb-driver-core" "com/mongodb/MongoCredential.html#createCredential(java.lang.String,java.lang.String,char[])" >}})
static factory method:

```java
String user;     // the user name
String source;   // the source where the user is defined
char[] password; // the password as a character array
// ...
MongoCredential credential = MongoCredential.createCredential(user, source, password);
```

and then construct a MongoClient with that credential.  Using the new (since 3.7) MongoClient API:

```java
MongoClient mongoClient = MongoClients.create(
        MongoClientSettings.builder()
                .applyToClusterSettings(builder ->
                        builder.hosts(Arrays.asList(new ServerAddress("host1", 27017))))
                .credential(credential)
                .build());
```

or using the legacy MongoClient API:

```java
MongoClient mongoClient = new MongoClient(
        MongoClientSettings.builder()
                .applyToClusterSettings(builder ->
                        builder.hosts(Arrays.asList(new ServerAddress("host1", 27017))))
                .credential(credential)
                .build());
```

Or use a connection string without explicitly specifying the authentication mechanism. Using the new MongoClient API:

```java
MongoClient mongoClient = MongoClients.create("mongodb://user1:pwd1@host1/?authSource=db1");
```

or using the legacy MongoClient API:

```java
MongoClient mongoClient = new MongoClient("mongodb://user1:pwd1@host1/?authSource=db1");
```

For challenge and response mechanisms, using the default authentication mechanism is the recommended approach as it will make upgrading
from MongoDB 2.6 to MongoDB 3.0 seamless, even after [upgrading the authentication schema]({{<docsref "release-notes/3.0-scram/" >}}).
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

To explicitly create a credential of type [`SCRAM-SHA-256`]({{<docsref "core/security-scram/" >}}), use the
[`createScramSha256Credential`]({{< apiref "mongodb-driver-core" "com/mongodb/MongoCredential.html#createScramSha256Credential(java.lang.String,java.lang.String,char[])" >}})
method:

```java
String user;     // the user name
String source;   // the source where the user is defined
char[] password; // the password as a character array
// ...
MongoCredential credential = MongoCredential.createScramSha256Credential(user, source, password);
```

and then construct a MongoClient with that credential.  Using the new (since 3.7) MongoClient API:

```java
MongoClient mongoClient = MongoClients.create(
        MongoClientSettings.builder()
                .applyToClusterSettings(builder ->
                        builder.hosts(Arrays.asList(new ServerAddress("host1", 27017))))
                .credential(credential)
                .build());
```

or using the legacy MongoClient API:

```java
MongoClient mongoClient = new MongoClient(
        MongoClientSettings.builder()
                .applyToClusterSettings(builder ->
                        builder.hosts(Arrays.asList(new ServerAddress("host1", 27017))))
                .credential(credential)
                .build());
```

Or use a connection string that explicitly specifies the `authMechanism=SCRAM-SHA-256`. Using the new MongoClient API:

```java
MongoClient mongoClient = MongoClients.create("mongodb://user1:pwd1@host1/?authSource=db1&authMechanism=SCRAM-SHA-256");
```

or using the legacy MongoClient API:

```java
MongoClient mongoClient = new MongoClient("mongodb://user1:pwd1@host1/?authSource=db1&authMechanism=SCRAM-SHA-256");
```


### SCRAM-SHA-1

To explicitly create a credential of type [`SCRAM-SHA-1`]({{<docsref "core/security-scram/" >}}), use the
[`createScramSha1Credential`]({{< apiref "mongodb-driver-core" "com/mongodb/MongoCredential.html#createScramSha1Credential(java.lang.String,java.lang.String,char[])" >}})
method:

```java
String user;     // the user name
String source;   // the source where the user is defined
char[] password; // the password as a character array
// ...
MongoCredential credential = MongoCredential.createScramSha1Credential(user, source, password);
```

and then construct a MongoClient with that credential.  Using the new (since 3.7) MongoClient API:

```java
MongoClient mongoClient = MongoClients.create(
        MongoClientSettings.builder()
                .applyToClusterSettings(builder ->
                        builder.hosts(Arrays.asList(new ServerAddress("host1", 27017))))
                .credential(credential)
                .build());
```

or using the legacy MongoClient API:

```java
MongoClient mongoClient = new MongoClient(
        MongoClientSettings.builder()
                .applyToClusterSettings(builder ->
                        builder.hosts(Arrays.asList(new ServerAddress("host1", 27017))))
                .credential(credential)
                .build());
```

Or use a connection string that explicitly specifies the `authMechanism=SCRAM-SHA-1`. Using the new MongoClient API:

```java
MongoClient mongoClient = MongoClients.create("mongodb://user1:pwd1@host1/?authSource=db1&authMechanism=SCRAM-SHA-1");
```

or using the legacy MongoClient API:

```java
MongoClient mongoClient = new MongoClient("mongodb://user1:pwd1@host1/?authSource=db1&authMechanism=SCRAM-SHA-1");
```

## MONGODB-CR

{{% note class="important" %}}
Starting in version 4.0, MongoDB removes support for the deprecated MongoDB Challenge-Response (`MONGODB-CR`) authentication mechanism.

If your deployment has user credentials stored in `MONGODB-CR` schema, you must upgrade to `SCRAM` before you upgrade to version 4.0.
For information on upgrading to `SCRAM`, see Upgrade to [SCRAM]({{<docsref "release-notes/3.0-scram/" >}}).
{{% /note %}}


To explicitly create a credential of type [`MONGODB-CR`]({{<docsref "core/security-mongodb-cr" >}}) use the [`createMongCRCredential`]({{< apiref "mongodb-driver-core" "com/mongodb/MongoCredential.html#createMongoCRCredential(java.lang.String,java.lang.String,char[])" >}})
static factory method:

```java
String user; // the user name
String database; // the name of the database in which the user is defined
char[] password; // the password as a character array
// ...
MongoCredential credential = MongoCredential.createMongoCRCredential(user, database, password);
```

and then construct a MongoClient with that credential.  Using the new (since 3.7) MongoClient API:

```java
MongoClient mongoClient = MongoClients.create(
        MongoClientSettings.builder()
                .applyToClusterSettings(builder ->
                        builder.hosts(Arrays.asList(new ServerAddress("host1", 27017))))
                .credential(credential)
                .build());
```

or using the legacy MongoClient API:

```java
MongoClient mongoClient = new MongoClient(
        MongoClientSettings.builder()
                .applyToClusterSettings(builder ->
                        builder.hosts(Arrays.asList(new ServerAddress("host1", 27017))))
                .credential(credential)
                .build());
```

Or use a connection string that explicitly specifies the `authMechanism=MONGODB-CR`. Using the new MongoClient API:

```java
MongoClient mongoClient = MongoClients.create("mongodb://user1:pwd1@host1/?authSource=db1&authMechanism=MONGODB-CR");
```

or using the legacy MongoClient API:

```java
MongoClient mongoClient = new MongoClient("mongodb://user1:pwd1@host1/?authSource=db1&authMechanism=MONGODB-CR");
```

{{% note %}}
After the [authentication schema upgrade]({{<docsref "release-notes/3.0-scram/" >}}) from MONGODB-CR to SCRAM,
MONGODB-CR credentials will fail to authenticate.
{{% /note %}}

## X.509

With [X.509]({{<docsref "core/security-x.509" >}}) mechanism, MongoDB uses the
X.509 certificate presented during SSL negotiation to
authenticate a user whose name is derived from the distinguished name
of the X.509 certificate.

X.509 authentication requires the use of SSL connections with
certificate validation and is available in MongoDB 2.6 and later. To
create a credential of this type use the
[`createMongoX509Credential`]({{< apiref "mongodb-driver-core" "com/mongodb/MongoCredential.html#createMongoX509Credential(java.lang.String)" >}}) static factory method:

```java
String user;     // The X.509 certificate derived user name, e.g. "CN=user,OU=OrgUnit,O=myOrg,..."
// ...
MongoCredential credential = MongoCredential.createMongoX509Credential(user);
```

and then construct a MongoClient with that credential.  Using the new (since 3.7) MongoClient API:

```java
MongoClient mongoClient = MongoClients.create(
        MongoClientSettings.builder()
                .applyToClusterSettings(builder ->
                        builder.hosts(Arrays.asList(new ServerAddress("host1", 27017))))
                .credential(credential)
                .build());
```

or using the legacy MongoClient API:

```java
MongoClient mongoClient = new MongoClient(
        MongoClientSettings.builder()
                .applyToClusterSettings(builder ->
                        builder.hosts(Arrays.asList(new ServerAddress("host1", 27017))))
                .credential(credential)
                .build());
```

Or use a connection string that explicitly specifies the `authMechanism=MONGODB-X509`. Using the new MongoClient API:

```java
MongoClient mongoClient = MongoClients.create("mongodb://subjectName@host1/?authMechanism=MONGODB-X509&ssl=true");
```

or using the legacy MongoClient API:

```java
MongoClient mongoClient = new MongoClient("mongodb://subjectName@host1/?authMechanism=MONGODB-X509&ssl=true");
```

See the MongoDB server [x.509 tutorial]({{<docsref "tutorial/configure-x509-client-authentication/#add-x-509-certificate-subject-as-a-user" >}})
for more information about determining the subject
name from the certificate.

## MONGODB-AWS

{{% note %}}
The MONGODB-AWS authentication mechanism is only available in MongoDB
versions 4.4 and later.
{{% /note %}}

The MONGODB-AWS authentication mechanism uses your Amazon Web Services
Identity and Access Management (AWS IAM) credentials to authenticate your
user.

The following code snippets show how to specify the authentication mechanism,
using the following placeholders:

* `username` - value of your `AWS_ACCESS_KEY_ID`
* `password` - value your `AWS_SECRET_ACCESS_KEY`
* `hostname` - network address of your MongoDB server, accessible by your client
* `port` - port number of your MongoDB server
* `authenticationDb` - MongoDB database that contains your user's
  authentication data. If you omit this parameter, the driver uses the
  default value `admin`.
* `awsSessionToken` - value of your `AWS_SESSION_TOKEN` *(optional)*

### Connection String

To specify the mechanism using a *connection string*, assign the
`authMechanism` parameter the value `MONGODB-AWS` in your connection string.
Your code to instantiate a `MongoClient` should resemble the following:

```java
MongoClient mongoClient = MongoClients.create("mongodb://<username>:<password>@<hostname>:<port>/?authSource=<authenticationDb>&authMechanism=MONGODB-AWS");
```

If you need to specify an AWS session token, include it in the
`authMechanismProperties` parameter as follows using the format
`AWS_SESSION_TOKEN:<awsSessionToken>`. Your code to instantiate a
`MongoClient` with a session token should resemble the following:

```java
MongoClient mongoClient = MongoClients.create("mongodb://<username>:<password>@<hostname>:<port>/?authSource=<authenticationDb>&authMechanism=MONGODB-AWS&authMechanismProperties=AWS_SESSION_TOKEN:<awsSessionToken>");
```

### MongoCredential

To specify the mechanism using the *MongoCredential* class, use the
[`createAwsCredential()`]({{< apiref "mongodb-driver-core" "com/mongodb/MongoCredential.html#createAwsCredential(java.lang.String,char%5B%5D)" >}})
method. Your code to instantiate a `MongoClient` should resemble the following:

```java
MongoCredential credential = MongoCredential.createAwsCredential("<username>", "<password>".toCharArray());

MongoClient mongoClient = MongoClients.create(
    MongoClientSettings.builder()
        .applyToClusterSettings(builder ->
            builder.hosts(Arrays.asList(new ServerAddress("<hostname>", "<port>"))))
        .credential(credential)
        .build());
```

If you need to specify an AWS session token, you can add it using one of
the following choices:

#### Specify your AWS Session Token in a Connection String

If you prefer to pass the AWS session token in the connection string, you
specify your authentication mechanism in the `authMechanism` parameter
and your session token in the `authMechanismProperties` parameter. Then,
add it to your `MongoClientSettings` by calling the
[`applyConnectionString()`]({{ "mongodb-driver-core" "com/mongodb/MongoClientSettings.Builder.html#applyConnectionString(com.mongodb.ConnectionString)" >}})
method as follows:

```java
MongoCredential credential = MongoCredential.createAwsCredential("<username>", "<password>".toCharArray());
String connectionString = "mongodb://<hostname>:<port>/?authSource=<authenticationDb>&authMechanism=MONGODB-AWS&authMechanismProperties=AWS_SESSION_TOKEN:<awsSessionToken>");

MongoClient mongoClient = MongoClients.create(
    MongoClientSettings.builder()
        .applyConnectionString(connectionString)
        .credential(credential)
        .build());
```

#### Specify your AWS Session Token in a MongoCredential

You can include your AWS session token in your `MongoCredential` instance by
specifying it in a call to the
[`withMechanismProperty()`]({{< apiref "mongodb-driver-core" "com/mongodb/MongoCredential.html#withMechanismProperty(java.lang.String,T)" >}})
method as shown below:

```java
MongoCredential.createAwsCredential("<username>", "<password>".toCharArray()) .withMechanismProperty("AWS_SESSION_TOKEN", "<awsSessionToken>")
```

#### Specify your AWS Session Token in an Environment Variable

In your client execution environment, set an environment variable called
`AWS_SESSION_TOKEN` and assign your token to it. The value is
automatically picked up by your MongoClient when you specify the
`MONGODB-AWS` authentication mechanism.

## Kerberos (GSSAPI) {#gssapi}

[MongoDB Enterprise](http://www.mongodb.com/products/mongodb-enterprise) supports proxy
authentication through Kerberos service. To create a credential of type
[Kerberos (GSSAPI)]({{<docsref "core/authentication/#kerberos-authentication" >}}), use the
[`createGSSAPICredential`]({{< apiref "mongodb-driver-core" "com/mongodb/MongoCredential.html#createGSSAPICredential(java.lang.String)" >}})
static factory method:

```java
String user;   // The Kerberos user name, including the realm, e.g. "user1@MYREALM.ME"
// ...
MongoCredential credential = MongoCredential.createGSSAPICredential(user);
```

and then construct a MongoClient with that credential.  Using the new (since 3.7) MongoClient API:

```java
MongoClient mongoClient = MongoClients.create(
        MongoClientSettings.builder()
                .applyToClusterSettings(builder ->
                        builder.hosts(Arrays.asList(new ServerAddress("host1", 27017))))
                .credential(credential)
                .build());
```

or using the legacy MongoClient API:

```java
MongoClient mongoClient = new MongoClient(
        MongoClientSettings.builder()
                .applyToClusterSettings(builder ->
                        builder.hosts(Arrays.asList(new ServerAddress("host1", 27017))))
                .credential(credential)
                .build());
```

Or use a connection string that explicitly specifies the `authMechanism=GSSAPI`. Using the new MongoClient API:

```java
MongoClient mongoClient = MongoClients.create("mongodb://username%40REALM.ME@host1/?authMechanism=GSSAPI");
```

or using the legacy MongoClient API:

```java
MongoClient mongoClient = new MongoClient("mongodb://username%40REALM.ME@host1/?authMechanism=GSSAPI");
```
{{% note %}}

The method refers to the `GSSAPI` authentication mechanism instead
of `Kerberos` because technically the driver authenticates via
the [`GSSAPI`](https://tools.ietf.org/html/rfc4752) SASL mechanism.

{{% /note %}}

To successfully authenticate via Kerberos, the application typically
must specify several system properties so that the underlying GSSAPI
Java libraries can acquire a Kerberos ticket:

```java
java.security.krb5.realm=MYREALM.ME
java.security.krb5.kdc=mykdc.myrealm.me
```

Depending on the Kerberos setup, additional property specifications may be required, either via the application code or, in some cases, the [withMechanismProperty()]({{< apiref "mongodb-driver-core" "com/mongodb/MongoCredential.html#withMechanismProperty(java.lang.String,T)" >}}) method of the `MongoCredential` instance:

- **[`SERVICE_NAME`]({{< apiref "mongodb-driver-core" "com/mongodb/MongoCredential.html#SERVICE_NAME_KEY" >}})**


- **[`CANONICALIZE_HOST_NAME`]({{< apiref "mongodb-driver-core" "com/mongodb/MongoCredential.html#CANONICALIZE_HOST_NAME_KEY" >}})**


- **[`JAVA_SUBJECT`]({{< apiref "mongodb-driver-core" "com/mongodb/MongoCredential.html#JAVA_SUBJECT_KEY" >}})**

- **[`JAVA_SASL_CLIENT_PROPERTIES`]({{< apiref "mongodb-driver-core" "com/mongodb/MongoCredential.html#JAVA_SASL_CLIENT_PROPERTIES_KEY" >}})**

For example, to specify the `SERVICE_NAME` property via the `MongoCredential` object:


```java
credential = credential.withMechanismProperty(MongoCredential.SERVICE_NAME_KEY, "othername");
```

Or via the `ConnectionString`:

```
mongodb://username%40MYREALM.com@myserver/?authMechanism=GSSAPI&authMechanismProperties=SERVICE_NAME:othername
```

{{% note %}}
On Windows, Oracle's JRE uses [LSA](https://msdn.microsoft.com/en-us/library/windows/desktop/aa378326.aspx) rather than
[SSPI](https://msdn.microsoft.com/en-us/library/windows/desktop/aa380493.aspx) in its implementation of GSSAPI, which limits
interoperability with Windows Active Directory and in particular the ability to implement single sign-on.

- [JDK-8054026](https://bugs.openjdk.java.net/browse/JDK-8054026)
- [JDK-6722928](https://bugs.openjdk.java.net/browse/JDK-6722928)
- [SO 23427343](https://stackoverflow.com/questions/23427343/cannot-retrieve-tgt-despite-allowtgtsessionkey-registry-entry)
{{% /note %}}


## LDAP (PLAIN)

[MongoDB Enterprise](http://www.mongodb.com/products/mongodb-enterprise) supports proxy authentication through a Lightweight Directory
Access Protocol (LDAP) service. To create a credential of type [LDAP]({{<docsref "core/authentication/#ldap-proxy-authority-authentication" >}}) use the
[`createPlainCredential`]({{< apiref "mongodb-driver-core" "com/mongodb/MongoCredential.html#createPlainCredential(java.lang.String,java.lang.String,char[])" >}}) static factory method:

```java
String user;          // The LDAP user name
char[] password;      // The LDAP password
// ...
MongoCredential credential = MongoCredential.createPlainCredential(user, "$external", password);
```

and then construct a MongoClient with that credential.  Using the new (since 3.7) MongoClient API:

```java
MongoClient mongoClient = MongoClients.create(
        MongoClientSettings.builder()
                .applyToClusterSettings(builder ->
                        builder.hosts(Arrays.asList(new ServerAddress("host1", 27017))))
                .credential(credential)
                .build());
```

or using the legacy MongoClient API:

```java
MongoClient mongoClient = new MongoClient(
        MongoClientSettings.builder()
                .applyToClusterSettings(builder ->
                        builder.hosts(Arrays.asList(new ServerAddress("host1", 27017))))
                .credential(credential)
                .build());
```

Or use a connection string that explicitly specifies the `authMechanism=PLAIN`.  Using the new MongoClient API:

```java
MongoClient mongoClient = MongoClients.create("mongodb://user1@host1/?authSource=$external&authMechanism=PLAIN");
```

or using the legacy MongoClient API:

```java
MongoClient mongoClient = new MongoClient("mongodb://user1@host1/?authSource=$external&authMechanism=PLAIN");
```

{{% note %}}
The method refers to the `plain` authentication mechanism instead
of `LDAP` because technically the driver authenticates via the
[`PLAIN`](https://www.ietf.org/rfc/rfc4616.txt) SASL mechanism.
{{% /note %}}
