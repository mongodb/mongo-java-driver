+++
date = "2016-05-29T13:26:13-04:00"
title = "Connect to MongoDB"
[menu.main]
  parent = "Scala Tutorials"
  identifier = "Scala Connect to MongoDB"
  weight = 10
  pre = "<i class='fa'></i>"
+++

## Connect to MongoDB

Use [`MongoClient()`]({{< apiref "org/mongodb/scala/MongoClient.html">}}) to make a connection to a running MongoDB instance.

{{% note class="important" %}}
The following examples are not meant to provide an exhaustive list
of ways to instantiate `MongoClient`. For a complete list of MongoClient companion methods, see the 
[`MongoClient API documentation`]({{< apiref "org/mongodb/scala/MongoClient.html">}}).

{{% /note %}}

{{% note %}}
It is *strongly recommended* that system keep-alive settings should be configured with shorter timeouts. 

See the 
['does TCP keep-alive time affect MongoDB deployments?']({{<docsref "/faq/diagnostics/#does-tcp-keepalive-time-affect-mongodb-deployments">}}) 
documentation for more information.
{{% /note %}}

## Prerequisites

- Running MongoDB deployments to which to connect. For example, to connect to a standalone, you must have a running standalone.

- The MongoDB Driver.  See [Installation]({{< relref "driver-scala/getting-started/installation.md" >}}) for instructions on how to install the MongoDB driver.

- The following import statements:

```scala
import org.mongodb.scala._

import scala.collection.JavaConverters._
```

## `MongoClient`

A [`MongoClient`]({{< apiref "org/mongodb/scala/MongoClient.html">}}) instance represents a pool of connections
to the database; you will only need one instance of class `MongoClient` even with multiple threads.

{{% note class="important" %}}

Typically you only create one `MongoClient` instance for a given MongoDB deployment (e.g. standalone, replica set, or a sharded cluster)
 and use it across your application. However, if you do create multiple instances:

 - All resource usage limits (e.g. max connections, etc.) apply per `MongoClient` instance.

 - To dispose of an instance, call `MongoClient.close()` to clean up resources.
{{% /note %}}

## Connect to a Standalone MongoDB Instance

To connect to a standalone MongoDB instance:

- You can create a `MongoClient` object without any parameters to
  connect to a MongoDB instance running on localhost on port `27017`:

    ```scala
    val mongoClient = MongoClient()
    ```

- You can explicitly specify the hostname to connect to a MongoDB
  instance running on the specified host on port `27017`:

    ```scala
    val mongoClient = MongoClient("mongodb://host1")
    ```

- You can explicitly specify the hostname and the port:

    ```scala
    val mongoClient = MongoClient("mongodb://host1:27017")
    ```

## Connect to a Replica Set

To connect to a [replica set]({{<docsref "replication/">}}), you must specify one or more members to the `MongoClients` create method.

{{% note %}}
MongoDB will auto-discover the primary and the secondaries.
{{% /note %}}

- You can specify the members using a [`ConnectionString`]({{< apiref "org/mongodb/scala/ConnectionString$.html">}}):

  - To specify at least two members of the replica set:

```scala
val mongoClient = MongoClient("mongodb://host1:27017,host2:27017,host3:27017")
```

  - With at least one member of the replica set and the `replicaSet` option specifying the replica set name:

```scala
val mongoClient = MongoClient("mongodb://host1:27017,host2:27017,host3:27017/?replicaSet=myReplicaSet")
```

- You can specify a list of the all the replica set members' [`ServerAddress`]({{<apiref "org/mongodb/scala/ServerAddress$.html">}}):

```scala
val mongoClient = MongoClient(
  MongoClientSettings.builder()
    .applyToClusterSettings((builder: ClusterSettings.Builder) => builder.hosts(List(
        new ServerAddress("host1", 27017),
        new ServerAddress("host2", 27017),
        new ServerAddress("host3", 27017)).asJava))
    .build())
```


## Connect to a Sharded Cluster

To connect to a [sharded cluster]({{<docsref "sharding/">}}), specify the `mongos` instance
or instances to a `MongoClients` create method.

To connect to a single `mongos` instance:

- You can specify the hostname and the port in a [`ConnectionString`]({{< apiref "org/mongodb/scala/ConnectionString$.html">}})

```scala
val mongoClient = MongoClient( "mongodb://localhost:27017" )
```

or leave the connection string out if the `mongos` is running on localhost:27017:

```scala
val mongoClient = MongoClient()
```

To connect to multiple `mongos` instances:

- You can specify the [`ConnectionString`]({{< apiref "org/mongodb/scala/ConnectionString$.html">}}) with their hostnames and ports:

    ```scala
    val mongoClient = MongoClient("mongodb://host1:27017,host2:27017")
    ```

- You can specify a list of the `mongos` instances' [`ServerAddress`]({{ <apiref "com/mongodb/ServerAddress.html">}}):

```scala
val mongoClient = MongoClient(
  MongoClientSettings.builder()
    .applyToClusterSettings((builder: ClusterSettings.Builder) => builder.hosts(List(
        new ServerAddress("host1", 27017),
        new ServerAddress("host2", 27017)).asJava))
    .build())
```

## Connection Options

You can specify the connection settings using either the
`ConnectionString` or `MongoClientSettings` or both.

For example, you can specify TLS/SSL and authentication setting in the connection string:

```scala
val mongoClient = MongoClient("mongodb://user1:pwd1@host1/?authSource=db1&ssl=true")
```

You can also use `MongoClientSettings` to specify TLS/SSL and the `MongoCredential` for the authentication information:

```scala
val user: String = ???          // the user name
val source: String = ???        // the source where the user is defined
val password: Array[Char] = ??? // the password as a character array
// ...
val credential = MongoCredential.createCredential(user, source, password)

val mongoClient: MongoClient = MongoClient(
  MongoClientSettings.builder()
    .applyToSslSettings((builder: SslSettings.Builder) => builder.enabled(true))
    .applyToClusterSettings((builder: ClusterSettings.Builder) => builder.hosts(List(new ServerAddress("host1", 27017)).asJava))
    .credential(credential)
    .build())
```

Finally, in some cases you may need to combine a connection string with programmatic configuration:

```scala
val connectionString = ConnectionString("mongodb://host1:27107,host2:27017/?ssl=true")
val myCommandListener: CommandListener = ???

val mongoClient = MongoClient( 
  MongoClientSettings.builder()
    .addCommandListener(myCommandListener)
    .applyConnectionString(connectionString)
    .build())
```
