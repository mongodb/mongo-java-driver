+++
date = "2016-05-29T13:26:13-04:00"
title = "Connect to MongoDB"
[menu.main]
  parent = "Reactive Tutorials"
  identifier = "Reactive Connect to MongoDB"
  weight = 10
  pre = "<i class='fa'></i>"
+++

## Connect to MongoDB

Use [`MongoClients.create()`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoClients.html" >}}) to make a connection to a running MongoDB instance.

{{% note class="important" %}}
The following examples are not meant to provide an exhaustive list
of ways to instantiate `MongoClient`. For a complete list of MongoClients factory methods, see the 
[`MongoClients API documentation`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoClients.html" >}}).

{{% /note %}}

{{% note %}}
It is *strongly recommended* that system keep-alive settings should be configured with shorter timeouts. 

See the 
['does TCP keep-alive time affect MongoDB deployments?']({{<docsref "/faq/diagnostics/#does-tcp-keepalive-time-affect-mongodb-deployments" >}}) 
documentation for more information.
{{% /note %}}

## Prerequisites

- Running MongoDB deployments to which to connect. For example, to connect to a standalone, you must have a running standalone.

- The MongoDB Driver.  See [Installation]({{< relref "driver-reactive/getting-started/installation.md" >}}) for instructions on how to install the MongoDB driver.

- The following import statements:

```java
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.MongoClientSettings;
import com.mongodb.ConnectionString;
import com.mongodb.ServerAddress;
import com.mongodb.MongoCredential;

import java.util.Arrays;
```

## `MongoClient`

A [`MongoClient`]({{< apiref "mongodb-driver-reactivestreams" "com/mongodb/reactivestreams/client/MongoClient.html" >}}) instance represents a pool of connections
to the database; you will only need one instance of class `MongoClient` even with multiple concurrent operations.

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

```java
    MongoClient mongoClient = MongoClients.create()
```

- You can explicitly specify the hostname to connect to a MongoDB
  instance running on the specified host on port `27017`:

```java
    MongoClient mongoClient = MongoClients.create("mongodb://host1");
```

- You can explicitly specify the hostname and the port:

    ```java
    MongoClient mongoClient = MongoClients.create("mongodb://host1:27017");
    ```

## Connect to a Replica Set

To connect to a [replica set]({{<docsref "replication/" >}}), you must specify one or more members to the `MongoClients` create method.

{{% note %}}
MongoDB will auto-discover the primary and the secondaries.
{{% /note %}}

- You can specify the members using a [`ConnectionString`]({{< apiref "mongodb-driver-core" "com/mongodb/ConnectionString.html" >}}):

  - To specify at least two members of the replica set:

```java
    MongoClient mongoClient = MongoClients.create("mongodb://host1:27017,host2:27017,host3:27017");
```

  - With at least one member of the replica set and the `replicaSet` option specifying the replica set name:

```java
    MongoClient mongoClient = MongoClients.create("mongodb://host1:27017,host2:27017,host3:27017/?replicaSet=myReplicaSet");
```

- You can specify a list of the all the replica set members' [`ServerAddress`]({{< apiref "mongodb-driver-core" "com/mongodb/ServerAddress.html" >}}):

```java
    MongoClient mongoClient = MongoClients.create(
            MongoClientSettings.builder()
                    .applyToClusterSettings(builder ->
                            builder.hosts(Arrays.asList(
                                    new ServerAddress("host1", 27017),
                                    new ServerAddress("host2", 27017),
                                    new ServerAddress("host3", 27017))))
                    .build());
```


## Connect to a Sharded Cluster

To connect to a [sharded cluster]({{<docsref "sharding/" >}}), specify the `mongos` instance
or instances to a `MongoClients` create method.

To connect to a single `mongos` instance:

- You can specify the hostname and the port in a [`ConnectionString`]({{< apiref "mongodb-driver-core" "com/mongodb/ConnectionString.html" >}})

```java
    MongoClient mongoClient = MongoClients.create( "mongodb://localhost:27017" );
```

or leave the connection string out if the `mongos` is running on localhost:27017:

```java
    MongoClient mongoClient = MongoClients.create();
```

To connect to multiple `mongos` instances:

- You can specify the [`ConnectionString`]({{< apiref "mongodb-driver-core" "com/mongodb/ConnectionString.html" >}}) with their hostnames and ports:

    ```java
    MongoClient mongoClient = MongoClients.create("mongodb://host1:27017,host2:27017");
    ```

- You can specify a list of the `mongos` instances' [`ServerAddress`]({{< apiref "mongodb-driver-core" "com/mongodb/ServerAddress.html" >}}):

```java
    MongoClient mongoClient = MongoClients.create(
            MongoClientSettings.builder()
                    .applyToClusterSettings(builder ->
                            builder.hosts(Arrays.asList(
                                    new ServerAddress("host1", 27017),
                                    new ServerAddress("host2", 27017))))
                    .build());
```

## Connection Options

You can specify the connection settings using either the
`ConnectionString` or `MongoClientSettings` or both.

For example, you can specify TLS/SSL and authentication setting in the connection string:

```java
    MongoClient mongoClient = MongoClients.create("mongodb://user1:pwd1@host1/?authSource=db1&ssl=true");
```

You can also use `MongoClientSettings` to specify TLS/SSL and the `MongoCredential` for the authentication information:

```java
    String user; // the user name
    String database; // the name of the database in which the user is defined
    char[] password; // the password as a character array
    // ...

    MongoCredential credential = MongoCredential.createCredential(user, database, password);

    MongoClientSettings settings = MongoClientSettings.builder()
            .credential(credential)
            .applyToSslSettings(builder -> builder.enabled(true))
            .applyToClusterSettings(builder -> 
                builder.hosts(Arrays.asList(new ServerAddress("host1", 27017))))
            .build();

    MongoClient mongoClient = MongoClients.create(settings);
```

Finally, in some cases you may need to combine a connection string with programmatic configuration:

```java
    ConnectionString connectionString = new ConnectionString("mongodb://host1:27107,host2:27017/?ssl=true");
    CommandListener myCommandListener = ...;
    MongoClientSettings settings = MongoClientSettings.builder()
            .addCommandListener(myCommandListener)
            .applyConnectionString(connectionString)
            .build();

    MongoClient mongoClient = MongoClients.create(settings);
```
