+++
date = "2015-03-19T12:53:30-04:00"
title = "Connect to MongoDB"
[menu.main]
  parent = "Async Tutorials"
  identifier = "Async Connection Settings"
  weight = 10
  pre = "<i class='fa'></i>"
+++

## Connect to MongoDB

To make a connection to a running MongoDB instance, use [`MongoClients.create`]({{< apiref "com/mongodb/async/client/MongoClients.html#create()" >}}) to create a new [`MongoClient`]({{< apiref "com/mongodb/async/client/MongoClient.html">}}) instance.

A [`MongoClient`]({{< apiref "com/mongodb/async/client/MongoClient.html">}}) instance actually represents a pool of connections
to the database; you will only need one instance of class
`MongoClient` even with multiple concurrently executing asynchronous operations.

{{% note class="important" %}}
Typically you only create one `MongoClient` instance for a given MongoDB
deployment (e.g. standalone, replica set, or a sharded cluster) and use it across your application. However, if you do create multiple instances:

-  All resource usage limits (max connections, etc.) apply per `MongoClient` instance.
-  To dispose of an instance, call `MongoClient.close()` to clean up resources.
{{% /note %}}

{{% note %}}
The 3.5 release deprecated socket keep-alive settings, also socket keep-alive checks are now on by default.
It is *strongly recommended* that system keep-alive settings should be configured with shorter timeouts. 

See the 
['does TCP keep-alive time affect MongoDB deployments?']({{<docsref "/faq/diagnostics/#does-tcp-keepalive-time-affect-mongodb-deployments">}}) 
documentation for more information.
{{% /note %}}


## Prerequisites

- Running MongoDB deployments to which to connect. For example, to connect to a standalone, you must have a running standalone.

- The MongoDB Asynchronous Driver.  See [Installation]({{< relref "driver-async/getting-started/installation.md" >}}) for instructions on how to install the MongoDB async driver.

- The following import statements:

    ```java
    import com.mongodb.ConnectionString;
    import com.mongodb.ServerAddress;
    import com.mongodb.async.client.*;
    import com.mongodb.connection.ClusterSettings;
    import java.util.Arrays;
    import static java.util.Arrays.asList;
    ```

## Connect to a Standalone MongoDB Instance

To connect to a standalone MongoDB instance:

- You can call [`MongoClients.create()`]({{< apiref "com/mongodb/async/client/MongoClients.html#create()" >}}) without any parameters to connect to a MongoDB instance running on localhost on port ``27017``:

    ```java
    MongoClient mongoClient = MongoClients.create();
    ```

- You can call [`MongoClients.create()`]({{< apiref "com/mongodb/async/client/MongoClients.html#create(java.lang.String)" >}}) with a string that specifies the connection string:

    ```java
    MongoClient mongoClient = MongoClients.create("mongodb://localhost");
    ```

    The connection string mostly follows [RFC 3986](http://tools.ietf.org/html/rfc3986), with the exception of the domain name. For MongoDB, it is possible to list multiple domain names separated by a comma. For more information on the connection string, see [connection string]({{< docsref "reference/connection-string" >}}).

- You can call [`MongoClients.create()`]({{< apiref "com/mongodb/async/client/MongoClients.html#create(com.mongodb.ConnectionString)" >}}) with a [`ConnectionString`]({{< apiref "com/mongodb/ConnectionString.html">}}) object:

    ```java
    MongoClient mongoClient = MongoClients.create(new ConnectionString("mongodb://localhost"));
    ```

- You can call [`MongoClients.create()`]({{< apiref "com/mongodb/async/client/MongoClients.html#create(com.mongodb.MongoClientSettings)" >}}) with a [`MongoClientSettings`]({{< apiref "com/mongodb/MongoClientSettings.html">}}) object. To specify the host information, use the [`ClusterSettings`]({{<apiref "com/mongodb/connection/ClusterSettings.html">}}).

    ```java
    ClusterSettings clusterSettings = ClusterSettings.builder()
                                      .hosts(asList(new ServerAddress("localhost")))
                                      .build();
    MongoClientSettings settings = MongoClientSettings.builder()
                                      .clusterSettings(clusterSettings).build();
    MongoClient mongoClient = MongoClients.create(settings);
    ```

{{% note class="tip" %}}
[`MongoClientSettings`]({{< apiref "com/mongodb/MongoClientSettings" >}}) provide more configuration options than a connection string.
{{% /note %}}

## Connect to a Replica Set

To connect to a [replica set]({{<docsref "replication/">}}), specify at least one or more members of the replica set in the connection string or `MongoClientSettings` object and pass to [`MongoClients.create()`]({{< apiref "com/mongodb/async/client/MongoClients.html#create()" >}}).

{{% note %}}
MongoDB will auto-discover the primary and the secondaries.
{{% /note %}}


- You can call [`MongoClients.create()`]({{< apiref "com/mongodb/async/client/MongoClients.html#create(java.lang.String)" >}}) with a connection string that specifies the members of the replica set:

  - Specify at least two members of the replica set if you are not specifying the replica set name

      ```java
      MongoClient mongoClient = MongoClients.create(
                  "mongodb://host1:27017,host2:27017,host3:27017");
      ```

  - Specify at least one member of the replica set and the replica set name

      ```java
      MongoClient mongoClient = MongoClients.create(
                  "mongodb://host1:27017,host2:27017,host3:27017/?replicaSet=myReplicaSet");
      ```

- You can call [`MongoClients.create()`]({{< apiref "com/mongodb/async/client/MongoClients.html#create(com.mongodb.ConnectionString)" >}}) with a [`ConnectionString`]({{< apiref "com/mongodb/ConnectionString.html">}}) object that specifies the members of the replica set:

  - Specify at least two members of the replica set if you are not specifying the replica set name

      ```java
      MongoClient mongoClient = MongoClients.create(
            new ConnectionString("mongodb://host1:27017,host2:27017,host3:27017"));
      ```

  - Specify at least one member of the replica set and the replica set name:

      ```java
      MongoClient mongoClient = MongoClients.create(
          new ConnectionString("mongodb://host1:27017,host2:27017,host3:27017/?replicaSet=myReplicaSet"));
      ```

- You can call [`MongoClients.create()`]({{< apiref "com/mongodb/async/client/MongoClients.html#create(com.mongodb.MongoClientSettings)" >}}) with a [`MongoClientSettings`]({{< apiref "com/mongodb/MongoClientSettings.html">}}) object. To specify the host information of the replica set members, use [`ClusterSettings`]({{<apiref "com/mongodb/connection/ClusterSettings.html">}}).

      ```java
      ClusterSettings clusterSettings = ClusterSettings.builder()
                                          .hosts(asList(
                                              new ServerAddress("host1", 27017),
                                              new ServerAddress("host2", 27017),
                                              new ServerAddress("host3", 27017)))
                                          .build();

      MongoClientSettings settings = MongoClientSettings.builder()
                                          .clusterSettings(clusterSettings).build();

      MongoClient mongoClient = MongoClients.create(settings);
      ```

## Connect to a Sharded Cluster

To connect to a [sharded cluster]({{<docsref "sharding/">}}), specify the `mongos` instance or instances to the `MongoClient` constructor.

To connect to a single `mongos` instance:

- You can call [`MongoClients.create()`]({{< apiref "com/mongodb/async/client/MongoClients.html#create()" >}}) without any parameters to connect to a :program:`mongos` running on localhost on port ``27017``:

    ```java
    MongoClient mongoClient = MongoClients.create();
    ```

- You can call [`MongoClients.create()`]({{< apiref "com/mongodb/async/client/MongoClients.html#create(java.lang.String)" >}}) with a string that specifies the host information of the `mongos` instance in the connection URI:

    ```java
    MongoClient mongoClient = MongoClients.create("mongodb://localhost");
    ```

- You can call [`MongoClients.create()`]({{< apiref "com/mongodb/async/client/MongoClients.html#create(com.mongodb.ConnectionString)" >}}) with a [`ConnectionString`]({{< apiref "com/mongodb/ConnectionString.html">}}) object that specifies the host information of the `mongos` instance:

    ```java
    MongoClient mongoClient = MongoClients.create(
                new ConnectionString("mongodb://localhost"));
    ```

- You can call [`MongoClients.create()`]({{< apiref "com/mongodb/async/client/MongoClients.html#create(com.mongodb.MongoClientSettings)" >}}) with a [`MongoClientSettings`]({{< apiref "com/mongodb/MongoClientSettings.html">}}) object. To specify the host information of the `mongos` instance, use [`ClusterSettings`]({{<apiref "com/mongodb/connection/ClusterSettings.html">}}):

    ```java
    ClusterSettings clusterSettings = ClusterSettings.builder()
                                        .hosts(asList(new ServerAddress("localhost")))
                                        .build();
    MongoClientSettings settings = MongoClientSettings.builder()
                                        .clusterSettings(clusterSettings)
                                        .build();
    MongoClient mongoClient = MongoClients.create(settings);
    ```

To connect to multiple `mongos` instances, specify the host and port of the `mongos` instances:

- You can call [`MongoClients.create()`]({{< apiref "com/mongodb/async/client/MongoClients.html#create(java.lang.String)" >}}) with a string that specifies the host and port information of the `mongos` instances in the connection URI:

    ```java
    MongoClient mongoClient = MongoClients.create("mongodb://host1:27017,host2:27017");
    ```

- You can call [`MongoClients.create()`]({{< apiref "com/mongodb/async/client/MongoClients.html#create(com.mongodb.ConnectionString)" >}}) with a [`ConnectionString`]({{< apiref "com/mongodb/ConnectionString.html">}}) object that specifies the host and port information of the `mongos` instances:

    ```java
    MongoClient mongoClient = MongoClients.create(
                new ConnectionString("mongodb://host1:27017,host2:27017"));
    ```

- You can call [`MongoClients.create()`]({{< apiref "com/mongodb/async/client/MongoClients.html#create(com.mongodb.MongoClientSettings)" >}}) with a [`MongoClientSettings`]({{< apiref "com/mongodb/MongoClientSettings.html">}}) object. To specify the host information of the `mongos` instances, use [`ClusterSettings`]({{<apiref "com/mongodb/connection/ClusterSettings.html">}}):


    ```java
    ClusterSettings clusterSettings = ClusterSettings.builder()
                                          .hosts(asList(
                                              new ServerAddress("host1", 27017),
                                              new ServerAddress("host2", 27017)))
                                          .build();


    MongoClientSettings settings = MongoClientSettings.builder()
                                          .clusterSettings(clusterSettings).build();
    MongoClient mongoClient = MongoClients.create(settings);
    ```


## Connection Options

You can specify the connection settings using either the
connection string (or `ConnectionString` object) or the `MongoClientSettings` or both.


### Netty Configuration

{{% note %}}
Netty is an optional dependency of the asynchronous driver. If your application requires Netty, it must explicitly add a dependency to
Netty artifacts.  The driver is currently tested against Netty 4.1.
{{% /note %}}

To configure the driver to use Netty,  

- Include the `streamType` option set to `netty` in the connection string

    ```java
    MongoClient client = MongoClients.create("mongodb://localhost/?streamType=netty");
    ```

- Configure [`MongoClientSettings`]({{< apiref "com/mongodb/MongoClientSettings.Builder.html#streamFactoryFactory(com.mongodb.connection.StreamFactoryFactory)">}}) with the `StreamFactory` set to use Netty:

    ```java
    MongoClient client = MongoClients.create(MongoClientSettings.builder()
            .applyConnectionString(new ConnectionString("mongodb://localhost"))
            .streamFactoryFactory(NettyStreamFactoryFactory.builder().build())
            .build());

    ```

{{% note %}}
The streamType connection string query parameter is deprecated as of the 3.10 release and will be removed in the next major release.
{{% /note %}}

{{% note %}}
Netty may also be configured by setting the `org.mongodb.async.type` system property to `netty`, but this should be considered as
deprecated as of the 3.1 driver release, and will be removed in the next major release.
{{% /note %}}
