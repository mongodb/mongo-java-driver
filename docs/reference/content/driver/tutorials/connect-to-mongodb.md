+++
date = "2016-05-29T13:26:13-04:00"
title = "Connect to MongoDB"
[menu.main]
  parent = "Sync Tutorials"
  identifier = "Connect to MongoDB"
  weight = 10
  pre = "<i class='fa'></i>"
+++

## Connect to MongoDB

Use [`MongoClient()`]({{< apiref "com/mongodb/MongoClient.html">}}) to make a connection to a running MongoDB instance.

{{% note class="important" %}}
The following examples are not meant to provide an exhaustive list
of ways to instantiate `MongoClient`. For a complete list of the
MongoClient constructors, see
[`MongoClient() API documentation`]({{< apiref "com/mongodb/MongoClient.html">}}).
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

- The MongoDB Driver.  See [Installation]({{< relref "driver/getting-started/installation.md" >}}) for instructions on how to install the MongoDB driver.

- The following import statements:

    ```java
    import com.mongodb.MongoClient;
    import com.mongodb.MongoClientURI;
    import com.mongodb.ServerAddress;
    import com.mongodb.MongoCredential;
    import com.mongodb.MongoClientOptions;

    import java.util.Arrays;
    ```

## `MongoClient`

The [`MongoClient()`]({{< apiref "com/mongodb/MongoClient.html">}}) instance represents a pool of connections
to the database; you will only need one instance of class
`MongoClient` even with multiple threads.

{{% note class="important" %}}

 Typically you only create one `MongoClient` instance for a given MongoDB deployment (e.g. standalone, replica set, or a sharded cluster) and use it across your application. However, if you do create multiple instances:

 - All resource usage limits (e.g. max connections, etc.) apply per `MongoClient` instance.

 - To dispose of an instance, call `MongoClient.close()` to clean up resources.
{{% /note %}}

## Connect to a Standalone MongoDB Instance

To connect to a standalone MongoDB instance:

- You can instantiate a `MongoClient` object without any parameters to
  connect to a MongoDB instance running on localhost on port `27017`:

    ```java
    MongoClient mongoClient = new MongoClient();
    ```

- You can explicitly specify the hostname to connect to a MongoDB
  instance running on the specified host on port `27017`:

    ```java
    MongoClient mongoClient = new MongoClient( "host1" );
    ```

- You can explicitly specify the hostname and the port:

    ```java
    MongoClient mongoClient = new MongoClient( "host1" , 27017 );
    ```

- You can specify the
  [`MongoClientURI`]({{< apiref "/com/mongodb/MongoClientURI.html">}}) connection string.

    ```java
    MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://host1:27017"));
    ```

## Connect to a Replica Set

To connect to a [replica set]({{<docsref "replication/">}}), you must specify  one or more members to the
`MongoClient` constructor.

{{% note %}}
MongoDB will auto-discover the primary and the secondaries.
{{% /note %}}

- You can specify the members using the [`MongoClientURI`]({{< apiref "/com/mongodb/MongoClientURI.html">}}) connection string:

  - To specify at least two members of the replica set:

        ```java
        MongoClient mongoClient = new MongoClient(
            new MongoClientURI("mongodb://host1:27017,host2:27017,host3:27017"));
        ```

  - With at least one member of the replica set and the `replicaSet` option:

        ```java
        MongoClient mongoClient = new MongoClient(
            new MongoClientURI(
              "mongodb://host1:27017,host2:27017,host3:27017/?replicaSet=myReplicaSet"));
        ```

- You can specify a list of the all the replica set members' [`ServerAddress`]({{<apiref "com/mongodb/ServerAddress.html">}}):

    ```java
    MongoClient mongoClient = new MongoClient(
    Arrays.asList(new ServerAddress("host1", 27017),
                  new ServerAddress("host2", 27017),
                  new ServerAddress("host3", 27017)));
    ```


## Connect to a Sharded Cluster

To connect to a [sharded cluster]({{<docsref "sharding/">}}), specify the `mongos` instance
or instances to the `MongoClient` constructor.

To connect to a single `mongos` instance:

- You can specify the hostname and the port (or you can omit the
  parameters if `mongos` is running on `localhost` and port
  `27017`)

    ```java
    MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
    ```

- You can specify the [`MongoClientURI`]({{< apiref "/com/mongodb/MongoClientURI.html">}}) connection string:

    ```java
    MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
    ```

To connect to multiple `mongos` instances:

- You can specify the [`MongoClientURI`]({{< apiref "/com/mongodb/MongoClientURI.html">}}) connection string with their hostnames and ports:

    ```java
    MongoClient mongoClient = new MongoClient(
       new MongoClientURI("mongodb://host1:27017,host2:27017"));
    ```

- You can specify a list of the `mongos` instances'
  [`ServerAddress`]({{ <apiref "com/mongodb/ServerAddress.html">}}):

    ```java
    MongoClient mongoClient = new MongoClient(
       Arrays.asList(new ServerAddress("host1", 27017),
                     new ServerAddress("host2", 27017)));
    ```

## Connection Options

You can specify the connection settings using either the
`MongoClientURI` or `MongoClientOptions` or both.

For example, you can specify TLS/SSL and authentication setting in the
`MongoClientURI` connection string:

```java
MongoClientURI uri = new MongoClientURI("mongodb://user1:pwd1@host1/?authSource=db1&ssl=true");
MongoClient mongoClient = new MongoClient(uri);
```

You can also use `MongoClientOptions` to specify TLS/SSL and the
`MongoCredential` for the authentication information:

```java

 String user; // the user name
 String database; // the name of the database in which the user is defined
 char[] password; // the password as a character array
 // ...

 MongoCredential credential = MongoCredential.createCredential(user, database, password);

 MongoClientOptions options = MongoClientOptions.builder().sslEnabled(true).build();

 MongoClient mongoClient = new MongoClient(new ServerAddress("host1", 27017),
                                           Arrays.asList(credential),
                                           options);
```
