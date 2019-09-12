+++
date = "2015-03-19T12:53:30-04:00"
title = "Connection Settings"
[menu.main]
  parent = "Connecting"
  identifier = "Connection Settings"
  weight = 10
  pre = "<i class='fa'></i>"
+++

## Connection Settings

The Scala driver has two ways of specifying the settings of a connection to a MongoDB server deployment.

### Connection String

The [connection string](http://docs.mongodb.org/manual/reference/connection-string/) is the simplest way to specify the properties of a 
connection. . A connection string mostly follows [RFC 3986](http://tools.ietf.org/html/rfc3986), with the exception of the domain name.
 For MongoDB, it is possible to list multiple domain names separated by a comma. Below are some example connection strings.


- For a standalone mongod, mongos, or a direct connection to a member of a replica set:

```ini
mongodb://host:27017
```

- To connect to multiple mongos or a replica set:

```ini
mongodb://host1:27017,host2:27017
```

The [authentication guide]({{< relref "reference/connecting/authenticating.md" >}}) contains information on how to provide credentials in 
the connection string.

#### The Database Component

The database component is optional and is used to indicate which database to authenticate against. When the database component is not
provided, the "admin" database is used.

```ini
mongodb://host:27017/mydb
```

Above, the database by the name of "mydb" is where the credentials are stored for the application.

{{% note %}}
Some drivers utilize the database component to indicate which database to work with by default. The Scala driver, while it parses the 
database component, does not use the database component for anything other than authentication.
{{% /note %}}

#### Options

Many options can be provided via the connection string. The ones that cannot may be provided in a 
[`MongoClientSettings`]({{< apiref "org/mongodb/scala/MongoClientSettings$" >}}) instance. To
provide an option, append a `?` to the connection string and separate options by an `&`.

```ini
mongodb://host:27017/?replicaSet=rs0&maxPoolSize=200
```

The above connection string sets the "replicaSet" value to "rs0" and the "maxPoolSize" to "200".

For a comprehensive list of the available options, see the [`ConnectionString`]({{< coreapiref "com/mongodb/ConnectionString" >}}) documentation.  


### MongoClient

A [`MongoClient`]({{< apiref "org/mongodb/scala/MongoClient$" >}}) instance will be the root object for all interaction with MongoDB. It is all 
that is needed to handle connecting to servers, monitoring servers, and performing operations against those servers. 

To create a `MongoClient` use the [`MongoClient()`]({{< apiref "org/mongodb/scala/MongoClient$.html#apply(uri:String):org.mongodb.scala.MongoClient" >}}) 
static helper.  Without any arguments `MongoClient()` will return a [`MongoClient`]({{< apiref "org/mongodb/scala/MongoClient$" >}}) 
instance will connect to "localhost" port 27017.  

```scala
val client: MongoClient = MongoClient()
```

Alternatively, a connection string may be provided:

```scala
val client: MongoClient = MongoClient("mongodb://host:27017,host2:27017/?replicaSet=rs0")
```

Finally, the [`MongoClientSettings`]({{< apiref "org/mongodb/scala/MongoClientSettings$" >}}) class provides an in-code way to set the 
same options from a connection string.  This is sometimes necessary, as the connection string does not allow an application to configure as 
many properties of the connection as  `MongoClientSettings`.  
[`MongoClientSettings`]({{< apiref "org/mongodb/scala/MongoClientSettings$" >}}) instances are immutable, so to create one an 
application uses a builder: 

```scala
import scala.collection.JavaConverters._

val settings: MongoClientSettings = MongoClientSettings.builder()
    .applyToClusterSettings(b => b.hosts(List(new ServerAddress("localhost")).asJava).description("Local Server"))
    .build()
val client: MongoClient = MongoClient(settings)
```

### Netty Configuration

By default, the async driver relies on the
[`AsynchronousSocketChannel`](http://docs.oracle.com/javase/7/docs/api/java/nio/channels/AsynchronousSocketChannel.html) class, introduced
in Java 7.  If configured properly, the driver will use [Netty](http://netty.io/) instead.  An application must use Netty for the 
following reasons:
      
* The application is configured to use SSL to communicate with the MongoDB server.
* The application runs on a Java 6 JVM.
         
To configure the driver to use Netty, the application must configure the MongoClientSettings appropriately:
         
```scala
MongoClientSettings.builder().streamFactoryFactory(NettyStreamFactoryFactory()).build()
```

By default the Netty-based streams will use the [NioEventLoopGroup](http://netty.io/4.0/api/io/netty/channel/nio/NioEventLoopGroup.html) 
and Netty's [default `ByteBufAllocator`](http://netty.io/4.0/api/io/netty/buffer/ByteBufAllocator.html#DEFAULT), but these are 
configurable via the [`NettyStreamFactoryFactory`]({{< apiref "org/mongodb/scala/connection/NettyStreamFactoryFactory$" >}}) constructor.   

