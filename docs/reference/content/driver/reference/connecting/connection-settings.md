+++
date = "2015-03-19T12:53:30-04:00"
title = "Connection Settings"
[menu.main]
  parent = "Sync Connecting"
  identifier = "Sync Connection Settings"
  weight = 10
  pre = "<i class='fa'></i>"
+++

## Connection Settings

The Java driver has two ways of specifying the settings of a connection to a MongoDB server deployment.

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

The [authentication guide]({{< relref "driver/reference/connecting/authenticating.md" >}}) contains information on how to provide credentials in the connection string.

#### The Database Component

The database component is optional and is used to indicate which database to authenticate against. When the database component is not
provided, the "admin" database is used.

```ini
mongodb://host:27017/mydb
```

Above, the database by the name of "mydb" is where the credentials are stored for the application.

{{% note %}}
Some drivers utilize the database component to indicate which database to work with by default. The Java driver, while it parses the 
database component, does not use the database component for anything other than authentication.
{{% /note %}}

#### Options

Many options can be provided via the connection string. The ones that cannot may be provided in a 
[`MongoClientOptions`]({{< apiref "com/mongodb/MongoClientOptions" >}}) instance. To
provide an option, append a `?` to the connection string and separate options by an `&`.

```ini
mongodb://host:27017/?replicaSet=rs0&maxPoolSize=200
```

The above connection string sets the "replicaSet" value to "rs0" and the "maxPoolSize" to "200".

For a comprehensive list of the available options, see the [`MongoClientURI`]({{< apiref "com/mongodb/MongoClientURI" >}}) documentation.  


### MongoClient

A [`MongoClient`]({{< apiref "com/mongodb/MongoClient" >}}) instance will be the root object for all interaction with MongoDB. It is all 
that is needed to handle connecting to servers, monitoring servers, and performing operations against those servers. Without any 
arguments, constructing a [`MongoClient`]({{< apiref "com/mongodb/MongoClient" >}}) instance will connect to "localhost" port 27017.  

```java
MongoClient client = new MongoClient();
```

Alternatively, a connection string may be provided:

```java
MongoClient client = new MongoClient(new MongoClientURI("mongodb://host:27017,host2:27017/?replicaSet=rs0"));
```

Finally, the [`MongoClientOptions`]({{< apiref "com/mongodb/MongoClientOptions" >}}) class provides an in-code way to set the same 
options from a connection string.  This is sometimes 
necessary, as the connection string does not allow an application to configure as many properties of the connection as 
`MongoClientOptions`.  
[`MongoClientOptions`]({{< apiref "com/mongodb/MongoClientOptions" >}}) instances are immutable, so to create one an application uses a
 builder: 

```java
MongoClientOptions options = MongoClientOptions.builder().cursorFinalizerEnabled(false).build();
MongoClient client = new MongoClient(options);
```

It's also possible to combine [`MongoClientOptions`]({{< apiref "com/mongodb/MongoClientOptions" >}}) with 
[`MongoClientURI`]({{< apiref "com/mongodb/MongoClientURI" >}}), for situations in which an application needs to set some options
in code but others via the connection string:

```java

MongoClientURI uri = new MongoClientURI("mongodb://host:27017,host2:27017/?replicaSet=rs0",
                                        MongoClientOptions.builder().cursorFinalizerEnabled(false))
MongoClient client = new MongoClient(uri);
```
