+++
date = "2015-03-19T12:53:26-04:00"
title = "Monitoring"
[menu.main]
  parent = "Scala Reference"
  identifier = "Scala Monitoring"
  weight = 20
  pre = "<i class='fa'></i>"
+++

# JMX Monitoring

The driver uses [JMX]({{< javaseref "technotes/guides/jmx/" >}}) to create
[MXBeans](http://docs.oracle.com/javase/tutorial/jmx/mbeans/mxbeans.html) that allow an
application or end user to monitor various aspects of the driver.

The driver creates MXBean instances of a single type:
[ConnectionPoolStatisticsMBean]({{< apiref "mongodb-driver-core" "com/mongodb/management/ConnectionPoolStatisticsMBean" >}}).
 The driver registers one `ConnectionPoolStatisticsMBean` instance per each server it connects to. For example, in the case of a replica
 set, the driver creates an instance per each non-hidden member of the replica set.

Each MXBean instance is required to be registered with a unique object name, which consists of a domain and a set of named properties. All
MXBean instances created by the driver are under the domain `"org.mongodb.driver"`.  Instances of `ConnectionPoolStatisticsMBean` will have
the following properties:

- `clusterId`: a client-generated unique identifier, required to ensure object name uniqueness in situations where an
application has multiple `MongoClient` instances connected to the same MongoDB server deployment
- `host`: the host name of the server
- `port`: the port on which the server is listening
- `minSize`: the minimum allowed size of the pool, including idle and in-use members
- `maxSize`: the maximum allowed size of the pool, including idle and in-use members
- `size`: the current size of the pool, including idle and in-use members
- `waitQueueSize`: the current size of the wait queue for a connection from this pool
- `checkedOutCount`: the current count of connections that are currently in use


JMX connection pool monitoring is disabled by default. To enable it add a `com.mongodb.management.JMXConnectionPoolListener` instance via 
`MongoClientSettings`:

```scala
val settings: MongoClientSettings =
        MongoClientSettings.builder()
        .applyToConnectionPoolSettings((builder: ConnectionPoolSettings.Builder) => builder.addConnectionPoolListener(new JMXConnectionPoolListener()))
        .build()
```

# Command Monitoring

The driver implements the
[command monitoring specification](https://github.com/mongodb/specifications/blob/master/source/command-monitoring/command-monitoring.rst),
allowing an application to be notified when a command starts and when it either succeeds or fails.

An application registers command listeners with a `MongoClient` by configuring `MongoClientSettings` with instances of classes
that implement the [`CommandListener`]({{< apiref "mongodb-driver-core" "com/mongodb/event/CommandListener" >}}) interface. Consider the following, somewhat
simplistic, implementation of the `CommandListener` interface:
 
```scala
case class TestCommandListener() extends CommandListener {

  override def commandStarted(event: CommandStartedEvent): Unit = {
    println(s"""Sent command '${event.getCommandName}:${event.getCommand.get(event.getCommandName)}'
         | with id ${event.getRequestId} to database '${event.getDatabaseName}'
         | on connection '${event.getConnectionDescription.getConnectionId}' to server
         | '${event.getConnectionDescription.getServerAddress}'""".stripMargin)
  }

  override def commandSucceeded(event: CommandSucceededEvent): Unit = {
    println(s"""Successfully executed command '${event.getCommandName}}'
               | with id ${event.getRequestId}
               | on connection '${event.getConnectionDescription.getConnectionId}' to server
               | '${event.getConnectionDescription.getServerAddress}'""".stripMargin)
  }

  override def commandFailed(event: CommandFailedEvent): Unit = {
    println(s"""Failed execution of command '${event.getCommandName}}'
               | with id ${event.getRequestId}
               | on connection '${event.getConnectionDescription.getConnectionId}' to server
               | '${event.getConnectionDescription.getServerAddress}
               | with exception '${event.getThrowable}'""".stripMargin)
  }
}                                                                                                                         
```


and an instance of `MongoClientSettings` configured with an instance of `TestCommandListener`:

```scala
val settings: MongoClientSettings = MongoClientSettings.builder()
        .addCommandListener(TestCommandListener())
        .build()
val client: MongoClient = MongoClient(settings)
```

A `MongoClient` configured with these options will print a message to `System.out` before sending each command to a MongoDB server, and
another message upon either successful completion or failure of each command.

# Cluster Monitoring

The driver implements the
[SDAM Monitoring specification](https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring-monitoring.rst),
allowing an application to be notified when the driver detects changes to the topology of the MongoDB cluster to which it is connected.

An application registers listeners with a `MongoClient` by configuring  `MongoClientSettings` with instances of classes that
implement any of the [`ClusterListener`]({{< apiref "mongodb-driver-core" "com/mongodb/event/ClusterListener" >}}),
 [`ServerListener`]({{< apiref "mongodb-driver-core" "com/mongodb/event/ServerListener" >}}),
or [`ServerMonitorListener`]({{< apiref "mongodb-driver-core" "com/mongodb/event/ServerMonitorListener" >}}) interfaces.

Consider the following, somewhat simplistic, example of a cluster listener:

```scala
  case class TestClusterListener(readPreference: ReadPreference) extends ClusterListener {
    var isWritable: Boolean = false
    var isReadable: Boolean = false

    override def clusterOpening(event: ClusterOpeningEvent): Unit = 
      println(s"Cluster with unique client identifier ${event.getClusterId} opening")

    override def clusterClosed(event: ClusterClosedEvent): Unit =
      println(s"Cluster with unique client identifier ${event.getClusterId} closed")

    override def clusterDescriptionChanged(event: ClusterDescriptionChangedEvent): Unit = {
      if (!isWritable) {
        if (event.getNewDescription.hasWritableServer) {
          isWritable = true
          println("Writable server available!")
        }
      } else {
        if (!event.getNewDescription.hasWritableServer) {
          isWritable = false
          println("No writable server available!")
        }
      }

      if (!isReadable) {
        if (event.getNewDescription.hasReadableServer(readPreference)) {
          isReadable = true
          println("Readable server available!")
        }
      } else {
        if (!event.getNewDescription.hasReadableServer(readPreference)) {
          isReadable = false
          println("No readable server available!")
        }
      }
    }
  }
```

and an instance of `MongoClientSettings` configured with an instance of `TestClusterListener`:

```scala
val settings: MongoClientSettings = MongoClientSettings.builder()
        .applyToClusterSettings((builder: ClusterSettings.Builder) =>
                builder.addClusterListener(TestClusterListener(ReadPreference.secondary())))
        .build()
val client: MongoClient = MongoClient(settings)
```

A `MongoClient` configured with these options will print a message to `System.out` when the MongoClient is created with these options,
and when that MongoClient is closed.  In addition, it will print a message when the client enters a state:

* with an available server that will accept writes
* without an available server that will accept writes
* with an available server that will accept reads using the configured `ReadPreference`
* without an available server that will accept reads using the configured `ReadPreference`

# Connection Pool Monitoring

The driver supports monitoring of connection pool-related events.

An application registers listeners with a `MongoClient` by configuring `MongoClientSettings` with instances of classes that
implement the [`ConnectionPoolListener`]({{< apiref "mongodb-driver-core" "com/mongodb/event/ConnectionPoolListener" >}}) interface.

Consider the following, simplistic, example of a connection pool listener:

```scala
 case class TestConnectionPoolListener() extends ConnectionPoolListener {

   override def connectionPoolOpened(event: ConnectionPoolOpenedEvent): Unit = println(event)
   
   override def connectionPoolClosed(event: ConnectionPoolClosedEvent): Unit = println(event)

   override def connectionCheckedOut(event: ConnectionCheckedOutEvent): Unit = println(event)

   override def connectionCheckedIn(event: ConnectionCheckedInEvent): Unit = println(event)

   override def waitQueueEntered(event: ConnectionPoolWaitQueueEnteredEvent): Unit = println(event)

   override def waitQueueExited(event: ConnectionPoolWaitQueueExitedEvent): Unit = println(event)

   override def connectionAdded(event: ConnectionAddedEvent): Unit = println(event)

   override def connectionRemoved(event: ConnectionRemovedEvent): Unit = println(event)
 }
```

and an instance of `MongoClientSettings` configured with an instance of `TestConnectionPoolListener`:

```scala
val settings: MongoClientSettings = MongoClientSettings.builder()
        .applyToConnectionPoolSettings((builder: ConnectionPoolSettings.Builder) =>
                builder.addConnectionPoolListener(TestConnectionPoolListener()))
        .build()
val client: MongoClient = MongoClient(settings)
```

A `MongoClient` configured with these options will print a message to `System.out` for each connection pool-related event for each MongoDB
server to which the MongoClient is connected.