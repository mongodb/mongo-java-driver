+++
date = "2015-03-19T12:53:26-04:00"
title = "Monitoring"
[menu.main]
  parent = "Async Reference"
  identifier = "Async Monitoring"
  weight = 20
  pre = "<i class='fa'></i>"
+++

# JMX Monitoring

The driver uses [JMX](http://docs.oracle.com/javase/8/docs/technotes/guides/jmx/) to create
[MXBeans](http://docs.oracle.com/javase/tutorial/jmx/mbeans/mxbeans.html) that allow an
application or end user to monitor various aspects of the driver.

The driver creates MXBean instances of a single type:
[ConnectionPoolStatisticsMBean]({{< apiref "com/mongodb/management/ConnectionPoolStatisticsMBean" >}}).
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
- `size`: the current size of the pool, including idle and and in-use members
- `checkedOutCount`: the current count of connections that are currently in use

JMX connection pool monitoring is disabled by default. To enable it add a `com.mongodb.management.JMXConnectionPoolListener` instance via 
`MongoClientSettings`:

```java
MongoClientSettings settings =
        MongoClientSettings.builder()
        .applyToConnectionPoolSettings(builder -> builder.addConnectionPoolListener(new JMXConnectionPoolListener()))
        .build();
```

# Command Monitoring

The driver implements the
[command monitoring specification](https://github.com/mongodb/specifications/blob/master/source/command-monitoring/command-monitoring.rst),
allowing an application to be notified when a command starts and when it either succeeds or fails.

An application registers command listeners with a `MongoClient` by configuring `MongoClientSettings` with instances of classes
that implement the [`CommandListener`]({{< apiref "com/mongodb/event/CommandListener" >}}) interface. Consider the following, somewhat
simplistic, implementation of the `CommandListener` interface:

```java
public class TestCommandListener implements CommandListener {
    @Override
    public void commandStarted(final CommandStartedEvent event) {
        System.out.println(String.format("Sent command '%s:%s' with id %s to database '%s' "
                                         + "on connection '%s' to server '%s'",
                                         event.getCommandName(),
                                         event.getCommand().get(event.getCommandName()),
                                         event.getRequestId(),
                                         event.getDatabaseName(),
                                         event.getConnectionDescription()
                                              .getConnectionId(),
                                         event.getConnectionDescription().getServerAddress()));
    }

    @Override
    public void commandSucceeded(final CommandSucceededEvent event) {
        System.out.println(String.format("Successfully executed command '%s' with id %s "
                                         + "on connection '%s' to server '%s'",
                                         event.getCommandName(),
                                         event.getRequestId(),
                                         event.getConnectionDescription()
                                              .getConnectionId(),
                                         event.getConnectionDescription().getServerAddress()));
    }

    @Override
    public void commandFailed(final CommandFailedEvent event) {
        System.out.println(String.format("Failed execution of command '%s' with id %s "
                                         + "on connection '%s' to server '%s' with exception '%s'",
                                         event.getCommandName(),
                                         event.getRequestId(),
                                         event.getConnectionDescription()
                                              .getConnectionId(),
                                         event.getConnectionDescription().getServerAddress(),
                                         event.getThrowable()));
    }
}
```

and an instance of `MongoClientSettings` configured with an instance of `TestCommandListener`:

```java
ClusterSettings clusterSettings = ...
MongoClientSettings settings = MongoClientSettings.builder()
                                       .clusterSettings(clusterSettings)
                                       .addCommandListener(new TestCommandListener())
                                       .build();
MongoClient client = MongoClients.create(settings);
```

A `MongoClient` configured with these options will print a message to `System.out` before sending each command to a MongoDB server, and
another message upon either successful completion or failure of each command.

# Cluster Monitoring

The driver implements the
[SDAM Monitoring specification](https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring-monitoring.rst),
allowing an application to be notified when the driver detects changes to the topology of the MongoDB cluster to which it is connected.

An application registers listeners with a `MongoClient` by configuring  `MongoClientSettings` with instances of classes that
implement any of the [`ClusterListener`]({{< apiref "com/mongodb/event/ClusterListener" >}}),
 [`ServerListener`]({{< apiref "com/mongodb/event/ServerListener" >}}),
or [`ServerMonitorListener`]({{< apiref "com/mongodb/event/ServerMonitorListener" >}}) interfaces.

Consider the following, somewhat simplistic, example of a cluster listener:

```java
public class TestClusterListener implements ClusterListener {
    private final ReadPreference readPreference;
    private boolean isWritable;
    private boolean isReadable;

    public TestClusterListener(final ReadPreference readPreference) {
        this.readPreference = readPreference;
    }

    @Override
    public void clusterOpening(final ClusterOpeningEvent clusterOpeningEvent) {
        System.out.println(String.format("Cluster with unique client identifier %s opening",
                clusterOpeningEvent.getClusterId()));
    }

    @Override
    public void clusterClosed(final ClusterClosedEvent clusterClosedEvent) {
        System.out.println(String.format("Cluster with unique client identifier %s closed",
                clusterClosedEvent.getClusterId()));
    }

    @Override
    public void clusterDescriptionChanged(final ClusterDescriptionChangedEvent event) {
        if (!isWritable) {
            if (event.getNewDescription().hasWritableServer()) {
                isWritable = true;
                System.out.println("Writable server available!");
            }
        } else {
            if (!event.getNewDescription().hasWritableServer()) {
                isWritable = false;
                System.out.println("No writable server available!");
            }
        }

        if (!isReadable) {
            if (event.getNewDescription().hasReadableServer(readPreference)) {
                isReadable = true;
                System.out.println("Readable server available!");
            }
        } else {
            if (!event.getNewDescription().hasReadableServer(readPreference)) {
                isReadable = false;
                System.out.println("No readable server available!");
            }
        }
    }
}
```

and an instance of `MongoClientSettings` configured with an instance of `TestClusterListener`:

```java
List<ServerAddress> seedList = ...
ClusterSettings clusterSettings = ClusterSettings.builder()
                                          .hosts(seedList)
                                          .addClusterListener(new TestClusterListener(ReadPreference.secondary()))
                                          .build();
MongoClientSettings settings = MongoClientSettings.builder()
                                       .clusterSettings(clusterSettings)
                                       .build();
MongoClient client = MongoClients.create(settings);
```

A `MongoClient` configured with these settings will print a message to `System.out` when the MongoClient is created with these options,
and when that MongoClient is closed.  In addition, it will print a message when the client enters a state:

* with an available server that will accept writes
* without an available server that will accept writes
* with an available server that will accept reads using the configured `ReadPreference`
* without an available server that will accept reads using the configured `ReadPreference`

# Connection Pool Monitoring

The driver supports monitoring of connection pool-related events.

An application registers listeners with a `MongoClient` by configuring `MongoClientSettings` with instances of classes that
implement the [`ConnectionPoolListener`]({{< apiref "com/mongodb/event/ConnectionPoolListener" >}}) interface.

Consider the following, simplistic, example of a connection pool listener:

```java
public class TestConnectionPoolListener implements ConnectionPoolListener {
    @Override
    public void connectionPoolOpened(final ConnectionPoolOpenedEvent event) {
        System.out.println(event);
    }

    @Override
    public void connectionPoolClosed(final ConnectionPoolClosedEvent event) {
        System.out.println(event);
    }

    @Override
    public void connectionCheckedOut(final ConnectionCheckedOutEvent event) {
        System.out.println(event);
    }

    @Override
    public void connectionCheckedIn(final ConnectionCheckedInEvent event) {
        System.out.println(event);
    }

    @Override
    public void connectionAdded(final ConnectionAddedEvent event) {
        System.out.println(event);
    }

    @Override
    public void connectionRemoved(final ConnectionRemovedEvent event) {
        System.out.println(event);
    }
}
```

and an instance of `MongoClientSettings` configured with an instance of `TestConnectionPoolListener`:

```java
List<ServerAddress> seedList = ...
TestConnectionPoolListener connectionPoolListener = new TestConnectionPoolListener();
ClusterSettings clusterSettings = ClusterSettings.builder()
                                          .hosts(seedList)
                                          .build();
ConnectionPoolSettings connectionPoolSettings = 
        ConnectionPoolSettings.builder()
                              .addConnectionPoolListener(connectionPoolListener)
                              .build();
MongoClientSettings settings = MongoClientSettings.builder()
                                       .clusterSettings(clusterSettings)
                                       .connectionPoolSettings(connectionPoolSettings)
                                       .build();
MongoClient client = MongoClients.create(settings);
```

A `MongoClient` configured with these options will print a message to `System.out` for each connection pool-related event for each MongoDB
server to which the MongoClient is connected.
