+++
date = "2015-03-19T12:53:26-04:00"
title = "Monitoring"
[menu.main]
  parent = "Management"
  identifier = "Monitoring"
  weight = 100
  pre = "<i class='fa'></i>"
+++

# Monitoring

The driver uses [JMX](http://docs.oracle.com/javase/8/docs/technotes/guides/jmx/) to create
[MXBeans](http://docs.oracle.com/javase/tutorial/jmx/mbeans/mxbeans.html) that allow an
application or end user to monitor various aspects of the driver.

The driver creates MXBean instances of a single type:
[ConnectionPoolStatisticsMBean](http://api.mongodb.org/java/3.0/com/mongodb/management/ConnectionPoolStatisticsMBean.html).
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
- `waitQueueSize`: the current size of the wait queue for a connection from this pool
- `checkedOutCount`: the current count of connections that are currently in use
