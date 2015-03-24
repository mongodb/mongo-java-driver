+++
date = "2015-03-19T12:53:26-04:00"
draft = true
title = "Monitoring"
[menu.main]
  parent = "Reference"
  weight = 50
  pre = "<i class='fa'></i>"
+++

# Monitoring

The driver uses [JMX](http://docs.oracle.com/javase/8/docs/technotes/guides/jmx/) to create
[MXBeans](http://docs.oracle.com/javase/tutorial/jmx/mbeans/mxbeans.html) that allow an
application or end user to monitor various aspects of the driver.

The driver creates MXBean instances of a single type:
[ConnectionPoolStatisticsMBean](http://api.mongodb.org/java/3.0/com/mongodb/management/ConnectionPoolStatisticsMBean.html).  The driver
registers one instance of this MXBean interface per server that it is connected to.  So in the case of a replica set, for example, there
will be an instance per non-hidden member of the replica set.

Each MXBean instance is required to be registered with a unique object name. All MBean instances created by the driver are under the
domain `org.mongodb.driver`.  Instances of `ConnectionPoolStatisticsMBean` will have the properties:

- `clusterId`: a client-generated unique identifier that is required for to ensure object name uniqueness in situations where an
application has mulitiple `MongoClient` instances connection to the same MongoDB server deployment
- `host`: the host name of the server.
- `port`: the port the server is listening on
