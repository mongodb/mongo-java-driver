+++
date = "2015-03-19T12:53:26-04:00"
title = "Monitoring"
[menu.main]
  parent = "Reference"
  weight = 100
  pre = "<i class='fa'></i>"
+++

# Monitoring

The driver currently MXBean instances of a single type:
[ConnectionPoolStatisticsMBean](http://api.mongodb.org/java/3.0/com/mongodb/management/ConnectionPoolStatisticsMBean.html).
 The driver registers one `ConnectionPoolStatisticsMBean` instance per each server it connects to. For example, in the case of a replica 
 set, the driver creates an instance per each non-hidden member of the replica set.

Each MXBean instance is required to be registered with a unique object name, which consists of a domain and a set of named properties. All 
MXBean instances created by the driver are under the domain `"org.mongodb.driver"`.  Instances of `ConnectionPoolStatisticsMBean` will have 
the following properties:

- `clusterId`: a client-generated unique identifier, required to ensure object name uniqueness in situations where an
application has multiple `MongoClient` instances connected to the same MongoDB server deployment
- `host`: the host name of the server
- `port`: the port the server is listening on
