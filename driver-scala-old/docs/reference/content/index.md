+++
date = "2015-03-17T15:36:56Z"
title = "MongoDB Scala Driver"
type = "index"
+++

## MongoDB Scala Driver Documentation

Welcome to the MongoDB Scala driver documentation hub.

### Getting Started

The [Getting Started]({{< relref "getting-started/index.md" >}}) guide contains installation instructions
and a simple tutorial to get up  and running quickly.


{{% note %}}
This implementation is built upon the MongoDB Async Driver and mirrors it's [API](http://mongodb.github.io/mongo-java-driver/). 

The MongoDB Scala driver provides an idiomatic Scala API that is built on top of the MongoDB Async Java driver.

In general, you should **only need** the `org.mongodb.scala` and `org.bson` namespaces in your code.  You should not need to import from the `com.mongodb` namespace as there are equivalent type aliases and companion objects in the Scala driver. The main exception is for advanced configuration via the builders in [MongoClientSettings]({{< apiref "org/mongodb/scala/MongoClientSettings$">}}) which is considered to be for advanced users.
{{% /note %}}


