+++
date = "2016-05-31T13:07:04-04:00"
title = "Run Commands"
[menu.main]
parent = "Scala Tutorials"
identifier = "Scala Run Commands"
weight = 90
pre = "<i class='fa'></i>"
+++

## Run Commands

Not all commands have a specific helper. However you can run any [MongoDB command]({{<docsref "reference/command" >}}) by using the MongoDatabase's [`runCommand()`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoDatabase.html#runCommand-org.bson.conversions.Bson-com.mongodb.ReadPreference-" >}}) method.

## Prerequisites

- The example below requires a `restaurants` collection in the `test` database. To create and populate the collection, follow the directions in [github](https://github.com/mongodb/docs-assets/tree/drivers).

- Include the following import statements:

     ```scala
     import org.mongodb.scala._
     ```

{{% note class="important" %}}
This guide uses the `Observable` implicits as covered in the [Quick Start Primer]({{< relref "driver-scala/getting-started/quick-start-primer.md" >}}).
{{% /note %}}

## Connect to a MongoDB Deployment

Connect to a MongoDB deployment and declare and define a `MongoDatabase` instance.

For example, include the following code to connect to a standalone MongoDB deployment running on localhost on port `27017` and define `database` to refer to the `test` database:

```scala
val mongoClient: MongoClient = MongoClient()
val database: MongoDatabase = mongoClient.getDatabase("test")
```

For additional information on connecting to MongoDB, see [Connect to MongoDB]({{< ref "connect-to-mongodb.md" >}}).

## Run `buildInfo` and `collStats` Commands

To run a command, construct a [`Document`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/bson/index.html#Document:org.mongodb.scala.bson.collection.immutable.Document.type" >}})
object that specifies the command and pass it to the `runCommand()` method.

The following runs the [`buildInfo`]({{<docsref "reference/command/buildInfo" >}}) command and the [`collStats`]({{<docsref "reference/command/collStats" >}}) command:

```scala
database.runCommand(Document("buildInfo" -> 1)).printResults()

database.runCommand(Document("collStats" -> "restaurants")).printResults()
```

For a list of available MongoDB commands, see [MongoDB commands]({{<docsref "reference/command" >}}).
