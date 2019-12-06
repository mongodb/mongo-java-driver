+++
date = "2016-05-31T13:07:04-04:00"
title = "Run Commands"
[menu.main]
parent = "Async Tutorials"
identifier = "Async Run Commands"
weight = 90
pre = "<i class='fa'></i>"
+++

## Run Commands

Not all commands have a specific helper. However, you can run any [MongoDB command]({{<docsref "reference/command">}}) by using the [`runCommand()`]({{< apiref "com/mongodb/async/client/MongoDatabase.html#runCommand(org.bson.conversions.Bson,com.mongodb.ReadPreference,com.mongodb.async.SingleResultCallback)">}}) method.

## Prerequisites

- The example below requires a `restaurants` collection in the `test` database. To create and populate the collection, follow the directions in [github] (https://github.com/mongodb/docs-assets/tree/drivers).

- Include the following import statements:

     ```java
     import com.mongodb.async.client.MongoClient;
     import com.mongodb.async.client.MongoClients;
     import com.mongodb.async.client.MongoDatabase;
     import com.mongodb.async.SingleResultCallback;
     import org.bson.Document;
     ```

## Connect to a MongoDB Deployment

Connect to a MongoDB deployment and declare and define a `MongoDatabase` instance.

For example, include the following code to connect to a standalone MongoDB deployment running on localhost on port `27017` and define `database` to refer to the `test` database:

```java
MongoClient mongoClient = MongoClients.create();
MongoDatabase database = mongoClient.getDatabase("mydb");
```

For additional information on connecting to MongoDB, see [Connect to MongoDB]({{< relref "driver-async/tutorials/connect-to-mongodb.md">}}).

## Run `buildInfo` and `collStats` Commands

To run a command, construct a [`Document`]({{< apiref "org/bson/Document.html" >}})
object that specifies the command and pass it to the `runCommand()` method.

The following runs the [`buildInfo`]({{<docsref "reference/command/buildInfo">}}) command and the [`collStats`]({{<docsref "reference/command/collStats">}}) method:

```java
database.runCommand(new Document("buildInfo", 1), new SingleResultCallback<Document>() {
    @Override
    public void onResult(final Document buildInfo, final Throwable t) {
        System.out.println(buildInfo);
    }
});

database.runCommand(new Document("collStats", 1), new SingleResultCallback<Document>() {
    @Override
    public void onResult(final Document collStats, final Throwable t) {
        System.out.println(collStats);
    }
});
```

For a list of available MongoDB commands, see [MongoDB commands]({{<docsref "reference/command">}}).
