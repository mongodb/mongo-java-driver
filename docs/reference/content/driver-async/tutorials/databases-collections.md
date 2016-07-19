+++
date = "2015-03-17T15:36:56Z"
title = "Databases and Collections"
[menu.main]
  parent = "Async Tutorials"
  identifier = "Async Databases and Collections"
  weight = 11
  pre = "<i class='fa'></i>"
+++

## Databases and Collections

MongoDB stores documents in collections; the collections in databases.

## Consideration

{{% note class="important" %}}
Always check for errors in any [`SingleResultCallback<T>`]({{< apiref "com/mongodb/async/SingleResultCallback.html">}}) implementation
and handle them appropriately.

For sake of brevity, this tutorial omits the error check logic in the code examples.
{{% /note %}}

## Prerequisites

- Include the following import statements:

    ```java
    import com.mongodb.Block;
    import com.mongodb.async.SingleResultCallback;
    import com.mongodb.async.client.MongoClient;
    import com.mongodb.async.client.MongoClients;
    import com.mongodb.async.client.MongoCollection;
    import com.mongodb.async.client.MongoDatabase;
    import com.mongodb.client.model.CreateCollectionOptions;
    import com.mongodb.client.model.Filters;
    import com.mongodb.client.model.ValidationOptions;
    import org.bson.Document;
    ```

- The following callback:

    ```java
    SingleResultCallback<Void> callbackWhenFinished = new SingleResultCallback<Void>() {
        @Override
        public void onResult(final Void result, final Throwable t) {
            System.out.println("Operation Finished!");
        }
    };
    ```

## Connect to a MongoDB Deployment

Connect to a running MongoDB deployment.

For example, include the following code to connect to a standalone MongoDB deployment running on localhost on port `27017`.

```java
MongoClient mongoClient = MongoClients.create();
```

For additional information on connecting to MongoDB, see [Connect to MongoDB]({{< relref "driver-async/tutorials/connect-to-mongodb.md" >}}).

## Access a Database

Once you have a `MongoClient` instance connected to a MongoDB deployment, use its [`getDatabase()`]({{<apiref "com/mongodb/async/client/MongoClient.html#getDatabase-java.lang.String-">}}) method to access a database.

Specify the name of the database to the `getDatabase()` method. If a database does not exist, MongoDB creates the database when you first store data for that database.

The following example accesses the ``test`` database:

```java
MongoDatabase database = mongoClient.getDatabase("test");
```

{{% note %}}
`MongoDatabase` instances are immutable.
{{% /note %}}

## Get A List of Databases

You can get a list of the available databases using the `MongoClient` instance's  [`listDatabaseNames`]({{ < apiref "com/mongodb/async/client/MongoClient.html#listDatabaseNames--">}}) method.

```java
mongoClient.listDatabaseNames().forEach(new Block<String>() {
    @Override
    public void apply(final String s) {
        System.out.println(s);
    }
}, callbackWhenFinished);
```

## Drop A Database

You can drop the current database using its [`drop`]({{<apiref "com/mongodb/async/client/MongoDatabase.html#drop-com.mongodb.async.SingleResultCallback-">}}):

```java
mongoClient.getDatabase("databaseToBeDropped").drop(callbackWhenFinished);
```

## Access a Collection

Once you have a `MongoDatabase` instance, use its [`getCollection()`]({{< apiref "com/mongodb/async/client/MongoDatabase.html#getCollection-java.lang.String-">}}) method to access a collection.

Specify the name of the collection to the `getCollection()` method.

For example, using the `database` instance, the following statement accesses the collection named `myTestCollection`:

```java
MongoCollection<Document> coll = database.getCollection("myTestCollection");
```

{{% note %}}
`MongoCollection` instances are immutable.
{{% /note %}}

If a collection does not exist, MongoDB creates the collection when you first store data for that collection.

You can also explicitly create a collection with various options, such as setting the maximum size or the documentation validation rules.

## Explicitly Create a Collection

The async driver provides the [`createCollection`]({{< apiref "com/mongodb/async/client/MongoDatabase.html#createCollection-java.lang.String-com.mongodb.async.SingleResultCallback-">}}) method to explicitly create a collection. When you explicitly create a collection, you can specify various collection options, such as a maximum size or the documentation validation rules, with the [`CreateCollectionOptions`]({{<apiref "com/mongodb/client/model/CreateCollectionOptions.html">}}) class. If you are not specifying these options, you do not need to explicitly create the collection since MongoDB creates new collections when you first store data for the collections.

### Capped Collection

For example, the following operation creates a [capped collection]({{<docsref "core/capped-collections">}}) sized to 1 megabyte:

```java
database.createCollection("cappedCollection",
          new CreateCollectionOptions().capped(true).sizeInBytes(0x100000),
          callbackWhenFinished);
```

### Document Validation

MongoDB provides the capability to [validate documents]({{<docsref "core/document-validation">}}) during updates and insertions. Validation rules are specified on a per-collection basis using the [`ValidationOptions`]({{< apiref "com/mongodb/client/model/ValidationOptions.html">}}), which takes a filter document that specifies the validation rules or expressions.

```java
ValidationOptions collOptions = new ValidationOptions().validator(
        Filters.or(Filters.exists("email"), Filters.exists("phone")));
database.createCollection("contacts",
        new CreateCollectionOptions().validationOptions(collOptions),
        callbackWhenFinished);
```

## Get A List of Collections

You can get a list of the collections in a database using the [`MongoDatabase.listCollectionNames()`]({{<apiref "com/mongodb/async/client/MongoDatabase.html#listCollectionNames--">}}) method:

```java
database.listCollectionNames().forEach(new Block<String>() {
    @Override
    public void apply(final String name) {
        System.out.println(name);
    }
}, callbackWhenFinished);
```

## Drop a Collection

You can drop a collection by using the [`MongoCollection.drop()`]({{<apiref "com/mongodb/async/client/MongoCollection.html#drop-com.mongodb.async.SingleResultCallback-">}}) method:

```java
collection.drop(callbackWhenFinished);
```
