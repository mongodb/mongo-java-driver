+++
date = "2016-05-31T13:40:45-04:00"
title = "Databases and Collections"
[menu.main]
parent = "Sync Tutorials"
identifier = "Databases and Collections"
weight = 11
pre = "<i class='fa'></i>"
+++
## Databases and Collections

MongoDB stores documents in collections; the collections in databases.

## Prerequisites

- Include following import statements:

    ```java
    import com.mongodb.client.MongoClients;
    import com.mongodb.client.MongoClient;
    import com.mongodb.client.MongoCollection;
    import com.mongodb.client.MongoDatabase;
    import static com.mongodb.client.model.Filters.*;
    import com.mongodb.client.model.CreateCollectionOptions;
    import com.mongodb.client.model.ValidationOptions;
    ```

## Connect to a MongoDB Deployment

Connect to a running MongoDB deployment.

For example, include the following code to connect to a standalone MongoDB deployment running on localhost on port `27017`.

```java
MongoClient mongoClient = MongoClients.create();
```

For more information on connecting to running MongoDB deployments, see
[Connect to MongoDB]({{< ref "driver/tutorials/connect-to-mongodb.md" >}}).

## Access a Database

Once you have a `MongoClient` instance connected to a MongoDB deployment, use its [`getDatabase()`]({{< apiref "mongodb-driver-sync" "com/mongodb/MongoClient.html#getDatabase(java.lang.String)" >}}) method to access a database.

Specify the name of the database to the `getDatabase()` method. If a database does not exist, MongoDB creates the database when you first store data for that database.

The following example accesses the ``test`` database:

```java
MongoDatabase database = mongoClient.getDatabase("test");
```

{{% note %}}
`MongoDatabase` instances are immutable.
{{% /note %}}

## Access a Collection

Once you have a `MongoDatabase` instance, use its [`getCollection()`]({{< apiref "mongodb-driver-sync" "com/mongodb/client/MongoDatabase.html#getCollection(java.lang.String)" >}})
method to access a collection.

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

The MongoDB driver provides the [`createCollection()`]({{< apiref "mongodb-driver-sync" "com/mongodb/client/MongoDatabase.html#createCollection(java.lang.String,com.mongodb.client.model.CreateCollectionOptions)" >}}) method to explicitly create a collection. When you explicitly create a collection, you can specify various collection options, such as a maximum size or the documentation validation rules, with the [`CreateCollectionOptions`]({{< apiref "mongodb-driver-core" "com/mongodb/client/model/CreateCollectionOptions.html" >}}) class. If you are not specifying these options, you do not need to explicitly create the collection since MongoDB creates new collections when you first store data for the collections.

### Capped Collection

For example, the following operation creates a [capped collection]({{<docsref "core/capped-collections" >}}) sized to 1 megabyte:

```java
database.createCollection("cappedCollection",
          new CreateCollectionOptions().capped(true).sizeInBytes(0x100000));
```

### Document Validation

MongoDB provides the capability to [validate documents]({{<docsref "core/document-validation" >}}) during updates and insertions. Validation rules are specified on a per-collection basis using the [`ValidationOptions`]({{< apiref "mongodb-driver-core" "com/mongodb/client/model/ValidationOptions.html" >}}), which takes a filter document that specifies the validation rules or expressions.

```java
ValidationOptions collOptions = new ValidationOptions().validator(
        Filters.or(Filters.exists("email"), Filters.exists("phone")));
database.createCollection("contacts",
        new CreateCollectionOptions().validationOptions(collOptions));
```

## Get A List of Collections

You can get a list of the collections in a database using the [`MongoDatabase.listCollectionNames()`]({{< apiref "mongodb-driver-sync" "com/mongodb/client/MongoDatabase.html#listCollectionNames()" >}}) method:

```java
for (String name : database.listCollectionNames()) {
    System.out.println(name);
}
```

## Drop a Collection

You can drop a collection by using the [`MongoCollection.drop()`]({{< apiref "mongodb-driver-sync" "com/mongodb/client/MongoCollection.html#drop()" >}}) method:

```java
MongoCollection<Document> collection = database.getCollection("contacts");
collection.drop();
```

## Immutability

`MongoDatabase` and `MongoCollection` instances are immutable. To create new instances from existing instances that
have different property values, such as [read concern]({{<docsref "reference/read-concern" >}}), [read preference]({{<docsref "reference/read-preference" >}}), and [write concern]({{<docsref "reference/write-concern" >}}), the `MongoDatabase` and `MongoCollection` class provides various methods:

- [`MongoDatabase.withReadConcern`]({{< apiref "mongodb-driver-sync" "com/mongodb/client/MongoDatabase.html#withReadConcern(com.mongodb.ReadConcern)" >}})

- [`MongoDatabase.withReadPreference`]({{< apiref "mongodb-driver-sync" "com/mongodb/client/MongoDatabase.html#withReadPreference(com.mongodb.ReadPreference)" >}})

- [`MongoDatabase.withWriteConcern`]({{< apiref "mongodb-driver-sync" "com/mongodb/client/MongoDatabase.html#withWriteConcern(com.mongodb.WriteConcern)" >}})

- [`MongoCollection.withReadConcern`]({{< apiref "mongodb-driver-sync" "com/mongodb/client/MongoCollection.html#withReadConcern(com.mongodb.ReadConcern)" >}})

- [`MongoCollection.withReadPreference`]({{< apiref "mongodb-driver-sync" "com/mongodb/client/MongoCollection.html#withReadPreference(com.mongodb.ReadPreference)" >}})

- [`MongoCollection.withWriteConcern`]({{< apiref "mongodb-driver-sync" "com/mongodb/client/MongoCollection.html#withWriteConcern(com.mongodb.WriteConcern)" >}})

For details, see [Read Operations]({{< relref  "driver/tutorials/perform-read-operations.md" >}}) and [Write Operations]({{< relref  "driver/tutorials/perform-write-operations.md" >}}).

## CodecRegistry

An overload of the `getCollection` method allows clients to specify a different class for representing BSON documents.  For example,
users of the legacy CRUD API from the 2.x driver series may wish to continue using `BasicDBObject` in order to ease the transition to the new
CRUD API:

```java
// Pass BasicDBObject.class as the second argument
MongoCollection<BasicDBObject> collection = database.getCollection("mycoll", BasicDBObject.class);

// insert a document
BasicDBObject document = new BasicDBObject("x", 1)
collection.insertOne(document);
document.append("x", 2).append("y", 3);

// replace a document
collection.replaceOne(Filters.eq("_id", document.get("_id")), document);

// find documents
List<BasicDBObject> foundDocument = collection.find().into(new ArrayList<BasicDBObject>());
```

There are two requirements that must be met for any class used in this way:

- a `Codec` for it must be registered in the `MongoCollection`'s `CodecRegistry`
- the `Codec` must be one that encodes and decodes a full BSON document (and not just, for example, a single BSON value like an Int32)

By default, a `MongoCollection` is configured with `Codec`s for three classes:

- `Document`
- `BasicDBObject`
- `BsonDocument`

Applications, however, are free to register `Codec` implementations for other classes by customizing the `CodecRegistry`.  New
`CodecRegistry` instances are configurable at three levels:

- In a `MongoClient` via `MongoClientSettings`
- In a `MongoDatabase` via its `withCodecRegistry` method
- In a `MongoCollection` via its `withCodecRegistry` method

Consider the case of encoding and decoding instances of the `UUID` class.  The Java driver by default encodes instances of `UUID` using a
byte ordering that is not compatible with other MongoDB drivers, and changing the default would be quite dangerous.  But it is
possible for new applications that require interoperability across multiple drivers to be able to change that default, and they can do
that with a `CodecRegistry`.   

```java
// Replaces the default UuidCodec with one that uses the new standard UUID representation
CodecRegistry codecRegistry =
CodecRegistries.fromRegistries(CodecRegistries.fromCodecs(new UuidCodec(UuidRepresentation.STANDARD)),
                               MongoClientSettings.getDefaultCodecRegistry());

// globally
MongoClientSettings settings = MongoClientSettings.builder()
        .codecRegistry(codecRegistry).build();
MongoClient client = MongoClients.create(settings);

// or per database
MongoDatabase database = client.getDatabase("mydb")
                               .withCodecRegistry(codecRegistry);

// or per collection
MongoCollection<Document> collection = database.getCollection("mycoll")
                                               .withCodecRegistry(codecRegistry);
```   

{{% note %}}
Starting with the 3.12 release of the driver, you can also change the encoding of `UUID` instances via the `uuidRepresentation` property of
`MongoClientSettings`.  See 
[`MongoClientSettings.getUuidRepresentation`]({{< apiref "mongodb-driver-core" "com/mongodb/MongoClientSettings.html#getUuidRepresentation()" >}}) for
details.
{{% /note %}}

