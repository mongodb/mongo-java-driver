+++
date = "2016-05-31T13:40:45-04:00"
title = "Databases and Collections"
[menu.main]
parent = "Scala Tutorials"
identifier = "Scala Databases and Collections"
weight = 11
pre = "<i class='fa'></i>"
+++
## Databases and Collections

MongoDB stores documents in collections; the collections in databases.

## Prerequisites

- Include following import statements:

    ```scala
    import org.mongodb.scala._
    import org.mongodb.scala.model.Filters._
    ```

{{% note class="important" %}}
This guide uses the `Observable` implicits as covered in the [Quick Start Primer]({{< relref "driver-scala/getting-started/quick-start-primer.md" >}}).
{{% /note %}}

## Connect to a MongoDB Deployment

Connect to a running MongoDB deployment.

For example, include the following code to connect to a standalone MongoDB deployment running on localhost on port `27017`.

```scala
val mongoClient = MongoClient()
```

For more information on connecting to running MongoDB deployments, see
[Connect to MongoDB]({{< ref "driver/tutorials/connect-to-mongodb.md" >}}).

## Access a Database

Once you have a `MongoClient` instance connected to a MongoDB deployment, use its [`getDatabase()`]({{<apiref "org/mongodb/scala/MongoClient.html#getDatabase(name:String):org.mongodb.scala.MongoDatabase">}}) method to access a database.

Specify the name of the database to the `getDatabase()` method. If a database does not exist, MongoDB creates the database when you first store data for that database.

The following example accesses the ``test`` database:

```scala
val database: MongoDatabase = mongoClient.getDatabase("test")
```

{{% note %}}
`MongoDatabase` instances are immutable.
{{% /note %}}

## Access a Collection

Once you have a `MongoDatabase` instance, use its [`getCollection()`]({{< apiref "org/mongodb/scala/MongoDatabase.html#getCollection[TResult](collectionName:String)(implicite:org.mongodb.scala.bson.DefaultHelper.DefaultsTo[TResult,org.mongodb.scala.Document],implicitct:scala.reflect.ClassTag[TResult]):org.mongodb.scala.MongoCollection[TResult]">}})
method to access a collection.

Specify the name of the collection to the `getCollection()` method.

For example, using the `database` instance, the following statement accesses the collection named `myTestCollection`:

```scala
val coll: MongoCollection[Document] = database.getCollection("myTestCollection")
```

{{% note %}}
`MongoCollection` instances are immutable.
{{% /note %}}

If a collection does not exist, MongoDB creates the collection when you first store data for that collection.

You can also explicitly create a collection with various options, such as setting the maximum size or the documentation validation rules.

## Explicitly Create a Collection

The MongoDB driver provides the [`createCollection()`]({{<apiref "org/mongodb/scala/MongoDatabase.html#createCollection-java.lang.String-com.mongodb.client.model.CreateCollectionOptions-">}}) method to explicitly create a collection. When you explicitly create a collection, you can specify various collection options, such as a maximum size or the documentation validation rules, with the [`CreateCollectionOptions`]({{<apiref "org/mongodb/scala/model/package$$CreateCollectionOptions$.html">}}) class. If you are not specifying these options, you do not need to explicitly create the collection since MongoDB creates new collections when you first store data for the collections.

### Capped Collection

For example, the following operation creates a [capped collection]({{<docsref "core/capped-collections">}}) sized to 1 megabyte:

```scala
database.createCollection("cappedCollection", CreateCollectionOptions().capped(true).sizeInBytes(0x100000))
        .printResults()
```

### Document Validation

MongoDB provides the capability to [validate documents]({{<docsref "core/document-validation">}}) during updates and insertions. Validation rules are specified on a per-collection basis using the [`ValidationOptions`]({{< apiref "org/mongodb/scala/model/package$$ValidationOptions$.html">}}), which takes a filter document that specifies the validation rules or expressions.

```scala
ValidationOptions collOptions = ValidationOptions().validator(
        Filters.or(Filters.exists("email"), Filters.exists("phone")))

database.createCollection("contacts", CreateCollectionOptions().validationOptions(collOptions))
        .printResults()
```

## Get A List of Collections

You can get a list of the collections in a database using the [`MongoDatabase.listCollectionNames()`]({{<apiref "org/mongodb/scala/MongoDatabase.html#listCollectionNames--">}}) method:

```scala
database.listCollectionNames().printResults()
```

## Drop a Collection

You can drop a collection by using the [`MongoCollection.drop()`]({{<apiref "org/mongodb/scala/MongoCollection.html#drop--">}}) method:

```scala
val collection: MongoCollection[Document] = database.getCollection("contacts")
collection.drop().printResults()
```

## Immutability

`MongoDatabase` and `MongoCollection` instances are immutable. To create new instances from existing instances that
have different property values, such as [read concern]({{<docsref "reference/read-concern">}}), [read preference]({{<docsref "reference/read-preference">}}), and [write concern]({{<docsref "reference/write-concern">}}), the `MongoDatabase` and `MongoCollection` class provides various methods:

- [`MongoDatabase.withReadConcern`]({{<apiref "org/mongodb/scala/MongoDatabase.html#withReadConcern(readConcern:org.mongodb.scala.ReadConcern):org.mongodb.scala.MongoDatabase">}})

- [`MongoDatabase.withReadPreference`]({{<apiref "org/mongodb/scala/MongoDatabase.html#withReadPreference(readPreference:org.mongodb.scala.ReadPreference):org.mongodb.scala.MongoDatabase">}})

- [`MongoDatabase.withWriteConcern`]({{<apiref "org/mongodb/scala/MongoDatabase.html#withWriteConcern(writeConcern:org.mongodb.scala.WriteConcern):org.mongodb.scala.MongoDatabase">}})

- [`MongoCollection.withReadConcern`]({{<apiref "org/mongodb/scala/MongoCollection.html#withReadPreference(readPreference:org.mongodb.scala.ReadPreference):org.mongodb.scala.MongoCollection[TResult]">}})

- [`MongoCollection.withReadPreference`]({{<apiref "org/mongodb/scala/MongoCollection.html#withReadPreference(readPreference:org.mongodb.scala.ReadPreference):org.mongodb.scala.MongoCollection[TResult]">}})

- [`MongoCollection.withWriteConcern`]({{<apiref "org/mongodb/scala/MongoCollection.html#withWriteConcern(writeConcern:org.mongodb.scala.WriteConcern):org.mongodb.scala.MongoCollection[TResult]">}})

For details, see [Read Operations]({{< relref  "driver/tutorials/perform-read-operations.md" >}}) and [Write Operations]({{< relref  "driver/tutorials/perform-write-operations.md" >}}).

## CodecRegistry

An overload of the `getCollection` method allows clients to specify a different class for representing BSON documents.  For example,
users may wish to use the strict and typesafe `BsonDocument` class with the CRUD API:

```scala
// Pass BsonDocument.class as the second argument
import org.mongodb.scala.bson._

val collection: MongoCollection[BsonDocument] = database.getCollection[BsonDocument]("mycoll")

// insert a document
val document = BsonDocument("{x: 1}")
collection.insertOne(document).printResults()

document.append("x", BsonInt32(2)).append("y", BsonInt32(3))

// replace a document
collection.replaceOne(Filters.equal("_id", document.get("_id")), document)
          .printResults()

// find documents
collection.find().printResults()
```

There are two requirements that must be met for any class used in this way:

- a `Codec` for it must be registered in the `MongoCollection`'s `CodecRegistry`
- the `Codec` must be one that encodes and decodes a full BSON document (and not just, for example, a single BSON value like an Int32)

By default, a `MongoCollection` is configured with `Codec`s for four classes:

- `Document` (The Scala BsonDocument wrapper)
- `BsonDocument`
- `Document` (The Java drivers loosely type Document class)
- `BasicDBObject` (The legacy Java drivers loosely type Document class)

Applications, however, are free to register `Codec` implementations for other classes by customizing the `CodecRegistry`.  New
`CodecRegistry` instances are configurable at three levels:

- In a `MongoClient` via `MongoClientSettings`
- In a `MongoDatabase` via its `withCodecRegistry` method
- In a `MongoCollection` via its `withCodecRegistry` method

Consider the case of encoding and decoding instances of the `UUID` class.  The Java driver by default encodes instances of `UUID` using a
byte ordering that is not compatible with other MongoDB drivers, and changing the default would be quite dangerous.  But it is
possible for new applications that require interoperability across multiple drivers to be able to change that default, and they can do
that with a `CodecRegistry`.   

```scala
// Replaces the default UuidCodec with one that uses the new standard UUID representation
import org.bson.UuidRepresentation
import org.bson.codecs.UuidCodec
import org.bson.codecs.configuration.CodecRegistries

val codecRegistry = CodecRegistries.fromRegistries(
  CodecRegistries.fromCodecs(new UuidCodec(UuidRepresentation.STANDARD)),
  MongoClient.DEFAULT_CODEC_REGISTRY)

// globally
val settings = MongoClientSettings.builder()
                .codecRegistry(codecRegistry).build()
val client = MongoClient(settings)

// or per database
val database = client.getDatabase("mydb")
                     .withCodecRegistry(codecRegistry)

// or per collection
val collection = database.getCollection("mycoll")
                         .withCodecRegistry(codecRegistry)
```
