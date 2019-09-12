+++
date = "2015-03-19T12:53:30-04:00"
title = "CRUD Operations"
[menu.main]
    parent = "Reference"
    identifier = "CRUD Operations"
    weight = 70
    pre = "<i class='fa'></i>"
+++


## CRUD

For a walkthrough of the main CRUD operations please refer to the [Quick Tour]({{< ref "getting-started/quick-tour.md" >}}).

All CRUD-related methods in the Scala driver are accessed through the 
[`MongoCollection`]({{< apiref "org/mongodb/scala/MongoCollection" >}}) case class.  Instances of 
`MongoCollection` can be obtained from a [`MongoClient`]({{< apiref "org/mongodb/scala/MongoClient" >}}) instance by way of a
[`MongoDatabase`]({{< apiref "org/mongodb/scala/MongoDatabase" >}}):

```scala
val client: MongoClient = MongoClient()
val database: MongoDatabase = client.getDatabase("mydb")
val collection: MongoCollection[Document] = database.getCollection("mycoll")
```

[`MongoCollection`]({{< apiref "org/mongodb/scala/MongoCollection" >}}) takes the type of `TDocument` which defines the  
class that clients use to insert or replace documents in a collection, and the default type returned from `find` and `aggregate`.
 
The single-argument `getCollection` method returns an instance of `MongoCollection[Document]`, and so with this type of collection 
an application uses instances of the immutable [`Document`]({{< relref "bson/documents.md#immutable-documents">}}) class:

```scala
val collection: MongoCollection[Document] = database.getCollection("mycoll")

// insert a document
val document: Document = Document("_id" -> 1, "x" -> 1)
val insertObservable: SingleObservable[Completed] = collection.insertOne(document)

insertObservable.subscribe(new Observer[Completed] {
  override def onNext(result: Completed): Unit = println(s"onNext: $result")
  override def onError(e: Throwable): Unit = println(s"onError: $e")
  override def onComplete(): Unit = println("onComplete")
})

...

val replacementDoc: Document = Document("_id" -> 1, "x" -> 2, "y" -> 3)

// replace a document
collection.replaceOne(Filters.eq("_id", 1), replacementDoc
    ).subscribe((updateResult: UpdateResult) => println(updateResult))

...

// find documents
collection.find().collect().subscribe((results: Seq[Document]) => println(s"Found: #${results.size}"))
```

{{% note %}}
See the [`Observables`]({{< relref "reference/observables.md">}}) documentation for more information about `Observables` and implicit helpers.
{{% /note %}}

### CodecRegistry

An overload of the `getCollection` method allows clients to specify a different class for representing BSON documents.  For example, 
users my wish their own class with the CRUD API directly. Below we use the `BsonDocument` class from the Scala driver directly:

```scala
// Pass BsonDocument class as the second argument
val collection: MongoCollection[BsonDocument] = database.getCollection("mycoll", classOf[BsonDocument])

// insert a document
val document: BsonDocument = new BsonDocument("_id", new BsonInt32(2)).append("x", new BsonInt32(1))
collection.insertOne(document).subscribe((x: Completed) => println("Inserted"))

...

val replacementDoc: BsonDocument = new BsonDocument("_id", new BsonInt32(2)).append("x", new BsonInt32(2)).append("y", new BsonInt32(3))

// replace a document
collection.replaceOne(Filters.eq("_id", document.getInt32("1")), replacementDoc).subscribe((updateResult: UpdateResult) => println(updateResult))

...

// find documents
collection.find().collect().subscribe((results: Seq[BsonDocument]) => println(s"Found BsonDocuments: #${results.size}"))
```

There are two requirements that must be met for any class used in this way:

- a `Codec` for it must be registered in the `MongoCollection`'s `CodecRegistry`
- the `Codec` must be one that encodes and decodes a full BSON document (and not just, for example, a single BSON value like an Int32)

By default, a `MongoCollection` is configured with `Codec`s for two classes:
 
- `Document`
- `BsonDocument`

Applications, however, are free to register `Codec` implementations for other classes by customizing the `CodecRegistry`.  New 
`CodecRegistry` instances are configurable at three levels:

- In a `MongoClient` via `MongoClientOptions`
- In a `MongoDatabase` via its `withCodecRegistry` method
- In a `MongoCollection` via its `withCodecRegistry` method

Consider the case of encoding and decoding instances of the `UUID` class.  The Scala driver by default encodes instances of `UUID` using a
byte ordering that is not compatible with other MongoDB drivers, and changing the default would be quite dangerous.  But it is 
possible for new applications that require interoperability across multiple drivers to be able to change that default, and they can do 
that with a `CodecRegistry`.   

```scala
// Replaces the default UuidCodec with one that uses the new standard UUID representation
val codecRegistry: CodecRegistry = 
CodecRegistries.fromRegistries(CodecRegistries.fromCodecs(new UuidCodec(UuidRepresentation.STANDARD)),
                               MongoClient.getDefaultCodecRegistry())

// globally
val clientSettings: MongoClientSettings = MongoClients.create("mongodb://localhost").getSettings()
newClientSettings = MongoClientSettings.builder(clientSettings).codecRegistry(codecRegistry).build()
val client: MongoClient = MongoClient(newClientSettings)
 

// or per database
val database: MongoDatabase = client.getDatabase("mydb")
                                    .withCodecRegistry(codecRegistry)

// or per collection
val collection: MongoCollection[Document] = database.getCollection("mycoll")
                                                    .withCodecRegistry(codecRegistry)
```


### Write Concern

Applications can configure the `WriteConcern` that a `MongoCollection` uses for write operations.  Like `CodecRegistry`, the 
`WriteConcern` can be configured at three levels:

- In a `MongoClient` via `MongoClientOptions`
- In a `MongoDatabase` via its `withWriteConcern` method
- In a `MongoCollection` via its `withWriteConcern` method


### Read Preference

Applications can configure the `ReadPreference` that a `MongoCollection` uses for read operations.  Like `WriteConcern`, the 
`ReadPreference` can be configured at three levels:

- In a `MongoClient` via `MongoClientOptions`
- In a `MongoDatabase` via its `withReadPreference` method
- In a `MongoCollection` via its `withReadPreference` method

### Immutability of MongoDatabase and MongoCollection

Instance of `MongoDatabase` and `MongoCollection` are immutable, so rather than mutate the state of the `MongoCollection` on which they
are invoked, the three methods discussed above return new instances.  Applications should therefore be sure to store the result of the 
method call.  For example:

```scala
// CORRECT: The results of the method calls are chained and the final one is referenced 
// by collection 
val collection: MongoCollection[Document] = database.getCollection("mycoll")
                                                .withWriteConcern(WriteConcern.JOURNALED)
                                                .withReadPreference(ReadPreference.primary())
                                                .withCodecRegistry(newRegistry)

// INCORRECT: withReadPreference returns a new instance of MongoCollection
// It does not modify the collection it's called on.  So this will
// have no effect
collection.withReadPreference(ReadPreference.secondary())
```
