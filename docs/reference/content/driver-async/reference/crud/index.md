+++
date = "2015-03-19T12:53:30-04:00"
title = "CRUD Operations"
[menu.main]
  parent = "Async Reference"
  identifier = "Async CRUD Operations"
  weight = 70
  pre = "<i class='fa'></i>"
+++


## CRUD

For a walkthrough of the main CRUD operations please refer to the [Quick Tour]({{< ref "driver-async/getting-started/quick-tour.md" >}}).

All CRUD-related methods in the Java driver are accessed through the 
[`MongoCollection`]({{< apiref "com/mongodb/async/client/MongoCollection" >}}) interface.  Instances of 
[`MongoCollection`]({{< apiref "com/mongodb/async/client/MongoCollection" >}}) can be obtained from a  
[`MongoClient`]({{< apiref "com/mongodb/async/client/MongoClient" >}}) instance by way of a
[`MongoDatabase`]({{< apiref "com/mongodb/async/client/MongoDatabase" >}}):

```java
MongoClient client = MongoClients.create();
MongoDatabase database = client.getDatabase("mydb");
MongoCollection<Document> collection = database.getCollection("mycoll");
```

[`MongoCollection`]({{< apiref "com/mongodb/async/client/MongoCollection" >}}) is a generic interface: the `TDocument` type parameter 
is the class that clients use to insert or replace documents in a collection, and the default type returned from `find` and `aggregate`.
 
The single-argument `getCollection` method returns an instance of `MongoCollection<Document>`, and so with this type of collection 
an application uses instances of the `Document` class:

```java
MongoCollection<Document> collection = database.getCollection("mycoll");

// insert a document
Document document = new Document("x", 1)
collection.insertOne(document, new SingleResultCallback<Void>() {
   @Override
   public void onResult(final Void result, final Throwable t) {
       System.out.println("Inserted!");
   }
});

...

document.append("x", 2).append("y", 3);

// replace a document
collection.replaceOne(Filters.eq("_id", document.get("_id"), document, 
    new SingleResultCallback<UpdateResult>() {
       @Override
       public void onResult(final UpdateResult result, final Throwable t) {
           System.out.println(result.getModifiedCount());
       }
   });

...

// find documents
collection.find().into(new ArrayList<Document>(), 
    new SingleResultCallback<List<Document>>() {
        @Override
        public void onResult(final List<Document> result, final Throwable t) {
            System.out.println("Found Documents: #" + result.size());
        }
    });
```

### CodecRegistry

An overload of the `getCollection` method allows clients to specify a different class for representing BSON documents.  For example, 
users my wish to use the type-safe [`BsonDocument`]({{< apiref "org/bson/BsonDocument" >}}) with the CRUD API:

```java
// Pass BsonDocument.class as the second argument
MongoCollection<BsonDocument> collection = database.getCollection("mycoll", BsonDocument.class);

// insert a document
BsonDocument document = new BsonDocument("x", new BsonInt32(1));
collection.insertOne(document, new SingleResultCallback<Void>() {
   @Override
   public void onResult(final Void result, final Throwable t) {
       System.out.println("Inserted!");
   }
});

...

document.append("x", new BsonInt32(2)).append("y", new BsonInt32(3));

// replace a document
collection.replaceOne(Filters.eq("_id", document.get("_id"), document, 
    new SingleResultCallback<UpdateResult>() {
       @Override
       public void onResult(final UpdateResult result, final Throwable t) {
           System.out.println(result.getModifiedCount());
       }
   });

...

// find documents
collection.find().into(new ArrayList<BsonDocument>(), 
    new SingleResultCallback<List<BsonDocument>>() {
        @Override
        public void onResult(final List<BsonDocument> result, final Throwable t) {
            System.out.println("Found BsonDocuments: #" + result.size());
        }
    });
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

Consider the case of encoding and decoding instances of the `UUID` class.  The Java driver by default encodes instances of `UUID` using a
byte ordering that is not compatible with other MongoDB drivers, and changing the default would be quite dangerous.  But it is 
possible for new applications that require interoperability across multiple drivers to be able to change that default, and they can do 
that with a `CodecRegistry`.   

```java
// Replaces the default UuidCodec with one that uses the new standard UUID representation
CodecRegistry codecRegistry = 
CodecRegistries.fromRegistries(CodecRegistries.fromCodecs(new UuidCodec(UuidRepresentation.STANDARD)),
                               MongoClient.getDefaultCodecRegistry());

// globally
MongoClientSettings clientSettings = MongoClients.create("mongodb://localhost").ggetSettings();
newClientSettings = MongoClientSettings.builder(clientSettings).codecRegistry(codecRegistry).build();
MongoClient client = MongoClients.create(newClientSettings);
 

// or per database
MongoDatabase database = client.getDatabase("mydb")
                               .withCodecRegistry(codecRegistry);

// or per collection
MongoCollection<Document> collection = database.getCollection("mycoll")
                                               .withCodecRegistry(codecRegistry);
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

```java
// CORRECT: The results of the method calls are chained and the final one is referenced 
// by collection 
MongoCollection<Document> collection = database.getCollection("mycoll")
                                                .withWriteConcern(WriteConcern.JOURNALED)
                                                .withReadPreference(ReadPreference.primary())
                                                .withCodecRegistry(newRegistry);

// INCORRECT: withReadPreference returns a new instance of MongoCollection
// It does not modify the collection it's called on.  So this will
// have no effect
collection.withReadPreference(ReadPreference.secondary());
```
