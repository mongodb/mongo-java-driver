+++
date = "2016-06-09T13:21:16-04:00"
title = "Write Operations"
[menu.main]
  parent = "Scala Tutorials"
  identifier = "Scala Perform Write Operations"
  weight = 20
  pre = "<i class='fa'></i>"
+++

## Write Operations (Insert, Update, Replace, Delete)

Perform write operations to insert new documents into a collection, update existing document or documents in a collection, replace an existing document in a collection, or delete existing document or documents from a collection.

## Prerequisites

- The example below requires a ``restaurants`` collection in the ``test`` database. To create and populate the collection, follow the directions in [github](https://github.com/mongodb/docs-assets/tree/drivers).

- Include the following import statements:

     ```scala
     import org.mongodb.scala._
     import org.mongodb.scala.model._
     import org.mongodb.scala.model.Filters._
     import org.mongodb.scala.model.Updates._
     import org.mongodb.scala.model.UpdateOptions
     import org.mongodb.scala.bson.BsonObjectId
     ```

{{% note class="important" %}}
This guide uses the `Observable` implicits as covered in the [Quick Start Primer]({{< relref "driver-scala/getting-started/quick-start-primer.md" >}}).
{{% /note %}}

## Connect to a MongoDB Deployment

Connect to a MongoDB deployment and declare and define a `MongoDatabase` instance.

For example, include the following code to connect to a standalone MongoDB deployment running on localhost on port `27017` and define `database` to refer to the `test` database and `collection` to refer to the `restaurants` collection:

```scala
val mongoClient: MongoClient = MongoClient()
val database: MongoDatabase = mongoClient.getDatabase("test")
val collection: MongoCollection[Document] = database.getCollection("restaurants")
```

For additional information on connecting to MongoDB, see [Connect to MongoDB]({{< ref "connect-to-mongodb.md" >}}).

## Insert New Document

To insert a single document into the collection, you can use the collection's [`insertOne()`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html#insertOne(document:TResult,options:org.mongodb.scala.model.InsertOneOptions):org.mongodb.scala.SingleObservable[org.mongodb.scala.Completed]" >}}) method.

```scala
val document = Document("name" -> "Café Con Leche" , 
                        "contact" -> Document("phone" -> "228-555-0149", 
                                              "email" -> "cafeconleche@example.com", 
                                              "location" -> Seq(-73.92502, 40.8279556)),
                        "stars" -> 3, "categories" -> Seq("Bakery", "Coffee", "Pastries"))

collection.insertOne(document).printResults()
```

{{% note %}}
If no top-level `_id` field is specified in the document, the Java driver automatically adds the `_id` field to the inserted document.
{{% /note %}}

## Insert Multiple Documents

To add multiple documents, you can use the collection's [`insertMany()`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html#insertMany(documents:Seq[_<:TResult]):org.mongodb.scala.SingleObservable[org.mongodb.scala.Completed]" >}}) method, which takes a list of documents to insert.

The following example inserts two documents to the collection:

```scala
val doc1 = Document("name" -> "Amarcord Pizzeria" , 
                    "contact" -> Document("phone" -> "264-555-0193", 
                                          "email" -> "amarcord.pizzeria@example.net", 
                                          "location" -> Seq(-73.88502, 40.749556)),
                    "stars" -> 2, "categories" -> Seq("Pizzeria", "Italian", "Pasta"))

val doc2 = Document("name" -> "Blue Coffee Bar" , 
                    "contact" -> Document("phone" -> "604-555-0102", 
                                          "email" -> "bluecoffeebar@example.com", 
                                          "location" -> Seq(-73.97902, 40.8479556)),
                    "stars" -> 5, "categories" -> Seq("Coffee", "Pastries"))

collection.insertMany(Seq(doc1, doc2)).printResults()
```
{{% note %}}
If no top-level `_id` field is specified in the documents, the Java driver automatically adds the `_id` field to the inserted documents.
{{% /note %}}

## Update Existing Documents

To update existing documents in a collection, you can use the collection's [`updateOne()`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html#updateOne(filter:org.mongodb.scala.bson.conversions.Bson,update:Seq[org.mongodb.scala.bson.conversions.Bson],options:org.mongodb.scala.model.UpdateOptions):org.mongodb.scala.SingleObservable[org.mongodb.scala.result.UpdateResult]" >}}) or [`updateMany`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html#updateMany(filter:org.mongodb.scala.bson.conversions.Bson,update:org.mongodb.scala.bson.conversions.Bson,options:org.mongodb.scala.model.UpdateOptions):org.mongodb.scala.SingleObservable[org.mongodb.scala.result.UpdateResult]" >}}) methods.

### Filters

You can pass in a filter document to the methods to specify which documents to update. The filter document specification is the same as for [read operations]({{< relref "driver-scala/tutorials/perform-read-operations.md" >}}). To facilitate creating filter objects, the Scala driver provides the [`Filters`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/model/Filters$.html" >}}) helper.

To specify an empty filter (i.e. match all documents in a collection), use an empty [`Document`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/bson/index.html#Document:org.mongodb.scala.bson.collection.immutable.Document.type" >}}) object.

### Update Operators

To change a field in a document, MongoDB provides [update operators]({{<docsref "reference/operator/update" >}}).  To specify the modification to perform using the update operators, use an updates document.

To facilitate the creation of updates documents, the Scala driver provides the [`Updates`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/model/Updates$.html" >}}) class.

{{% note class="important" %}}
The `_id` field is immutable; i.e. you cannot change the value of the `_id` field.
{{% /note %}}

### Update a Single Document

The [`updateOne()`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html#updateOne(filter:org.mongodb.scala.bson.conversions.Bson,update:Seq[org.mongodb.scala.bson.conversions.Bson],options:org.mongodb.scala.model.UpdateOptions):org.mongodb.scala.SingleObservable[org.mongodb.scala.result.UpdateResult]" >}}) method updates at most a single document, even if the filter condition matches multiple documents in the collection.

The following operation on the `restaurants` collection updates a document whose `_id` field equals `BsonObjectId("57506d62f57802807471dd41")`.

```scala
collection.updateOne(
                equal("_id", BsonObjectId("57506d62f57802807471dd41")),
                combine(set("stars", 1), set("contact.phone", "228-555-9999"), currentDate("lastModified")))
          .printResults()
```

Specifically, the operation uses:

- [`Updates.set`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/model/Updates$.html#set[TItem](fieldName:String,value:TItem):org.mongodb.scala.bson.conversions.Bson" >}}) to set the value of the `stars` field to `1` and the `contact.phone` field to `"228-555-9999"`, and

- [`Updates.currentDate`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/model/Updates$.html#currentDate(fieldName:String):org.mongodb.scala.bson.conversions.Bson" >}}) to modify the `lastModified` field to the current date. If the `lastModified` field does not exist, the operator adds the field to the document.  

{{% note class="tip" %}}
In some cases where you may need to update many fields in a document, it may be more efficient to replace the document.  See [Replace a Document](#replace-a-document).
{{% /note %}}

### Update Multiple Documents

The [`updateMany`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html#updateMany(filter:org.mongodb.scala.bson.conversions.Bson,update:Seq[org.mongodb.scala.bson.conversions.Bson],options:org.mongodb.scala.model.UpdateOptions):org.mongodb.scala.SingleObservable[org.mongodb.scala.result.UpdateResult]" >}}) method updates all documents that match the filter condition.

The following operation on the `restaurants` collection updates all documents whose `stars` field equals `2`.

```scala
collection.updateMany(
              equal("stars", 2),
              combine(set("stars", 0), currentDate("lastModified")))
          .println()
```

Specifically, the operation uses:

- [`Updates.set`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/model/Updates$.html#set[TItem](fieldName:String,value:TItem):org.mongodb.scala.bson.conversions.Bson" >}}) to set the value of the `stars` field to `0` , and

- [`Updates.currentDate`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/model/Updates$.html#currentDate(fieldName:String):org.mongodb.scala.bson.conversions.Bson" >}}) to modify the `lastModified` field to the current date. If the `lastModified` field does not exist, the operator adds the field to the document.  

### Update Options

With the [`updateOne()`]({{< apiref "mongo-scala-driver" """org/mongodb/scala/MongoCollection.html#updateOne(filter:org.mongodb.scala.bson.conversions.Bson,update:Seq[org.mongodb.scala.bson.conversions.Bson],options:org.mongodb.scala.model.UpdateOptions):org.mongodb.scala.SingleObservable[org.mongodb.scala.result.UpdateResult]" >}}) and [`updateMany`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html#updateOne(filter:org.mongodb.scala.bson.conversions.Bson,update:Seq[org.mongodb.scala.bson.conversions.Bson],options:org.mongodb.scala.model.UpdateOptions):org.mongodb.scala.SingleObservable[org.mongodb.scala.result.UpdateResult]" >}}) methods, you can include an [`UpdateOptions`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/model/index.html#UpdateOptions=com.mongodb.client.model.UpdateOptions" >}}) document to specify the [`upsert`]({{<docsref "reference/method/db.collection.update/#upsert-option" >}}) option or the [`bypassDocumentationValidation`]({{<docsref "core/document-validation/#bypass-document-validation" >}}) option.

```scala
collection.updateOne(
                equal("_id", 1),
                combine(set("name", "Fresh Breads and Tulips"), currentDate("lastModified")),
                UpdateOptions().upsert(true).bypassDocumentValidation(true))
          .printResults()
```
## Replace an Existing Document

To replace an existing document in a collection, you can use the collection's [`replaceOne`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html#replaceOne(filter:org.mongodb.scala.bson.conversions.Bson,replacement:TResult,options:org.mongodb.scala.model.ReplaceOptions):org.mongodb.scala.SingleObservable[org.mongodb.scala.result.UpdateResult]" >}}) method.

{{% note class="important" %}}
The `_id` field is immutable; i.e. you cannot replace the `_id` field value.
{{% /note %}}

### Filters

You can pass in a filter document to the method to specify which document to replace. The filter document specification is the same as for [read operations]({{< relref "driver-scala/tutorials/perform-read-operations.md" >}}). To facilitate creating filter objects, the Scala driver provides the [`Filters`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/model/Filters$.html" >}}) helper.

To specify an empty filter (i.e. match all documents in a collection), use an empty [`Document`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/bson/index.html#Document:org.mongodb.scala.bson.collection.immutable.Document.type" >}}) object.

The [`replaceOne`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html#replaceOne(filter:org.mongodb.scala.bson.conversions.Bson,replacement:TResult,options:org.mongodb.scala.model.ReplaceOptions):org.mongodb.scala.SingleObservable[org.mongodb.scala.result.UpdateResult]" >}}) method replaces at most a single document, even if the filter condition matches multiple documents in the collection.

### Replace a Document

To replace a document, pass a new document to the [`replaceOne`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html#replaceOne(filter:org.mongodb.scala.bson.conversions.Bson,replacement:TResult,options:org.mongodb.scala.model.ReplaceOptions):org.mongodb.scala.SingleObservable[org.mongodb.scala.result.UpdateResult]/org/mongodb/scala/MongoCollection.html#replaceOne(filter:org.mongodb.scala.bson.conversions.Bson,replacement:TResult,options:org.mongodb.scala.model.ReplaceOptions):org.mongodb.scala.SingleObservable[org.mongodb.scala.result.UpdateResult]" >}}) method.

{{% note class="important" %}}
The replacement document can have different fields from the original document. In the replacement document, you can omit the `_id` field since the `_id` field is immutable; however, if you do include the `_id` field, you cannot specify a different value for the `_id` field.
{{% /note %}}

The following operation on the `restaurants` collection replaces the document whose `_id` field equals `BsonObjectId("57506d62f57802807471dd41")`.

```scala
collection.replaceOne(
                equal("_id", BsonObjectId("57506d62f57802807471dd41")),
                Document("name" -> "Green Salads Buffet", "contact" -> "TBD",
                         "categories" -> Seq("Salads", "Health Foods", "Buffet")))
         .printResults()
```

See also [Update a Document](#update-a-single-document).

### Update Options

With the [`replaceOne`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html#updateOne(filter:org.mongodb.scala.bson.conversions.Bson,update:Seq[org.mongodb.scala.bson.conversions.Bson],options:org.mongodb.scala.model.UpdateOptions):org.mongodb.scala.SingleObservable[org.mongodb.scala.result.UpdateResult]" >}}), you can include an [`UpdateOptions`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/model/index.html#UpdateOptions=com.mongodb.client.model.UpdateOptions" >}}) document to specify the [`upsert`]({{<docsref "reference/method/db.collection.update/#upsert-option" >}}) option or the [`bypassDocumentationValidation`]({{<docsref "core/document-validation/#bypass-document-validation" >}}) option.

```scala
collection.replaceOne(
                equal("name", "Orange Patisserie and Gelateria"),
                Document("stars" -> 5, "contact"  -> "TBD", 
                         "categories" -> Seq("Cafe", "Pastries", "Ice Cream")),
                UpdateOptions().upsert(true).bypassDocumentValidation(true))
          .printResults()
```

## Delete Documents

To delete documents in a collection, you can use the
[`deleteOne`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html#deleteOne(filter:org.mongodb.scala.bson.conversions.Bson,options:org.mongodb.scala.model.DeleteOptions):org.mongodb.scala.SingleObservable[org.mongodb.scala.result.DeleteResult]" >}}) and [`deleteMany`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html#deleteMany(filter:org.mongodb.scala.bson.conversions.Bson,options:org.mongodb.scala.model.DeleteOptions):org.mongodb.scala.SingleObservable[org.mongodb.scala.result.DeleteResult]" >}}) methods.

### Filters

You can pass in a filter document to the methods to specify which documents to delete. The filter document specification is the same as for [read operations]({{< relref "driver-scala/tutorials/perform-read-operations.md" >}}). To facilitate creating filter objects, the Scala driver provides the [`Filters`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/model/Filters$.html" >}}) helper.

To specify an empty filter (i.e. match all documents in a collection), use an empty [`Document`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/bson/index.html#Document:org.mongodb.scala.bson.collection.immutable.Document.type" >}}) object.

### Delete a Single Document

The [`deleteOne`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html#deleteOne(filter:org.mongodb.scala.bson.conversions.Bson,options:org.mongodb.scala.model.DeleteOptions):org.mongodb.scala.SingleObservable[org.mongodb.scala.result.DeleteResult]" >}}) method deletes at most a single document, even if the filter condition matches multiple documents in the collection.

The following operation on the `restaurants` collection deletes a document whose `_id` field equals `ObjectId("57506d62f57802807471dd41")`.

```scala
collection.deleteOne(equal("_id", new ObjectId("57506d62f57802807471dd41"))).subscribe(new ObservableSubscriber<DeleteResult>())
```

### Delete Multiple Documents

The [`deleteMany`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html?search=UpdateOptio#deleteMany(filter:org.mongodb.scala.bson.conversions.Bson,options:org.mongodb.scala.model.DeleteOptions):org.mongodb.scala.SingleObservable[org.mongodb.scala.result.DeleteResult]" >}}) method deletes all documents that match the filter condition.

The following operation on the `restaurants` collection deletes all documents whose `stars` field equals `4`.

```scala
collection.deleteMany(equal("stars", 4)).printResults()
```

See also [Drop a Collection]({{< relref  "driver/tutorials/databases-collections.md" >}}).

## Write Concern

[Write concern]({{<docsref "reference/write-concern" >}}) describes the level of acknowledgement requested from MongoDB for write operations.

Applications can configure [write concern]({{<docsref "reference/write-concern" >}}) at three levels:

- In a [`MongoClient()`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoClient$.html" >}})

  - Via [`MongoClientSettings`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoClientSettings$.html" >}}):

      ```scala
      val mongoClient: MongoClient = MongoClient(MongoClientSettings.builder()
                                                    .applyConnectionString(ConnectionString("mongodb://host1,host2"))
                                                    .writeConcern(WriteConcern.MAJORITY)
                                                    .build())
      ```

  - Via [`ConnectionString`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/ConnectionString$.html" >}}), as in the following example:

      ```scala
      val mongoClientt = MongoClient("mongodb://host1:27017,host2:27017/?w=majority")
      ```

- In a [`MongoDatabase`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoDatabase.html" >}}) via its [`withWriteConcern`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoDatabase.html#withWriteConcern(writeConcern:org.mongodb.scala.WriteConcern):org.mongodb.scala.MongoDatabase" >}}) method, as in the following example:

    ```scala
     val database = mongoClient.getDatabase("test").withWriteConcern(WriteConcern.MAJORITY)
    ```

- In a [`MongoCollection`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html" >}}) via its [`withWriteConcern`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html#withWriteConcern(writeConcern:org.mongodb.scala.WriteConcern):org.mongodb.scala.MongoCollection[TResult]" >}}) method, as in the following example:

    ```scala
     val collection = database.getCollection("restaurants").withWriteConcern(WriteConcern.MAJORITY)
    ```

`MongoDatabase` and `MongoCollection` instances are immutable. Calling `.withWriteConcern()` on an existing `MongoDatabase` or `MongoCollection` instance returns a new instance and does not affect the instance on which the method is called.

For example, in the following, the `collWithWriteConcern` instance has the write concern of majority whereas the write concern of the `collection` is unaffected.

```scala
val collWithWriteConcern = collection.withWriteConcern(WriteConcern.MAJORITY)
```

You can build `MongoClientSettings`, `MongoDatabase`, or `MongoCollection` to include a combination of write concern, [read concern]({{<docsref "reference/read-concern" >}}), and [read preference]({{<docsref "reference/read-preference" >}}).

For example, the following sets all three at the collection level:

```scala
val collection = database.getCollection("restaurants")
                         .withReadPreference(ReadPreference.primary())
                         .withReadConcern(ReadConcern.MAJORITY)
                         .withWriteConcern(WriteConcern.MAJORITY)
```
