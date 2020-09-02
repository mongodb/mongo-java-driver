+++
date = "2015-03-19T14:27:51-04:00"
title = "Extended JSON"
[menu.main]
  parent = "Scala BSON"
  identifier = "Scala Extended JSON"
  weight = 50
  pre = "<i class='fa'></i>"
+++

## MongoDB Extended JSON

The Scala driver supports reading and writing BSON documents represented as  
[MongoDB Extended JSON](http://docs.mongodb.org/manual/reference/mongodb-extended-json/).  Both variants are supported: 

- Strict Mode: representations of BSON types that conform to the [JSON RFC](http://www.json.org/). This is the 
format that [mongoexport](http://docs.mongodb.org/manual/reference/program/mongoexport/) produces and 
[mongoimport](http://docs.mongodb.org/manual/reference/program/mongoimport/) consumes.
- Shell Mode: a superset of JSON that the 
[MongoDB shell](http://docs.mongodb.org/manual/tutorial/getting-started-with-the-mongo-shell/) can parse. 

Furthermore, the `Document` provides two sets of convenience methods for this purpose:

- Document.toJson(): a set of overloaded methods that convert a `Document` instance to a JSON string
- Document(json): a set of overloaded static factory methods that convert a JSON string to a `Document` instance
 
## Writing JSON

Consider the task of implementing a [mongoexport](http://docs.mongodb.org/manual/reference/program/mongoexport/)-like tool using the 
Scala driver.  
    
```scala
val fileName =    // initialize to the path of the file to write to
val collection =  // initialize the collection from which you want to query

val writer: PrintWriter = new PrintWriter(fileName)
collection.find().subscribe(
      (doc: Document) => output.write(s"${doc.toJson}\r\n"),
      (t: Throwable) => // handle failure,
      () => output.close())
```

The `Document.toJson()` method constructs an instance of a `JsonWriter` with its default settings, which will write in strict mode with no new lines or indentation.  

You can override this default behavior by using one of the overloads of `toJson()`.  As an example, consider the task of writing a JSON string 
that can be copied and pasted into the MongoDB shell:
 
```scala
import java.text.SimpleDateFormat

val fmt = new SimpleDateFormat("dd/MM/yy")
val first = fmt.parse("01/01/2014")
val second = fmt.parse("01/01/2015")
val doc = Document("startDate" -> Document("$gt" -> first, "$lt" -> second))
println(doc.toJson(new JsonWriterSettings(JsonMode.SHELL)))
```

This code snippet will print out MongoDB shell-compatible JSON, which can then be pasted into the shell:
 
```javascript
{ "startDate" : { "$gt" : ISODate("2014-01-01T05:00:00.000Z"), "$lt" : ISODate("2015-01-01T05:00:00.000Z") } }
```

## Reading JSON

Consider the task of implementing a [mongoimport](http://docs.mongodb.org/manual/reference/program/mongoimport/)-like tool using the 
Java driver.  
    
```scala
import scala.io.Source
val fileName =    // initialize to the path of the file to read from
val collection =  // initialize the collection from which you want to import to

try {
  for (json <- Source.fromFile(fileName).getLines()) {
    collection.insertOne(Document(json)).head()
  }
} catch {
  case ex: Exception => println("Bummer, an exception happened.")
}
```

The `Document(json)` companion helper method constructs an instance of a `JsonReader` with the given string and returns an instance of an
equivalent Document instance. `JsonReader` automatically detects the JSON flavor in the string, so you do not need to specify it. 

{{% note %}}
In the [tools]({{< srcref "examples/scripts" >}}) examples directory, there is sample code for `mongoimport` and `mongoexport`.
These examples are more fully featured than the above code snippets. They also provide an example of asynchronous error handling, as well 
as chaining observables to enforce insertion order on import.
{{% /note %}}

### Reading and Writing JSON Directly
If you do not need a document and only want to deal with JSON, you can use `JsonObject` to read and write JSON directly. `JsonObject` 
is simply a wrapper class that takes in a `String` in the constructor and returns the `String` in the `getJson()` method. 
Reading and writing JSON directly is more efficient than constructing a `Document` first and then calling `toJson()`, and it is also more efficient than calling `Document#parse`. 
The codec responsible for reading/writing JSON (`JsonObjectCodec`) is part of the default registry, so doing this is very simple 
and demonstrated by the following example:

```scala
val database: MongoDatabase = mongoClient.getDatabase("mydb")
val collection: MongoCollection[JsonObject] = database.getCollection("test")
collection.insertOne(new JsonObject("{hello: 1}")).printResults()
val jsonObject: SingleObservable[JsonObject] = collection.find.first()
```

### Reading and Writing JSON with CustomSettings
You can also provide custom `JsonWriterSettings` to the `JsonObjectCodec`, by constructing the codec yourself and then creating your own registry:

```scala
val codecRegistry =
      fromRegistries(
        fromCodecs(new JsonObjectCodec(JsonWriterSettings
           .builder()
           .outputMode(JsonMode.EXTENDED)
           .build())),
        DEFAULT_CODEC_REGISTRY
      )
val database: MongoDatabase = mongoClient.getDatabase("mydb").withCodecRegistry(codecRegistry)
val collection: MongoCollection[JsonObject] = database.getCollection("test")
collection.insertOne(new JsonObject("{hello: 1}")).printResults()
val jsonObject: SingleObservable[JsonObject] = collection.find.first()
```
