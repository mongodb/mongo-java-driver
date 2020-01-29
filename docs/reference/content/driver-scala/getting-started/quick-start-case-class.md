+++
date = "2017-05-17T15:36:57Z"
title = "Quick Start - Case Classes"
[menu.main]
  parent = "MongoDB Scala Driver"
  identifier = "Scala Quick Start - Case Classes"
  weight = 15
  pre = "<i class='fa'></i>"
+++

# Quick Start with case classes

The following code snippets come from the `QuickTourCaseClass.scala` example code that can be found with the 
[driver source]({{< srcref "examples/src/test/scala/tour/QuickStartCaseClass.scala" >}}).

{{% note class="important" %}}
This follows on from the [quick tour]({{< relref "driver-scala/getting-started/quick-start.md" >}}).

See the [Bson macros]({{< relref "driver-scala/bson/macros.md" >}}) documentation for in-depth information about using macros for configuring case class
support with your `MongoCollection`.
{{% /note %}}


First we'll create the case class we want to use to represent the documents in the collection. In the following we create a `Person` case
class and companion object:

```scala
import org.mongodb.scala.bson.ObjectId
object Person {
  def apply(firstName: String, lastName: String): Person =
    Person(new ObjectId(), firstName, lastName)
}
case class Person(_id: ObjectId, firstName: String, lastName: String)
```

{{% note %}}
You'll notice in the companion object the apply method can automatically assign a `_id` when creating new instances that don't include it. 
In MongoDB the `_id` field represents the primary key for a document, so by having a `_id` field in the case class it allows access to the 
primary key. 
{{% /note %}}

## Configuring case classes

Then when using `Person` with a collection, there must be a `Codec` that can convert it to and from `BSON`. The 
`org.mongodb.scala.bson.codecs.Macros` companion object provides macros that can automatically generate a codec for case classes at compile 
time. In the following example we create a new `CodecRegistry` that includes a codec for the `Person` case class:


```scala
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY
import org.bson.codecs.configuration.CodecRegistries.{fromRegistries, fromProviders}

val codecRegistry = fromRegistries(fromProviders(classOf[Person]), DEFAULT_CODEC_REGISTRY )
```

Once the `codecRegistry` is configured, the next step is to create a `MongoCollection[Person]`. The following example uses `test` 
collection on the `mydb` database.

```scala
// To directly connect to the default server localhost on port 27017
val mongoClient: MongoClient = MongoClient()
val database: MongoDatabase = mongoClient.getDatabase("mydb").withCodecRegistry(codecRegistry)
val collection: MongoCollection[Person] = database.getCollection("test")
```

{{% note %}}
The `codecRegistry` can be set when creating a `MongoClient`, at the database level or at the collection level. The API is flexible, 
allowing for different `CodecRegistries` as required.
{{% /note %}}

## Insert a person

With the correctly configured `MongoCollection`, inserting `Person` instances into the collection is simple:

```scala
val person: Person = Person("Ada", "Lovelace")
collection.insertOne(person).results()
```

## Add multiple instances

To add multiple `Person` instances, use the `insertMany()`. The following uses the `printResults()` implicit and blocks until the observer 
is completed and then prints each result:

```scala
val people: Seq[Person] = Seq(
  Person("Charles", "Babbage"),
  Person("George", "Boole"),
  Person("Gertrude", "Blanch"),
  Person("Grace", "Hopper"),
  Person("Ida", "Rhodes"),
  Person("Jean", "Bartik"),
  Person("John", "Backus"),
  Person("Lucy", "Sanders"),
  Person("Tim", "Berners Lee"),
  Person("Zaphod", "Beeblebrox")
)
collection.insertMany(people).printResults()
```

It will output the following message:

```
The operation completed successfully
```

## Querying the collection

Use the [find()]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html#find[C](filter:org.bson.conversions.Bson)(implicite:org.mongodb.scala.bson.DefaultHelper.DefaultsTo[C,org.mongodb.scala.collection.immutable.Document],implicitct:scala.reflect.ClassTag[C]):org.mongodb.scala.FindObservable[C]" >}})
method to query the collection.

### Find the first person in a collection

Querying the collection is the same as shown in the [quick tour]({{< relref "driver-scala/getting-started/quick-start.md" >}}):

```scala
collection.find().first().printHeadResult()
```

The example will print the first `Person` in the database:

```
Person(58dd0a68218de22333435fa4, Ada, Lovelace)
```

### Find all people in the collection

To retrieve all the people in the collection, use the `find()` method. The `find()` method returns a `FindObservable` instance that
provides a fluent interface for chaining or controlling find operations. The following uses prints all the people in the collection:

```scala
collection.find().printResults()
```


## Get a single person with a query filter

To return a subset of the documents in our collection, pass a filter to the find() method . For example, the following will return the first 
`Person` whose first name is Ida:

```scala
import org.mongodb.scala.model.Filters._

collection.find(equal("firstName", "Ida")).first().printHeadResult()
```

This will print:

```
Person(58dd0a68218de22333435fa4, Ida, Rhodes)
```

{{% note %}}
Use the [`Filters`]({{< relref "builders/filters.md" >}}), [`Sorts`]({{< relref "builders/sorts.md" >}}),
[`Projections`]({{< relref "builders/projections.md" >}}) and [`Updates`]({{< relref "builders/updates.md" >}})
helpers for simple and concise ways of building up queries.
{{% /note %}}

## Get a set of people with a query

The following filter will find all `Person` instances where the `firstName` starts with `G`, sorted by `lastName`:

```scala
collection.find(regex("firstName", "^G")).sort(ascending("lastName")).printResults()
```

Which will print out the Person instances for Gertrude, George and Grace.

## Updating documents

There are numerous [update operators](http://docs.mongodb.org/manual/reference/operator/update-field/)
supported by MongoDB.  Use the [Updates]({{< apiref "mongo-scala-driver" "org/mongodb/scala/model/Updates$" >}}) helpers to help update documents in the database.

The following update corrects the hyphenation for Tim Berners-Lee: 

```scala
collection.updateOne(equal("lastName", "Berners Lee"), set("lastName", "Berners-Lee")).printHeadResult("Update Result: ")
```

The update methods return an [`UpdateResult`]({{< apiref "mongodb-driver-core" "com/mongodb/client/result/UpdateResult.html" >}}),
which provides information about the operation including the number of documents modified by the update.

## Deleting documents

To delete at most a single document (may be 0 if none match the filter) use the [`deleteOne`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/MongoCollection.html#deleteOne(filter:org.bson.conversions.Bson):org.mongodb.scala.Observable[org.mongodb.scala.result.DeleteResult]" >}}) 
method:

```scala
collection.deleteOne(equal("firstName", "Zaphod")).printHeadResult("Delete Result: ")
```

As you can see the API allows for easy use of CRUD operations with case classes. See the [Bson macros]({{< relref "driver-scala/bson/macros.md" >}}) 
documentation for further information about the macros.
