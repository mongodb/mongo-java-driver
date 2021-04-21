+++
date = "2016-11-20T14:14:00-00:00"
title = "Macros"
[menu.main]
  parent = "Scala BSON"
  weight = 30
  pre = "<i class='fa fa-cog'></i>"
+++

## Macros

New in 2.0, the Scala driver allows you to use case classes to represent documents in a collection via the 
[`Macros`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/bson/codecs/Macros$" >}}) helper.  Simple case classes and nested case classes are supported. 
Hierarchical modelling can be achieved by using a sealed trait or class and having case classes implement the parent trait.

Many simple Scala types are supported and they will be marshaled into their corresponding 
`BsonValue` type. Below is a list of Scala types and their type-safe BSON representation:
   
| Scala type                        | BSON type         |
|-----------------------------------|-------------------|
| case class                        | Document          |
| `Iterable`                        | Array             |
| `Date`                            | Date              |
| `Boolean`                         | Boolean           |
| `Double`                          | Double            |
| `Int`                             | Int32             |
| `Long`                            | Int64             |
| `String`                          | String            |
| `Array[Byte]`                     | Binary            |
| `None`                            | Null              |


## Creating Codecs

To create a codec for your case class use the `Macros` object helper methods. Unless there is a good reason you should use the 
`Macros.createCodecProvider` method to create a [`CodecProvider`]({{< apiref "bson" "org/bson/codecs/configuration/CodecProvider.html" >}}). 
A `CodecProvider` will pass the configured [`CodecRegistry`]({{< apiref "bson" "org/bson/codecs/configuration/CodecRegistry.html" >}}) to the 
underlying [`Codec`]({{< apiref "bson" "org/bson/codecs/Codec.html" >}}) and provide access to all the configured codecs.

To create a `CodecProvider` all you need to do is to set the case class type when calling `createCodecProvider` like so:

```scala
import org.mongodb.scala.bson.codecs.Macros

case class Person(firstName: String, secondName: String)

val personCodecProvider = Macros.createCodecProvider[Person]()
```

The `personCodecProvider` can then be used when converted into a `CodecRegistry` by using the [`CodecRegistries`]({{< apiref "bson" "org/bson/codecs/configuration/CodecRegistries.html" >}}) static helpers. Below we create a new codec registry combining the new `personCodecProvider` and the default codec registry:

```scala
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.bson.codecs.configuration.CodecRegistries.{fromRegistries, fromProviders}

val codecRegistry = fromRegistries( fromProviders(personCodecProvider), DEFAULT_CODEC_REGISTRY )
```

The `Macros` helper also has an implicit `createCodecProvider` method that takes the `Class[T]` and will create a `CodecProvider` from that.
As you can see in the example below it's much more concise especially when defining multiple providers:

```scala
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.bson.codecs.configuration.CodecRegistries.{fromRegistries, fromProviders}

case class Address(firstLine: String, secondLine: String, thirdLine: String, town: String, zipCode: String)
case class ClubMember(person: Person, address: Address, paid: Boolean)

val codecRegistry = fromRegistries( fromProviders(classOf[ClubMember], classOf[Person], classOf[Address]), DEFAULT_CODEC_REGISTRY )
```

## Sealed classes and ADTs

Hierarchical class structures are supported via sealed traits and classes. Each subclass is handled specifically by the generated codec, so you only 
need create a `CodecProvider` for the parent sealed trait/class. Internally an extra field (`_t`) is stored alongside the data so that 
the correct subclass can be hydrated when decoding the data.  Below is an example of a tree like structure containing branch and leaf nodes:


```scala
sealed class Tree
case class Branch(b1: Tree, b2: Tree, value: Int) extends Tree
case class Leaf(value: Int) extends Tree

val codecRegistry = fromRegistries( fromProviders(classOf[Tree]), DEFAULT_CODEC_REGISTRY )
```


## Options and None values.

By default `Option` values are always stored. In 2.1.0 a new macro helpers were added so that `None` values would not be stored in the 
database. In the following example only if an address is present will it be stored in the database:

```scala
import org.mongodb.scala.bson.codecs.Macros

case class Person(firstName: String, secondName: String, address: Option[Address])

val personCodecProvider = Macros.createCodecProviderIgnoreNone[Person]()
```


## Alternative field names

The [`BsonProperty`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/bson/annotations/BsonProperty" >}}) annotation can be used to configure bson 
field key to be used for a given property. In the following example uses the `BsonProperty` annotation to change how the `firstName` 
property is stored:

```scala

case class Person(@BsonProperty("first_name") firstName: String, secondName: String)

```
