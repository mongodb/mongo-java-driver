/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mongodb.scala.bson.codecs

import java.nio.ByteBuffer
import java.util
import java.util.Date
import org.bson.*
import org.bson.codecs.configuration.{CodecProvider, CodecRegistries, CodecRegistry}
import org.bson.codecs.{BsonValueCodecProvider, Codec, DecoderContext, EncoderContext, ValueCodecProvider}
import org.bson.io.{BasicOutputBuffer, ByteBufferBsonInput, OutputBuffer}
import org.bson.types.ObjectId
import org.mongodb.scala.bson.Document
import org.mongodb.scala.bson.codecs.{DocumentCodecProvider, IterableCodecProvider}
import org.mongodb.scala.bson.collection.immutable.Document
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import Models.*
import Macros.createCodecProvider

import scala.language.implicitConversions
import scala.collection.immutable.Vector
import scala.reflect.ClassTag


//scalastyle:off
class MacrosSpec extends AnyFlatSpec with Matchers {

  val DEFAULT_CODEC_REGISTRY: CodecRegistry = CodecRegistries.fromProviders(
    DocumentCodecProvider(),
    IterableCodecProvider(),
    new ValueCodecProvider(),
    new BsonValueCodecProvider()
  )


  "Macros" should "be able to round trip simple case classes" in {
    roundTrip(Empty(), "{}", classOf[Empty])
    roundTrip(Person("Bob", "Jones"), """{firstName: "Bob", lastName: "Jones"}""", classOf[Person])
    roundTrip(DefaultValue(name = "Bob"), """{name: "Bob", active: false}""", classOf[DefaultValue])
    roundTrip(
      SeqOfStrings("Bob", Seq("scala", "jvm")),
      """{name: "Bob", value: ["scala", "jvm"]}""",
      classOf[SeqOfStrings]
    )
    roundTrip(
      RecursiveSeq("Bob", Seq(RecursiveSeq("Charlie", Seq.empty[RecursiveSeq]))),
      """{name: "Bob", value: [{name: "Charlie", value: []}]}""",
      classOf[RecursiveSeq]
    )
    roundTrip(AnnotatedClass("Bob"), """{annotated_name: "Bob"}""", classOf[AnnotatedClass])
    roundTrip(
      MapOfStrings("Bob", Map("brother" -> "Tom Jones")),
      """{name: "Bob", value: {brother: "Tom Jones"}}""",
      classOf[MapOfStrings]
    )
    roundTrip(
      MapOfStringAliases("Bob", Map("brother" -> "Tom Jones")),
      """{name: "Bob", value: {brother: "Tom Jones"}}""",
      classOf[MapOfStringAliases]
    )
    roundTrip(
      SeqOfMapOfStrings("Bob", Seq(Map("brother" -> "Tom Jones"))),
      """{name: "Bob", value: [{brother: "Tom Jones"}]}""",
      classOf[SeqOfMapOfStrings]
    )
    roundTrip(
      ContainsSet("Bob", Set("Tom", "Charlie")),
      """{name: "Bob", friends: ["Tom","Charlie"]}""",
      classOf[ContainsSet]
    )
    roundTrip(
      ContainsVector("Bob", Vector("Tom", "Charlie")),
      """{name: "Bob", friends: ["Tom","Charlie"]}""",
      classOf[ContainsVector]
    )
    roundTrip(
      ContainsList("Bob", List("Tom", "Charlie")),
      """{name: "Bob", friends: ["Tom","Charlie"]}""",
      classOf[ContainsList]
    )
    roundTrip(
      ContainsLazyList("Bob", LazyList("Tom", "Charlie")),
      """{name: "Bob", friends: ["Tom","Charlie"]}""",
      classOf[ContainsLazyList]
    )
  }

  it should "be able to ignore fields" in {
    roundTrip(
      IgnoredFieldClass("Bob", "singer"),
      IgnoredFieldClass("Bob"),
      """{name: "Bob"}""",
      classOf[IgnoredFieldClass]
    )

    roundTrip(
      ContainsIgnoredField(Vector(MetaIgnoredField("Bob", List("singer")), LeafCountIgnoredField(1, 10))),
      ContainsIgnoredField(Vector(MetaIgnoredField("Bob"), LeafCountIgnoredField(1))),
      """{"list" : [{"_t" : "MetaIgnoredField", "data" : "Bob" }, {"_t" : "LeafCountIgnoredField", "branchCount": 1}]}""",
      classOf[ContainsIgnoredField],
      classOf[WithIgnored]
    )
  }

  it should "be able to round trip polymorphic nested case classes in a sealed class" in {
    roundTrip(
      ContainsSealedClass(List(SealedClassA("test"), SealedClassB(12))),
      """{"list" : [{"_t" : "SealedClassA", "stringField" : "test"}, {"_t" : "SealedClassB", "intField" : 12}]}""",
      classOf[ContainsSealedClass],
      classOf[SealedClass]
    )
  }

  it should "be able to round trip polymorphic nested case classes in a sealed abstract class" in {
    roundTrip(
      ContainsSealedAbstractClass(List(SealedAbstractClassA("test"), SealedAbstractClassB(12))),
      """{"list" : [{"_t" : "SealedAbstractClassA", "stringField" : "test"}, {"_t" : "SealedAbstractClassB", "intField" : 12}]}""",
      classOf[ContainsSealedAbstractClass],
      classOf[SealedAbstractClass]
    )
  }

  it should "be able to round trip polymorphic nested case classes in a sealed class with parameters" in {
    roundTrip(
      ContainsSealedClassWithParams(
        List(SealedClassWithParamsA("test", "tested1"), SealedClassWithParamsB(12, "tested2"))
      ),
      """{"list" : [{"_t" : "SealedClassWithParamsA", "stringField" : "test", "superField" : "tested1"}, {"_t" : "SealedClassWithParamsB", "intField" : 12, "superField" : "tested2"}]}""",
      classOf[ContainsSealedClassWithParams],
      classOf[SealedClassWithParams]
    )
  }

  it should "be able to round trip polymorphic nested case classes in a sealed abstract class with parameters" in {
    roundTrip(
      ContainsSealedAbstractClassWithParams(
        List(SealedAbstractClassWithParamsA("test", "tested1"), SealedAbstractClassWithParamsB(12, "tested2"))
      ),
      """{"list" : [{"_t" : "SealedAbstractClassWithParamsA", "stringField" : "test", "superField" : "tested1"}, {"_t" : "SealedAbstractClassWithParamsB", "intField" : 12, "superField" : "tested2"}]}""",
      classOf[ContainsSealedAbstractClassWithParams],
      classOf[SealedAbstractClassWithParams]
    )
  }

  it should "be able to round trip polymorphic nested case classes in a sealed trait" in {
    roundTrip(
      ContainsSealedTrait(List(SealedTraitA("test"), SealedTraitB(12))),
      """{"list" : [{"_t" : "SealedTraitA", "stringField" : "test"}, {"_t" : "SealedTraitB", "intField" : 12}]}""",
      classOf[ContainsSealedTrait],
      classOf[SealedTrait]
    )
  }

  it should "be able to round trip nested case classes" in {
    roundTrip(
      ContainsCaseClass("Charlie", Person("Bob", "Jones")),
      """{name: "Charlie", friend: {firstName: "Bob", lastName: "Jones"}}""",
      classOf[ContainsCaseClass],
      classOf[Person]
    )
    roundTrip(
      ContainsSeqCaseClass("Charlie", Seq(Person("Bob", "Jones"))),
      """{name: "Charlie", friends: [{firstName: "Bob", lastName: "Jones"}]}""",
      classOf[ContainsSeqCaseClass],
      classOf[Person]
    )
    roundTrip(
      ContainsNestedSeqCaseClass("Charlie", Seq(Seq(Person("Bob", "Jones")), Seq(Person("Tom", "Jones")))),
      """{name: "Charlie", friends: [[{firstName: "Bob", lastName: "Jones"}], [{firstName: "Tom", lastName: "Jones"}]]}""",
      classOf[ContainsNestedSeqCaseClass],
      classOf[Person]
    )
  }

  it should "be able to round trip nested case classes in maps" in {
    roundTrip(
      ContainsMapOfCaseClasses("Bob", Map("name" -> Person("Jane", "Jones"))),
      """{name: "Bob", friends: {name: {firstName: "Jane", lastName: "Jones"}}}""",
      classOf[ContainsMapOfCaseClasses],
      classOf[Person]
    )
    roundTrip(
      ContainsMapOfMapOfCaseClasses("Bob", Map("maternal" -> Map("mother" -> Person("Jane", "Jones")))),
      """{name: "Bob", friends: {maternal: {mother: {firstName: "Jane", lastName: "Jones"}}}}""",
      classOf[ContainsMapOfMapOfCaseClasses],
      classOf[Person]
    )
  }

  it should "be able to round trip optional values" in {
    roundTrip(OptionalValue("Bob", None), """{name: "Bob", value: null}""", classOf[OptionalValue])
    roundTrip(OptionalValue("Bob", Some("value")), """{name: "Bob", value: "value"}""", classOf[OptionalValue])
    roundTrip(OptionalCaseClass("Bob", None), """{name: "Bob", value: null}""", classOf[OptionalCaseClass])
    roundTrip(
      OptionalCaseClass("Bob", Some(Person("Charlie", "Jones"))),
      """{name: "Bob", value: {firstName: "Charlie", lastName: "Jones"}}""",
      classOf[OptionalCaseClass],
      classOf[Person]
    )

    roundTrip(OptionalRecursive("Bob", None), """{name: "Bob", value: null}""", classOf[OptionalRecursive])
    roundTrip(
      OptionalRecursive("Bob", Some(OptionalRecursive("Charlie", None))),
      """{name: "Bob", value: {name: "Charlie", value: null}}""",
      classOf[OptionalRecursive]
    )
  }

  it should "be able to round trip Map values where the top level implementations don't include type information" in {
    roundTrip(
      ContainsTypeLessMap(BsonDocument.parse("""{b: "c"}""")),
      """{a: {b: "c"}}""",
      classOf[ContainsTypeLessMap]
    )
  }

  it should "be able to decode case classes missing optional values" in {
    val registry = toRegistry(classOf[OptionalValue])
    val buffer = encode(registry.get(classOf[Document]), Document("name" -> "Bob"))

    decode(registry.get(classOf[OptionalValue]), buffer) should equal(OptionalValue("Bob", None))
  }

  it should "be able to round trip default values" in {
    roundTrip(
      ContainsCaseClassWithDefault("Charlie"),
      """{name: "Charlie", friend: { firstName: "Frank", lastName: "Sinatra"}}""",
      classOf[ContainsCaseClassWithDefault],
      classOf[Person]
    )
  }

  it should "rountrip case classes containing vals" in {
    val id = new ObjectId
    roundTrip(
      CaseClassWithVal(id, "Bob"),
      s"""{"_id": {"$$oid": "${id.toHexString}" }, "name" : "Bob"}""",
      classOf[CaseClassWithVal]
    )
  }

  it should "be able to decode case class with vals" in {
    val registry = toRegistry(classOf[CaseClassWithVal])

    val id = new ObjectId
    val buffer = encode(
      registry.get(classOf[Document]),
      Document("_id" -> id, "name" -> "Bob")
    )

    decode(
      registry.get(classOf[CaseClassWithVal]),
      buffer
    ) should equal(CaseClassWithVal(id, "Bob"))
  }

  it should "be able to round trip optional values, when None is ignored" in {
    roundTrip(OptionalValue("Bob", None), """{name: "Bob"}""", Macros.createCodecProviderIgnoreNone[OptionalValue]())
    roundTrip(
      OptionalValue("Bob", Some("value")),
      """{name: "Bob", value: "value"}""",
      Macros.createCodecProviderIgnoreNone[OptionalValue]()
    )
    roundTrip(OptionalCaseClass("Bob", None), """{name: "Bob"}""", Macros.createCodecProviderIgnoreNone[OptionalCaseClass]())
    roundTrip(
      OptionalCaseClass("Bob", Some(Person("Charlie", "Jones"))),
      """{name: "Bob", value: {firstName: "Charlie", lastName: "Jones"}}""",
      Macros.createCodecProviderIgnoreNone[OptionalCaseClass](),
      Macros.createCodecProviderIgnoreNone[Person]()
    )

    roundTrip(OptionalRecursive("Bob", None), """{name: "Bob"}""", Macros.createCodecProviderIgnoreNone[OptionalRecursive]())
    roundTrip(
      OptionalRecursive("Bob", Some(OptionalRecursive("Charlie", None))),
      """{name: "Bob", value: {name: "Charlie"}}""",
      Macros.createCodecProviderIgnoreNone[OptionalRecursive]()
    )
  }

  it should "roundtrip all the supported bson types" in {
    roundTrip(
      AllTheBsonTypes(
        Map("a" -> "b"),
        Seq("a", "b", "c"),
        new Date(123),
        boolean = true,
        1.0,
        10,
        100L,
        "string",
        Binary(Array[Byte](123)),
        None
      ),
      """{"documentMap" : { "a" : "b" }, "array" : ["a", "b", "c"], "date" : { "$date" : 123 }, "boolean" : true,
        | "double" : 1.0, "int32" : 10, "int64" : { "$numberLong" : "100" }, "string" : "string",
        | "binary" : { "binary": { "$binary" : "ew==", "$type" : "00" } }, "none" : null }""".stripMargin,
      classOf[Binary],
      classOf[AllTheBsonTypes]
    )
  }

  it should "support ADT sealed case classes" in {
    val leaf = Leaf(1)
    val branch = Branch(Branch(Leaf(1), Leaf(2), 3), Branch(Leaf(4), Leaf(5), 6), 3) // scalastyle:ignore
    val leafJson = createTreeJson(leaf)
    val branchJson = createTreeJson(branch)

    roundTrip(leaf, leafJson, classOf[Tree])
    roundTrip(branch, branchJson, classOf[Tree])

    roundTrip(ContainsADT("Bob", leaf), s"""{name: "Bob", tree: $leafJson}""", classOf[ContainsADT], classOf[Tree])
    roundTrip(ContainsADT("Bob", branch), s"""{name: "Bob", tree: $branchJson}""", classOf[ContainsADT], classOf[Tree])

    roundTrip(
      ContainsSeqADT("Bob", List(leaf, branch)),
      s"""{name: "Bob", trees: [$leafJson, $branchJson]}""",
      classOf[ContainsSeqADT],
      classOf[Tree]
    )
    roundTrip(
      ContainsNestedSeqADT("Bob", List(List(leaf), List(branch))),
      s"""{name: "Bob", trees: [[$leafJson], [$branchJson]]}""",
      classOf[ContainsNestedSeqADT],
      classOf[Tree]
    )
  }

  it should "write the type of sealed classes and traits with only one subclass" in {
    roundTrip(SingleSealedClassImpl(), """{ "_t" : "SingleSealedClassImpl" }""".stripMargin, classOf[SingleSealedClass])
    roundTrip(
      SingleSealedAbstractClassImpl(),
      """{ "_t" : "SingleSealedAbstractClassImpl" }""".stripMargin,
      classOf[SingleSealedAbstractClass]
    )
    roundTrip(SingleSealedTraitImpl(), """{ "_t" : "SingleSealedTraitImpl" }""".stripMargin, classOf[SingleSealedTrait])
  }

  it should "support optional values in ADT sealed classes" in {
    val nodeA = Node("nodeA", None)
    val nodeB = Node("nodeB", Some(nodeA))

    val nodeAJson = """{_t: "Node", name: "nodeA", value: null}"""
    val nodeBJson = s"""{_t: "Node", name: "nodeB", value: $nodeAJson}"""

    roundTrip(nodeA, nodeAJson, classOf[Graph])
    roundTrip(nodeB, nodeBJson, classOf[Graph])
  }

  it should "support type aliases in case classes" in {
    roundTrip(
      ContainsSimpleTypeAlias("c", Map("d" -> "c")),
      """{a: "c", b: {d: "c"}}""",
      classOf[ContainsSimpleTypeAlias]
    )
    roundTrip(
      ContainsCaseClassTypeAlias("c", Person("Tom", "Jones")),
      """{a: "c", b: {firstName: "Tom", lastName: "Jones"}}""",
      classOf[ContainsCaseClassTypeAlias],
      classOf[CaseClassTypeAlias]
    )

    val branch = Branch(Branch(Leaf(1), Leaf(2), 3), Branch(Leaf(4), Leaf(5), 6), 3) // scalastyle:ignore
    val branchJson = createTreeJson(branch)
    roundTrip(
      ContainsADTCaseClassTypeAlias("c", ContainsADT("Tom", branch)),
      s"""{a: "c", b: {name: "Tom", tree: $branchJson}}""",
      classOf[ContainsADTCaseClassTypeAlias],
      classOf[ADTCaseClassTypeAlias],
      classOf[Tree]
    )
  }

  it should "support tagged types in case classes" in {
    assume(!scala.util.Properties.versionNumberString.startsWith("2.11"))
    val a = 1.asInstanceOf[Int & Tag]
    val b = "b".asInstanceOf[String & Tag]
    val c = Map("c" -> 0).asInstanceOf[Map[String & Tag, Int & Tag]]
    val d = Empty().asInstanceOf[Empty & Tag]
    roundTrip(
      ContainsTaggedTypes(a, b, c, d),
      """{a: 1, b: "b", c: {c: 0}, d: {}}""",
      classOf[ContainsTaggedTypes],
      classOf[Empty]
    )
  }

  it should "be able to support value classes" in {
    val valueClassCodecProvider = new CodecProvider {
      override def get[T](clazz: Class[T], registry: CodecRegistry): Codec[T] = {
        if (clazz == classOf[IsValueClass]) {
          new Codec[IsValueClass] {
            override def encode(writer: BsonWriter, value: IsValueClass, encoderContext: EncoderContext): Unit =
              writer.writeInt32(value.id)

            override def getEncoderClass: Class[IsValueClass] = classOf[IsValueClass]

            override def decode(reader: BsonReader, decoderContext: DecoderContext): IsValueClass =
              IsValueClass(reader.readInt32())
          }.asInstanceOf[Codec[T]]
        } else {
          null // scalastyle:ignore
        }
      }
    }
    roundTrip(
      ContainsValueClass(IsValueClass(1), "string value"),
      """{id: 1, myString: 'string value'}""",
      Macros.createCodecProvider[ContainsValueClass](),
      valueClassCodecProvider
    )
  }

  it should "support case object enum types" in {
    roundTrip(Alpha, """{_t:"Alpha"}""", classOf[CaseObjectEnum])
    roundTrip(Bravo, """{_t:"Bravo"}""", classOf[CaseObjectEnum])
    roundTrip(Charlie, """{_t:"Charlie"}""", classOf[CaseObjectEnum])

    roundTrip(
      ContainsEnumADT("Bob", Alpha),
      """{name:"Bob", enum:{_t:"Alpha"}}""",
      classOf[ContainsEnumADT],
      classOf[CaseObjectEnum]
    )
  }

  it should "support extra fields in the document" in {
    val json =
      """{firstName: "Bob", lastName: "Jones", address: {number: 1, street: "Acacia Avenue"}, aliases: ["Robert", "Rob"]}"""
    decode(Person("Bob", "Jones"), json, Macros.createCodec[Person]())
  }

  it should "support throw a CodecConfigurationException missing _t field" in {
    val missing_t = """{name: "nodeA", value: null}"""
    val registry = toRegistry(classOf[Graph])

    val buffer = encode(registry.get(classOf[Document]), Document(missing_t))

    an[BsonInvalidOperationException] should be thrownBy {
      decode(registry.get(classOf[Graph]), buffer)
    }
  }

  it should "support throw a CodecConfigurationException with an unknown class name in the _t field" in {
    val missing_t = """{_t: "Wibble", name: "nodeA", value: null}"""
    val registry = toRegistry(classOf[Graph])
    val buffer = encode(registry.get(classOf[Document]), Document(missing_t))

    an[BsonInvalidOperationException] should be thrownBy {
      decode(registry.get(classOf[Graph]), buffer)
    }
  }

  it should "throw a CodecConfigurationException when encountering null values in case classes" in {
    val registry = toRegistry(classOf[Person])
    an[BsonInvalidOperationException] should be thrownBy {
      encode(registry.get(classOf[Person]), null)
    }

    an[BsonInvalidOperationException] should be thrownBy {
      encode(registry.get(classOf[Person]), Person(null, null))
    }
  }

  it should "support multiple @BsonProperty annotations" in {
    roundTrip(
      MultiAnnotated("Bob", "Jones", 30),
      """{first: "Bob", last: "Jones", age: 30}""",
      classOf[MultiAnnotated]
    )
  }

  it should "support mixed @BsonProperty and @BsonIgnore annotations" in {
    roundTrip(
      MixedAnnotations("Bob", "singer", "bob@test.com"),
      MixedAnnotations("Bob", "default", "bob@test.com"),
      """{n: "Bob", email: "bob@test.com"}""",
      classOf[MixedAnnotations]
    )
  }

  it should "handle multiple default values" in {
    roundTrip(
      MultipleDefaults(required = "yes"),
      """{name: "unknown", active: false, count: 0, required: "yes"}""",
      classOf[MultipleDefaults]
    )
    roundTrip(
      MultipleDefaults("Bob", true, 5, "yes"),
      """{name: "Bob", active: true, count: 5, required: "yes"}""",
      classOf[MultipleDefaults]
    )
  }

  it should "handle default + Option combinations" in {
    roundTrip(
      DefaultOption(),
      """{name: "unknown", age: null, email: "default@test.com"}""",
      classOf[DefaultOption]
    )
    roundTrip(
      DefaultOption("Bob", Some(30), Some("bob@test.com")),
      """{name: "Bob", age: 30, email: "bob@test.com"}""",
      classOf[DefaultOption]
    )
  }

  it should "handle nested default values" in {
    roundTrip(
      WithNestedDefault("Charlie"),
      """{name: "Charlie", friend: {firstName: "Default", lastName: "Friend"}}""",
      classOf[WithNestedDefault],
      classOf[Person]
    )
  }

  it should "round trip all primitive types" in {
    roundTrip(
      AllPrimitives(true, 1.toShort, 42, 100L, 1.5f, 2.5, "hello"),
      """{b: true, sh: 1, i: 42, l: {"$numberLong": "100"}, f: 1.5, d: 2.5, str: "hello"}""",
      classOf[AllPrimitives]
    )
  }

  it should "round trip empty collections" in {
    roundTrip(
      EmptyCollections(Seq.empty, Map.empty, Set.empty, List.empty),
      """{emptySeq: [], emptyMap: {}, emptySet: [], emptyList: []}""",
      classOf[EmptyCollections]
    )
  }

  it should "handle case classes with many fields" in {
    roundTrip(
      ManyFields("a", 1, true, 2L, 3.0, "b", 4, false, 5L, 6.0, "c", 7, true, 8L, 9.0, "d", 10, false, 11L, 12.0),
      """{f1:"a",f2:1,f3:true,f4:{"$numberLong":"2"},f5:3.0,f6:"b",f7:4,f8:false,f9:{"$numberLong":"5"},f10:6.0,f11:"c",f12:7,f13:true,f14:{"$numberLong":"8"},f15:9.0,f16:"d",f17:10,f18:false,f19:{"$numberLong":"11"},f20:12.0}""",
      classOf[ManyFields]
    )
  }

  it should "handle deeply nested case classes" in {
    roundTrip(
      Level1(Level2(Level3("deep"))),
      """{inner: {inner: {value: "deep"}}}""",
      classOf[Level1],
      classOf[Level2],
      classOf[Level3]
    )
  }

  it should "support sealed traits with many subclasses" in {
    roundTrip(
      Dog("Rex", "Labrador"),
      """{_t: "Dog", name: "Rex", breed: "Labrador"}""",
      classOf[Animal]
    )
    roundTrip(
      Cat("Whiskers", true),
      """{_t: "Cat", name: "Whiskers", indoor: true}""",
      classOf[Animal]
    )
    roundTrip(
      Bird("Parrot"),
      """{_t: "Bird", species: "Parrot"}""",
      classOf[Animal]
    )
    roundTrip(
      Fish("Goldfish", true),
      """{_t: "Fish", species: "Goldfish", freshwater: true}""",
      classOf[Animal]
    )
  }

  it should "support self-referential case classes via Option" in {
    roundTrip(
      LinkedNode("first", None),
      """{value: "first", next: null}""",
      classOf[LinkedNode]
    )
    roundTrip(
      LinkedNode("first", Some(LinkedNode("second", None))),
      """{value: "first", next: {value: "second", next: null}}""",
      classOf[LinkedNode]
    )
  }

  it should "support mixed sealed hierarchies with case objects and case classes" in {
    roundTrip(
      DataNode("hello"),
      """{_t: "DataNode", value: "hello"}""",
      classOf[MixedADT]
    )
    roundTrip(
      EmptyNode,
      """{_t: "EmptyNode"}""",
      classOf[MixedADT]
    )
  }

  it should "support maps with case class values" in {
    roundTrip(
      MapOfCaseClassValues(Map("friend" -> Person("Jane", "Doe"))),
      """{entries: {friend: {firstName: "Jane", lastName: "Doe"}}}""",
      classOf[MapOfCaseClassValues],
      classOf[Person]
    )
  }

  it should "use default values when decoding documents with missing fields" in {
    val registry = toRegistry(classOf[MultipleDefaults])
    val codec = registry.get(classOf[MultipleDefaults])

    // Decode document with only the required field — all others should use defaults
    decode(MultipleDefaults(required = "yes"), """{required: "yes"}""", codec)

    // Decode document with some optional fields provided
    decode(MultipleDefaults("Bob", true, 0, "yes"), """{name: "Bob", active: true, required: "yes"}""", codec)
  }

  it should "use default values for Option fields when missing from document" in {
    val registry = toRegistry(classOf[DefaultOption])
    val codec = registry.get(classOf[DefaultOption])

    // Decode empty document — all fields should use their defaults
    decode(DefaultOption(), """{  }""", codec)

    // Decode with only name — age and email should use their defaults (None and Some("default@test.com"))
    decode(DefaultOption("Bob"), """{name: "Bob"}""", codec)
  }

  it should "use nested default values when decoding documents with missing fields" in {
    val registry = toRegistry(classOf[WithNestedDefault], classOf[Person])
    val codec = registry.get(classOf[WithNestedDefault])

    // Decode document missing the friend field — should use default Person("Default", "Friend")
    decode(WithNestedDefault("Charlie"), """{name: "Charlie"}""", codec)
  }

  it should "use default value for DefaultValue case class when active is missing" in {
    val registry = toRegistry(classOf[DefaultValue])
    val codec = registry.get(classOf[DefaultValue])

    // Decode document with only name — active should default to false
    decode(DefaultValue("Bob"), """{name: "Bob"}""", codec)
  }

  it should "not compile case classes with unsupported values" in {
    "Macros.createCodecProvider(classOf[UnsupportedTuple])" shouldNot compile
    "Macros.createCodecProvider(classOf[UnsupportedMap])" shouldNot compile
  }

  it should "not compile if there are no concrete implementations of a sealed class or trait" in {
    "Macros.createCodecProvider(classOf[NotImplementedSealedClass])" shouldNot compile
    "Macros.createCodecProvider(classOf[NotImplementedSealedTrait])" shouldNot compile
  }

  it should "error when reading unexpected lists" in {
    val registry = toRegistry(classOf[ContainsCaseClass], classOf[Person])
    an[BsonInvalidOperationException] should be thrownBy {
      val json = """{name: "Bob", friend: [{firstName: "Jane", lastName: "Ada"}]}"""
      decode(ContainsCaseClass("Bob", Person("Jane", "Ada")), json, registry.get(classOf[ContainsCaseClass]))
    }
  }

  it should "error when reading unexpected documents" in {
    val registry = toRegistry(classOf[ContainsCaseClass], classOf[Person])
    an[BsonInvalidOperationException] should be thrownBy {
      val json = """{name: "Bob", friend: {first: {firstName: "Jane", lastName: "Ada"}}}"""
      decode(ContainsCaseClass("Bob", Person("Jane", "Ada")), json, registry.get(classOf[ContainsCaseClass]))
    }
  }

  def toRegistry(providers: CodecProvider*): CodecRegistry = {
    CodecRegistries.fromRegistries(
      CodecRegistries.fromProviders(providers*),
      DEFAULT_CODEC_REGISTRY
    )
  }

  def roundTrip[T](
                    value: T,
                    expected: String,
                    providers: CodecProvider*
                  )(implicit
                    ct: ClassTag[T]
                  ): Unit = {
    val codecRegistry = toRegistry(providers*)
    val codec = codecRegistry.get(ct.runtimeClass).asInstanceOf[Codec[T]]
    roundTripCodec(value, Document(expected), codec)
  }

  def roundTrip[T](
                    value: T,
                    expected: String,
                    codecRegistry: CodecRegistry
                  )(implicit
                    ct: ClassTag[T]
                  ): Unit = {
    val codec = codecRegistry.get(ct.runtimeClass).asInstanceOf[Codec[T]]
    roundTripCodec(value, Document(expected), codec)
  }

  def roundTripWithRegistry[T](
                                value: T,
                                expected: String,
                                codecRegistry: CodecRegistry
                              )(implicit
                                ct: ClassTag[T]
                              ): Unit = {
    val codec = codecRegistry.get(ct.runtimeClass).asInstanceOf[Codec[T]]
    roundTripCodec(value, Document(expected), codec)
  }

  def roundTrip[T](
                    value: T,
                    decodedValue: T,
                    expected: String,
                    providers: CodecProvider*
                  )(implicit
                    ct: ClassTag[T]
                  ): Unit = {
    val codec = toRegistry(providers*).get(ct.runtimeClass).asInstanceOf[Codec[T]]
    roundTripCodec(value, decodedValue, Document(expected), codec)
  }


  def roundTripCodec[T](value: T, expected: Document, codec: Codec[T]): Unit = {
    val encoded = encode(codec, value)
    val actual = decode(documentCodec, encoded)
    assert(expected == actual, s"Encoded document: (${actual.toJson()}) did not equal: (${expected.toJson()})")

    val roundTripped = decode(codec, encode(codec, value))
    assert(roundTripped == value, s"Round Tripped case class: ($roundTripped) did not equal the original: ($value)")
  }

  def roundTripCodec[T](value: T, decodedValue: T, expected: Document, codec: Codec[T]): Unit = {
    val encoded = encode(codec, value)
    val actual = decode(documentCodec, encoded)
    assert(expected == actual, s"Encoded document: (${actual.toJson()}) did not equal: (${expected.toJson()})")

    val roundTripped = decode(codec, encode(codec, value))
    assert(
      roundTripped == decodedValue,
      s"Round Tripped case class: ($roundTripped) did not equal the expected: ($decodedValue)"
    )
  }

  def encode[T](codec: Codec[T], value: T): OutputBuffer = {
    val buffer = new BasicOutputBuffer()
    val writer = new BsonBinaryWriter(buffer)
    codec.encode(writer, value, EncoderContext.builder.build)
    buffer
  }

  def decode[T](codec: Codec[T], buffer: OutputBuffer): T = {
    val reader = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(buffer.toByteArray))))
    codec.decode(reader, DecoderContext.builder().build())
  }

  def decode[T](value: T, json: String, codec: Codec[T]): Unit = {
    val roundTripped = decode(codec, encode(documentCodec, Document(json)))
    assert(roundTripped == value, s"Round Tripped case class: ($roundTripped) did not equal the original: ($value)")
  }

  def assertEncodes[T](
                        value: T,
                        decodedValue: String,
                        providers: CodecProvider*
                      )(implicit
                        ct: ClassTag[T]
                      ): Unit = {
    val codec = toRegistry(providers *).get(ct.runtimeClass).asInstanceOf[Codec[T]]
    val encoded = encode(codec, value)
    val expected = Document(decodedValue)
    val actual = decode(documentCodec, encoded)
    assert(expected == actual, s"Encoded document: (${actual.toJson()}) did not equal: (${expected.toJson()})")
  }

  def assertDecodes[T](
                        value: T,
                        decodedValue: String,
                        providers: CodecProvider*
                      )(implicit
                        ct: ClassTag[T]
                      ): Unit = {
    val codec = toRegistry(providers *).get(ct.runtimeClass).asInstanceOf[Codec[T]]
    val expected = Document(decodedValue)
    val buffer = encode(documentCodec, expected)
    val actual = decode(codec, buffer)
    assert(value == actual, s"Decoded document: (${expected.toJson()}) did not equal: ($value)")
  }

  val documentCodec: Codec[Document] = DEFAULT_CODEC_REGISTRY.get(classOf[Document])

  def createTreeJson(tree: Tree): String = {
    tree match {
      case l: Leaf => s"""{_t: "Leaf", value: ${l.value}}"""
      case b: Branch =>
        s"""{_t: "Branch", l1: ${createTreeJson(b.b1)}, r1: ${createTreeJson(b.b2)}, value: ${b.value}}"""
      case _ => "{}"
    }
  }

}
