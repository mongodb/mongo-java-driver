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

import org.bson._
import org.bson.codecs.configuration.{ CodecProvider, CodecRegistries, CodecRegistry }
import org.bson.codecs.{ Codec, DecoderContext, EncoderContext }
import org.bson.io.{ BasicOutputBuffer, ByteBufferBsonInput, OutputBuffer }
import org.bson.types.ObjectId
import org.mongodb.scala.bson.BaseSpec
import org.mongodb.scala.bson.annotations.{ BsonIgnore, BsonProperty }
import org.mongodb.scala.bson.codecs.Macros.{ createCodecProvider, createCodecProviderIgnoreNone }
import org.mongodb.scala.bson.codecs.Registry.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.collection.immutable.Document

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

//scalastyle:off
class MacrosSpec extends BaseSpec {

  case class Empty()
  case class Person(firstName: String, lastName: String)
  case class DefaultValue(name: String, active: Boolean = false)
  case class SeqOfStrings(name: String, value: Seq[String])
  case class RecursiveSeq(name: String, value: Seq[RecursiveSeq])
  case class AnnotatedClass(@BsonProperty("annotated_name") name: String)
  case class IgnoredFieldClass(name: String, @BsonIgnore meta: String = "ignored_default")

  case class Binary(binary: Array[Byte]) {

    /**
     * Custom equals
     *
     * Because `Array[Byte]` only does equality based on identity we use toSeq helper to compare the actual values.
     *
     * @param arg the other value
     * @return true if equal else false
     */
    override def equals(arg: Any): Boolean = arg match {
      case that: Binary => that.binary.toSeq == binary.toSeq
      case _            => false
    }
  }
  case class AllTheBsonTypes(
      documentMap: Map[String, String],
      array: Seq[String],
      date: Date,
      boolean: Boolean,
      double: Double,
      int32: Int,
      int64: Long,
      string: String,
      binary: Binary,
      none: Option[String]
  )

  case class MapOfStrings(name: String, value: Map[String, String])
  case class SeqOfMapOfStrings(name: String, value: Seq[Map[String, String]])
  case class RecursiveMapOfStrings(name: String, value: Seq[Map[String, RecursiveMapOfStrings]])

  type StringAlias = String
  case class MapOfStringAliases(name: String, value: Map[StringAlias, StringAlias])

  case class ContainsCaseClass(name: String, friend: Person)
  case class ContainsSeqCaseClass(name: String, friends: Seq[Person])
  case class ContainsNestedSeqCaseClass(name: String, friends: Seq[Seq[Person]])
  case class ContainsMapOfCaseClasses(name: String, friends: Map[String, Person])
  case class ContainsMapOfMapOfCaseClasses(name: String, friends: Map[String, Map[String, Person]])
  case class ContainsCaseClassWithDefault(name: String, friend: Person = Person("Frank", "Sinatra"))

  case class ContainsSet(name: String, friends: Set[String])
  case class ContainsVector(name: String, friends: Vector[String])
  case class ContainsList(name: String, friends: List[String])
  case class ContainsStream(name: String, friends: Stream[String])

  case class CaseClassWithVal(_id: ObjectId, name: String) {
    val id: String = _id.toString
  }

  case class OptionalValue(name: String, value: Option[String])
  case class OptionalCaseClass(name: String, value: Option[Person])
  case class OptionalRecursive(name: String, value: Option[OptionalRecursive])

  sealed class Tree
  case class Branch(@BsonProperty("l1") b1: Tree, @BsonProperty("r1") b2: Tree, value: Int) extends Tree
  case class Leaf(value: Int) extends Tree

  sealed trait WithIgnored
  case class MetaIgnoredField(data: String, @BsonIgnore meta: Seq[String] = Vector("ignore_me")) extends WithIgnored
  case class LeafCountIgnoredField(branchCount: Int, @BsonIgnore leafCount: Int = 100) extends WithIgnored
  case class ContainsIgnoredField(list: Seq[WithIgnored])

  case class ContainsADT(name: String, tree: Tree)
  case class ContainsSeqADT(name: String, trees: Seq[Tree])
  case class ContainsNestedSeqADT(name: String, trees: Seq[Seq[Tree]])

  sealed class Graph
  case class Node(name: String, value: Option[Graph]) extends Graph

  sealed class NotImplementedSealedClass
  sealed trait NotImplementedSealedTrait
  case class UnsupportedTuple(value: (String, String))
  case class UnsupportedMap(value: Map[Int, Int])

  type SimpleTypeAlias = Map[String, String]
  case class ContainsSimpleTypeAlias(a: String, b: SimpleTypeAlias = Map.empty)
  type CaseClassTypeAlias = Person
  case class ContainsCaseClassTypeAlias(a: String, b: CaseClassTypeAlias)
  type ADTCaseClassTypeAlias = ContainsADT
  case class ContainsADTCaseClassTypeAlias(a: String, b: ADTCaseClassTypeAlias)

  trait Tag
  case class ContainsTaggedTypes(
      a: Int with Tag,
      b: String with Tag,
      c: Map[String with Tag, Int with Tag] with Tag,
      d: Empty with Tag
  ) extends Tag

  case class ContainsTypeLessMap(a: BsonDocument)

  sealed class SealedClassCaseObject
  object SealedClassCaseObject {
    case object Alpha extends SealedClassCaseObject
  }

  sealed trait CaseObjectEnum
  case object Alpha extends CaseObjectEnum
  case object Bravo extends CaseObjectEnum
  case object Charlie extends CaseObjectEnum

  case class ContainsEnumADT(name: String, enum: CaseObjectEnum)

  sealed class SealedClass
  case class SealedClassA(stringField: String) extends SealedClass
  case class SealedClassB(intField: Int) extends SealedClass
  case class ContainsSealedClass(list: List[SealedClass])

  sealed abstract class SealedAbstractClass
  case class SealedAbstractClassA(stringField: String) extends SealedAbstractClass
  case class SealedAbstractClassB(intField: Int) extends SealedAbstractClass
  case class ContainsSealedAbstractClass(list: List[SealedAbstractClass])

  sealed class SealedClassWithParams(val superField: String)
  case class SealedClassWithParamsA(stringField: String, override val superField: String)
      extends SealedClassWithParams(superField)
  case class SealedClassWithParamsB(intField: Int, override val superField: String)
      extends SealedClassWithParams(superField)
  case class ContainsSealedClassWithParams(list: List[SealedClassWithParams])

  sealed abstract class SealedAbstractClassWithParams(val superField: String)
  case class SealedAbstractClassWithParamsA(stringField: String, override val superField: String)
      extends SealedAbstractClassWithParams(superField)
  case class SealedAbstractClassWithParamsB(intField: Int, override val superField: String)
      extends SealedAbstractClassWithParams(superField)
  case class ContainsSealedAbstractClassWithParams(list: List[SealedAbstractClassWithParams])

  sealed trait SealedTrait
  case class SealedTraitA(stringField: String) extends SealedTrait
  case class SealedTraitB(intField: Int) extends SealedTrait
  case class ContainsSealedTrait(list: List[SealedTrait])

  sealed class SingleSealedClass
  case class SingleSealedClassImpl() extends SingleSealedClass

  sealed abstract class SingleSealedAbstractClass
  case class SingleSealedAbstractClassImpl() extends SingleSealedAbstractClass

  sealed trait SingleSealedTrait
  case class SingleSealedTraitImpl() extends SingleSealedTrait

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
      Macros.createCodecProvider(classOf[ContainsSet])
    )
    roundTrip(
      ContainsVector("Bob", Vector("Tom", "Charlie")),
      """{name: "Bob", friends: ["Tom","Charlie"]}""",
      Macros.createCodecProvider(classOf[ContainsVector])
    )
    roundTrip(
      ContainsList("Bob", List("Tom", "Charlie")),
      """{name: "Bob", friends: ["Tom","Charlie"]}""",
      Macros.createCodecProvider(classOf[ContainsList])
    )
    roundTrip(
      ContainsStream("Bob", Stream("Tom", "Charlie")),
      """{name: "Bob", friends: ["Tom","Charlie"]}""",
      Macros.createCodecProvider(classOf[ContainsStream])
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
      ContainsMapOfCaseClasses("Bob", Map("mother" -> Person("Jane", "Jones"))),
      """{name: "Bob", friends: {mother: {firstName: "Jane", lastName: "Jones"}}}""",
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
    val registry =
      CodecRegistries.fromRegistries(CodecRegistries.fromProviders(classOf[OptionalValue]), DEFAULT_CODEC_REGISTRY)
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
    val registry = CodecRegistries.fromRegistries(
      CodecRegistries.fromProviders(classOf[CaseClassWithVal]),
      DEFAULT_CODEC_REGISTRY
    )

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
    roundTrip(OptionalValue("Bob", None), """{name: "Bob"}""", createCodecProviderIgnoreNone[OptionalValue]())
    roundTrip(
      OptionalValue("Bob", Some("value")),
      """{name: "Bob", value: "value"}""",
      createCodecProviderIgnoreNone[OptionalValue]()
    )
    roundTrip(OptionalCaseClass("Bob", None), """{name: "Bob"}""", createCodecProviderIgnoreNone[OptionalCaseClass]())
    roundTrip(
      OptionalCaseClass("Bob", Some(Person("Charlie", "Jones"))),
      """{name: "Bob", value: {firstName: "Charlie", lastName: "Jones"}}""",
      createCodecProviderIgnoreNone[OptionalCaseClass](),
      createCodecProviderIgnoreNone[Person]()
    )

    roundTrip(OptionalRecursive("Bob", None), """{name: "Bob"}""", createCodecProviderIgnoreNone[OptionalRecursive]())
    roundTrip(
      OptionalRecursive("Bob", Some(OptionalRecursive("Charlie", None))),
      """{name: "Bob", value: {name: "Charlie"}}""",
      createCodecProviderIgnoreNone[OptionalRecursive]()
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
    val a = 1.asInstanceOf[Int with Tag]
    val b = "b".asInstanceOf[String with Tag]
    val c = Map("c" -> 0).asInstanceOf[Map[String with Tag, Int with Tag] with Tag]
    val d = Empty().asInstanceOf[Empty with Tag]
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
      classOf[ContainsValueClass],
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
    val registry = CodecRegistries.fromRegistries(CodecRegistries.fromProviders(classOf[Graph]), DEFAULT_CODEC_REGISTRY)

    val buffer = encode(registry.get(classOf[Document]), Document(missing_t))

    an[BsonInvalidOperationException] should be thrownBy {
      decode(registry.get(classOf[Graph]), buffer)
    }
  }

  it should "support throw a CodecConfigurationException with an unknown class name in the _t field" in {
    val missing_t = """{_t: "Wibble", name: "nodeA", value: null}"""
    val registry = CodecRegistries.fromRegistries(CodecRegistries.fromProviders(classOf[Graph]), DEFAULT_CODEC_REGISTRY)
    val buffer = encode(registry.get(classOf[Document]), Document(missing_t))

    an[BsonInvalidOperationException] should be thrownBy {
      decode(registry.get(classOf[Graph]), buffer)
    }
  }

  it should "throw a CodecConfigurationException when encountering null values in case classes" in {
    val registry =
      CodecRegistries.fromRegistries(CodecRegistries.fromProviders(classOf[Person]), DEFAULT_CODEC_REGISTRY)
    an[BsonInvalidOperationException] should be thrownBy {
      encode(registry.get(classOf[Person]), null)
    }

    an[BsonInvalidOperationException] should be thrownBy {
      encode(registry.get(classOf[Person]), Person(null, null))
    }
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
    val registry = CodecRegistries.fromRegistries(
      CodecRegistries.fromProviders(classOf[ContainsCaseClass], classOf[Person]),
      DEFAULT_CODEC_REGISTRY
    )
    an[BsonInvalidOperationException] should be thrownBy {
      val json = """{name: "Bob", friend: [{firstName: "Jane", lastName: "Ada"}]}"""
      decode(ContainsCaseClass("Bob", Person("Jane", "Ada")), json, registry.get(classOf[ContainsCaseClass]))
    }
  }

  it should "error when reading unexpected documents" in {
    val registry = CodecRegistries.fromRegistries(
      CodecRegistries.fromProviders(classOf[ContainsCaseClass], classOf[Person]),
      DEFAULT_CODEC_REGISTRY
    )
    an[BsonInvalidOperationException] should be thrownBy {
      val json = """{name: "Bob", friend: {first: {firstName: "Jane", lastName: "Ada"}}}"""
      decode(ContainsCaseClass("Bob", Person("Jane", "Ada")), json, registry.get(classOf[ContainsCaseClass]))
    }
  }

  def roundTrip[T](value: T, expected: String, provider: CodecProvider, providers: CodecProvider*)(
      implicit ct: ClassTag[T]
  ): Unit = {
    val codecProviders: util.List[CodecProvider] = (provider +: providers).asJava
    val registry = CodecRegistries.fromRegistries(CodecRegistries.fromProviders(codecProviders), DEFAULT_CODEC_REGISTRY)
    val codec = registry.get(ct.runtimeClass).asInstanceOf[Codec[T]]
    roundTripCodec(value, Document(expected), codec)
  }

  def roundTrip[T](value: T, decodedValue: T, expected: String, provider: CodecProvider, providers: CodecProvider*)(
      implicit ct: ClassTag[T]
  ): Unit = {
    val codecProviders: util.List[CodecProvider] = (provider +: providers).asJava
    val registry = CodecRegistries.fromRegistries(CodecRegistries.fromProviders(codecProviders), DEFAULT_CODEC_REGISTRY)
    val codec = registry.get(ct.runtimeClass).asInstanceOf[Codec[T]]
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

case class IsValueClass(id: Int) extends AnyVal
case class ContainsValueClass(id: IsValueClass, myString: String)
