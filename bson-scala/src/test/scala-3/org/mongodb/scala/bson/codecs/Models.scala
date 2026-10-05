package org.mongodb.scala.bson.codecs

import org.bson.BsonDocument
import org.bson.types.ObjectId
import org.mongodb.scala.bson.annotations.{BsonIgnore, BsonProperty}

import java.util.Date

object Models {

  case class IsValueClass(id: Int) extends AnyVal

  case class ContainsValueClass(id: IsValueClass, myString: String)

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
  case class ContainsLazyList(name: String, friends: LazyList[String])

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
                                  a: Int & Tag,
                                  b: String & Tag,
                                  c: Map[String & Tag, Int & Tag],
                                  d: Empty & Tag
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

  case class ContainsEnumADT(name: String, `enum`: CaseObjectEnum)

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

  // --- Multiple @BsonProperty annotations ---
  case class MultiAnnotated(
    @BsonProperty("first") firstName: String,
    @BsonProperty("last") lastName: String,
    age: Int
  )

  // --- Mixed @BsonProperty + @BsonIgnore ---
  case class MixedAnnotations(
    @BsonProperty("n") name: String,
    @BsonIgnore hidden: String = "default",
    email: String
  )

  // --- Multiple default values ---
  case class MultipleDefaults(
    name: String = "unknown",
    active: Boolean = false,
    count: Int = 0,
    required: String
  )

  // --- Default + Option combinations ---
  case class DefaultOption(
    name: String = "unknown",
    age: Option[Int] = None,
    email: Option[String] = Some("default@test.com")
  )

  // --- Nested default values ---
  case class WithNestedDefault(
    name: String,
    friend: Person = Person("Default", "Friend")
  )

  // --- All primitives ---
  case class AllPrimitives(
    b: Boolean,
    sh: Short,
    i: Int,
    l: Long,
    f: Float,
    d: Double,
    str: String
  )

  // --- Empty collections ---
  case class EmptyCollections(
    emptySeq: Seq[String],
    emptyMap: Map[String, String],
    emptySet: Set[String],
    emptyList: List[String]
  )

  // --- Many fields (20) ---
  case class ManyFields(
    f1: String, f2: Int, f3: Boolean, f4: Long, f5: Double,
    f6: String, f7: Int, f8: Boolean, f9: Long, f10: Double,
    f11: String, f12: Int, f13: Boolean, f14: Long, f15: Double,
    f16: String, f17: Int, f18: Boolean, f19: Long, f20: Double
  )

  // --- Deeply nested ---
  case class Level3(value: String)
  case class Level2(inner: Level3)
  case class Level1(inner: Level2)

  // --- Sealed hierarchy with many subclasses ---
  sealed trait Animal
  case class Dog(name: String, breed: String) extends Animal
  case class Cat(name: String, indoor: Boolean) extends Animal
  case class Bird(species: String) extends Animal
  case class Fish(species: String, freshwater: Boolean) extends Animal

  // --- Self-referential via Option ---
  case class LinkedNode(value: String, next: Option[LinkedNode])

  // --- Map with case class values ---
  case class MapOfCaseClassValues(entries: Map[String, Person])

  // --- Sealed with case objects and case classes mixed ---
  sealed trait MixedADT
  case class DataNode(value: String) extends MixedADT
  case object EmptyNode extends MixedADT

}
