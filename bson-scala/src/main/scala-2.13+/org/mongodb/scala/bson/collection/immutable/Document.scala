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

package org.mongodb.scala.bson.collection.immutable

import scala.jdk.CollectionConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.{ Iterable, IterableOps, SpecificIterableFactory, StrictOptimizedIterableOps, mutable }
import org.mongodb.scala.bson._
import org.mongodb.scala.bson.collection.BaseDocument

/**
 * The immutable [[Document]] companion object for easy creation.
 */
object Document extends SpecificIterableFactory[(String, BsonValue), Document] {

  import BsonMagnets._

  /**
   * Create a new empty Document
   * @return a new Document
   */
  def empty: Document = apply()

  /**
   * Create a new Document
   * @return a new Document
   */
  def apply(): Document = new Document(new BsonDocument())

  /**
   * Parses a string in MongoDB Extended JSON format to a `Document`
   *
   * @param json the JSON string
   * @throws org.bson.json.JsonParseException if passed an invalid json string
   * @return a corresponding `Document` object
   * @see org.bson.json.JsonReader
   * @see [[http://docs.mongodb.com/manual/reference/mongodb-extended-json/ MongoDB Extended JSON]]
   */
  def apply(json: String): Document = new Document(BsonDocument(json))

  /**
   * Create a new document from the elems
   * @param elems   the key/value pairs that make up the Document. This can be any valid `(String, BsonValue)` pair that can be
   *                transformed into a [[BsonElement]] via [[BsonMagnets.CanBeBsonElement]] implicits and any [[BsonTransformer]]s that
   *                are in scope.
   * @return        a new Document consisting key/value pairs given by `elems`.
   */
  def apply(elems: CanBeBsonElement*): Document = {
    val underlying = new BsonDocument()
    elems.foreach(elem => underlying.put(elem.key, elem.value))
    new Document(underlying)
  }

  /**
   * Create a new document from the elems
   * @param elems  a sequence of key/values that make up the Document. This can be any valid sequence of `(String, BsonValue)` pairs that
   *               can be transformed into a sequence of [[BsonElement]]s via [[BsonMagnets.CanBeBsonElements]] implicits and any
   *               [[BsonTransformer]]s
   *               that are in scope.
   * @return       a new Document consisting key/value pairs given by `elems`.
   */
  def apply(elems: CanBeBsonElements): Document = {
    val underlying = new BsonDocument()
    elems.values.foreach(el => underlying.put(el.key, el.value))
    new Document(underlying)
  }

  def builder: mutable.Builder[(String, BsonValue), Document] = ListBuffer[(String, BsonValue)]() mapResult fromSeq

  def fromSeq(ts: Seq[(String, BsonValue)]): Document = {
    val underlying = new BsonDocument()
    ts.foreach(kv => underlying.put(kv._1, kv._2))
    apply(underlying)
  }

  override def newBuilder: mutable.Builder[(String, BsonValue), Document] = builder
  override def fromSpecific(it: IterableOnce[(String, BsonValue)]): Document = fromSeq(it.iterator.toSeq)
}

/**
 * An immutable Document implementation.
 *
 * A strictly typed `Map[String, BsonValue]` like structure that traverses the elements in insertion order. Unlike native scala maps there
 * is no variance in the value type and it always has to be a `BsonValue`.
 *
 * @param underlying the underlying BsonDocument which stores the data.
 *
 */
case class Document(protected[scala] val underlying: BsonDocument)
    extends BaseDocument[Document]
    with IterableOps[(String, BsonValue), Iterable, Document]
    with StrictOptimizedIterableOps[(String, BsonValue), Iterable, Document] {

  /**
   * Creates a new immutable document
   * @param underlying the underlying BsonDocument
   * @return a new document
   */
  protected[scala] def apply(underlying: BsonDocument) = new Document(underlying)

  /**
   * Applies a function `f` to all elements of this document.
   *
   * @param  f   the function that is applied for its side-effect to every element.
   *             The result of function `f` is discarded.
   *
   * @tparam  U  the type parameter describing the result of function `f`.
   *             This result will always be ignored. Typically `U` is `Unit`,
   *             but this is not necessary.
   *
   */
  override def foreach[U](f: ((String, BsonValue)) => U): Unit = underlying.asScala foreach f

  // Mandatory overrides of `fromSpecific`, `newSpecificBuilder`,
  // and `empty`, from `IterableOps`
  override protected def fromSpecific(coll: IterableOnce[(String, BsonValue)]): Document = Document.fromSpecific(coll)
  override protected def newSpecificBuilder: mutable.Builder[(String, BsonValue), Document] = Document.newBuilder
  override def empty: Document = Document.empty

  // Overloading of `appended`, `prepended`, `appendedAll`, `prependedAll`,
  // `map`, `flatMap` and `concat` to return an `RNA` when possible
  def concat(suffix: IterableOnce[(String, BsonValue)]): Document = strictOptimizedConcat(suffix, newSpecificBuilder)
  // scalastyle:off method.name
  @inline final def ++(suffix: IterableOnce[(String, BsonValue)]): Document = concat(suffix)
  // scalastyle:on method.name
  def map[B](f: ((String, BsonValue)) => (String, BsonValue)): Document = strictOptimizedMap(newSpecificBuilder, f)

}
