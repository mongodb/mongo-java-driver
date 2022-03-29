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

package org.mongodb.scala.bson.collection.mutable

import scala.collection.JavaConverters._
import scala.collection._
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.ListBuffer

import org.mongodb.scala.bson._
import org.mongodb.scala.bson.collection.BaseDocument

/**
 * Mutable [[Document]] companion object for easy creation.
 */
object Document {

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
  def apply(): Document = Document(BsonDocument())

  /**
   * Parses a string in MongoDB Extended JSON format to a `Document`
   *
   * @param json the JSON string
   * @return a corresponding `Document` object
   * @see org.bson.json.JsonReader
   * @see [[https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/ MongoDB Extended JSON]]
   */
  def apply(json: String): Document = Document(BsonDocument(json))

  /**
   * Create a new document from the elems
   * @param elems the key/value pairs that make up the Document. This can be any valid `(String, BsonValue)` pair that can be
   *              transformed into a [[BsonElement]] via [[BsonMagnets.CanBeBsonElement]] implicits and any [[BsonTransformer]]s that are
   *              in scope.
   * @return      a new Document consisting key/value pairs given by `elems`.
   */
  def apply(elems: CanBeBsonElement*): Document = {
    val underlying = new BsonDocument()
    elems.foreach(elem => underlying.put(elem.key, elem.value))
    new Document(underlying)
  }

  /**
   * Create a new document from the elems
   * @param elem   a sequence of key/values that make up the Document. This can be any valid sequence of `(String, BsonValue)` pairs that
   *               can be transformed into a sequence of [[BsonElement]]s via [[BsonMagnets.CanBeBsonElements]] implicits and any
   *               [[BsonTransformer]]s
   *               that are in scope.
   * @return       a new Document consisting key/value pairs given by `elems`.
   */
  def apply(elem: CanBeBsonElements): Document = {
    val underlying = new BsonDocument()
    elem.values.foreach(kv => underlying.put(kv.key, kv.value))
    new Document(underlying)
  }

  /**
   * A implicit builder factory.
   *
   * @return a builder factory.
   */
  implicit def canBuildFrom: CanBuildFrom[Traversable[(String, BsonValue)], (String, BsonValue), Document] = {
    new CanBuildFrom[Traversable[(String, BsonValue)], (String, BsonValue), Document] {
      def apply(): mutable.Builder[(String, BsonValue), Document] = builder
      def apply(from: Traversable[(String, BsonValue)]): mutable.Builder[(String, BsonValue), Document] = builder
    }
  }

  private def builder: mutable.Builder[(String, BsonValue), Document] =
    ListBuffer[(String, BsonValue)]() mapResult fromSeq

  private def fromSeq(ts: Seq[(String, BsonValue)]): Document = {
    val underlying = new BsonDocument()
    ts.foreach(kv => underlying.put(kv._1, kv._2))
    apply(underlying)
  }
}

/**
 * An mutable Document implementation.
 *
 * A strictly typed `Map[String, BsonValue]` like structure that traverses the elements in insertion order. Unlike native scala maps there
 * is no variance in the value type and it always has to be a `BsonValue`.
 *
 * @param underlying the underlying BsonDocument which stores the data.
 */
case class Document(protected[scala] val underlying: BsonDocument)
    extends BaseDocument[Document]
    with TraversableLike[(String, BsonValue), Document]
    with Mutable {

  import BsonMagnets._

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

  /**
   * Creates a new builder for this collection type.
   */
  override def newBuilder: mutable.Builder[(String, BsonValue), Document] = Document.builder

  // scalastyle:off method.name
  /**
   *  Adds a new key/value pair to this document.
   *  If the document already contains a mapping for the key, it will be overridden by the new value.
   *
   *  @param    elems the key/value pair. This can be any valid `(String, BsonValue)` pair that can be transformed into a [[BsonElement]]
   *                  via [[BsonMagnets.CanBeBsonElement]] implicits and any [[BsonTransformer]]s that are in scope.
   *  @return   the document itself
   */
  def +=(elems: CanBeBsonElement*): Document = {
    elems.foreach(elem => underlying.put(elem.key, elem.value))
    this
  }

  /**
   * Adds all elements produced by a TraversableOnce to this document.
   *
   *  @param elems a sequence of key/values that make up the Document. This can be any valid sequence of `(String, BsonValue)` pairs that
   *               can be transformed into a sequence of [[BsonElement]]s via [[BsonMagnets.CanBeBsonElements]] implicits and
   *               any [[BsonTransformer]]s
   *               that are in scope.
   *  @return      the document itself.
   */
  def ++=(elems: CanBeBsonElements): Document = {
    elems.values.foreach(elem => underlying.put(elem.key, elem.value))
    this
  }
  // scalastyle:on method.name

  /**
   * Adds a new key/value pair to this map.
   * If the document already contains a mapping for the key, it will be overridden by the new value.
   *
   *  @param key    The key to update
   *  @param value  The new value
   */
  def update[B](key: String, value: B)(implicit transformer: BsonTransformer[B]): Unit = { this += ((key, value)) }

  /**
   *  Adds a new key/value pair to this document and optionally returns previously bound value.
   *  If the document already contains a mapping for the key, it will be overridden by the new value.
   *
   * @param key    the key to update
   * @param value  the new value
   * @return an option value containing the value associated with the key before the `put` operation was executed, or
   *         `None` if `key` was not defined in the document before.
   */
  def put[B](key: String, value: B)(implicit transformer: BsonTransformer[B]): Option[BsonValue] = {
    val r = get(key)
    update(key, value)
    r
  }

  /**
   * If given key is already in this document, returns associated value.
   *
   *  Otherwise, computes value from given expression `op`, stores with key in document and returns that value.
   *  @param  key the key to test
   *  @param  op  the computation yielding the value to associate with `key`, if `key` is previously unbound.
   *  @return     the value associated with key (either previously or as a result of executing the method).
   */
  def getOrElseUpdate[B](key: String, op: => B)(implicit transformer: BsonTransformer[B]): BsonValue = {
    if (get(key).isEmpty) this += ((key, op))
    this(key)
  }

  // scalastyle:off method.name
  /**
   * Removes a key from this document.
   *  @param    key the key to be removed
   *  @return   the document itself.
   */
  def -=(key: String): Document = { underlying.remove(key); this }

  /**
   * Removes two or more elements from this document.
   *
   *  @param elems the remaining elements to remove.
   *  @return the document itself
   */
  def -=(elems: String*): Document = {
    this --= elems
  }

  /**
   * Removes all elements produced by an iterator from this document.
   *
   *  @param xs   the iterator producing the elements to remove.
   *  @return the document itself
   */
  def --=(xs: TraversableOnce[String]): Document = { xs foreach -=; this }
  // scalastyle:on method.name

  /**
   * Removes a key from this document, returning the value associated previously with that key as an option.
   *  @param    key the key to be removed
   *  @return   an option value containing the value associated previously with `key`,
   *            or `None` if `key` was not defined in the document before.
   */
  def remove(key: String): Option[BsonValue] = {
    val r = get(key)
    this -= key
    r
  }

  /**
   * Retains only those mappings for which the predicate `p` returns `true`.
   *
   * @param p  The test predicate
   */
  def retain(p: (String, BsonValue) => Boolean): Document = {
    for ((k, v) <- this)
      if (!p(k, v)) underlying.remove(k)
    this
  }

  /**
   * Removes all bindings from the document. After this operation has completed the document will be empty.
   */
  def clear(): Unit = underlying.clear()

  /**
   * Applies a transformation function to all values contained in this document.
   *  The transformation function produces new values from existing keys associated values.
   *
   * @param f  the transformation to apply
   * @return   the document itself.
   */
  def transform[B](f: (String, BsonValue) => B)(implicit transformer: BsonTransformer[B]): Document = {
    this.foreach(kv => update(kv._1, f(kv._1, kv._2)))
    this
  }

  /**
   * Copies the document and creates a new one
   *
   * @return a new document with a copy of the underlying BsonDocument
   */
  def copy(): Document = Document(copyBsonDocument())
}
