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

package org.mongodb.scala.bson.collection

import scala.collection.JavaConverters._
import scala.collection.{ GenTraversableOnce, Traversable }
import scala.reflect.ClassTag
import scala.util.{ Failure, Success, Try }

import org.bson.json.JsonWriterSettings

import org.mongodb.scala.bson.DefaultHelper._
import org.mongodb.scala.bson._
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.bson.conversions.Bson

import org.mongodb.scala.bson.BsonMagnets

/**
 * Base Document trait.
 *
 * A strictly typed `Traversable[(String, BsonValue)]` and provides the underlying immutable document behaviour.
 * See [[immutable.Document]] or [[mutable.Document]] for the concrete implementations.
 *
 * @tparam T The concrete Document implementation
 */
private[bson] trait BaseDocument[T] extends Traversable[(String, BsonValue)] with Bson {

  import BsonMagnets._

  /**
   * The underlying bson document
   *
   * Restricted access to the underlying BsonDocument
   */
  protected[scala] val underlying: BsonDocument

  /**
   * Create a concrete document instance
   *
   * @param underlying the underlying BsonDocument
   * @return a concrete document instance
   */
  protected[scala] def apply(underlying: BsonDocument): T

  /**
   * Retrieves the value which is associated with the given key or throws a `NoSuchElementException`.
   *
   * @param  key the key
   * @return     the value associated with the given key, or throws `NoSuchElementException`.
   */
  def apply[TResult <: BsonValue](key: String)(implicit e: TResult DefaultsTo BsonValue, ct: ClassTag[TResult]): TResult = {
    get[TResult](key) match {
      case Some(value) => value
      case None => throw new NoSuchElementException("key not found: " + key)
    }
  }

  /**
   * Returns the value associated with a key, or a default value if the key is not contained in the map.
   * @param   key      the key.
   * @param   default  The default value in case no binding for `key` is found in the Document.
   *                   This can be any [[BsonValue]] type or any native type that has an implicit [[BsonTransformer]] in scope.
   * @tparam  B        the result type of the default computation.
   * @return  the value associated with `key` if it exists,
   *          otherwise the result of the `default` computation.
   */
  def getOrElse[B >: BsonValue](key: String, default: CanBeBsonValue): B = get(key) match {
    case Some(v) => v
    case None => default.value
  }

  //scalastyle:off spaces.after.plus  method.name
  /**
   * Creates a new document containing a new key/value and all the existing key/values.
   *
   * Mapping `kv` will override existing mappings from this document with the same key.
   *
   * @param elems the key/value mapping to be added. This can be any valid `(String, BsonValue)` pair that can be transformed into a
   *              [[BsonElement]] via [[BsonMagnets.CanBeBsonElement]] implicits and any [[BsonTransformer]]s that are in scope.
   * @return      a new document containing mappings of this document and the mapping `kv`.
   */
  def +(elems: CanBeBsonElement*): T = {
    val bsonDocument: BsonDocument = copyBsonDocument()
    elems.foreach(elem => bsonDocument.put(elem.key, elem.value))
    apply(bsonDocument)
  }
  //scalastyle:on spaces.after.plus

  /**
   * Removes one or more elements to this document and returns a new document.
   *
   * @param elems the remaining elements to remove.
   * @return A new document with the keys removed.
   */
  def -(elems: String*): T = --(elems)

  /**
   * Removes a number of elements provided by a traversable object and returns a new document without the removed elements.
   *
   * @param xs      the traversable object consisting of key-value pairs.
   * @return        a new document with the bindings of this document and those from `xs`.
   */
  def --(xs: GenTraversableOnce[String]): T = {
    val keysToIgnore = xs.toList
    val newUnderlying = new BsonDocument()
    for ((k, v) <- iterator if !keysToIgnore.contains(k)) {
      newUnderlying.put(k, v)
    }
    apply(newUnderlying)
  }
  // scalastyle:on method.name

  /**
   * Creates a new Document consisting of all key/value pairs of the current document
   * plus a new pair of a given key and value.
   *
   * @param key    The key to add
   * @param value  The new value
   * @return       A fresh immutable document with the binding from `key` to `value` added to the new document.
   */
  def updated[B](key: String, value: B)(implicit transformer: BsonTransformer[B]): T = this + ((key, value))

  /**
   * Creates a new Document consisting of all key/value pairs of the current document
   * plus a new pair of a given key and value.
   *
   * @param elems  The key/values to add. This can be any valid `(String, BsonValue)` pair that can be transformed into a
   *              [[BsonElement]] via [[BsonMagnets.CanBeBsonElement]] implicits and any [[BsonTransformer]]s that are in scope.
   * @return       A fresh immutable document with the binding from `key` to `value` added to the new document.
   */
  def updated(elems: CanBeBsonElement*): T = this + (elems: _*)

  /**
   * Optionally returns the value associated with a key.
   *
   * @param  key  the key we want to lookup
   * @return an option value containing the value associated with `key` in this document,
   *         or `None` if none exists.
   */
  def get[TResult <: BsonValue](key: String)(implicit e: TResult DefaultsTo BsonValue, ct: ClassTag[TResult]): Option[TResult] = {
    underlying.containsKey(key) match {
      case true => Try(ct.runtimeClass.cast(underlying.get(key))) match {
        case Success(v) => Some(v.asInstanceOf[TResult])
        case Failure(ex) => None
      }
      case false => None
    }
  }

  /**
   * Creates a new iterator over all key/value pairs in this document
   *
   * @return the new iterator
   */
  def iterator: Iterator[(String, BsonValue)] = underlying.asScala.iterator

  /**
   * Filters this document by retaining only keys satisfying a predicate.
   * @param  p   the predicate used to test keys
   * @return a new document consisting only of those key value pairs of this map where the key satisfies
   *         the predicate `p`.
   */
  def filterKeys(p: String => Boolean): T = this -- keys.filterNot(p)

  /**
   * Tests whether this map contains a binding for a key
   *
   * @param key the key
   * @return true if there is a binding for key in this document, false otherwise.
   */
  def contains(key: String): Boolean = underlying.containsKey(key)

  /**
   * Collects all keys of this document in a set.
   *
   * @return  a set containing all keys of this document.
   */
  def keySet: Set[String] = underlying.keySet().asScala.toSet

  /**
   * Collects all keys of this document in an iterable collection.
   *
   * @return the keys of this document as an iterable.
   */
  def keys: Iterable[String] = keySet.toIterable

  /**
   * Creates an iterator for all keys.
   *
   * @return an iterator over all keys.
   */
  def keysIterator: Iterator[String] = keySet.toIterator

  /**
   * Collects all values of this document in an iterable collection.
   *
   * @return the values of this document as an iterable.
   */
  def values: Iterable[BsonValue] = underlying.values().asScala

  /**
   * Creates an iterator for all values in this document.
   *
   * @return an iterator over all values that are associated with some key in this document.
   */
  def valuesIterator: Iterator[BsonValue] = values.toIterator

  /**
   * Gets a JSON representation of this document
   *
   * @return a JSON representation of this document
   */
  def toJson(): String = underlying.toJson

  /**
   * Gets a JSON representation of this document using the given `JsonWriterSettings`.
   * @param settings the JSON writer settings
   * @return a JSON representation of this document
   */
  def toJson(settings: JsonWriterSettings): String = underlying.toJson(settings)

  /**
   * Returns a copy of the underlying BsonDocument
   */
  def toBsonDocument: BsonDocument = copyBsonDocument()

  override def toBsonDocument[TDocument](documentClass: Class[TDocument], codecRegistry: CodecRegistry): BsonDocument = underlying

  /**
   * Copies the BsonDocument
   * @return the copied BsonDocument
   */
  private[collection] def copyBsonDocument(): BsonDocument = {
    val bsonDocument = BsonDocument()
    for (entry <- underlying.entrySet().asScala) bsonDocument.put(entry.getKey, entry.getValue)
    bsonDocument
  }

}
