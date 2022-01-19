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

package org.mongodb.scala.bson

import java.util.Date

import scala.jdk.CollectionConverters._
import scala.util.matching.Regex

import org.bson.{ BsonDocument => JBsonDocument }

/**
 * Companion helper for a BsonArray
 *
 * @since 1.0
 */
object BsonArray {
  import BsonMagnets._

  /**
   * Create an empty BsonArray
   * @return the BsonArray
   */
  def apply(): BsonArray = new BsonArray()

  /**
   * Create a BsonArray from the provided values
   *
   * @param elems the `BsonValues` to become the `BsonArray`
   * @return the BsonArray
   */
  @deprecated("Use `fromIterable` instead", "2.7.0")
  def apply(elems: Iterable[BsonValue]): BsonArray = fromIterable(elems)

  /**
   * Create a BsonArray from the provided values
   *
   * @param elems the `BsonValues` to become the `BsonArray`
   * @return the BsonArray
   */
  def fromIterable(elems: Iterable[BsonValue]): BsonArray = new BsonArray(elems.toList.asJava)

  /**
   * Creates a BsonArray from the provided values
   *
   *
   * @param elems the values that can be transformed into a `BsonValue`
   * @return the BsonArray
   */
  def apply(elems: CanBeBsonValue*): BsonArray = new BsonArray(elems.map(_.value).asJava)
}

/**
 * Companion helper for a BsonBinary
 * @since 1.0
 */
object BsonBinary {

  /**
   * Creates the BsonBinary form the provided bytes
   *
   * @param value the bytes
   * @return the BsonBinary
   */
  def apply(value: Array[Byte]): BsonBinary = new BsonBinary(value)
}

/**
 * Companion helper for a BsonBoolean
 * @since 1.0
 */
object BsonBoolean {

  /**
   * Creates a `BsonBoolean`
   *
   * @param value the value
   * @return the BsonBoolean
   */
  def apply(value: Boolean): BsonBoolean = new BsonBoolean(value)
}

/**
 * Companion helper for a BsonDateTime
 * @since 1.0
 */
object BsonDateTime {

  /**
   * Creates a BsonDateTime
   *
   * @param value the number of milliseconds since the Unix epoch
   * @return the BsonDateTime
   */
  def apply(value: Long): BsonDateTime = new BsonDateTime(value)

  /**
   * Creates a BsonDateTime
   *
   * @param date a `java.util.Date`
   * @return the BsonDateTime
   */
  def apply(date: Date): BsonDateTime = new BsonDateTime(date.getTime)
}

/**
 * Companion helper for a BsonDecimal128
 * @since 1.2
 */
object BsonDecimal128 {

  /**
   * Creates a `BsonDecimal128`
   *
   * @param value the `Decimal128`
   * @return the BigDecimal
   */
  def apply(value: Decimal128): BsonDecimal128 = new BsonDecimal128(value)

  /**
   * Creates a `BsonDecimal128`
   *
   * @param value the `BigDecimal`
   * @return the BigDecimal
   */
  def apply(value: BigDecimal): BsonDecimal128 = apply(new Decimal128(value.bigDecimal))

  /**
   * Creates a `BsonDecimal128`
   *
   * @param value the long value to convert
   * @return the BigDecimal
   */
  def apply(value: Long): BsonDecimal128 = apply(new Decimal128(value))

  /**
   * Creates a `BsonDecimal128`
   *
   * @param value the string value to convert
   * @return the BigDecimal
   */
  def apply(value: String): BsonDecimal128 = apply(org.bson.types.Decimal128.parse(value))
}

/**
 * Companion helper for a BsonDocument
 * @since 1.0
 */
object BsonDocument {

  import BsonMagnets._

  /**
   * Creates an empty `BsonDocument`
   * @return the BsonDocument
   */
  def apply(): BsonDocument = new JBsonDocument()

  /**
   * Creates a `BsonDocument` from the key value pairs
   *
   * @param elems a traversable of key, value pairs
   * @return the BsonDocument
   */
  def apply(elems: Iterable[(String, BsonValue)]): BsonDocument = {
    val bsonDocument = new JBsonDocument()
    elems.foreach(kv => bsonDocument.put(kv._1, kv._2))
    bsonDocument
  }

  /**
   * Creates a `BsonDocument` from key value pairs
   *
   * @param elems the key, value pairs
   * @return the BsonDocument
   */
  def apply(elems: CanBeBsonElement*): BsonDocument = {
    val bsonDocument = new JBsonDocument()
    elems.foreach(elem => bsonDocument.put(elem.key, elem.value))
    bsonDocument
  }

  /**
   * Creates a `BsonDocument` from a json String
   *
   * @param json the json string
   * @return the BsonDocumet
   */
  def apply(json: String): BsonDocument = JBsonDocument.parse(json)
}

/**
 * Companion helper for a BsonDouble
 * @since 1.0
 */
object BsonDouble {

  /**
   * Creates a `BsonDouble`
   *
   * @param value the BsonDouble value
   * @return the BsonDouble
   */
  def apply(value: Double): BsonDouble = new BsonDouble(value)
}

/**
 * Companion helper for a BsonInt32
 * @since 1.0
 */
object BsonInt32 {

  /**
   * Creates a `BsonInt32`
   *
   * @param value the BsonInt32 value
   * @return the BsonInt32
   */
  def apply(value: Int): BsonInt32 = new BsonInt32(value)
}

/**
 * Companion helper for a BsonInt64
 * @since 1.0
 */
object BsonInt64 {

  /**
   * Creates a `BsonInt64`
   *
   * @param value the BsonInt64 value
   * @return the BsonInt64
   */
  def apply(value: Long): BsonInt64 = new BsonInt64(value)
}

/**
 * Companion helper for a BsonJavaScript
 * @since 1.0
 */
object BsonJavaScript {

  /**
   * Creates a `BsonJavaScript`
   *
   * @param value the javascript function
   * @return the BsonJavaScript
   */
  def apply(value: String): BsonJavaScript = new BsonJavaScript(value)
}

/**
 * Companion helper for a BsonJavaScriptWithScope
 * @since 1.0
 */
object BsonJavaScriptWithScope {
  import BsonMagnets._

  /**
   * Creates a `BsonJavaScript`
   *
   * @param value the javascript function
   * @param scope the function scope
   * @return the BsonJavaScript
   */
  def apply(value: String, scope: BsonDocument): BsonJavaScriptWithScope = new BsonJavaScriptWithScope(value, scope)

  /**
   * Creates a `BsonJavaScript`
   *
   * @param value the javascript function
   * @param scope the function scope
   * @return the BsonJavaScript
   */
  def apply(value: String, scope: CanBeBsonElement*): BsonJavaScriptWithScope =
    new BsonJavaScriptWithScope(value, BsonDocument(scope: _*))

  /**
   * Creates a `BsonJavaScript`
   *
   * @param value the javascript function
   * @param scope the function scope
   * @return the BsonJavaScript
   */
  def apply(value: String, scope: Iterable[(String, BsonValue)]): BsonJavaScriptWithScope =
    new BsonJavaScriptWithScope(value, BsonDocument(scope))
}

/**
 * Companion helper for a BsonMaxKey
 * @since 1.0
 */
object BsonMaxKey {

  /**
   * Creates a `BsonMaxKey`
   * @return the BsonMaxKey
   */
  def apply(): BsonMaxKey = new BsonMaxKey()
}

/**
 * Companion helper for a BsonMinKey
 * @since 1.0
 */
object BsonMinKey {

  /**
   * Creates a `BsonMinKey`
   * @return the BsonMinKey
   */
  def apply(): BsonMinKey = new BsonMinKey()
}

/**
 * Companion helper for a BsonNull
 * @since 1.0
 */
object BsonNull {

  /**
   * Creates a `BsonNull`
   * @return the BsonNull
   */
  def apply(): BsonNull = new BsonNull()
}

/**
 * Companion helper for a BsonNumber
 * @since 1.0
 */
object BsonNumber {

  /**
   * Creates a `BsonNumber`
   *
   * @param value the value
   * @return the BsonNumber
   */
  def apply(value: Int): BsonNumber = new BsonInt32(value)

  /**
   * Creates a `BsonNumber`
   *
   * @param value the value
   * @return the BsonNumber
   */
  def apply(value: Long): BsonNumber = new BsonInt64(value)

  /**
   * Creates a `BsonNumber`
   *
   * @param value the value
   * @return the BsonNumber
   */
  def apply(value: Double): BsonNumber = new BsonDouble(value)
}

/**
 * Companion helper for a BsonObjectId
 * @since 1.0
 */
object BsonObjectId {

  /**
   * Creates a new `BsonObjectId`
   *
   * @return the BsonObjectId
   */
  def apply(): BsonObjectId = new BsonObjectId(new ObjectId())

  /**
   * Creates a new `BsonObjectId`
   *
   * @param value the 24-byte hexadecimal string representation of an `ObjectId`.
   * @return the BsonObjectId
   */
  def apply(value: String): BsonObjectId = new BsonObjectId(new ObjectId(value))

  /**
   * Creates a new `BsonObjectId`
   *
   * @param value the `ObjectId`.
   * @return the BsonObjectId
   */
  def apply(value: ObjectId): BsonObjectId = new BsonObjectId(value)
}

/**
 * Companion helper for a BsonRegularExpression
 * @since 1.0
 */
object BsonRegularExpression {

  /**
   * Creates a new `BsonRegularExpression`
   *
   * @param value the `Regex`.
   * @return the BsonRegularExpression
   */
  def apply(value: Regex): BsonRegularExpression = new BsonRegularExpression(value.regex)

  /**
   * Creates a new `BsonRegularExpression`
   *
   * @param value the Regex string.
   * @return the BsonRegularExpression
   */
  def apply(value: String): BsonRegularExpression = new BsonRegularExpression(value)

  /**
   * Creates a new `BsonRegularExpression`
   *
   * @param value the Regex string.
   * @param options the regex options tring
   * @return the BsonRegularExpression
   */
  def apply(value: String, options: String): BsonRegularExpression = new BsonRegularExpression(value, options)
}

/**
 * Companion helper for a BsonString
 * @since 1.0
 */
object BsonString {

  /**
   * Creates a new `BsonString`
   *
   * @param value the string.
   * @return the BsonString
   */
  def apply(value: String): BsonString = new BsonString(value)
}

/**
 * Companion helper for a BsonSymbol
 * @since 1.0
 */
object BsonSymbol {

  /**
   * Creates a new `BsonSymbol`
   *
   * @param value the Symbol.
   * @return the BsonSymbol
   */
  def apply(value: Symbol): BsonSymbol = new BsonSymbol(value.name)
}

/**
 * Companion helper for a BsonTimestamp
 * @since 1.0
 */
object BsonTimestamp {

  /**
   * Creates a new `BsonTimestamp`
   * @return the BsonTimestamp
   */
  def apply(): BsonTimestamp = new BsonTimestamp(0, 0)

  /**
   * Creates a new `BsonTimestamp`
   * @param time the time in seconds since epoch
   * @param inc  an incrementing ordinal for operations within a given second
   * @return the BsonTimestamp
   */
  def apply(time: Int, inc: Int): BsonTimestamp = new BsonTimestamp(time, inc)
}

/**
 * Companion helper for a BsonUndefined
 * @since 1.0
 */
object BsonUndefined {

  /**
   * Creates a new `BsonUndefined`
   * @return the BsonUndefined
   */
  def apply(): BsonUndefined = new BsonUndefined()
}
