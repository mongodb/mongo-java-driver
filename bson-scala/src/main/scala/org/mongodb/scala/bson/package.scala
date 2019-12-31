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

package org.mongodb.scala

/**
 * The bson package, contains mirrors and companion objects for `Bson` values.
 */
package object bson {

  /**
   * An immutable Document implementation.
   *
   * A strictly typed `Map[String, BsonValue]` like structure that traverses the elements in insertion order. Unlike native scala maps there
   * is no variance in the value type and it always has to be a `BsonValue`.
   */
  type Document = collection.Document

  /**
   * An immutable Document implementation.
   *
   * A strictly typed `Map[String, BsonValue]` like structure that traverses the elements in insertion order. Unlike native scala maps there
   * is no variance in the value type and it always has to be a `BsonValue`.
   */
  val Document = collection.Document

  /**
   * Alias to `org.bson.BsonArray`
   */
  type BsonArray = org.bson.BsonArray

  /**
   * Alias to `org.bson.BsonBinary`
   */
  type BsonBinary = org.bson.BsonBinary

  /**
   * Alias to `org.bson.BsonBoolean`
   */
  type BsonBoolean = org.bson.BsonBoolean

  /**
   * Alias to `org.bson.BsonDateTime`
   */
  type BsonDateTime = org.bson.BsonDateTime

  /**
   * Alias to `org.bson.BsonDecimal128`
   * @since 1.2
   */
  type BsonDecimal128 = org.bson.BsonDecimal128

  /**
   * Alias to `org.bson.BsonDocument`
   */
  type BsonDocument = org.bson.BsonDocument

  /**
   * Alias to `org.bson.BsonDouble`
   */
  type BsonDouble = org.bson.BsonDouble

  /**
   * Alias to `org.bson.BsonInt32`
   */
  type BsonInt32 = org.bson.BsonInt32

  /**
   * Alias to `org.bson.BsonInt64`
   */
  type BsonInt64 = org.bson.BsonInt64

  /**
   * Alias to `org.bson.BsonJavaScript`
   */
  type BsonJavaScript = org.bson.BsonJavaScript

  /**
   * Alias to `org.bson.BsonJavaScriptWithScope`
   */
  type BsonJavaScriptWithScope = org.bson.BsonJavaScriptWithScope

  /**
   * Alias to `org.bson.BsonMaxKey`
   */
  type BsonMaxKey = org.bson.BsonMaxKey

  /**
   * Alias to `org.bson.BsonMinKey`
   */
  type BsonMinKey = org.bson.BsonMinKey

  /**
   * Alias to `org.bson.BsonNull`
   */
  type BsonNull = org.bson.BsonNull

  /**
   * Alias to `org.bson.BsonNumber`
   */
  type BsonNumber = org.bson.BsonNumber

  /**
   * Alias to `org.bson.BsonObjectId`
   */
  type BsonObjectId = org.bson.BsonObjectId

  /**
   * Alias to `org.bson.BsonRegularExpression`
   */
  type BsonRegularExpression = org.bson.BsonRegularExpression

  /**
   * Alias to `org.bson.BsonString`
   */
  type BsonString = org.bson.BsonString

  /**
   * Alias to `org.bson.BsonSymbol`
   */
  type BsonSymbol = org.bson.BsonSymbol

  /**
   * Alias to `org.bson.BsonTimestamp`
   */
  type BsonTimestamp = org.bson.BsonTimestamp

  /**
   * Alias to `org.bson.BsonUndefined`
   */
  type BsonUndefined = org.bson.BsonUndefined

  /**
   * Alias to `org.bson.BsonValue`
   */
  type BsonValue = org.bson.BsonValue

  /**
   * Alias to `org.bson.BsonElement`
   */
  type BsonElement = org.bson.BsonElement

  /**
   * Alias to `org.bson.ObjectId`
   * @since 1.2
   */
  type ObjectId = org.bson.types.ObjectId

  /**
   * Alias to `org.bson.Decimal128`
   * @since 1.2
   */
  type Decimal128 = org.bson.types.Decimal128

  /**
   * Implicit value class for a [[BsonElement]] allowing easy access to the key/value pair
   *
   * @param self the bsonElement
   */
  implicit class RichBsonElement(val self: BsonElement) extends AnyVal {
    def key: String = self.getName
    def value: BsonValue = self.getValue
  }

}
