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

import scala.annotation.implicitNotFound
import scala.collection.JavaConverters._
import scala.util.matching.Regex

import org.mongodb.scala.bson.collection.immutable.{ Document => IDocument }
import org.mongodb.scala.bson.collection.mutable.{ Document => MDocument }

/**
 * BsonTransformers allow the transformation of type `T` to their corresponding [[BsonValue]].
 *
 * Custom implementations can be written to implicitly to convert a `T` into a [[BsonValue]] so it can be stored in a `Document`.
 *
 * @tparam T the type of value to be transformed into a [[BsonValue]].
 * @since 1.0
 */
@implicitNotFound(
  "No bson implicit transformer found for type ${T}. Implement or import an implicit BsonTransformer for this type."
)
trait BsonTransformer[-T] {

  /**
   * Convert the object into a [[BsonValue]]
   */
  def apply(value: T): BsonValue
}

/**
 * Maps the following native scala types to BsonValues:
 *
 *  - `BsonValue => BsonValue`
 *  - `BigDecimal` => BsonDecimal128
 *  - `Boolean => BsonBoolean`
 *  - `String => BsonString`
 *  - `Array[Byte] => BsonBinary`
 *  - `Regex => BsonRegex`
 *  - `Date => BsonDateTime`
 *  - `Decimal128` => BsonDecimal128
 *  - `ObjectId => BsonObjectId`
 *  - `Int => BsonInt32`
 *  - `Long => BsonInt64`
 *  - `Double => BsonDouble`
 *  - `None => BsonNull`
 *  - `immutable.Document => BsonDocument`
 *  - `mutable.Document => BsonDocument`
 *  - `Option[T] => BsonValue` where `T` is one of the above types
 *  - `Seq[(String, T)] => BsonDocument` where `T` is one of the above types
 *  - `Seq[T] => BsonArray` where `T` is one of the above types
 */
object BsonTransformer extends DefaultBsonTransformers {}

/**
 * Default BsonTransformers for native types.
 */
trait DefaultBsonTransformers extends LowPrio {

  /**
   * Noop transformer for `BsonValue`s
   */
  implicit object TransformBsonValue extends BsonTransformer[BsonValue] {
    def apply(value: BsonValue): BsonValue = value
  }

  /**
   * Transforms `BigDecimal` to `BsonDecimal128`
   */
  implicit object TransformBigDecimal extends BsonTransformer[BigDecimal] {
    def apply(value: BigDecimal): BsonDecimal128 = BsonDecimal128(value)
  }

  /**
   * Transforms `Boolean` to `BsonBoolean`
   */
  implicit object TransformBoolean extends BsonTransformer[Boolean] {
    def apply(value: Boolean): BsonBoolean = BsonBoolean(value)
  }

  /**
   * Transforms `String` to `BsonString`
   */
  implicit object TransformString extends BsonTransformer[String] {
    def apply(value: String): BsonString = BsonString(value)
  }

  /**
   * Transforms `Array[Byte]` to `BsonBinary`
   */
  implicit object TransformBinary extends BsonTransformer[Array[Byte]] {
    def apply(value: Array[Byte]): BsonBinary = BsonBinary(value)
  }

  /**
   * Transforms `Regex` to `BsonRegex`
   */
  implicit object TransformRegex extends BsonTransformer[Regex] {
    def apply(value: Regex): BsonRegularExpression = BsonRegularExpression(value)
  }

  /**
   * Transforms `Date` to `BsonDateTime`
   */
  implicit object TransformDateTime extends BsonTransformer[Date] {
    def apply(value: Date): BsonDateTime = BsonDateTime(value)
  }

  /**
   * Transforms `Decimal128` to `BsonDecimal128`
   */
  implicit object TransformDecimal128 extends BsonTransformer[Decimal128] {
    def apply(value: Decimal128): BsonDecimal128 = BsonDecimal128(value)
  }

  /**
   * Transforms `ObjectId` to `BsonObjectId`
   */
  implicit object TransformObjectId extends BsonTransformer[ObjectId] {
    def apply(value: ObjectId): BsonObjectId = BsonObjectId(value)
  }

  /**
   * Transforms `Int` to `BsonInt32`
   */
  implicit object TransformInt extends BsonTransformer[Int] {
    def apply(value: Int): BsonInt32 = BsonInt32(value)
  }

  /**
   * Transforms `Long` to `BsonInt64`
   */
  implicit object TransformLong extends BsonTransformer[Long] {
    def apply(value: Long): BsonInt64 = BsonInt64(value)
  }

  /**
   * Transforms `Double` to `BsonDouble`
   */
  implicit object TransformDouble extends BsonTransformer[Double] {
    def apply(value: Double): BsonDouble = BsonDouble(value)
  }

  /**
   * Transforms `None` to `BsonNull`
   */
  implicit object TransformNone extends BsonTransformer[Option[Nothing]] {
    def apply(value: Option[Nothing]): BsonNull = BsonNull()
  }

  /**
   * Transforms `Option[T]` to `BsonValue`
   */
  implicit def transformOption[T](implicit transformer: BsonTransformer[T]): BsonTransformer[Option[T]] = {
    new BsonTransformer[Option[T]] {
      def apply(value: Option[T]): BsonValue = value match {
        case Some(transformable) => transformer(transformable)
        case None                => BsonNull()
      }
    }
  }

}

trait LowPrio {

  /**
   * Transforms `immutable.Document` to `BsonDocument`
   */
  implicit object TransformImmutableDocument extends BsonTransformer[IDocument] {
    def apply(value: IDocument): BsonDocument = value.toBsonDocument
  }

  /**
   * Transforms `mutable.Document` to `BsonDocument`
   */
  implicit object TransformMutableDocument extends BsonTransformer[MDocument] {
    def apply(value: MDocument): BsonDocument = value.underlying
  }

  /**
   * Transforms `Seq[(String, T)]` to `BsonDocument`
   *
   * @param transformer implicit transformer for type `T`
   * @tparam T the type of the values
   * @return a BsonDocument containing the values
   */
  implicit def transformKeyValuePairs[T](
      implicit transformer: BsonTransformer[T]
  ): BsonTransformer[Seq[(String, T)]] = {
    new BsonTransformer[Seq[(String, T)]] {
      def apply(values: Seq[(String, T)]): BsonDocument = {
        BsonDocument(values.map(kv => (kv._1, transformer(kv._2))).toList)
      }
    }
  }

  /**
   * Transforms `Seq[T]` to `BsonArray`
   *
   * @param transformer implicit transformer for type `T`
   * @tparam T the type of the values
   * @return a BsonArray containing all the values
   */
  implicit def transformSeq[T](implicit transformer: BsonTransformer[T]): BsonTransformer[Seq[T]] = {
    new BsonTransformer[Seq[T]] {
      def apply(values: Seq[T]): BsonValue = {
        new BsonArray(values.map(transformer.apply).toList.asJava)
      }
    }
  }
}
