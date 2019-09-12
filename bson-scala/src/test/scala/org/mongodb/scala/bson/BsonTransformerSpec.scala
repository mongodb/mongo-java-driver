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

import scala.language.implicitConversions

import org.mongodb.scala.bson.collection.{ immutable, mutable }
import org.scalatest.{ FlatSpec, Matchers }

class BsonTransformerSpec extends FlatSpec with Matchers {

  "The BsonTransformer companion" should "not transform BsonValues" in {
    transform(BsonString("abc")) should equal(BsonString("abc"))
  }
  it should "transform Binary" in {
    transform(Array[Byte](128.toByte)) should equal(BsonBinary(Array[Byte](128.toByte)))
  }
  it should "transform BigDecmial" in {
    transform(BigDecimal(100)) should equal(BsonDecimal128(100))
  }
  it should "transform Boolean" in {
    transform(true) should equal(BsonBoolean(true))
  }
  it should "transform DateTime" in {
    transform(new Date(100)) should equal(BsonDateTime(100))
  }
  it should "transform Decimal128" in {
    transform(new Decimal128(100)) should equal(BsonDecimal128(100))
  }
  it should "transform Double" in {
    transform(2.0) should equal(BsonDouble(2.0))
  }
  it should "transform ImmutableDocument" in {
    transform(immutable.Document("a" -> 1, "b" -> "two", "c" -> false)) should equal(BsonDocument("a" -> 1, "b" -> "two", "c" -> false))
  }

  it should "transform Int" in {
    transform(1) should equal(BsonInt32(1))
  }
  it should "transform KeyValuePairs[T]" in {
    transform(Seq("a" -> "a", "b" -> "b", "c" -> "c")) should equal(BsonDocument("a" -> "a", "b" -> "b", "c" -> "c"))
  }
  it should "transform Long" in {
    transform(1L) should equal(BsonInt64(1))
  }
  it should "transform MutableDocument" in {
    transform(mutable.Document("a" -> 1, "b" -> "two", "c" -> false)) should equal(BsonDocument("a" -> 1, "b" -> "two", "c" -> false))
  }
  it should "transform None" in {
    transform(None) should equal(BsonNull())
  }
  it should "transform ObjectId" in {
    val objectId = new ObjectId()
    transform(objectId) should equal(BsonObjectId(objectId))
  }
  it should "transform Option[T]" in {
    transform(Some(1)) should equal(new BsonInt32(1))
  }
  it should "transform Regex" in {
    transform("/.*/".r) should equal(BsonRegularExpression("/.*/"))
  }
  it should "transform Seq[T]" in {
    transform(Seq("a", "b", "c")) should equal(BsonArray("a", "b", "c"))
  }
  it should "transform String" in {
    transform("abc") should equal(BsonString("abc"))
  }

  it should "thrown a runtime exception when no transformer available" in {
    "transform(BigInt(12))" shouldNot compile
  }

  implicit def transform[T](v: T)(implicit transformer: BsonTransformer[T]): BsonValue = transformer(v)

}
