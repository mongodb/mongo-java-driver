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

import scala.collection.JavaConverters._

import org.scalatest.{ FlatSpec, Matchers }

class BsonValueSpec extends FlatSpec with Matchers {

  "BsonArray companion" should "create a BsonArray" in {
    BsonArray() should equal(new BsonArray())

    val values: List[BsonNumber] = List(BsonInt32(1), BsonInt64(2), new BsonDouble(3.0))
    val bsonArray = BsonArray.fromIterable(values)
    val expected = new BsonArray(values.asJava)

    bsonArray should equal(expected)

    val implicitBsonArray = BsonArray(1, 2L, 3.0)
    implicitBsonArray should equal(expected)
  }

  "BsonBinary companion" should "create a BsonBinary" in {
    val byteArray = Array[Byte](80.toByte, 5, 4, 3, 2, 1)
    BsonBinary(byteArray) should equal(new BsonBinary(byteArray))
  }

  "BsonBoolean companion" should "create a BsonBoolean" in {
    BsonBoolean(false) should equal(new BsonBoolean(false))
    BsonBoolean(true) should equal(new BsonBoolean(true))
  }

  "BsonDateTime companion" should "create a BsonDateTime" in {
    val date = new Date()

    BsonDateTime(date) should equal(new BsonDateTime(date.getTime))
    BsonDateTime(1000) should equal(new BsonDateTime(1000))
  }

  "BsonDecimal128 companion" should "create a BsonDecimal128" in {
    val expected = new BsonDecimal128(new Decimal128(100))

    BsonDecimal128(100) should equal(expected)
    BsonDecimal128("100") should equal(expected)
    BsonDecimal128(BigDecimal(100)) should equal(expected)
    BsonDecimal128(new Decimal128(100)) should equal(expected)
  }

  "BsonDocument companion" should "create a BsonDocument" in {
    val expected = new BsonDocument("a", BsonInt32(1))
    expected.put("b", BsonDouble(2.0))

    BsonDocument() should equal(new BsonDocument())
    BsonDocument("a" -> 1, "b" -> 2.0) should equal(expected)
    BsonDocument(Seq(("a", BsonInt32(1)), ("b", BsonDouble(2.0)))) should equal(expected)
    BsonDocument("{a: 1, b: 2.0}") should equal(expected)
  }

  "BsonDouble companion" should "create a BsonDouble" in {
    BsonDouble(2.0) should equal(new BsonDouble(2.0))
  }

  "BsonInt32 companion" should "create a BsonInt32" in {
    BsonInt32(1) should equal(new BsonInt32(1))
  }

  "BsonInt64 companion" should "create a BsonInt64" in {
    BsonInt64(1) should equal(new BsonInt64(1))
  }

  "BsonJavaScript companion" should "create a BsonJavaScript" in {
    BsonJavaScript("function(){}") should equal(new BsonJavaScript("function(){}"))
  }

  "BsonJavaScriptWithScope companion" should "create a BsonJavaScriptWithScope" in {
    val function = "function(){}"
    val scope = new BsonDocument("a", new BsonInt32(1))
    val expected = new BsonJavaScriptWithScope(function, scope)

    BsonJavaScriptWithScope(function, scope) should equal(expected)
    BsonJavaScriptWithScope(function, "a" -> 1) should equal(expected)
    BsonJavaScriptWithScope(function, Document("a" -> 1)) should equal(expected)
  }

  "BsonMaxKey companion" should "create a BsonMaxKey" in {
    BsonMaxKey() should equal(new BsonMaxKey())
  }

  "BsonMinKey companion" should "create a BsonMinKey" in {
    BsonMinKey() should equal(new BsonMinKey())
  }

  "BsonNull companion" should "create a BsonNull" in {
    BsonNull() should equal(new BsonNull())
  }

  "BsonNumber companion" should "create a BsonNumber" in {
    BsonNumber(1) should equal(BsonInt32(1))
    BsonNumber(1L) should equal(BsonInt64(1))
    BsonNumber(1.0) should equal(BsonDouble(1.0))
  }

  "BsonObjectId companion" should "create a BsonObjectId" in {
    val bsonObjectId = BsonObjectId()
    val objectId = bsonObjectId.getValue
    val hexString = objectId.toHexString
    val expected = new BsonObjectId(bsonObjectId.getValue)

    bsonObjectId should equal(expected)
    BsonObjectId(hexString) should equal(expected)
    BsonObjectId(objectId) should equal(expected)
  }

  "BsonRegularExpression companion" should "create a BsonRegularExpression" in {
    BsonRegularExpression("/(.*)/") should equal(new BsonRegularExpression("/(.*)/"))
    BsonRegularExpression("/(.*)/".r) should equal(new BsonRegularExpression("/(.*)/"))
    BsonRegularExpression("/(.*)/", "?i") should equal(new BsonRegularExpression("/(.*)/", "?i"))
  }

  "BsonString companion" should "create a BsonString" in {
    BsonString("aBc") should equal(new BsonString("aBc"))
  }

  "BsonSymbol companion" should "create a BsonSymbol" in {
    BsonSymbol(Symbol("sym")) should equal(new BsonSymbol("sym"))
  }

  "BsonTimestamp companion" should "create a BsonTimestamp" in {
    BsonTimestamp() should equal(new BsonTimestamp(0, 0))
    BsonTimestamp(10, 1) should equal(new BsonTimestamp(10, 1))
  }

  "BsonUndefined companion" should "create a BsonUndefined" in {
    BsonUndefined() should equal(new BsonUndefined())
  }

}
