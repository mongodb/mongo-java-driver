/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONObjectITIONS OF ANY KINObject, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mongodb.scala.model

import java.lang.reflect.Modifier._

import org.bson.BsonDocument

import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.Updates._
import org.mongodb.scala.{MongoClient, model}
import org.scalatest.{FlatSpec, Matchers}

class UpdatesSpec extends FlatSpec with Matchers {
  val registry = MongoClient.DEFAULT_CODEC_REGISTRY

  def toBson(bson: Bson): Document = Document(bson.toBsonDocument(classOf[BsonDocument], MongoClient.DEFAULT_CODEC_REGISTRY))

  "Updates" should "have the same methods as the wrapped Updates" in {
    val wrapped = classOf[com.mongodb.client.model.Updates].getDeclaredMethods
      .filter(f => isStatic(f.getModifiers) && isPublic(f.getModifiers)).map(_.getName).toSet
    val local = model.Updates.getClass.getDeclaredMethods.filter(f => isPublic(f.getModifiers)).map(_.getName).toSet

    local should equal(wrapped)
  }

  it should "should render $set" in {
    toBson(set("x", 1)) should equal(Document("""{$set : { x : 1} }"""))
    toBson(set("x", null)) should equal(Document("""{$set : { x : null } }"""))
  }

  it should "should render $setOnInsert" in {
    toBson(setOnInsert("x", 1)) should equal(Document("""{$setOnInsert : { x : 1} }"""))
    toBson(setOnInsert("x", List(1, 2, 3))) should equal(Document("""{$setOnInsert : { x : [1, 2, 3]} }"""))
    toBson(setOnInsert("x", Map("a" -> 1, "b" -> 2, "c" -> 3))) should equal(Document("""{$setOnInsert : { x : {a: 1, b: 2, c: 3}} }"""))
    toBson(setOnInsert("x", null)) should equal(Document("""{$setOnInsert : { x : null } }"""))
  }

  it should "should render $unset" in {
    toBson(unset("x")) should equal(Document("""{$unset : { x : ""} }"""))
  }

  it should "should render $rename" in {
    toBson(rename("x", "y")) should equal(Document("""{$rename : { x : "y"} }"""))
  }

  it should "should render $inc" in {
    toBson(inc("x", 1)) should equal(Document("""{$inc : { x : 1} }"""))
    toBson(inc("x", 5L)) should equal(Document("""{$inc : { x : {$numberLong : "5"}} }"""))
    toBson(inc("x", 3.4d)) should equal(Document("""{$inc : { x : 3.4} }"""))
  }

  it should "should render $mul" in {
    toBson(mul("x", 1)) should equal(Document("""{$mul : { x : 1} }"""))
    toBson(mul("x", 5L)) should equal(Document("""{$mul : { x : {$numberLong : "5"}} }"""))
    toBson(mul("x", 3.4d)) should equal(Document("""{$mul : { x : 3.4} }"""))
  }

  it should "should render $min" in {
    toBson(min("x", 42)) should equal(Document("""{$min : { x : 42} }"""))
  }

  it should "should render $max" in {
    toBson(max("x", 42)) should equal(Document("""{$max : { x : 42} }"""))
  }

  it should "should render $currentDate" in {
    toBson(currentDate("x")) should equal(Document("""{$currentDate : { x : true} }"""))
    toBson(currentTimestamp("x")) should equal(Document("""{$currentDate : { x : {$type : "timestamp"} } }"""))
  }

  it should "should render $addToSet" in {
    toBson(addToSet("x", 1)) should equal(Document("""{$addToSet : { x : 1} }"""))
    toBson(addEachToSet("x", 1, 2, 3)) should equal(Document("""{$addToSet : { x : { $each : [1, 2, 3] } } }"""))
  }

  it should "should render $push" in {
    toBson(push("x", 1)) should equal(Document("""{$push : { x : 1} }"""))
    toBson(pushEach("x", 1, 2, 3)) should equal(Document("""{$push : { x : { $each : [1, 2, 3] } } }"""))
    toBson(pushEach("x", PushOptions(), 1, 2, 3)) should equal(Document("""{$push : { x : { $each : [1, 2, 3] } } }"""))
    toBson(pushEach("x", PushOptions().position(0).slice(3).sortDocument(Document("{score : -1}")),
      Document("""{score : 89}"""), Document("""{score : 65}"""))) should equal(
      Document("""{$push : { x : { $each : [{score : 89}, {score : 65}], $position : 0, $slice : 3, $sort : { score : -1 } } } }""")
    )

    toBson(pushEach("x", PushOptions().position(0).slice(3).sort(-1), 89, 65)) should equal(
      Document("""{$push : { x : { $each : [89, 65], $position : 0, $slice : 3, $sort : -1 } } }""")
    )
  }

  it should "should render `$pull`" in {
    toBson(pull("x", 1)) should equal(Document("""{$pull : { x : 1} }"""))
    toBson(pullByFilter(Filters.gte("x", 5))) should equal(Document("""{$pull : { x : { $gte : 5 }} }"""))
  }

  it should "should render `$pullAll`" in {
    toBson(pullAll("x")) should equal(Document("""{$pullAll : { x : []} }"""))
    toBson(pullAll("x", 1, 2, 3)) should equal(Document("""{$pullAll : { x : [1, 2, 3]} }"""))
  }

  it should "should render $pop" in {
    toBson(popFirst("x")) should equal(Document("""{$pop : { x : -1} }"""))
    toBson(popLast("x")) should equal(Document("""{$pop : { x : 1} }"""))
  }

  it should "should render $bit" in {
    toBson(bitwiseAnd("x", 5)) should equal(Document("""{$bit : { x : {and : 5} } }"""))
    toBson(bitwiseAnd("x", 5L)) should equal(Document("""{$bit : { x : {and : {$numberLong : "5"} } } }"""))
    toBson(bitwiseOr("x", 5)) should equal(Document("""{$bit : { x : {or : 5} } }"""))
    toBson(bitwiseOr("x", 5L)) should equal(Document("""{$bit : { x : {or : {$numberLong : "5"} } } }"""))
    toBson(bitwiseXor("x", 5)) should equal(Document("""{$bit : { x : {xor : 5} } }"""))
    toBson(bitwiseXor("x", 5L)) should equal(Document("""{$bit : { x : {xor : {$numberLong : "5"} } } }"""))
  }

  it should "should combine updates" in {
    toBson(combine(set("x", 1))) should equal(Document("""{$set : { x : 1} }"""))
    toBson(combine(set("x", 1), set("x", 2))) should equal(Document("""{$set : { x : 2} }"""))
    toBson(combine(set("x", 1), inc("z", 3), set("y", 2), inc("a", 4))) should equal(Document("""{
                                                                                            $set : { x : 1, y : 2},
                                                                                            $inc : { z : 3, a : 4}}
                                                                                          }"""))

    toBson(combine(combine(set("x", 1)))) should equal(Document("""{$set : { x : 1} }"""))
    toBson(combine(combine(set("x", 1), set("y", 2)))) should equal(Document("""{$set : { x : 1, y : 2} }"""))
    toBson(combine(combine(set("x", 1), set("x", 2)))) should equal(Document("""{$set : { x : 2} }"""))

    toBson(combine(combine(set("x", 1), inc("z", 3), set("y", 2), inc("a", 4)))) should equal(Document("""{
                                                                                                   $set : { x : 1, y : 2},
                                                                                                   $inc : { z : 3, a : 4}
                                                                                                  }"""))
  }

}
