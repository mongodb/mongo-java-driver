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
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala.{ model, BaseSpec, MongoClient }

class SortsSpec extends BaseSpec {
  val registry = MongoClient.DEFAULT_CODEC_REGISTRY

  def toBson(bson: Bson): Document =
    Document(bson.toBsonDocument(classOf[BsonDocument], MongoClient.DEFAULT_CODEC_REGISTRY))

  "Sorts" should "have the same methods as the wrapped Sorts" in {
    val wrapped = classOf[com.mongodb.client.model.Sorts].getDeclaredMethods
      .filter(f => isStatic(f.getModifiers) && isPublic(f.getModifiers))
      .map(_.getName)
      .toSet
    val local = model.Sorts.getClass.getDeclaredMethods.filter(f => isPublic(f.getModifiers)).map(_.getName).toSet

    local should equal(wrapped)
  }

  it should "ascending" in {
    toBson(ascending("x")) should equal(Document("""{x : 1}"""))
    toBson(ascending("x", "y")) should equal(Document("""{x : 1, y : 1}"""))
    toBson(ascending(Seq("x", "y"): _*)) should equal(Document("""{x : 1, y : 1}"""))
  }

  it should "descending" in {
    toBson(descending("x")) should equal(Document("""{x : -1}"""))
    toBson(descending("x", "y")) should equal(Document("""{x : -1, y : -1}"""))
    toBson(descending(Seq("x", "y"): _*)) should equal(Document("""{x : -1, y : -1}"""))
  }

  it should "metaTextScore" in {
    toBson(metaTextScore("x")) should equal(Document("""{x : {$meta : "textScore"}}"""))
  }

  it should "orderBy" in {
    toBson(orderBy(Seq(ascending("x"), descending("y")): _*)) should equal(Document("""{x : 1, y : -1}"""))
    toBson(orderBy(ascending("x"), descending("y"))) should equal(Document("""{x : 1, y : -1}"""))
    toBson(orderBy(ascending("x"), descending("y"), descending("x"))) should equal(Document("""{y : -1, x : -1}"""))
    toBson(orderBy(ascending("x", "y"), descending("a", "b"))) should equal(
      Document("""{x : 1, y : 1, a : -1, b : -1}""")
    )
  }

}
