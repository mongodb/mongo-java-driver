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
import org.mongodb.scala.model.Indexes._
import org.mongodb.scala.{MongoClient, model}
import org.scalatest.{FlatSpec, Matchers}

class IndexesSpec extends FlatSpec with Matchers {
  val registry = MongoClient.DEFAULT_CODEC_REGISTRY

  def toBson(bson: Bson): Document = Document(bson.toBsonDocument(classOf[BsonDocument], MongoClient.DEFAULT_CODEC_REGISTRY))

  "Indexes" should "have the same methods as the wrapped Updates" in {
    val wrapped = classOf[com.mongodb.client.model.Indexes].getDeclaredMethods
      .filter(f => isStatic(f.getModifiers) && isPublic(f.getModifiers)).map(_.getName).toSet
    val local = model.Indexes.getClass.getDeclaredMethods.filter(f => isPublic(f.getModifiers)).map(_.getName).toSet

    local should equal(wrapped)
  }

  it should "ascending" in {
    toBson(ascending("x")) should equal(Document("""{x : 1}"""))
    toBson(ascending("x", "y")) should equal(Document("""{x : 1, y : 1}"""))
  }

  it should "descending" in {
    toBson(descending("x")) should equal(Document("""{x : -1}"""))
    toBson(descending("x", "y")) should equal(Document("""{x : -1, y : -1}"""))
  }

  it should "geo2dsphere" in {
    toBson(geo2dsphere("x")) should equal(Document("""{x : "2dsphere"}"""))
    toBson(geo2dsphere("x", "y")) should equal(Document("""{x : "2dsphere", y : "2dsphere"}"""))
  }

  it should "geo2d" in {
    toBson(geo2d("x")) should equal(Document("""{x : "2d"}"""))
  }

  it should "geoHaystack" in {
    toBson(geoHaystack("x", descending("b"))) should equal(Document("""{x : "geoHaystack", b: -1}"""))
  }

  it should "text" in {
    toBson(text("x")) should equal(Document("""{x : "text"}"""))
  }

  it should "hashed" in {
    toBson(hashed("x")) should equal(Document("""{x : "hashed"}"""))
  }

  it should "compoundIndex" in {
    toBson(compoundIndex(ascending("x"), descending("y"))) should equal(Document("""{x : 1, y : -1}"""))
    toBson(compoundIndex(ascending("x"), descending("y"), descending("x"))) should equal(Document("""{y : -1, x : -1}"""))
    toBson(compoundIndex(ascending("x", "y"), descending("a", "b"))) should equal(Document("""{x : 1, y : 1, a : -1, b : -1}"""))
  }

}

