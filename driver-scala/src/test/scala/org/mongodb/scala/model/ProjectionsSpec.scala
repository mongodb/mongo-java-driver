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
import org.mongodb.scala.{BaseSpec, MongoClient, model}

class ProjectionsSpec extends BaseSpec {
  val registry = MongoClient.DEFAULT_CODEC_REGISTRY

  def toBson(bson: Bson): Document = Document(bson.toBsonDocument(classOf[BsonDocument], MongoClient.DEFAULT_CODEC_REGISTRY))

  "Projections" should "have the same methods as the wrapped Projections" in {
    val wrapped = classOf[com.mongodb.client.model.Projections].getDeclaredMethods
      .filter(f => isStatic(f.getModifiers) && isPublic(f.getModifiers)).map(_.getName).toSet
    val local = model.Projections.getClass.getDeclaredMethods.filter(f => isPublic(f.getModifiers)).map(_.getName).toSet

    local should equal(wrapped)
  }

  it should "include" in {
    toBson(model.Projections.include("x")) should equal(Document("""{x : 1}"""))
    toBson(model.Projections.include("x", "y")) should equal(Document("""{x : 1, y : 1}"""))
  }

  it should "exclude" in {
    toBson(model.Projections.exclude("x")) should equal(Document("""{x : 0}"""))
    toBson(model.Projections.exclude("x", "y")) should equal(Document("""{x : 0, y : 0}"""))
  }

  it should "excludeId" in {
    toBson(model.Projections.excludeId) should equal(Document("""{_id : 0}"""))
  }

  it should "firstElem" in {
    toBson(model.Projections.elemMatch("x")) should equal(Document("""{"x.$" : 1}"""))
  }

  it should "elemMatch" in {
    toBson(model.Projections.elemMatch("x", Filters.and(model.Filters.eq("y", 1), model.Filters.eq("z", 2)))) should equal(Document("""{x : {$elemMatch : {y : 1, z : 2}}}"""))
  }

  it should "slice" in {
    toBson(model.Projections.slice("x", 5)) should equal(Document("""{x : {$slice : 5}}"""))
    toBson(model.Projections.slice("x", 5, 10)) should equal(Document("""{x : {$slice : [5, 10]}}"""))
  }

  it should "metaTextScore" in {
    toBson(model.Projections.metaTextScore("x")) should equal(Document("""{x : {$meta : "textScore"}}"""))
  }

  it should "computed" in {
    toBson(model.Projections.computed("c", "$y")) should equal(Document("""{c : "$y"}"""))
  }

  it should "combine fields" in {
    toBson(model.Projections.fields(model.Projections.include("x", "y"), model.Projections.exclude("_id"))) should equal(Document("""{x : 1, y : 1, _id : 0}"""))
    toBson(model.Projections.fields(model.Projections.include("x", "y"), model.Projections.exclude("x"))) should equal(Document("""{y : 1, x : 0}"""))
  }

}
