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

package org.mongodb.scala.model

import java.lang.reflect.Modifier._

import org.bson.{ BsonDocument, BsonString }
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.SearchIndexDefinition._
import org.mongodb.scala.model.VectorSearchIndexFields._
import org.mongodb.scala.{ model, BaseSpec, MongoClient }

class SearchIndexDefinitionSpec extends BaseSpec {

  def toBson(bson: Bson): Document =
    Document(bson.toBsonDocument(classOf[BsonDocument], MongoClient.DEFAULT_CODEC_REGISTRY))

  "SearchIndexDefinition" should "have the same methods as the wrapped SearchIndexDefinition" in {
    val wrapped = classOf[com.mongodb.client.model.SearchIndexDefinition].getDeclaredMethods
      .filter(f => isStatic(f.getModifiers) && isPublic(f.getModifiers))
      .map(_.getName)
      .toSet
    val local = model.SearchIndexDefinition.getClass.getDeclaredMethods
      .filter(f => isPublic(f.getModifiers))
      .map(_.getName)
      .toSet -- DEFAULT_EXCLUSIONS

    local should equal(wrapped)
  }

  it should "create a vectorSearch definition with varargs" in {
    toBson(
      vectorSearch(
        vectorField("plot_embedding").numDimensions(1536).similarity("euclidean"),
        filterField("genre")
      )
    ) should equal(
      Document(
        """{"fields": [
          |{"type": "vector", "path": "plot_embedding", "numDimensions": 1536, "similarity": "euclidean"},
          |{"type": "filter", "path": "genre"}
          |]}""".stripMargin.replaceAll("\n", " ")
      )
    )
  }

  it should "create a vectorSearch definition with a Seq" in {
    toBson(
      vectorSearch(
        Seq(
          vectorField("embedding").numDimensions(768).similarity("cosine"),
          filterField("category")
        )
      )
    ) should equal(
      Document(
        """{"fields": [
          |{"type": "vector", "path": "embedding", "numDimensions": 768, "similarity": "cosine"},
          |{"type": "filter", "path": "category"}
          |]}""".stripMargin.replaceAll("\n", " ")
      )
    )
  }

  it should "create a SearchIndexModel with VectorSearchIndexDefinition" in {
    val definition = vectorSearch(
      vectorField("embedding").numDimensions(1536).similarity("cosine")
    )
    val model = SearchIndexModel("my_index", definition)

    model.getName should equal("my_index")
    model.getDefinition should equal(definition)
    model.getType.toBsonValue should equal(new BsonString("vectorSearch"))
  }
}
