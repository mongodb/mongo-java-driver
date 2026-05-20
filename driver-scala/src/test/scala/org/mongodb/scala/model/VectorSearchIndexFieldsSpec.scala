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

import org.bson.BsonDocument
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.VectorSearchIndexFields._
import org.mongodb.scala.{ model, BaseSpec, MongoClient }

class VectorSearchIndexFieldsSpec extends BaseSpec {

  def toBson(bson: Bson): Document =
    Document(bson.toBsonDocument(classOf[BsonDocument], MongoClient.DEFAULT_CODEC_REGISTRY))

  "VectorSearchIndexFields" should "have the same methods as the wrapped VectorSearchIndexFields" in {
    val wrapped = classOf[com.mongodb.client.model.VectorSearchIndexFields].getDeclaredMethods
      .filter(f => isStatic(f.getModifiers) && isPublic(f.getModifiers))
      .map(_.getName)
      .toSet
    val local = model.VectorSearchIndexFields.getClass.getDeclaredMethods
      .filter(f => isPublic(f.getModifiers))
      .map(_.getName)
      .toSet -- DEFAULT_EXCLUSIONS

    local should equal(wrapped)
  }

  it should "create a vectorField with minimal options" in {
    toBson(vectorField("vec")) should equal(Document("""{"type": "vector", "path": "vec"}"""))
  }

  it should "create a vectorField with all options" in {
    toBson(
      vectorField("embedding")
        .numDimensions(1536)
        .similarity("cosine")
        .indexingMethod("hnsw")
        .hnswOptions(HnswSearchIndexOptions().maxEdges(16))
    ) should equal(
      Document(
        """{"type": "vector", "path": "embedding", "numDimensions": 1536,
          |"similarity": "cosine", "indexingMethod": "hnsw", "hnswOptions": {"maxEdges": 16}}""".stripMargin
          .replaceAll("\n", " ")
      )
    )
  }

  it should "create a filterField" in {
    toBson(filterField("status")) should equal(Document("""{"type": "filter", "path": "status"}"""))
  }

  it should "create an autoEmbedField with minimal options" in {
    toBson(autoEmbedField("content").modality("text").model("voyage-4")) should equal(
      Document("""{"type": "autoEmbed", "path": "content", "modality": "text", "model": "voyage-4"}""")
    )
  }

  it should "reject an autoEmbedField missing modality" in {
    an[IllegalArgumentException] should be thrownBy {
      toBson(autoEmbedField("content").model("voyage-4"))
    }
  }

  it should "reject an autoEmbedField missing model" in {
    an[IllegalArgumentException] should be thrownBy {
      toBson(autoEmbedField("content").modality("text"))
    }
  }

  it should "create an autoEmbedField with all options" in {
    toBson(
      autoEmbedField("product.description")
        .modality("text")
        .model("voyage-4-large")
        .numDimensions(256)
        .quantization("binary")
        .similarity("euclidean")
        .indexingMethod("hnsw")
        .hnswOptions(HnswSearchIndexOptions().maxEdges(16))
    ) should equal(
      Document(
        """{"type": "autoEmbed", "path": "product.description", "modality": "text",
          |"model": "voyage-4-large", "numDimensions": 256, "quantization": "binary",
          |"similarity": "euclidean", "indexingMethod": "hnsw", "hnswOptions": {"maxEdges": 16}}""".stripMargin
          .replaceAll("\n", " ")
      )
    )
  }
}
