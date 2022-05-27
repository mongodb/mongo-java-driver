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
package org.mongodb.scala.model.search

import org.bson.BsonDocument
import org.mongodb.scala.{ BaseSpec, MongoClient }
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.geojson.{ Point, Position }
import org.mongodb.scala.model.search.SearchFuzzy.defaultSearchFuzzy
import org.mongodb.scala.model.search.SearchOperator.{
  autocomplete,
  compound,
  dateRange,
  exists,
  near,
  numberRange,
  text
}
import org.mongodb.scala.model.search.SearchPath.{ fieldPath, wildcardPath }
import org.mongodb.scala.model.search.SearchScore.function
import org.mongodb.scala.model.search.SearchScoreExpression.{ constantExpression, logExpression }

import java.time.{ Duration, Instant }
import scala.collection.JavaConverters._

class SearchOperatorSpec extends BaseSpec {
  it should "render all operators" in {
    toDocument(
      compound()
        .should(Seq(
          exists(fieldPath("fieldName1")),
          text("term1", fieldPath("fieldName2"))
            .score(function(logExpression(constantExpression(3)))),
          text(Seq("term2", "term3"), Seq(wildcardPath("wildc*rd"), fieldPath("fieldName3")))
            .fuzzy(defaultSearchFuzzy()
              .maxEdits(1)
              .prefixLength(2)
              .maxExpansions(3)),
          autocomplete(
            "term4",
            fieldPath("title")
              // multi must be ignored
              .multi("keyword")
          ),
          autocomplete(Seq("Traffic in", "term5"), fieldPath("title"))
            .fuzzy(defaultSearchFuzzy())
            .sequentialTokenOrder(),
          numberRange(Seq(fieldPath("fieldName4"), fieldPath("fieldName5")))
            .gtLt(1, 1.5),
          dateRange(fieldPath("fieldName6"))
            .lte(Instant.ofEpochMilli(1)),
          near(0, 1.5, Seq(fieldPath("fieldName7"), fieldPath("fieldName8"))),
          near(Instant.ofEpochMilli(1), Duration.ofMillis(3), fieldPath("fieldName9")),
          near(Point(Position(114.15, 22.28)), 1234.5, fieldPath("address.location"))
        ).asJava)
        .minimumShouldMatch(1)
        .mustNot(Seq(
          compound().must(Seq(exists(fieldPath("fieldName"))).asJava)
        ).asJava)
    ) should equal(
      Document("""{
        "compound": {
          "should": [
            { "exists": { "path": "fieldName1" } },
            { "text": { "query": "term1", "path": "fieldName2", "score": { "function": { "log": { "constant": 3.0 } } } } },
            { "text": {
              "query": [ "term2", "term3" ],
              "path": [ { "wildcard": "wildc*rd" }, "fieldName3" ],
              "fuzzy": { "maxEdits": 1, "prefixLength": 2, "maxExpansions": 3 } } },
            { "autocomplete": { "query": "term4", "path": "title" } },
            { "autocomplete": { "query": ["Traffic in", "term5"], "path": "title", "fuzzy": {}, "tokenOrder": "sequential" } },
            { "range": { "path": [ "fieldName4", "fieldName5" ], "gt": 1, "lt": 1.5 } },
            { "range": { "path": "fieldName6", "lte": { "$date": "1970-01-01T00:00:00.001Z" } } },
            { "near": { "origin": 0, "pivot": 1.5, "path": [ "fieldName7", "fieldName8" ] } },
            { "near": { "origin": { "$date": "1970-01-01T00:00:00.001Z" }, "pivot": { "$numberLong": "3" }, "path": "fieldName9" } },
            { "near": { "origin": { type: "Point", coordinates: [ 114.15, 22.28 ] }, "pivot": 1234.5, "path": "address.location" } }
          ],
          "minimumShouldMatch": 1,
          "mustNot": [
            { "compound": { "must": [ { "exists": { "path": "fieldName" } } ] } }
          ]
        }
      }""")
    )
  }

  def toDocument(bson: Bson): Document =
    Document(bson.toBsonDocument(classOf[BsonDocument], MongoClient.DEFAULT_CODEC_REGISTRY))
}
