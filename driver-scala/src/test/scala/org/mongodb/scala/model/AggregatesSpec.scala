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

import com.mongodb.client.model.GeoNearOptions.geoNearOptions
import com.mongodb.client.model.fill.FillOutputField

import java.lang.reflect.Modifier._
import org.bson.BsonDocument
import org.mongodb.scala.bson.BsonArray
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.Accumulators._
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.MongoTimeUnit.DAY
import org.mongodb.scala.model.Projections._
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala.model.Windows.Bound.{ CURRENT, UNBOUNDED }
import org.mongodb.scala.model.Windows.{ documents, range }
import org.mongodb.scala.model.densify.DensifyRange.fullRangeWithStep
import org.mongodb.scala.model.fill.FillOptions.fillOptions
import org.mongodb.scala.model.geojson.{ Point, Position }
import org.mongodb.scala.model.search.SearchCount.total
import org.mongodb.scala.model.search.SearchFacet.stringFacet
import org.mongodb.scala.model.search.SearchHighlight.paths
import org.mongodb.scala.model.search.SearchCollector
import org.mongodb.scala.model.search.SearchOperator.exists
import org.mongodb.scala.model.search.SearchOptions.searchOptions
import org.mongodb.scala.model.search.SearchPath.{ fieldPath, wildcardPath }
import org.mongodb.scala.model.search.VectorSearchOptions.{ approximateVectorSearchOptions, exactVectorSearchOptions }
import org.mongodb.scala.{ BaseSpec, MongoClient, MongoNamespace }

class AggregatesSpec extends BaseSpec {
  val registry = MongoClient.DEFAULT_CODEC_REGISTRY

  def toBson(bson: Bson): Document =
    Document(bson.toBsonDocument(classOf[BsonDocument], MongoClient.DEFAULT_CODEC_REGISTRY))

  "Aggregates" should "have the same methods as the wrapped Aggregates" in {
    val wrapped = classOf[com.mongodb.client.model.Aggregates].getDeclaredMethods
      .filter(f => isPublic(f.getModifiers))
      .map(_.getName)
      .toSet
    val aliases = Set("filter")
    val local = Aggregates.getClass.getDeclaredMethods
      .filter(f => isPublic(f.getModifiers))
      .map(_.getName)
      .toSet -- aliases

    local should equal(wrapped)
  }

  it should "have the same methods as the wrapped Accumulators" in {
    val wrapped = classOf[com.mongodb.client.model.Accumulators].getDeclaredMethods
      .filter(f => isPublic(f.getModifiers))
      .map(_.getName)
      .toSet
    val local = Accumulators.getClass.getDeclaredMethods.filter(f => isPublic(f.getModifiers)).map(_.getName).toSet
    local should equal(wrapped)
  }

  it should "render $addFields" in {
    toBson(addFields(Field("newField", "hello"))) should equal(Document("""{$addFields: { "newField": "hello"}}"""))
  }

  it should "render $set" in {
    toBson(set(Field("newField", "hello"))) should equal(Document("""{$set: { "newField": "hello"}}"""))
  }

  // scalastyle:off magic.number
  it should "render $bucket" in {
    toBson(bucket("$screenSize", 0, 24, 32, 50, 100000)) should equal(
      Document("""{$bucket: { groupBy: "$screenSize", boundaries: [0, 24, 32, 50, 100000] } } """)
    )

    toBson(bucket("$screenSize", new BucketOptions().defaultBucket("other"), 0, 24, 32, 50, 100000)) should equal(
      Document("""{$bucket: { groupBy: "$screenSize", boundaries: [0, 24, 32, 50, 100000], default: "other"} } """)
    )
  }

  it should "render $bucketAuto" in {
    toBson(bucketAuto("$price", 4)) should equal(Document("""{ $bucketAuto: { groupBy: "$price", buckets: 4  } }"""))
    toBson(
      bucketAuto(
        "$price",
        4,
        BucketAutoOptions()
          .granularity(BucketGranularity.R5)
          .output(sum("count", 1), avg("avgPrice", "$price"))
      )
    ) should equal(Document("""{$bucketAuto: {
        groupBy: "$price",
        buckets: 4,
        output: {
        count: {$sum: 1},
        avgPrice: {$avg: "$price"},
      },
        granularity: "R5"
      }
    }"""))
  }

  it should "render $count" in {
    toBson(count()) should equal(Document("""{$count: "count"}"""))
    toBson(count("total")) should equal(Document("""{$count: "total"}"""))
  }

  it should "render $match" in {
    toBson(`match`(Filters.eq("author", "dave"))) should equal(Document("""{ $match : { author : "dave" } }"""))
    toBson(filter(Filters.eq("author", "dave"))) should equal(Document("""{ $match : { author : "dave" } }"""))
  }

  it should "render $facet" in {
    toBson(
      facet(
        Facet(
          "Screen Sizes",
          unwind("$attributes"),
          filter(Filters.equal("attributes.name", "screen size")),
          group(null, sum("count", 1))
        ),
        Facet(
          "Manufacturer",
          filter(Filters.equal("attributes.name", "manufacturer")),
          group("$attributes.value", sum("count", 1)),
          sort(descending("count")),
          limit(5)
        )
      )
    ) should equal(
      Document(
        """{$facet: { "Screen Sizes": [{$unwind: "$attributes"}, {$match: {"attributes.name": "screen size"}},
            {$group: { _id: null, count: {$sum: 1} }}],
      "Manufacturer": [ {$match: {"attributes.name": "manufacturer"}}, {$group: {_id: "$attributes.value", count: {$sum: 1}}},
            {$sort: {count: -1}}, {$limit: 5}]}}"""
      )
    )
  }

  it should "render $graphLookup" in {
    toBson(graphLookup("contacts", "$friends", "friends", "name", "socialNetwork")) should equal(
      Document(
        """{ $graphLookup:{ from: "contacts", startWith: "$friends", connectFromField: "friends", connectToField: "name",
          |  as: "socialNetwork" } }""".stripMargin
      )
    )

    toBson(
      graphLookup("contacts", "$friends", "friends", "name", "socialNetwork", GraphLookupOptions().maxDepth(1))
    ) should equal(
      Document(
        """{ $graphLookup: { from: "contacts", startWith: "$friends", connectFromField: "friends", connectToField: "name",
          |  as: "socialNetwork", maxDepth: 1 } }""".stripMargin
      )
    )

    toBson(
      graphLookup(
        "contacts",
        "$friends",
        "friends",
        "name",
        "socialNetwork",
        GraphLookupOptions().maxDepth(1).depthField("master")
      )
    ) should equal(
      Document(
        """{ $graphLookup: { from: "contacts", startWith: "$friends", connectFromField: "friends", connectToField: "name",
          |  as: "socialNetwork", maxDepth: 1, depthField: "master" } }""".stripMargin
      )
    )

    toBson(
      graphLookup(
        "contacts",
        "$friends",
        "friends",
        "name",
        "socialNetwork",
        GraphLookupOptions()
          .depthField("master")
      )
    ) should equal(
      Document(
        """{ $graphLookup: { from: "contacts", startWith: "$friends", connectFromField: "friends",
          |  connectToField: "name", as: "socialNetwork", depthField: "master" } }""".stripMargin
      )
    )
  }

  it should "render $project" in {
    toBson(project(fields(Projections.include("title", "author"), computed("lastName", "$author.last")))) should equal(
      Document("""{ $project : { title : 1 , author : 1, lastName : "$author.last" } }""")
    )
  }

  it should "render $replaceRoot" in {
    toBson(replaceRoot("$a1")) should equal(Document("""{$replaceRoot: {newRoot: "$a1"}}"""))
  }

  it should "render $sort" in {
    toBson(sort(ascending("title", "author"))) should equal(Document("""{ $sort : { title : 1 , author : 1 } }"""))
  }

  it should "render $sortByCount" in {
    toBson(sortByCount("someField")) should equal(Document("""{ $sortByCount : "someField" }"""))
  }

  it should "render $limit" in {
    toBson(limit(5)) should equal(Document("""{ $limit : 5 }"""))
  }

  it should "render $lookup" in {
    toBson(lookup("from", "localField", "foreignField", "as")) should equal(
      Document("""{ $lookup : { from: "from", localField: "localField", foreignField: "foreignField", as: "as" } }""")
    )

    val pipeline = Seq(filter(Filters.expr(Filters.eq("x", 1))))
    toBson(lookup("from", pipeline, "as")) ==
      Document("""{ $lookup : { from: "from",
      pipeline : [{ $match : { $expr: { $eq : [ "x" , "1" ]}}}],
      as: "as" }}""")

    toBson(lookup("from", Seq(Variable("var1", "expression1")), pipeline, "as")) ==
      Document("""{ $lookup : { from: "from",
      let: { var1: "expression1" },
      pipeline : [{ $match : { $expr: { $eq : [ "x" , "1" ]}}}],
      as: "as" }}""")

  }

  it should "render $skip" in {
    toBson(skip(5)) should equal(Document("""{ $skip : 5 }"""))
  }

  it should "render $sample" in {
    toBson(sample(5)) should equal(Document("""{ $sample : { size: 5} }"""))
  }

  it should "render $unwind" in {
    toBson(unwind("$sizes")) should equal(Document("""{ $unwind : "$sizes" }"""))
    toBson(unwind("$sizes", UnwindOptions().preserveNullAndEmptyArrays(null))) should equal(
      Document("""{ $unwind : { path : "$sizes" } }""")
    )
    toBson(unwind("$sizes", UnwindOptions().preserveNullAndEmptyArrays(false))) should equal(
      Document("""
    { $unwind : { path : "$sizes", preserveNullAndEmptyArrays : false } }""")
    )
    toBson(unwind("$sizes", UnwindOptions().preserveNullAndEmptyArrays(true))) should equal(
      Document("""
    { $unwind : { path : "$sizes", preserveNullAndEmptyArrays : true } }""")
    )
    toBson(unwind("$sizes", UnwindOptions().includeArrayIndex(null))) should equal(
      Document("""{ $unwind : { path : "$sizes" } }""")
    )
    toBson(unwind("$sizes", UnwindOptions().includeArrayIndex("$a"))) should equal(
      Document("""
    { $unwind : { path : "$sizes", includeArrayIndex : "$a" } }""")
    )
    toBson(unwind("$sizes", UnwindOptions().preserveNullAndEmptyArrays(true).includeArrayIndex("$a"))) should equal(
      Document("""
    { $unwind : { path : "$sizes", preserveNullAndEmptyArrays : true, includeArrayIndex : "$a" } }""")
    )
  }

  it should "render $out" in {
    toBson(out("authors")) should equal(Document("""{ $out : "authors" }"""))
  }

  it should "render $merge" in {
    toBson(merge("authors")) should equal(Document("""{ $merge : {into: "authors" }}"""))
    toBson(merge(MongoNamespace("db1", "authors"))) should equal(
      Document("""{ $merge : {into: {db: "db1", coll: "authors" }}}""")
    )

    toBson(merge("authors", MergeOptions().uniqueIdentifier("ssn"))) should equal(
      Document("""{ $merge : {into: "authors", on: "ssn" }}""")
    )

    toBson(merge("authors", MergeOptions().uniqueIdentifier("ssn", "otherId"))) should equal(
      Document("""{ $merge : {into: "authors", on: ["ssn", "otherId"] }}""")
    )

    toBson(
      merge(
        "authors",
        MergeOptions().whenMatched(MergeOptions.WhenMatched.REPLACE)
      )
    ) should equal(
      Document("""{ $merge : {into: "authors", whenMatched: "replace" }}""")
    )
    toBson(
      merge(
        "authors",
        MergeOptions().whenMatched(MergeOptions.WhenMatched.KEEP_EXISTING)
      )
    ) should equal(
      Document("""{ $merge : {into: "authors", whenMatched: "keepExisting" }}""")
    )
    toBson(
      merge(
        "authors",
        MergeOptions().whenMatched(MergeOptions.WhenMatched.MERGE)
      )
    ) should equal(
      Document("""{ $merge : {into: "authors", whenMatched: "merge" }}""")
    )
    toBson(
      merge(
        "authors",
        MergeOptions().whenMatched(MergeOptions.WhenMatched.FAIL)
      )
    ) should equal(
      Document("""{ $merge : {into: "authors", whenMatched: "fail" }}""")
    )

    toBson(
      merge(
        "authors",
        MergeOptions().whenNotMatched(MergeOptions.WhenNotMatched.INSERT)
      )
    ) should equal(
      Document("""{ $merge : {into: "authors", whenNotMatched: "insert" }}""")
    )
    toBson(
      merge(
        "authors",
        MergeOptions().whenNotMatched(MergeOptions.WhenNotMatched.DISCARD)
      )
    ) should equal(
      Document("""{ $merge : {into: "authors", whenNotMatched: "discard" }}""")
    )
    toBson(
      merge(
        "authors",
        MergeOptions().whenNotMatched(MergeOptions.WhenNotMatched.FAIL)
      )
    ) should equal(
      Document("""{ $merge : {into: "authors", whenNotMatched: "fail" }}""")
    )

    toBson(
      merge(
        "authors",
        MergeOptions()
          .whenMatched(MergeOptions.WhenMatched.PIPELINE)
          .variables(Variable("y", 2), Variable("z", 3))
          .whenMatchedPipeline(addFields(Field("x", 1)))
      )
    ) should equal(
      Document("""{ $merge : {into: "authors", let: {y: 2, z: 3}, whenMatched: [{$addFields: {x: 1}}]}}""")
    )
  }

  it should "render $group" in {
    toBson(group("$customerId")) should equal(Document("""{ $group : { _id : "$customerId" } }"""))
    toBson(group(null)) should equal(Document("""{ $group : { _id : null } }"""))

    toBson(
      group(Document("""{ month: { $month: "$date" }, day: { $dayOfMonth: "$date" }, year: { $year: "$date" } }"""))
    ) should equal(
      Document(
        """{ $group : { _id : { month: { $month: "$date" }, day: { $dayOfMonth: "$date" }, year: { $year: "$date" } } } }"""
      )
    )

    val groupDocument = Document("""{
      $group : {
        _id : null,
        sum: { $sum: { $multiply: [ "$price", "$quantity" ] } },
        avg: { $avg: "$quantity" },
        percentile: { $percentile: { input: "$quantity", method: "approximate", p: [0.95, 0.3] } },
        median: { $median: { input: "$quantity", method: "approximate" } },
        min: { $min: "$quantity" },
        minN: { $minN: { input: "$quantity", n: 2 } },
        max: { $max: "$quantity" },
        maxN: { $maxN: { input: "$quantity", n: 2 } },
        first: { $first: "$quantity" },
        firstN: { $firstN: { input: "$quantity", n: 2 } },
        top: { $top: { sortBy: { quantity: 1 }, output: "$quantity" } },
        topN: { $topN: { sortBy: { quantity: 1 }, output: "$quantity", n: 2 } },
        last: { $last: "$quantity" },
        lastN: { $lastN: { input: "$quantity", n: 2 } },
        bottom: { $bottom: { sortBy: { quantity: 1 }, output: ["$quantity", "$quality"] } },
        bottomN: { $bottomN: { sortBy: { quantity: 1 }, output: ["$quantity", "$quality"], n: 2 } },
        all: { $push: "$quantity" },
        unique: { $addToSet: "$quantity" },
        stdDevPop: { $stdDevPop: "$quantity" },
        stdDevSamp: { $stdDevSamp: "$quantity" }
       }
    }""")

    toBson(
      group(
        null,
        sum("sum", Document("""{ $multiply: [ "$price", "$quantity" ] }""")),
        avg("avg", "$quantity"),
        percentile("percentile", "$quantity", List(0.95, 0.3), QuantileMethod.approximate),
        median("median", "$quantity", QuantileMethod.approximate),
        min("min", "$quantity"),
        minN("minN", "$quantity", 2),
        max("max", "$quantity"),
        maxN("maxN", "$quantity", 2),
        first("first", "$quantity"),
        firstN("firstN", "$quantity", 2),
        top("top", ascending("quantity"), "$quantity"),
        topN("topN", ascending("quantity"), "$quantity", 2),
        last("last", "$quantity"),
        lastN("lastN", "$quantity", 2),
        bottom("bottom", ascending("quantity"), List("$quantity", "$quality")),
        bottomN("bottomN", ascending("quantity"), List("$quantity", "$quality"), 2),
        push("all", "$quantity"),
        addToSet("unique", "$quantity"),
        stdDevPop("stdDevPop", "$quantity"),
        stdDevSamp("stdDevSamp", "$quantity")
      )
    ) should equal(groupDocument)
  }

  it should "render $setWindowFields" in {
    val window: Window = documents(1, 2)
    toBson(
      setWindowFields(
        Some("$partitionByField"),
        Some(ascending("sortByField")),
        WindowOutputFields.of(
          BsonField.apply(
            "newField00",
            Document(
              "$sum" -> "$field00",
              "window" -> Windows.of(Document("range" -> BsonArray(1, "current"))).toBsonDocument
            )
          )
        ),
        WindowOutputFields.sum("newField01", "$field01", Some(range(1, CURRENT))),
        WindowOutputFields.avg("newField02", "$field02", Some(range(UNBOUNDED, 1))),
        WindowOutputFields.percentile(
          "newField02P",
          "$field02P",
          List(0.3, 0.9),
          QuantileMethod.approximate,
          Some(range(UNBOUNDED, 1))
        ),
        WindowOutputFields.median("newField02M", "$field02M", QuantileMethod.approximate, Some(range(UNBOUNDED, 1))),
        WindowOutputFields.stdDevSamp("newField03", "$field03", Some(window)),
        WindowOutputFields.stdDevPop("newField04", "$field04", Some(window)),
        WindowOutputFields.min("newField05", "$field05", Some(window)),
        WindowOutputFields.minN("newField05N", "$field05N", 2, Some(window)),
        WindowOutputFields.max("newField06", "$field06", Some(window)),
        WindowOutputFields.maxN("newField06N", "$field06N", 2, Some(window)),
        WindowOutputFields.count("newField07", Some(window)),
        WindowOutputFields.derivative("newField08", "$field08", window),
        WindowOutputFields.timeDerivative("newField09", "$field09", window, DAY),
        WindowOutputFields.integral("newField10", "$field10", window),
        WindowOutputFields.timeIntegral("newField11", "$field11", window, DAY),
        WindowOutputFields.covarianceSamp("newField12", "$field12_1", "$field12_2", Some(window)),
        WindowOutputFields.covariancePop("newField13", "$field13_1", "$field13_2", Some(window)),
        WindowOutputFields.expMovingAvg("newField14", "$field14", 3),
        WindowOutputFields.expMovingAvg("newField15", "$field15", 0.5),
        WindowOutputFields.push("newField16", "$field16", Some(window)),
        WindowOutputFields.addToSet("newField17", "$field17", Some(window)),
        WindowOutputFields.first("newField18", "$field18", Some(window)),
        WindowOutputFields.firstN("newField18N", "$field18N", 2, Some(window)),
        WindowOutputFields.last("newField19", "$field19", Some(window)),
        WindowOutputFields.lastN("newField19N", "$field19N", 2, Some(window)),
        WindowOutputFields.shift("newField20", "$field20", Some("defaultConstantValue"), -3),
        WindowOutputFields.documentNumber("newField21"),
        WindowOutputFields.rank("newField22"),
        WindowOutputFields.denseRank("newField23"),
        WindowOutputFields.bottom("newField24", ascending("sortByField"), "$field24", Some(window)),
        WindowOutputFields.bottomN("newField24N", ascending("sortByField"), "$field24N", 2, Some(window)),
        WindowOutputFields.top("newField25", ascending("sortByField"), "$field25", Some(window)),
        WindowOutputFields.topN("newField25N", ascending("sortByField"), "$field25N", 2, Some(window)),
        WindowOutputFields.locf("newField26", "$field26"),
        WindowOutputFields.linearFill("newField27", "$field27")
      )
    ) should equal(
      Document(
        """{
        "$setWindowFields": {
          "partitionBy": "$partitionByField",
          "sortBy": { "sortByField" : 1 },
          "output": {
            "newField00": { "$sum": "$field00", "window": { "range": [{"$numberInt": "1"}, "current"] } },
            "newField01": { "$sum": "$field01", "window": { "range": [{"$numberLong": "1"}, "current"] } },
            "newField02": { "$avg": "$field02", "window": { "range": ["unbounded", {"$numberLong": "1"}] } },
            "newField02P": { "$percentile": { input: "$field02P", p: [0.3, 0.9], method: "approximate"} "window": { "range": ["unbounded", {"$numberLong": "1"}] } },
            "newField02M": { "$median": { input: "$field02M", method: "approximate"} "window": { "range": ["unbounded", {"$numberLong": "1"}] } },
            "newField03": { "$stdDevSamp": "$field03", "window": { "documents": [1, 2] } },
            "newField04": { "$stdDevPop": "$field04", "window": { "documents": [1, 2] } },
            "newField05": { "$min": "$field05", "window": { "documents": [1, 2] } },
            "newField05N": { "$minN": { "input": "$field05N", "n": 2 }, "window": { "documents": [1, 2] } },
            "newField06": { "$max": "$field06", "window": { "documents": [1, 2] } },
            "newField06N": { "$maxN": { "input": "$field06N", "n": 2 }, "window": { "documents": [1, 2] } },
            "newField07": { "$count": {}, "window": { "documents": [1, 2] } },
            "newField08": { "$derivative": { "input": "$field08" }, "window": { "documents": [1, 2] } },
            "newField09": { "$derivative": { "input": "$field09", "unit": "day" }, "window": { "documents": [1, 2] } },
            "newField10": { "$integral": { "input": "$field10"}, "window": { "documents": [1, 2] } },
            "newField11": { "$integral": { "input": "$field11", "unit": "day" }, "window": { "documents": [1, 2] } },
            "newField12": { "$covarianceSamp": ["$field12_1", "$field12_2"], "window": { "documents": [1, 2] } },
            "newField13": { "$covariancePop": ["$field13_1", "$field13_2"], "window": { "documents": [1, 2] } },
            "newField14": { "$expMovingAvg": { "input": "$field14", "N": 3 } },
            "newField15": { "$expMovingAvg": { "input": "$field15", "alpha": 0.5 } },
            "newField16": { "$push": "$field16", "window": { "documents": [1, 2] } },
            "newField17": { "$addToSet": "$field17", "window": { "documents": [1, 2] } },
            "newField18": { "$first": "$field18", "window": { "documents": [1, 2] } },
            "newField18N": { "$firstN": { "input": "$field18N", "n": 2 }, "window": { "documents": [1, 2] } },
            "newField19": { "$last": "$field19", "window": { "documents": [1, 2] } },
            "newField19N": { "$lastN": { "input": "$field19N", "n": 2 }, "window": { "documents": [1, 2] } },
            "newField20": { "$shift": { "output": "$field20", "by": -3, "default": "defaultConstantValue" } },
            "newField21": { "$documentNumber": {} },
            "newField22": { "$rank": {} },
            "newField23": { "$denseRank": {} },
            "newField24": { "$bottom": { "sortBy": { "sortByField": 1}, "output": "$field24"}, "window": { "documents": [1, 2] } },
            "newField24N": { "$bottomN": { "sortBy": { "sortByField": 1}, "output": "$field24N", "n": 2 }, "window": { "documents": [1, 2] } },
            "newField25": { "$top": { "sortBy": { "sortByField": 1}, "output": "$field25"}, "window": { "documents": [1, 2] } },
            "newField25N": { "$topN": { "sortBy": { "sortByField": 1}, "output": "$field25N", "n": 2 }, "window": { "documents": [1, 2] } },
            "newField26": { "$locf": "$field26" },
            "newField27": { "$linearFill": "$field27" }
          }
        }
      }"""
      )
    )
  }

  it should "render $setWindowFields with no partitionBy/sortBy" in {
    toBson(
      setWindowFields(None, None, WindowOutputFields.sum("newField01", "$field01", Some(documents(1, 2))))
    ) should equal(
      Document("""{
        "$setWindowFields": {
          "output": {
            "newField01": { "$sum": "$field01", "window": { "documents": [1, 2] } }
          }
        }
      }""")
    )
  }

  it should "render $densify" in {
    toBson(
      Aggregates.densify(
        "fieldName",
        fullRangeWithStep(1)
      )
    ) should equal(
      Document("""{
        "$densify": {
          "field": "fieldName",
          "range": { "bounds": "full", "step": 1 }
        }
      }""")
    )
  }

  it should "render $fill" in {
    toBson(
      Aggregates.fill(
        fillOptions().partitionByFields("fieldName3").sortBy(ascending("fieldName4")),
        FillOutputField.linear("fieldName1"),
        FillOutputField.locf("fieldName2")
      )
    ) should equal(
      Document("""{
        "$fill": {
          "partitionByFields": ["fieldName3"],
          "sortBy": { "fieldName4": 1 },
          "output": {
            "fieldName1": { "method": "linear" },
            "fieldName2": { "method": "locf" }
          }
        }
      }""")
    )
  }

  it should "render $search" in {
    toBson(
      Aggregates.search(
        exists(fieldPath("fieldName")),
        searchOptions()
      )
    ) should equal(
      Document("""{
        "$search": {
          "exists": { "path": "fieldName" }
        }
      }""")
    )
    toBson(
      Aggregates.search(
        SearchCollector
          .facet(exists(fieldPath("fieldName")), List(stringFacet("stringFacetName", fieldPath("fieldName1")))),
        searchOptions()
          .index("indexName")
          .count(total())
          .highlight(
            paths(
              fieldPath("fieldName1"),
              fieldPath("fieldName2").multi("analyzerName"),
              wildcardPath("field.name*")
            )
          )
      )
    ) should equal(
      Document("""{
        "$search": {
          "facet": {
            "operator": { "exists": { "path": "fieldName" } },
            "facets": {
              "stringFacetName": { "type" : "string", "path": "fieldName1" }
            }
          },
          "index": "indexName",
          "count": { "type": "total" },
          "highlight": {
            "path": [
              "fieldName1",
              { "value": "fieldName2", "multi": "analyzerName" },
              { "wildcard": "field.name*" }
            ]
          }
        }
      }""")
    )
  }

  it should "render $search with no options" in {
    toBson(
      Aggregates.search(
        exists(fieldPath("fieldName"))
      )
    ) should equal(
      Document("""{
        "$search": {
          "exists": { "path": "fieldName" }
        }
      }""")
    )
    toBson(
      Aggregates.search(
        SearchCollector.facet(
          exists(fieldPath("fieldName")),
          List(
            stringFacet("facetName", fieldPath("fieldName"))
              .numBuckets(3)
          )
        )
      )
    ) should equal(
      Document("""{
        "$search": {
          "facet": {
            "operator": { "exists": { "path": "fieldName" } },
            "facets": {
              "facetName": { "type": "string", "path": "fieldName", "numBuckets": 3 }
            }
          }
        }
      }""")
    )
  }

  it should "render $searchMeta" in {
    toBson(
      Aggregates.searchMeta(
        exists(fieldPath("fieldName")),
        searchOptions()
      )
    ) should equal(
      Document("""{
        "$searchMeta": {
          "exists": { "path": "fieldName" }
        }
      }""")
    )
    toBson(
      Aggregates.searchMeta(
        SearchCollector
          .facet(exists(fieldPath("fieldName")), List(stringFacet("stringFacetName", fieldPath("fieldName1")))),
        searchOptions()
          .index("indexName")
          .count(total())
          .highlight(
            paths(
              fieldPath("fieldName1"),
              fieldPath("fieldName2").multi("analyzerName"),
              wildcardPath("field.name*")
            )
          )
      )
    ) should equal(
      Document("""{
        "$searchMeta": {
          "facet": {
            "operator": { "exists": { "path": "fieldName" } },
            "facets": {
              "stringFacetName": { "type" : "string", "path": "fieldName1" }
            }
          },
          "index": "indexName",
          "count": { "type": "total" },
          "highlight": {
            "path": [
              "fieldName1",
              { "value": "fieldName2", "multi": "analyzerName" },
              { "wildcard": "field.name*" }
            ]
          }
        }
      }""")
    )
  }

  it should "render $searchMeta with no options" in {
    toBson(
      Aggregates.searchMeta(
        exists(fieldPath("fieldName"))
      )
    ) should equal(
      Document("""{
        "$searchMeta": {
          "exists": { "path": "fieldName" }
        }
      }""")
    )
    toBson(
      Aggregates.searchMeta(
        SearchCollector.facet(
          exists(fieldPath("fieldName")),
          List(
            stringFacet("facetName", fieldPath("fieldName"))
              .numBuckets(3)
          )
        )
      )
    ) should equal(
      Document("""{
        "$searchMeta": {
          "facet": {
            "operator": { "exists": { "path": "fieldName" } },
            "facets": {
              "facetName": { "type": "string", "path": "fieldName", "numBuckets": 3 }
            }
          }
        }
      }""")
    )
  }

  it should "render approximate $vectorSearch" in {
    toBson(
      Aggregates.vectorSearch(
        fieldPath("fieldName").multi("ignored"),
        List(1.0d, 2.0d),
        "indexName",
        1,
        approximateVectorSearchOptions(2)
          .filter(Filters.ne("fieldName", "fieldValue"))
      )
    ) should equal(
      Document(
        """{
        "$vectorSearch": {
            "path": "fieldName",
            "queryVector": [1.0, 2.0],
            "index": "indexName",
            "numCandidates": {"$numberLong": "2"},
            "limit": {"$numberLong": "1"},
            "filter": {"fieldName": {"$ne": "fieldValue"}}
        }
      }"""
      )
    )
  }

  it should "render exact $vectorSearch" in {
    toBson(
      Aggregates.vectorSearch(
        fieldPath("fieldName").multi("ignored"),
        List(1.0d, 2.0d),
        "indexName",
        1,
        exactVectorSearchOptions()
          .filter(Filters.ne("fieldName", "fieldValue"))
      )
    ) should equal(
      Document(
        """{
        "$vectorSearch": {
            "path": "fieldName",
            "queryVector": [1.0, 2.0],
            "index": "indexName",
            "exact": true,
            "limit": {"$numberLong": "1"},
            "filter": {"fieldName": {"$ne": "fieldValue"}}
        }
      }"""
      )
    )
  }

  it should "render $unset" in {
    toBson(
      Aggregates.unset("title", "author.first")
    ) should equal(
      Document("""{ $unset: ['title', 'author.first'] }""")
    )
    toBson(
      Aggregates.unset("author.first")
    ) should equal(
      Document("""{ "$unset": "author.first" }""")
    )
  }

  it should "render $geoNear" in {

    toBson(
      Aggregates.geoNear(
        Point(Position(-73.99279, 40.719296)),
        "dist.calculated"
      )
    ) should equal(
      Document("""{
                 |   $geoNear: {
                 |      near: { type: 'Point', coordinates: [ -73.99279 , 40.719296 ] },
                 |      distanceField: 'dist.calculated'
                 |   }
                 |}""".stripMargin)
    )
    toBson(
      Aggregates.geoNear(
        Point(Position(-73.99279, 40.719296)),
        "dist.calculated",
        geoNearOptions()
          .minDistance(0)
          .maxDistance(2)
          .query(Document("""{ "category": "Parks" }"""))
          .includeLocs("dist.location")
          .spherical()
          .key("location")
          .distanceMultiplier(10.0)
      )
    ) should equal(
      Document("""{
                 |   $geoNear: {
                 |      near: { type: 'Point', coordinates: [ -73.99279 , 40.719296 ] },
                 |      distanceField: 'dist.calculated',
                 |      minDistance: 0,
                 |      maxDistance: 2,
                 |      query: { category: 'Parks' },
                 |      includeLocs: 'dist.location',
                 |      spherical: true,
                 |      key: 'location',
                 |      distanceMultiplier: 10.0
                 |   }
                 |}""".stripMargin)
    )
  }

  it should "render $documents" in {
    toBson(
      Aggregates.documents(
        org.mongodb.scala.bson.BsonDocument("""{a: 1, b: {$add: [1, 1]} }"""),
        Document("""{a: 3, b: 4}""")
      )
    ) should equal(
      Document("""{$documents: [{a: 1, b: {$add: [1, 1]}}, {a: 3, b: 4}]}""")
    )
  }
}
