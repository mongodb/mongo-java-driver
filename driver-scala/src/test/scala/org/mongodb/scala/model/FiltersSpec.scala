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

import org.bson.{ BsonDocument, BsonType }
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.geojson.{ Point, Polygon, Position }
import org.mongodb.scala.{ model, BaseSpec, MongoClient }

class FiltersSpec extends BaseSpec {
  val registry = MongoClient.DEFAULT_CODEC_REGISTRY

  def toBson(bson: Bson): Document =
    Document(bson.toBsonDocument(classOf[BsonDocument], MongoClient.DEFAULT_CODEC_REGISTRY))

  "Filters" should "have the same methods as the wrapped Filters" in {
    val wrapped = classOf[com.mongodb.client.model.Filters].getDeclaredMethods
      .filter(f => isStatic(f.getModifiers) && isPublic(f.getModifiers))
      .map(_.getName)
      .toSet
    val aliases = Set("equal", "notEqual", "bsonType")
    val ignore = Set("$anonfun$geoWithinPolygon$1")
    val local = model.Filters.getClass.getDeclaredMethods
      .filter(f => isPublic(f.getModifiers))
      .map(_.getName)
      .toSet -- aliases -- ignore

    local should equal(wrapped)
  }

  it should "render without $eq" in {
    toBson(model.Filters.eq("x", 1)) should equal(Document("""{x : 1}"""))
    toBson(model.Filters.eq("x", null)) should equal(Document("""{x : null}"""))

    toBson(model.Filters.equal("x", 1)) should equal(Document("""{x : 1}"""))
    toBson(model.Filters.equal("x", null)) should equal(Document("""{x : null}"""))
  }

  it should "render $ne" in {
    toBson(model.Filters.ne("x", 1)) should equal(Document("""{x : {$ne : 1} }"""))
    toBson(model.Filters.ne("x", null)) should equal(Document("""{x : {$ne : null} }"""))
  }

  it should "render $not" in {
    toBson(model.Filters.not(model.Filters.eq("x", 1))) should equal(Document("""{x : {$not: {$eq: 1}}}"""))
    toBson(model.Filters.not(model.Filters.gt("x", 1))) should equal(Document("""{x : {$not: {$gt: 1}}}"""))
    toBson(model.Filters.not(model.Filters.regex("x", "^p.*"))) should equal(Document("""{x : {$not: /^p.*/}}"""))
    toBson(model.Filters.not(model.Filters.and(model.Filters.gt("x", 1), model.Filters.eq("y", 20)))) should equal(
      Document("""{$not: {$and: [{x: {$gt: 1}}, {y: 20}]}}""")
    )
    toBson(model.Filters.not(model.Filters.and(model.Filters.eq("x", 1), model.Filters.eq("x", 2)))) should equal(
      Document("""{$not: {$and: [{x: 1}, {x: 2}]}}""")
    )
    toBson(model.Filters.not(model.Filters.and(model.Filters.in("x", 1, 2), model.Filters.eq("x", 3)))) should equal(
      Document("""{$not: {$and: [{x: {$in: [1, 2]}}, {x: 3}]}}""")
    )
    toBson(model.Filters.not(model.Filters.or(model.Filters.gt("x", 1), model.Filters.eq("y", 20)))) should equal(
      Document("""{$not: {$or: [{x: {$gt: 1}}, {y: 20}]}}""")
    )
    toBson(model.Filters.not(model.Filters.or(model.Filters.eq("x", 1), model.Filters.eq("x", 2)))) should equal(
      Document("""{$not: {$or: [{x: 1}, {x: 2}]}}""")
    )
    toBson(model.Filters.not(model.Filters.or(model.Filters.in("x", 1, 2), model.Filters.eq("x", 3)))) should equal(
      Document("""{$not: {$or: [{x: {$in: [1, 2]}}, {x: 3}]}}""")
    )
    toBson(model.Filters.not(Document("$in" -> List(1)))) should equal(Document("""{$not: {$in: [1]}}"""))
  }

  it should "render $nor" in {
    toBson(model.Filters.nor(model.Filters.eq("price", 1))) should equal(Document("""{$nor : [{price: 1}]}"""))
    toBson(model.Filters.nor(model.Filters.eq("price", 1), model.Filters.eq("sale", true))) should equal(
      Document("""{$nor : [{price: 1}, {sale: true}]}""")
    )
  }

  it should "render $gt" in {
    toBson(model.Filters.gt("x", 1)) should equal(Document("""{x : {$gt : 1} }"""))
  }

  it should "render $lt" in {
    toBson(model.Filters.lt("x", 1)) should equal(Document("""{x : {$lt : 1} }"""))
  }

  it should "render $gte" in {
    toBson(model.Filters.gte("x", 1)) should equal(Document("""{x : {$gte : 1} }"""))
  }

  it should "render $lte" in {
    toBson(model.Filters.lte("x", 1)) should equal(Document("""{x : {$lte : 1} }"""))
  }

  it should "render $exists" in {
    toBson(model.Filters.exists("x")) should equal(Document("""{x : {$exists : true} }"""))
    toBson(model.Filters.exists("x", false)) should equal(Document("""{x : {$exists : false} }"""))
  }

  it should "or should render empty or using $or" in {
    toBson(model.Filters.or()) should equal(Document("""{$or : []}"""))
  }

  it should "render $or" in {
    toBson(model.Filters.or(model.Filters.eq("x", 1), model.Filters.eq("y", 2))) should equal(
      Document("""{$or : [{x : 1}, {y : 2}]}""")
    )
  }

  it should "and should render empty and using $and" in {
    toBson(model.Filters.and()) should equal(Document("""{$and : []}"""))
  }

  it should "and should render using $and" in {
    toBson(model.Filters.and(model.Filters.eq("x", 1), model.Filters.eq("y", 2))) should equal(
      Document("""{$and: [{x : 1}, {y : 2}]}""")
    )
  }

  it should "and should flatten multiple operators for the same key" in {
    toBson(model.Filters.and(model.Filters.gt("a", 1), model.Filters.lt("a", 9))) should equal(
      Document("""{$and: [{a : {$gt : 1}}, {a: {$lt : 9}}]}""")
    )
  }

  it should "and should flatten nested" in {
    toBson(
      model.Filters.and(model.Filters.and(model.Filters.eq("a", 1), model.Filters.eq("b", 2)), model.Filters.eq("c", 3))
    ) should equal(Document("""{$and: [{$and: [{a : 1}, {b : 2}]}, {c : 3}]}"""))
    toBson(
      model.Filters.and(model.Filters.and(model.Filters.eq("a", 1), model.Filters.eq("a", 2)), model.Filters.eq("c", 3))
    ) should equal(Document("""{$and: [{$and:[{a : 1}, {a : 2}]}, {c : 3}] }"""))
    toBson(model.Filters.and(model.Filters.lt("a", 1), model.Filters.lt("b", 2))) should equal(
      Document("""{$and: [{a : {$lt : 1}}, {b : {$lt : 2} }]}""")
    )
    toBson(model.Filters.and(model.Filters.lt("a", 1), model.Filters.lt("a", 2))) should equal(
      Document("""{$and : [{a : {$lt : 1}}, {a : {$lt : 2}}]}""")
    )
  }

  it should "render $all" in {
    toBson(model.Filters.all("a", 1, 2, 3)) should equal(Document("""{a : {$all : [1, 2, 3]} }"""))
  }

  it should "render $elemMatch" in {
    toBson(model.Filters.elemMatch("results", Document("$gte" -> 80, "$lt" -> 85))) should equal(
      Document("""{results : {$elemMatch : {$gte: 80, $lt: 85}}}""")
    )
    toBson(
      model.Filters
        .elemMatch("results", model.Filters.and(model.Filters.eq("product", "xyz"), model.Filters.gt("score", 8)))
    ) should equal(Document("""{ results : {$elemMatch : {$and: [{product : "xyz"}, {score : {$gt : 8}}]}}}"""))
  }

  it should "render $in" in {
    toBson(model.Filters.in("a", 1, 2, 3)) should equal(Document("""{a : {$in : [1, 2, 3]} }"""))
  }

  it should "render $nin" in {
    toBson(model.Filters.nin("a", 1, 2, 3)) should equal(Document("""{a : {$nin : [1, 2, 3]} }"""))
  }

  it should "render $mod" in {
    toBson(model.Filters.mod("a", 100, 7)) should equal(Document("a" -> Document("$mod" -> List(100L, 7L))))
  }

  it should "render $size" in {
    toBson(model.Filters.size("a", 13)) should equal(Document("""{a : {$size : 13} }"""))
  }

  it should "render $type" in {
    toBson(model.Filters.`type`("a", BsonType.ARRAY)) should equal(Document("""{a : {$type : 4} }"""))
  }

  it should "render $bitsAllClear" in {
    toBson(model.Filters.bitsAllClear("a", 13)) should equal(
      Document("""{a : {$bitsAllClear : { "$numberLong" : "13" }} }""")
    )
  }

  it should "render $bitsAllSet" in {
    toBson(model.Filters.bitsAllSet("a", 13)) should equal(
      Document("""{a : {$bitsAllSet : { "$numberLong" : "13" }} }""")
    )
  }

  it should "render $bitsAnyClear" in {
    toBson(model.Filters.bitsAnyClear("a", 13)) should equal(
      Document("""{a : {$bitsAnyClear : { "$numberLong" : "13" }} }""")
    )
  }

  it should "render $bitsAnySet" in {
    toBson(model.Filters.bitsAnySet("a", 13)) should equal(
      Document("""{a : {$bitsAnySet : { "$numberLong" : "13" }} }""")
    )
  }

  it should "render $text" in {
    toBson(model.Filters.text("mongoDB for GIANT ideas")) should equal(
      Document("""{$text: {$search: "mongoDB for GIANT ideas"} }""")
    )
    toBson(model.Filters.text("mongoDB for GIANT ideas", new TextSearchOptions().language("english"))) should equal(
      Document("""{$text : {$search : "mongoDB for GIANT ideas", $language : "english"} }""")
    )
    toBson(model.Filters.text("mongoDB for GIANT ideas", new TextSearchOptions().caseSensitive(true))) should equal(
      Document("""{$text : {$search : "mongoDB for GIANT ideas", $caseSensitive : true} }""")
    )
    toBson(model.Filters.text("mongoDB for GIANT ideas", new TextSearchOptions().diacriticSensitive(false))) should equal(
      Document("""{$text : {$search : "mongoDB for GIANT ideas", $diacriticSensitive : false} }""")
    )
    toBson(
      model.Filters.text(
        "mongoDB for GIANT ideas",
        new TextSearchOptions()
          .language("english")
          .caseSensitive(false)
          .diacriticSensitive(true)
      )
    ) should equal(
      Document(
        """{$text : {$search : "mongoDB for GIANT ideas", $language : "english", $caseSensitive : false,
              $diacriticSensitive : true} }"""
      )
    )
  }

  it should "render $regex" in {
    toBson(model.Filters.regex("name", "acme.*corp")) should equal(
      Document("""{name : {$regex : "acme.*corp", $options : ""}}""")
    )
    toBson(model.Filters.regex("name", "acme.*corp", "si")) should equal(
      Document("""{name : {$regex : "acme.*corp", $options : "si"}}""")
    )
    toBson(model.Filters.regex("name", "acme.*corp".r)) should equal(
      Document("""{name : {$regex : "acme.*corp", $options : ""}}""")
    )
  }

  it should "render $where" in {
    toBson(model.Filters.where("this.credits == this.debits")) should equal(
      Document("""{$where: "this.credits == this.debits"}""")
    )
  }

  it should "render $geoWithin" in {
    val polygon = Polygon(
      Seq(
        Position(40.0, 18.0),
        Position(40.0, 19.0),
        Position(41.0, 19.0),
        Position(40.0, 18.0)
      )
    )

    toBson(model.Filters.geoWithin("loc", polygon)) should equal(
      Document("""{
                                                          loc: {
                                                            $geoWithin: {
                                                              $geometry: {
                                                                type: "Polygon",
                                                                coordinates: [
                                                                               [
                                                                                 [40.0, 18.0], [40.0, 19.0], [41.0, 19.0], [40.0, 18.0]
                                                                               ]
                                                                             ]
                                                              }
                                                            }
                                                          }
                                                        }""")
    )

    toBson(model.Filters.geoWithin("loc", Document(polygon.toJson()))) should equal(
      Document("""{
                                                                          loc: {
                                                                            $geoWithin: {
                                                                              $geometry: {
                                                                                type: "Polygon",
                                                                                coordinates: [
                                                                                               [
                                                                                                 [40.0, 18.0], [40.0, 19.0], [41.0, 19.0],
                                                                                                 [40.0, 18.0]
                                                                                               ]
                                                                                             ]
                                                                              }
                                                                            }
                                                                          }
                                                                        }""")
    )
  }

  it should "render $geoWithin with $box" in {
    toBson(model.Filters.geoWithinBox("loc", 1d, 2d, 3d, 4d)) should equal(Document("""{
                                                                    loc: {
                                                                      $geoWithin: {
                                                                        $box:  [
                                                                                 [ 1.0, 2.0 ], [ 3.0, 4.0 ]
                                                                               ]
                                                                      }
                                                                    }
                                                                  }"""))
  }

  it should "render $geoWithin with $polygon" in {
    toBson(model.Filters.geoWithinPolygon("loc", List(List(0d, 0d), List(3d, 6d), List(6d, 0d)))) should equal(
      Document("""{
                                                                                        loc: {
                                                                                          $geoWithin: {
                                                                                            $polygon: [
                                                                                                        [ 0.0, 0.0 ], [ 3.0, 6.0 ],
                                                                                                        [ 6.0, 0.0 ]
                                                                                                      ]
                                                                                          }
                                                                                        }
                                                                                      }""")
    )
  }

  it should "render $geoWithin with $center" in {
    toBson(model.Filters.geoWithinCenter("loc", -74d, 40.74d, 10d)) should equal(
      Document("""{ loc: { $geoWithin: { $center: [ [-74.0, 40.74], 10.0 ] } } }""")
    )
  }

  it should "render $geoWithin with $centerSphere" in {
    toBson(model.Filters.geoWithinCenterSphere("loc", -74d, 40.74d, 10d)) should equal(Document("""{
                                                                                 loc: {
                                                                                   $geoWithin: {
                                                                                     $centerSphere: [
                                                                                                      [-74.0, 40.74], 10.0
                                                                                                    ]
                                                                                   }
                                                                                 }
                                                                              }"""))
  }

  it should "render $geoIntersects" in {
    val polygon = Polygon(
      Seq(
        Position(40.0d, 18.0d),
        Position(40.0d, 19.0d),
        Position(41.0d, 19.0d),
        Position(40.0d, 18.0d)
      )
    )

    toBson(model.Filters.geoIntersects("loc", polygon)) should equal(Document("""{
                                                               loc: {
                                                                 $geoIntersects: {
                                                                   $geometry: {
                                                                      type: "Polygon",
                                                                      coordinates: [
                                                                                     [
                                                                                       [40.0, 18.0], [40.0, 19.0], [41.0, 19.0],
                                                                                       [40.0, 18.0]
                                                                                     ]
                                                                                   ]
                                                                   }
                                                                 }
                                                               }
                                                            }"""))

    toBson(model.Filters.geoIntersects("loc", Document(polygon.toJson))) should equal(
      Document("""{
                                                                              loc: {
                                                                                $geoIntersects: {
                                                                                  $geometry: {
                                                                                    type: "Polygon",
                                                                                    coordinates: [
                                                                                                   [
                                                                                                     [40.0, 18.0], [40.0, 19.0], [41.0, 19.0],
                                                                                                     [40.0, 18.0]
                                                                                                   ]
                                                                                                 ]
                                                                                  }
                                                                                }
                                                                              }
                                                                            }""")
    )
  }

  it should "render $near" in {
    val point = Point(Position(-73.9667, 40.78))
    val pointDocument = Document(point.toJson)

    toBson(model.Filters.near("loc", point)) should equal(Document("""{
                                                                   loc : {
                                                                      $near: {
                                                                         $geometry: {
                                                                            type : "Point",
                                                                            coordinates : [ -73.9667, 40.78 ]
                                                                         },
                                                                      }
                                                                   }
                                                                 }"""))

    toBson(model.Filters.near("loc", point, Some(5000d), Some(1000d))) should equal(
      Document("""{
                                                                         loc : {
                                                                            $near: {
                                                                               $geometry: {
                                                                                  type : "Point",
                                                                                  coordinates : [ -73.9667, 40.78 ]
                                                                               },
                                                                               $maxDistance: 5000.0,
                                                                               $minDistance: 1000.0,
                                                                            }
                                                                         }
                                                                       }""")
    )

    toBson(model.Filters.near("loc", point, Some(5000d), None)) should equal(Document("""{
                                                                        loc : {
                                                                           $near: {
                                                                              $geometry: {
                                                                                 type : "Point",
                                                                                 coordinates : [ -73.9667, 40.78 ]
                                                                              },
                                                                              $maxDistance: 5000.0,
                                                                           }
                                                                        }
                                                                      }"""))

    toBson(model.Filters.near("loc", point, None, Some(1000d))) should equal(Document("""{
                                                                        loc : {
                                                                           $near: {
                                                                              $geometry: {
                                                                                 type : "Point",
                                                                                 coordinates : [ -73.9667, 40.78 ]
                                                                              },
                                                                              $minDistance: 1000.0,
                                                                           }
                                                                        }
                                                                      }"""))

    toBson(model.Filters.near("loc", point, None, None)) should equal(Document("""{
                                                                        loc : {
                                                                           $near: {
                                                                              $geometry: {
                                                                                 type : "Point",
                                                                                 coordinates : [ -73.9667, 40.78 ]
                                                                              },
                                                                           }
                                                                        }
                                                                      }"""))

    toBson(model.Filters.near("loc", pointDocument)) should equal(Document("""{
                                                                           loc : {
                                                                              $near: {
                                                                                 $geometry: {
                                                                                    type : "Point",
                                                                                    coordinates : [ -73.9667, 40.78 ]
                                                                                 },
                                                                              }
                                                                           }
                                                                         }"""))

    toBson(model.Filters.near("loc", pointDocument, Some(5000d), Some(1000d))) should equal(
      Document("""{
                                                                                 loc : {
                                                                                    $near: {
                                                                                       $geometry: {
                                                                                          type : "Point",
                                                                                          coordinates : [ -73.9667, 40.78 ]
                                                                                       },
                                                                                       $maxDistance: 5000.0,
                                                                                       $minDistance: 1000.0,
                                                                                    }
                                                                                 }
                                                                               }""")
    )

    toBson(model.Filters.near("loc", pointDocument, Some(5000d), None)) should equal(
      Document("""{
                                                                                loc : {
                                                                                   $near: {
                                                                                      $geometry: {
                                                                                         type : "Point",
                                                                                         coordinates : [ -73.9667, 40.78 ]
                                                                                      },
                                                                                      $maxDistance: 5000.0,
                                                                                   }
                                                                                }
                                                                              }""")
    )

    toBson(model.Filters.near("loc", pointDocument, None, Some(1000d))) should equal(
      Document("""{
                                                                                loc : {
                                                                                   $near: {
                                                                                      $geometry: {
                                                                                         type : "Point",
                                                                                         coordinates : [ -73.9667, 40.78 ]
                                                                                      },
                                                                                      $minDistance: 1000.0,
                                                                                   }
                                                                                }
                                                                              }""")
    )

    toBson(model.Filters.near("loc", pointDocument, None, None)) should equal(Document("""{
                                                                                loc : {
                                                                                   $near: {
                                                                                      $geometry: {
                                                                                         type : "Point",
                                                                                         coordinates : [ -73.9667, 40.78 ]
                                                                                      },
                                                                                   }
                                                                                }
                                                                              }"""))

    toBson(model.Filters.near("loc", -73.9667, 40.78)) should equal(Document("""{
                                                                             loc : {
                                                                                $near: [-73.9667, 40.78],
                                                                                }
                                                                             }
                                                                           }"""))

    toBson(model.Filters.near("loc", -73.9667, 40.78, Some(5000d), Some(1000d))) should equal(
      Document("""{
                                                                                   loc : {
                                                                                      $near: [-73.9667, 40.78],
                                                                                      $maxDistance: 5000.0,
                                                                                      $minDistance: 1000.0,
                                                                                      }
                                                                                   }
                                                                                 }""")
    )

    toBson(model.Filters.near("loc", -73.9667, 40.78, Some(5000d), None)) should equal(
      Document("""{
                                                                                  loc : {
                                                                                     $near: [-73.9667, 40.78],
                                                                                     $maxDistance: 5000.0,
                                                                                     }
                                                                                  }
                                                                                }""")
    )

    toBson(model.Filters.near("loc", -73.9667, 40.78, None, Some(1000d))) should equal(
      Document("""{
                                                                                  loc : {
                                                                                     $near: [-73.9667, 40.78],
                                                                                     $minDistance: 1000.0,
                                                                                     }
                                                                                  }
                                                                                }""")
    )

    toBson(model.Filters.near("loc", -73.9667, 40.78, None, None)) should equal(Document("""{
                                                                                  loc : {
                                                                                     $near: [-73.9667, 40.78],
                                                                                     }
                                                                                  }
                                                                                }"""))
  }

  it should "render $nearSphere" in {
    val point = Point(Position(-73.9667, 40.78))
    val pointDocument = Document(point.toJson)

    toBson(model.Filters.nearSphere("loc", point)) should equal(Document("""{
                                                                         loc : {
                                                                            $nearSphere: {
                                                                               $geometry: {
                                                                                  type : "Point",
                                                                                  coordinates : [ -73.9667, 40.78 ]
                                                                               },
                                                                            }
                                                                         }
                                                                       }"""))

    toBson(model.Filters.nearSphere("loc", point, Some(5000d), Some(1000d))) should equal(
      Document("""{
                                                                               loc : {
                                                                                  $nearSphere: {
                                                                                     $geometry: {
                                                                                        type : "Point",
                                                                                        coordinates : [ -73.9667, 40.78 ]
                                                                                     },
                                                                                     $maxDistance: 5000.0,
                                                                                     $minDistance: 1000.0,
                                                                                  }
                                                                               }
                                                                             }""")
    )

    toBson(model.Filters.nearSphere("loc", point, Some(5000d), None)) should equal(Document("""{
                                                                             loc:
                                                                             {
                                                                                 $nearSphere:
                                                                                 {
                                                                                     $geometry:
                                                                                     {
                                                                                         type: "Point",
                                                                                         coordinates:
                                                                                         [-73.9667, 40.78]
                                                                                     },
                                                                                     $maxDistance: 5000.0,
                                                                                 }
                                                                             }
                                                                         }"""))

    toBson(model.Filters.nearSphere("loc", point, None, Some(1000d))) should equal(
      Document("""{
                                                                              loc : {
                                                                                 $nearSphere: {
                                                                                    $geometry: {
                                                                                       type : "Point",
                                                                                       coordinates : [ -73.9667, 40.78 ]
                                                                                    },
                                                                                    $minDistance: 1000.0,
                                                                                 }
                                                                              }
                                                                            }""")
    )

    toBson(model.Filters.nearSphere("loc", point, None, None)) should equal(Document("""{
                                                                              loc : {
                                                                                 $nearSphere: {
                                                                                    $geometry: {
                                                                                       type : "Point",
                                                                                       coordinates : [ -73.9667, 40.78 ]
                                                                                    },
                                                                                 }
                                                                              }
                                                                            }"""))

    toBson(model.Filters.nearSphere("loc", pointDocument)) should equal(Document("""{
                                                                                 loc : {
                                                                                    $nearSphere: {
                                                                                       $geometry: {
                                                                                          type : "Point",
                                                                                          coordinates : [ -73.9667, 40.78 ]
                                                                                       },
                                                                                    }
                                                                                 }
                                                                               }"""))

    toBson(model.Filters.nearSphere("loc", pointDocument, Some(5000d), Some(1000d))) should equal(
      Document("""{
                                                                                       loc : {
                                                                                          $nearSphere: {
                                                                                             $geometry: {
                                                                                                type : "Point",
                                                                                                coordinates : [ -73.9667, 40.78 ]
                                                                                             },
                                                                                             $maxDistance: 5000.0,
                                                                                             $minDistance: 1000.0,
                                                                                          }
                                                                                       }
                                                                                     }""")
    )

    toBson(model.Filters.nearSphere("loc", pointDocument, Some(5000d), None)) should equal(
      Document("""{
                                                                                      loc : {
                                                                                         $nearSphere: {
                                                                                            $geometry: {
                                                                                               type : "Point",
                                                                                               coordinates : [ -73.9667, 40.78 ]
                                                                                            },
                                                                                            $maxDistance: 5000.0,
                                                                                         }
                                                                                      }
                                                                                    }""")
    )

    toBson(model.Filters.nearSphere("loc", pointDocument, None, Some(1000d))) should equal(
      Document("""{
                                                                                      loc : {
                                                                                         $nearSphere: {
                                                                                            $geometry: {
                                                                                               type : "Point",
                                                                                               coordinates : [ -73.9667, 40.78 ]
                                                                                            },
                                                                                            $minDistance: 1000.0,
                                                                                         }
                                                                                      }
                                                                                    }""")
    )

    toBson(model.Filters.nearSphere("loc", pointDocument, None, None)) should equal(
      Document("""{
                                                                                      loc : {
                                                                                         $nearSphere: {
                                                                                            $geometry: {
                                                                                               type : "Point",
                                                                                               coordinates : [ -73.9667, 40.78 ]
                                                                                            },
                                                                                         }
                                                                                      }
                                                                                    }""")
    )

    toBson(model.Filters.nearSphere("loc", -73.9667, 40.78)) should equal(Document("""{
                                                                                   loc : {
                                                                                      $nearSphere: [-73.9667, 40.78],
                                                                                      }
                                                                                   }
                                                                                 }"""))

    toBson(model.Filters.nearSphere("loc", -73.9667, 40.78, Some(5000d), Some(1000d))) should equal(
      Document("""{
                                                                                         loc : {
                                                                                            $nearSphere: [-73.9667, 40.78],
                                                                                            $maxDistance: 5000.0,
                                                                                            $minDistance: 1000.0,
                                                                                            }
                                                                                         }
                                                                                       }""")
    )

    toBson(model.Filters.nearSphere("loc", -73.9667, 40.78, Some(5000d), None)) should equal(
      Document("""{
                                                                                        loc : {
                                                                                           $nearSphere: [-73.9667, 40.78],
                                                                                           $maxDistance: 5000.0,
                                                                                           }
                                                                                        }
                                                                                      }""")
    )

    toBson(model.Filters.nearSphere("loc", -73.9667, 40.78, None, Some(1000d))) should equal(
      Document("""{
                                                                                        loc : {
                                                                                           $nearSphere: [-73.9667, 40.78],
                                                                                           $minDistance: 1000.0,
                                                                                           }
                                                                                        }
                                                                                      }""")
    )

    toBson(model.Filters.nearSphere("loc", -73.9667, 40.78, None, None)) should equal(
      Document("""{
                                                                                        loc : {
                                                                                           $nearSphere: [-73.9667, 40.78],
                                                                                           }
                                                                                        }
                                                                                      }""")
    )
  }

  it should "render $expr" in {
    toBson(model.Filters.expr(Document("{$gt: ['$spent', '$budget']}"))) should equal(
      Document("""{$expr: {$gt: ["$spent", "$budget"]}}""")
    )
  }

  it should "render $jsonSchema" in {
    toBson(model.Filters.jsonSchema(Document("{bsonType: 'object'}"))) should equal(
      Document("""{$jsonSchema: {bsonType:  "object"}}""")
    )
  }

  it should "render an empty document" in {
    toBson(model.Filters.empty()) should equal(
      Document("""{}""")
    )
  }

}
