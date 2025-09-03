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

package com.mongodb.client.model

import com.mongodb.client.model.geojson.Point
import com.mongodb.client.model.geojson.Polygon
import com.mongodb.client.model.geojson.Position
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.BsonType
import org.bson.Document
import spock.lang.Specification

import java.util.regex.Pattern

import static Filters.and
import static Filters.exists
import static Filters.or
import static com.mongodb.client.model.BsonHelper.toBson
import static com.mongodb.client.model.Filters.all
import static com.mongodb.client.model.Filters.bitsAllClear
import static com.mongodb.client.model.Filters.bitsAllSet
import static com.mongodb.client.model.Filters.bitsAnyClear
import static com.mongodb.client.model.Filters.bitsAnySet
import static com.mongodb.client.model.Filters.elemMatch
import static com.mongodb.client.model.Filters.empty
import static com.mongodb.client.model.Filters.eq
import static com.mongodb.client.model.Filters.expr
import static com.mongodb.client.model.Filters.geoIntersects
import static com.mongodb.client.model.Filters.geoWithin
import static com.mongodb.client.model.Filters.geoWithinBox
import static com.mongodb.client.model.Filters.geoWithinCenter
import static com.mongodb.client.model.Filters.geoWithinCenterSphere
import static com.mongodb.client.model.Filters.geoWithinPolygon
import static com.mongodb.client.model.Filters.gt
import static com.mongodb.client.model.Filters.gte
import static com.mongodb.client.model.Filters.jsonSchema
import static com.mongodb.client.model.Filters.lt
import static com.mongodb.client.model.Filters.lte
import static com.mongodb.client.model.Filters.mod
import static com.mongodb.client.model.Filters.ne
import static com.mongodb.client.model.Filters.near
import static com.mongodb.client.model.Filters.nearSphere
import static com.mongodb.client.model.Filters.nin
import static com.mongodb.client.model.Filters.nor
import static com.mongodb.client.model.Filters.not
import static com.mongodb.client.model.Filters.regex
import static com.mongodb.client.model.Filters.size
import static com.mongodb.client.model.Filters.text
import static com.mongodb.client.model.Filters.type
import static com.mongodb.client.model.Filters.where
import static org.bson.BsonDocument.parse

class FiltersSpecification extends Specification {

    def 'eq should render without $eq'() {
        expect:
        toBson(eq('x', 1)) == parse('{x : 1}')
        toBson(eq('x', null)) == parse('{x : null}')
        toBson(eq(1)) == parse('{_id : 1}')
    }

    def 'should render $ne'() {
        expect:
        toBson(ne('x', 1)) == parse('{x : {$ne : 1} }')
        toBson(ne('x', null)) == parse('{x : {$ne : null} }')
    }

    def 'should render $not'() {
        expect:
        toBson(not(eq('x', 1))) == parse('{x : {$not: {$eq: 1}}}')
        toBson(not(gt('x', 1))) == parse('{x : {$not: {$gt: 1}}}')
        toBson(not(regex('x', '^p.*'))) == parse('{x : {$not: /^p.*/}}')

        toBson(not(and(gt('x', 1), eq('y', 20)))) == parse('{$not: {$and: [{x: {$gt: 1}}, {y: 20}]}}')
        toBson(not(and(eq('x', 1), eq('x', 2)))) == parse('{$not: {$and: [{x: 1}, {x: 2}]}}')
        toBson(not(and(Filters.in('x', 1, 2), eq('x', 3)))) == parse('{$not: {$and: [{x: {$in: [1, 2]}}, {x: 3}]}}')

        toBson(not(or(gt('x', 1), eq('y', 20)))) == parse('{$not: {$or: [{x: {$gt: 1}}, {y: 20}]}}')
        toBson(not(or(eq('x', 1), eq('x', 2)))) == parse('{$not: {$or: [{x: 1}, {x: 2}]}}')
        toBson(not(or(Filters.in('x', 1, 2), eq('x', 3)))) == parse('{$not: {$or: [{x: {$in: [1, 2]}}, {x: 3}]}}')

        toBson(not(parse('{$in: [1]}'))) == parse('{$not: {$in: [1]}}')

        toBson(not(eq('x', parse('{a: 1, b: 1}')))) == parse('{x: {$not: {$eq: {"a": 1, "b": 1}}}}')
        toBson(not(eq('x', parse('{$ref: "1", $id: "1"}')))) == parse('{x: {$not: {$eq: {"$ref": "1", "$id": "1"}}}}')
        toBson(not(eq('x', parse('{$ref: "1", $id: "1", $db: "db"}')))) == parse('{x: {$not: {$eq: {"$ref": "1", "$id": "1", $db: "db"}}}}')
    }

    def 'should render $nor'() {
        expect:
        toBson(nor(eq('price', 1))) == parse('{$nor : [{price: 1}]}')
        toBson(nor(eq('price', 1), eq('sale', true))) == parse('{$nor : [{price: 1}, {sale: true}]}')
    }

    def 'should render $gt'() {
        expect:
        toBson(gt('x', 1)) == parse('{x : {$gt : 1} }')
    }

    def 'should render $lt'() {
        expect:
        toBson(lt('x', 1)) == parse('{x : {$lt : 1} }')
    }

    def 'should render $gte'() {
        expect:
        toBson(gte('x', 1)) == parse('{x : {$gte : 1} }')
    }

    def 'should render $lte'() {
        expect:
        toBson(lte('x', 1)) == parse('{x : {$lte : 1} }')
    }

    def 'should render $exists'() {
        expect:
        toBson(exists('x')) == parse('{x : {$exists : true} }')
        toBson(exists('x', false)) == parse('{x : {$exists : false} }')
    }

    def 'or should render empty or using $or'() {
        expect:
        toBson(or([])) == parse('{$or : []}')
        toBson(or()) == parse('{$or : []}')
    }

    def 'should render $or'() {
        expect:
        toBson(or([eq('x', 1), eq('y', 2)])) == parse('{$or : [{x : 1}, {y : 2}]}')
        toBson(or(eq('x', 1), eq('y', 2))) == parse('{$or : [{x : 1}, {y : 2}]}')
    }

    def 'and should render empty and using $and'() {
        expect:
        toBson(and([])) == parse('{$and : []}')
        toBson(and()) == parse('{$and : []}')
    }

    def 'and should render using $and'() {
        expect:
        toBson(and([eq('x', 1), eq('y', 2)])) == parse('{$and: [{x : 1}, {y : 2}]}')
        toBson(and(eq('x', 1), eq('y', 2))) == parse('{$and: [{x : 1}, {y : 2}]}')
    }

    def 'and should render $and with clashing keys'() {
        expect:
        toBson(and([eq('a', 1), eq('a', 2)])) == parse('{$and: [{a: 1}, {a: 2}]}')
    }

    def 'and should not flatten nested'() {
        expect:
        toBson(and([and([eq('a', 1), eq('b', 2)]), eq('c', 3)])) ==
                parse('{$and: [{$and: [{a : 1}, {b : 2}]}, {c : 3}]}')
        toBson(and([and([eq('a', 1), eq('a', 2)]), eq('c', 3)])) ==
                parse('{$and:[{$and: [{a : 1}, {a : 2}]}, {c : 3}]} }')
        toBson(and([lt('a', 1), lt('b', 2)])) ==
                parse('{$and: [{a : {$lt : 1}}, {b : {$lt : 2}}]}')
        toBson(and([lt('a', 1), lt('a', 2)])) ==
                parse('{$and : [{a : {$lt : 1}}, {a : {$lt : 2}}]}')
    }

    def '$and should be explicit when using $not'() {
        expect:
        toBson(and(lt('item', 10), not(lt('item', 5)))) ==
                parse('''{
                  $and:
                      [
                          { item: { $lt: 10 } },
                          { item: { $not: { $lt: 5 } } }
                      ]
                  }
                ''')

        toBson(and(lt('item', 100), gt('item', 10), not(gt('item', 50)))) ==
                parse('''{
                  $and:
                      [
                          { item: { $lt: 100 } },
                          { item: { $gt: 10 } },
                          { item: { $not: { $gt: 50 } } }
                      ]
                  }
                ''')

        toBson(and(not(lt('item', 10)), lt('item', 100), not(gt('item', 50)))) ==
                parse('''{
                  $and:
                      [
                          { item: { $not: { $lt: 10 } } },
                          { item: { $lt: 100 } },
                          { item: { $not: { $gt: 50 } } }
                      ]
                  }
                ''')
    }

    def 'should render $all'() {
        expect:
        toBson(all('a', [1, 2, 3])) == parse('{a : {$all : [1, 2, 3]} }')
        toBson(all('a', 1, 2, 3)) == parse('{a : {$all : [1, 2, 3]} }')
    }

    def 'should render $elemMatch'() {
        expect:
        toBson(elemMatch('results', new BsonDocument('$gte', new BsonInt32(80)).append('$lt', new BsonInt32(85)))) ==
                parse('{results : {$elemMatch : {$gte: 80, $lt: 85}}}')

        toBson(elemMatch('results', and(eq('product', 'xyz'), gt('score', 8)))) ==
                parse('{ results : {$elemMatch : {$and: [{product : "xyz"}, {score : {$gt : 8}}]}}}')
    }

    def 'should render $in'() {
        expect:
        toBson(Filters.in('a', [1, 2, 3])) == parse('{a : {$in : [1, 2, 3]} }')
        toBson(Filters.in('a', 1, 2, 3)) == parse('{a : {$in : [1, 2, 3]} }')
    }

    def 'should render $nin'() {
        expect:
        toBson(nin('a', [1, 2, 3])) == parse('{a : {$nin : [1, 2, 3]} }')
        toBson(nin('a', 1, 2, 3)) == parse('{a : {$nin : [1, 2, 3]} }')
    }

    def 'should render $mod'() {
        expect:
        toBson(mod('a', 100, 7)) == new BsonDocument('a', new BsonDocument('$mod', new BsonArray([new BsonInt64(100), new BsonInt64(7)])))
    }

    def 'should render $size'() {
        expect:
        toBson(size('a', 13)) == parse('{a : {$size : 13} }')
    }

    def 'should render $bitsAllClear'() {
        expect:
        toBson(bitsAllClear('a', 13)) == parse('{a : {$bitsAllClear : { "$numberLong" : "13" }} }')
    }

    def 'should render $bitsAllSet'() {
        expect:
        toBson(bitsAllSet('a', 13)) == parse('{a : {$bitsAllSet : { "$numberLong" : "13" }} }')
    }

    def 'should render $bitsAnyClear'() {
        expect:
        toBson(bitsAnyClear('a', 13)) == parse('{a : {$bitsAnyClear : { "$numberLong" : "13" }} }')
    }

    def 'should render $bitsAnySet'() {
        expect:
        toBson(bitsAnySet('a', 13)) == parse('{a : {$bitsAnySet : { "$numberLong" : "13" }} }')
    }

    def 'should render $type'() {
        expect:
        toBson(type('a', BsonType.ARRAY)) == parse('{a : {$type : 4} }')
        toBson(type('a', 'number')) == parse('{a : {$type : "number"} }')
    }

    def 'should render $text'() {
        expect:
        toBson(text('mongoDB for GIANT ideas')) == parse('{$text: {$search: "mongoDB for GIANT ideas"} }')
        toBson(text('mongoDB for GIANT ideas', new TextSearchOptions().language('english'))) == parse('''
            {$text : {$search : "mongoDB for GIANT ideas", $language : "english"} }'''
        )
        toBson(text('mongoDB for GIANT ideas', new TextSearchOptions().caseSensitive(true))) == parse('''
            {$text : {$search : "mongoDB for GIANT ideas", $caseSensitive : true} }'''
        )
        toBson(text('mongoDB for GIANT ideas', new TextSearchOptions().diacriticSensitive(false))) == parse('''
            {$text : {$search : "mongoDB for GIANT ideas", $diacriticSensitive : false} }'''
        )
        toBson(text('mongoDB for GIANT ideas', new TextSearchOptions().language('english').caseSensitive(false)
                .diacriticSensitive(true))) == parse('''
            {$text : {$search : "mongoDB for GIANT ideas", $language : "english", $caseSensitive : false, $diacriticSensitive : true} }'''
        )
    }

    def 'should render $regex'() {
        expect:
        toBson(regex('name', 'acme.*corp')) == parse('{name : {$regex : "acme.*corp", $options : ""}}')
        toBson(regex('name', 'acme.*corp', 'si')) == parse('{name : {$regex : "acme.*corp", $options : "si"}}')
        toBson(regex('name', Pattern.compile('acme.*corp'))) == parse('{name : {$regex : "acme.*corp", $options : ""}}')
    }

    def 'should render $where'() {
        expect:
        toBson(where('this.credits == this.debits')) == parse('{$where: "this.credits == this.debits"}')
    }

    def 'should render $expr'() {
        expect:
        toBson(expr(new BsonDocument('$gt', new BsonArray([new BsonString('$spent'), new BsonString('$budget')])))) ==
                parse('{$expr: { $gt: [ "$spent" , "$budget" ] } }')
    }

    def 'should render $geoWithin'() {
        given:
        def polygon = new Polygon([new Position([40.0d, 18.0d]),
                                   new Position([40.0d, 19.0d]),
                                   new Position([41.0d, 19.0d]),
                                   new Position([40.0d, 18.0d])])
        expect:
        toBson(geoWithin('loc', polygon)) == parse('''{
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
                                                      }''')

        toBson(geoWithin('loc', parse(polygon.toJson()))) == parse('''{
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
                                                                      }''')
    }

    def 'should render $geoWithin with $box'() {
        expect:
        toBson(geoWithinBox('loc', 1d, 2d, 3d, 4d)) == parse('''{
                                                                  loc: {
                                                                    $geoWithin: {
                                                                      $box:  [
                                                                               [ 1.0, 2.0 ], [ 3.0, 4.0 ]
                                                                             ]
                                                                    }
                                                                  }
                                                                }''')
    }

    def 'should render $geoWithin with $polygon'() {
        expect:
        toBson(geoWithinPolygon('loc', [[0d, 0d], [3d, 6d], [6d, 0d]])) == parse('''{
                                                                                      loc: {
                                                                                        $geoWithin: {
                                                                                          $polygon: [
                                                                                                      [ 0.0, 0.0 ], [ 3.0, 6.0 ],
                                                                                                      [ 6.0, 0.0 ]
                                                                                                    ]
                                                                                        }
                                                                                      }
                                                                                    }''')
    }

    def 'should render $geoWithin with $center'() {
        expect:
        toBson(geoWithinCenter('loc', -74d, 40.74d, 10d)) == parse('{ loc: { $geoWithin: { $center: [ [-74.0, 40.74], 10.0 ] } } }')
    }

    def 'should render $geoWithin with $centerSphere'() {
        expect:
        toBson(geoWithinCenterSphere('loc', -74d, 40.74d, 10d)) == parse('''{
                                                                               loc: {
                                                                                 $geoWithin: {
                                                                                   $centerSphere: [
                                                                                                    [-74.0, 40.74], 10.0
                                                                                                  ]
                                                                                 }
                                                                               }
                                                                            }''')
    }

    def 'should render $geoIntersects'() {
        given:
        def polygon = new Polygon([new Position([40.0d, 18.0d]),
                                   new Position([40.0d, 19.0d]),
                                   new Position([41.0d, 19.0d]),
                                   new Position([40.0d, 18.0d])])
        expect:
        toBson(geoIntersects('loc', polygon)) == parse('''{
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
                                                          }''')

        toBson(geoIntersects('loc', parse(polygon.toJson()))) == parse('''{
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
                                                                          }''')
    }

    def 'should render $near'() {
        given:
        def point = new Point(new Position(-73.9667, 40.78))
        def pointDocument = parse(point.toJson())

        expect:
        toBson(near('loc', point, 5000d, 1000d)) == parse('''{
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
                                                                     }''')

        toBson(near('loc', point, 5000d, null)) == parse('''{
                                                                      loc : {
                                                                         $near: {
                                                                            $geometry: {
                                                                               type : "Point",
                                                                               coordinates : [ -73.9667, 40.78 ]
                                                                            },
                                                                            $maxDistance: 5000.0,
                                                                         }
                                                                      }
                                                                    }''')

        toBson(near('loc', point, null, 1000d)) == parse('''{
                                                                      loc : {
                                                                         $near: {
                                                                            $geometry: {
                                                                               type : "Point",
                                                                               coordinates : [ -73.9667, 40.78 ]
                                                                            },
                                                                            $minDistance: 1000.0,
                                                                         }
                                                                      }
                                                                    }''')

        toBson(near('loc', pointDocument, 5000d, 1000d)) == parse('''{
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
                                                                             }''')

        toBson(near('loc', pointDocument, 5000d, null)) == parse('''{
                                                                              loc : {
                                                                                 $near: {
                                                                                    $geometry: {
                                                                                       type : "Point",
                                                                                       coordinates : [ -73.9667, 40.78 ]
                                                                                    },
                                                                                    $maxDistance: 5000.0,
                                                                                 }
                                                                              }
                                                                            }''')

        toBson(near('loc', pointDocument, null, 1000d)) == parse('''{
                                                                              loc : {
                                                                                 $near: {
                                                                                    $geometry: {
                                                                                       type : "Point",
                                                                                       coordinates : [ -73.9667, 40.78 ]
                                                                                    },
                                                                                    $minDistance: 1000.0,
                                                                                 }
                                                                              }
                                                                            }''')

        toBson(near('loc', -73.9667, 40.78, 5000d, 1000d)) == parse('''{
                                                                                 loc : {
                                                                                    $near: [-73.9667, 40.78],
                                                                                    $maxDistance: 5000.0,
                                                                                    $minDistance: 1000.0,
                                                                                    }
                                                                                 }
                                                                               }''')

        toBson(near('loc', -73.9667, 40.78, 5000d, null)) == parse('''{
                                                                                loc : {
                                                                                   $near: [-73.9667, 40.78],
                                                                                   $maxDistance: 5000.0,
                                                                                   }
                                                                                }
                                                                              }''')

        toBson(near('loc', -73.9667, 40.78, null, 1000d)) == parse('''{
                                                                                loc : {
                                                                                   $near: [-73.9667, 40.78],
                                                                                   $minDistance: 1000.0,
                                                                                   }
                                                                                }
                                                                              }''')
    }

    def 'should render $nearSphere'() {
        given:
        def point = new Point(new Position(-73.9667, 40.78))
        def pointDocument = parse(point.toJson())

        expect:
        toBson(nearSphere('loc', point, 5000d, 1000d)) == parse('''{
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
                                                                           }''')

        toBson(nearSphere('loc', point, 5000d, null)) == parse('''{
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
                                                                       }''')

        toBson(nearSphere('loc', point, null, 1000d)) == parse('''{
                                                                            loc : {
                                                                               $nearSphere: {
                                                                                  $geometry: {
                                                                                     type : "Point",
                                                                                     coordinates : [ -73.9667, 40.78 ]
                                                                                  },
                                                                                  $minDistance: 1000.0,
                                                                               }
                                                                            }
                                                                          }''')

        toBson(nearSphere('loc', pointDocument, 5000d, 1000d)) == parse('''{
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
                                                                                   }''')

        toBson(nearSphere('loc', pointDocument, 5000d, null)) == parse('''{
                                                                                    loc : {
                                                                                       $nearSphere: {
                                                                                          $geometry: {
                                                                                             type : "Point",
                                                                                             coordinates : [ -73.9667, 40.78 ]
                                                                                          },
                                                                                          $maxDistance: 5000.0,
                                                                                       }
                                                                                    }
                                                                                  }''')

        toBson(nearSphere('loc', pointDocument, null, 1000d)) == parse('''{
                                                                                    loc : {
                                                                                       $nearSphere: {
                                                                                          $geometry: {
                                                                                             type : "Point",
                                                                                             coordinates : [ -73.9667, 40.78 ]
                                                                                          },
                                                                                          $minDistance: 1000.0,
                                                                                       }
                                                                                    }
                                                                                  }''')

        toBson(nearSphere('loc', -73.9667, 40.78, 5000d, 1000d)) == parse('''{
                                                                                       loc : {
                                                                                          $nearSphere: [-73.9667, 40.78],
                                                                                          $maxDistance: 5000.0,
                                                                                          $minDistance: 1000.0,
                                                                                          }
                                                                                       }
                                                                                     }''')

        toBson(nearSphere('loc', -73.9667, 40.78, 5000d, null)) == parse('''{
                                                                                      loc : {
                                                                                         $nearSphere: [-73.9667, 40.78],
                                                                                         $maxDistance: 5000.0,
                                                                                         }
                                                                                      }
                                                                                    }''')

        toBson(nearSphere('loc', -73.9667, 40.78, null, 1000d)) == parse('''{
                                                                                      loc : {
                                                                                         $nearSphere: [-73.9667, 40.78],
                                                                                         $minDistance: 1000.0,
                                                                                         }
                                                                                      }
                                                                                    }''')
    }

    def 'should render $jsonSchema'() {
        expect:
        toBson(jsonSchema(new BsonDocument('bsonType', new BsonString('object')))) == parse( '''{
                                                                                       $jsonSchema : {
                                                                                          bsonType : "object"
                                                                                       }
                                                                                    }''')
    }

    def 'should render an empty document'() {
        expect:
        toBson(empty()) == parse('''{}''')
    }

    def 'should render with iterable value'() {
        expect:
        toBson(eq('x', new Document())) == parse('''{
                                                  x : {}
                                               }''')

        toBson(eq('x', [1, 2, 3])) == parse('''{
                                                  x : [1, 2, 3]
                                               }''')
    }

    def 'should create string representation for simple filter'() {
        expect:
        eq('x', 1).toString() == 'Filter{fieldName=\'x\', value=1}'
    }

    def 'should create string representation for regex filter'() {
        expect:
        regex('x', '.*').toString() == 'Operator Filter{fieldName=\'x\', operator=\'$eq\', ' +
                'value=BsonRegularExpression{pattern=\'.*\', options=\'\'}}'
    }

    def 'should create string representation for simple operator filter'() {
        expect:
        gt('x', 1).toString() == 'Operator Filter{fieldName=\'x\', operator=\'$gt\', value=1}'
    }

    def 'should create string representation for compound filters'() {
        expect:
        and(eq('x', 1), eq('y', 2)).toString() == 'And Filter{filters=[Filter{fieldName=\'x\', value=1}, Filter{fieldName=\'y\', value=2}]}'
        or(eq('x', 1), eq('y', 2)).toString() == 'Or Filter{filters=[Filter{fieldName=\'x\', value=1}, Filter{fieldName=\'y\', value=2}]}'
        nor(eq('x', 1), eq('y', 2)).toString() == 'Nor Filter{filters=[Filter{fieldName=\'x\', value=1}, Filter{fieldName=\'y\', value=2}]}'
        not(eq('x', 1)).toString() == 'Not Filter{filter=Filter{fieldName=\'x\', value=1}}'
    }

    def 'should create string representation for geo filters'() {
        expect:
        geoIntersects('x', new Point(new Position(1, 2))).toString() == 'Geometry Operator Filter{fieldName=\'x\', ' +
                'operator=\'$geoIntersects\', geometry=Point{coordinate=Position{values=[1.0, 2.0]}}, maxDistance=null, minDistance=null}'
        near('x', new Point(new Position(1, 2)), 3.0, 4.0).toString() == 'Geometry Operator Filter{fieldName=\'x\', ' +
                'operator=\'$near\', geometry=Point{coordinate=Position{values=[1.0, 2.0]}}, maxDistance=3.0, minDistance=4.0}'
    }

    def 'should create string representation for text filter'() {
        expect:
        text('java', new TextSearchOptions().language('French').caseSensitive(true).diacriticSensitive(true)).toString() ==
                'Text Filter{search=\'java\', textSearchOptions=Text Search Options{language=\'French\', caseSensitive=true, ' +
                'diacriticSensitive=true}}'
    }

    def 'should create string representation for iterable operator filters'() {
        expect:
        all('x', [1, 2, 3]).toString() == 'Operator Filter{fieldName=\'x\', operator=\'$all\', value=[1, 2, 3]}'
    }

    def 'should test equals for SimpleFilter'() {
        expect:
        regex('x', 'acme.*corp').equals(regex('x', 'acme.*corp'))
    }

    def 'should test hashCode for SimpleFilter'() {
        expect:
        regex('x', 'acme.*corp').hashCode() == regex('x', 'acme.*corp').hashCode()
    }

    def 'should test equals for OperatorFilter'() {
        expect:
        ne('x', 1).equals(ne('x', 1))
        exists('x').equals(exists('x', true))
        exists('x', false).equals(exists('x', false))
        type('a', BsonType.ARRAY).equals(type('a', BsonType.ARRAY))
        !type('a', 'number').equals(type('a', BsonType.ARRAY))
    }

    def 'should test hashCode for OperatorFilter'() {
        expect:
        ne('x', 1).hashCode() == ne('x', 1).hashCode()
        exists('x').hashCode() == exists('x', true).hashCode()
        exists('x', false).hashCode() == exists('x', false).hashCode()
        type('a', BsonType.ARRAY).hashCode() == type('a', BsonType.ARRAY).hashCode()
        type('a', 'number').hashCode() != type('a', BsonType.ARRAY).hashCode()
    }

    def 'should test equals for AndFilter'() {
        expect:
        and([]).equals(and())
        and([eq('x', 1), eq('y', 2)])
                .equals(and(eq('x', 1), eq('y', 2)))
    }

    def 'should test hashCode for AndFilter'() {
        expect:
        and([]).hashCode() == and().hashCode()
        and([eq('x', 1), eq('y', 2)]).hashCode() ==
                and(eq('x', 1), eq('y', 2)).hashCode()
    }

    def 'should test equals for OrNorFilter'() {
        expect:
        or([]).equals(or())
        nor(eq('x', 1), eq('x', 2)).equals(nor(eq('x', 1), eq('x', 2)))
        !nor(eq('x', 1), eq('x', 2)).equals(or(eq('x', 1), eq('x', 2)))
    }

    def 'should test hashCode for OrNorFilter'() {
        expect:
        or([]).hashCode() == or().hashCode()
        nor(eq('x', 1), eq('x', 2)).hashCode() == nor(eq('x', 1), eq('x', 2)).hashCode()
        nor(eq('x', 1), eq('x', 2)).hashCode() != or(eq('x', 1), eq('x', 2)).hashCode()
    }

    def 'should test equals for IterableOperatorFilter'() {
        expect:
        Filters.in('a', [1, 2, 3]).equals(Filters.in('a', 1, 2, 3))
        !nin('a', [1, 2, 3]).equals(nin('a', 1, 2))
        !all('a', [1, 2, 3]).equals(nin('a', 1, 2, 3))
        all('a', []).equals(all('a'))
    }

    def 'should test hashCode for IterableOperatorFilter'() {
        expect:
        Filters.in('a', [1, 2, 3]).hashCode() == Filters.in('a', 1, 2, 3).hashCode()
        nin('a', [1, 2, 3]).hashCode() != nin('a', 1, 2).hashCode()
        all('a', [1, 2, 3]).hashCode() != nin('a', 1, 2, 3).hashCode()
        all('a', []).hashCode() == all('a').hashCode()
    }

    def 'should test equals for SimpleEncodingFilter'() {
        expect:
        eq('x', 1).equals(eq('x', 1))
        !eq('x', 1).equals(ne('x', 1))
        !eq('x', 1).equals(eq('x', 2))
        !eq('y', 1).equals(eq('x', 1))
        !eq('x', 1).equals(parse('{x : 1}'))
        expr(new BsonDocument('$gt', new BsonArray([new BsonString('$spent'), new BsonString('$budget')])))
                .equals(expr(new BsonDocument('$gt', new BsonArray([new BsonString('$spent'), new BsonString('$budget')]))))
    }

    def 'should test hashCode for SimpleEncodingFilter'() {
        expect:
        eq('x', 1).hashCode() == eq('x', 1).hashCode()
        eq('x', 1).hashCode() != ne('x', 1).hashCode()
        eq('x', 1).hashCode() != eq('x', 2).hashCode()
        eq('y', 1).hashCode() != eq('x', 1).hashCode()
        eq('x', 1).hashCode() != parse('{x : 1}').hashCode()
        expr(new BsonDocument('$gt', new BsonArray([new BsonString('$spent'), new BsonString('$budget')]))).hashCode() ==
                expr(new BsonDocument('$gt', new BsonArray([new BsonString('$spent'), new BsonString('$budget')]))).hashCode()
    }

    def 'should test equals for NotFilter'() {
        expect:
        not(eq('x', 1)).equals(not(eq('x', 1)))
    }

    def 'should test hashCode for NotFilter'() {
        expect:
        not(eq('x', 1)).hashCode() == not(eq('x', 1)).hashCode()
    }

    def 'should test equals for GeometryOperatorFilter'() {
        def polygon = new Polygon([new Position([40.0d, 18.0d]),
                                   new Position([40.0d, 19.0d]),
                                   new Position([41.0d, 19.0d]),
                                   new Position([40.0d, 18.0d])])
        expect:
        geoWithin('loc', polygon).equals(geoWithin('loc', polygon))
        !geoWithinBox('loc', 1d, 2d, 3d, 4d)
                .equals(geoWithinBox('loc', 1d, 2d, 3d, 5d))

        geoWithinPolygon('loc', [[0d, 0d], [3d, 6d], [6d, 0d]])
                .equals(geoWithinPolygon('loc', [[0d, 0d], [3d, 6d], [6d, 0d]]))
    }

    def 'should test hashCode for GeometryOperatorFilter'() {
        def polygon = new Polygon([new Position([40.0d, 18.0d]),
                                   new Position([40.0d, 19.0d]),
                                   new Position([41.0d, 19.0d]),
                                   new Position([40.0d, 18.0d])])
        expect:
        geoWithin('loc', polygon).hashCode() == geoWithin('loc', polygon).hashCode()
        geoWithinBox('loc', 1d, 2d, 3d, 4d).hashCode() !=
                geoWithinBox('loc', 1d, 2d, 3d, 5d).hashCode()

        geoWithinPolygon('loc', [[0d, 0d], [3d, 6d], [6d, 0d]]).hashCode() ==
                geoWithinPolygon('loc', [[0d, 0d], [3d, 6d], [6d, 0d]]).hashCode()
    }

    def 'should test equals for TextFilter'() {
        expect:
        text('mongoDB for GIANT ideas').equals(text('mongoDB for GIANT ideas'))
        text('mongoDB for GIANT ideas', new TextSearchOptions().language('english'))
                .equals(text('mongoDB for GIANT ideas', new TextSearchOptions().language('english')))
    }

    def 'should test hashCode for TextFilter'() {
        expect:
        text('mongoDB for GIANT ideas').hashCode() == text('mongoDB for GIANT ideas').hashCode()
        text('mongoDB for GIANT ideas', new TextSearchOptions().language('english')).hashCode() ==
                text('mongoDB for GIANT ideas', new TextSearchOptions().language('english')).hashCode()
    }
}
