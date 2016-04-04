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

package com.mongodb.client.model

import com.mongodb.client.model.geojson.Point
import com.mongodb.client.model.geojson.Polygon
import com.mongodb.client.model.geojson.Position
import com.mongodb.client.model.geojson.codecs.GeoJsonCodecProvider
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonType
import org.bson.Document
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.DocumentCodecProvider
import org.bson.codecs.IterableCodecProvider
import org.bson.codecs.ValueCodecProvider
import org.bson.conversions.Bson
import spock.lang.Specification

import java.util.regex.Pattern

import static Filters.and
import static Filters.exists
import static Filters.or
import static com.mongodb.client.model.Filters.all
import static com.mongodb.client.model.Filters.bitsAllClear
import static com.mongodb.client.model.Filters.bitsAllSet
import static com.mongodb.client.model.Filters.bitsAnyClear
import static com.mongodb.client.model.Filters.bitsAnySet
import static com.mongodb.client.model.Filters.elemMatch
import static com.mongodb.client.model.Filters.eq
import static com.mongodb.client.model.Filters.geoIntersects
import static com.mongodb.client.model.Filters.geoWithin
import static com.mongodb.client.model.Filters.geoWithinBox
import static com.mongodb.client.model.Filters.geoWithinCenter
import static com.mongodb.client.model.Filters.geoWithinCenterSphere
import static com.mongodb.client.model.Filters.geoWithinPolygon
import static com.mongodb.client.model.Filters.gt
import static com.mongodb.client.model.Filters.gte
import static com.mongodb.client.model.Filters.lt
import static com.mongodb.client.model.Filters.lte
import static com.mongodb.client.model.Filters.mod
import static com.mongodb.client.model.Filters.ne
import static com.mongodb.client.model.Filters.nin
import static com.mongodb.client.model.Filters.nor
import static com.mongodb.client.model.Filters.not
import static com.mongodb.client.model.Filters.regex
import static com.mongodb.client.model.Filters.size
import static com.mongodb.client.model.Filters.text
import static com.mongodb.client.model.Filters.type
import static com.mongodb.client.model.Filters.where
import static java.util.Arrays.asList
import static org.bson.BsonDocument.parse
import static org.bson.codecs.configuration.CodecRegistries.fromProviders

class FiltersSpecification extends Specification {
    def registry = fromProviders([new BsonValueCodecProvider(), new ValueCodecProvider(), new GeoJsonCodecProvider(),
                                  new DocumentCodecProvider(),
                                  new IterableCodecProvider()])

    def 'eq should render without $eq'() {
        expect:
        toBson(eq('x', 1)) == parse('{x : 1}')
        toBson(eq('x', null)) == parse('{x : null}')
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

        toBson(not(new BsonDocument('$in', new BsonArray(asList(new BsonInt32(1)))))) == parse('{$not: {$in: [1]}}')
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

    def 'and should render and without using $and'() {
        expect:
        toBson(and([eq('x', 1), eq('y', 2)])) == parse('{x : 1, y : 2}')
        toBson(and(eq('x', 1), eq('y', 2))) == parse('{x : 1, y : 2}')
    }

    def 'and should render $and with clashing keys'() {
        expect:
        toBson(and([eq('a', 1), eq('a', 2)])) == parse('{$and: [{a: 1}, {a: 2}]}');
    }

    def 'and should flatten multiple operators for the same key'() {
        expect:
        toBson(and([gt('a', 1), lt('a', 9)])) == parse('{a : {$gt : 1, $lt : 9}}');
    }

    def 'and should flatten nested'() {
        expect:
        toBson(and([and([eq('a', 1), eq('b', 2)]), eq('c', 3)])) == parse('{a : 1, b : 2, c : 3}')
        toBson(and([and([eq('a', 1), eq('a', 2)]), eq('c', 3)])) == parse('{$and:[{a : 1}, {a : 2}, {c : 3}] }')
        toBson(and([lt('a', 1), lt('b', 2)])) == parse('{a : {$lt : 1}, b : {$lt : 2} }')
        toBson(and([lt('a', 1), lt('a', 2)])) == parse('{$and : [{a : {$lt : 1}}, {a : {$lt : 2}}]}')
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
                parse('{ results : {$elemMatch : {product : "xyz", score : {$gt : 8}}}}')
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

    @SuppressWarnings('deprecated')
    def 'should render $text'() {
        expect:
        toBson(text('mongoDB for GIANT ideas')) == parse('{$text: {$search: "mongoDB for GIANT ideas"} }')
        toBson(text('mongoDB for GIANT ideas', 'english')) == parse('{$text: {$search: "mongoDB for GIANT ideas", $language : "english"}}')
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
        toBson(regex('name', 'acme.*corp')) == parse('{name : {$regex : "acme.*corp"}}')
        toBson(regex('name', 'acme.*corp', 'si')) == parse('{name : {$regex : "acme.*corp", $options : "si"}}')
        toBson(regex('name', Pattern.compile('acme.*corp'))) == parse('{name : {$regex : "acme.*corp"}}')
    }

    def 'should render $where'() {
        expect:
        toBson(where('this.credits == this.debits')) == parse('{$where: "this.credits == this.debits"}')
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
        toBson(Filters.near('loc', point, 5000d, 1000d)) == parse('''{
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

        toBson(Filters.near('loc', point, 5000d, null)) == parse('''{
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

        toBson(Filters.near('loc', point, null, 1000d)) == parse('''{
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

        toBson(Filters.near('loc', pointDocument, 5000d, 1000d)) == parse('''{
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

        toBson(Filters.near('loc', pointDocument, 5000d, null)) == parse('''{
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

        toBson(Filters.near('loc', pointDocument, null, 1000d)) == parse('''{
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

        toBson(Filters.near('loc', -73.9667, 40.78, 5000d, 1000d)) == parse('''{
                                                                                 loc : {
                                                                                    $near: [-73.9667, 40.78],
                                                                                    $maxDistance: 5000.0,
                                                                                    $minDistance: 1000.0,
                                                                                    }
                                                                                 }
                                                                               }''')

        toBson(Filters.near('loc', -73.9667, 40.78, 5000d, null)) == parse('''{
                                                                                loc : {
                                                                                   $near: [-73.9667, 40.78],
                                                                                   $maxDistance: 5000.0,
                                                                                   }
                                                                                }
                                                                              }''')

        toBson(Filters.near('loc', -73.9667, 40.78, null, 1000d)) == parse('''{
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
        toBson(Filters.nearSphere('loc', point, 5000d, 1000d)) == parse('''{
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

        toBson(Filters.nearSphere('loc', point, 5000d, null)) == parse('''{
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

        toBson(Filters.nearSphere('loc', point, null, 1000d)) == parse('''{
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

        toBson(Filters.nearSphere('loc', pointDocument, 5000d, 1000d)) == parse('''{
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

        toBson(Filters.nearSphere('loc', pointDocument, 5000d, null)) == parse('''{
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

        toBson(Filters.nearSphere('loc', pointDocument, null, 1000d)) == parse('''{
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

        toBson(Filters.nearSphere('loc', -73.9667, 40.78, 5000d, 1000d)) == parse('''{
                                                                                       loc : {
                                                                                          $nearSphere: [-73.9667, 40.78],
                                                                                          $maxDistance: 5000.0,
                                                                                          $minDistance: 1000.0,
                                                                                          }
                                                                                       }
                                                                                     }''')

        toBson(Filters.nearSphere('loc', -73.9667, 40.78, 5000d, null)) == parse('''{
                                                                                      loc : {
                                                                                         $nearSphere: [-73.9667, 40.78],
                                                                                         $maxDistance: 5000.0,
                                                                                         }
                                                                                      }
                                                                                    }''')

        toBson(Filters.nearSphere('loc', -73.9667, 40.78, null, 1000d)) == parse('''{
                                                                                      loc : {
                                                                                         $nearSphere: [-73.9667, 40.78],
                                                                                         $minDistance: 1000.0,
                                                                                         }
                                                                                      }
                                                                                    }''')
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

    def toBson(Bson bson) {
        bson.toBsonDocument(BsonDocument, registry)
    }
}
