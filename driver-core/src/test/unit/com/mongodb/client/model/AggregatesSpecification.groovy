/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.model

import com.mongodb.client.model.geojson.codecs.GeoJsonCodecProvider
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.Document
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.DocumentCodecProvider
import org.bson.codecs.ValueCodecProvider
import org.bson.conversions.Bson
import spock.lang.IgnoreIf
import spock.lang.Specification

import static BucketGranularity.R5
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.client.model.Accumulators.addToSet
import static com.mongodb.client.model.Accumulators.avg
import static com.mongodb.client.model.Accumulators.first
import static com.mongodb.client.model.Accumulators.last
import static com.mongodb.client.model.Accumulators.max
import static com.mongodb.client.model.Accumulators.min
import static com.mongodb.client.model.Accumulators.push
import static com.mongodb.client.model.Accumulators.stdDevPop
import static com.mongodb.client.model.Accumulators.stdDevSamp
import static com.mongodb.client.model.Accumulators.sum
import static com.mongodb.client.model.Aggregates.addFields
import static com.mongodb.client.model.Aggregates.bucket
import static com.mongodb.client.model.Aggregates.bucketAuto
import static com.mongodb.client.model.Aggregates.count
import static com.mongodb.client.model.Aggregates.facet
import static com.mongodb.client.model.Aggregates.graphLookup
import static com.mongodb.client.model.Aggregates.group
import static com.mongodb.client.model.Aggregates.limit
import static com.mongodb.client.model.Aggregates.lookup
import static com.mongodb.client.model.Aggregates.match
import static com.mongodb.client.model.Aggregates.out
import static com.mongodb.client.model.Aggregates.project
import static com.mongodb.client.model.Aggregates.replaceRoot
import static com.mongodb.client.model.Aggregates.sample
import static com.mongodb.client.model.Aggregates.skip
import static com.mongodb.client.model.Aggregates.sort
import static com.mongodb.client.model.Aggregates.sortByCount
import static com.mongodb.client.model.Aggregates.unwind
import static com.mongodb.client.model.Filters.eq
import static com.mongodb.client.model.Projections.computed
import static com.mongodb.client.model.Projections.fields
import static com.mongodb.client.model.Projections.include
import static com.mongodb.client.model.Sorts.ascending
import static com.mongodb.client.model.Sorts.descending
import static java.util.Arrays.asList
import static org.bson.BsonDocument.parse
import static org.bson.codecs.configuration.CodecRegistries.fromProviders

class AggregatesSpecification extends Specification {
    def registry = fromProviders([new BsonValueCodecProvider(), new DocumentCodecProvider(), new ValueCodecProvider(),
                                  new GeoJsonCodecProvider()])

    @IgnoreIf({ !serverVersionAtLeast(3, 4) })
    def 'should render $addFields'() {
        expect:
        toBson(addFields(new Field('newField', null))) == parse('{$addFields: {newField: null}}')
        toBson(addFields(new Field('newField', 'hello'))) == parse('{$addFields: {newField: "hello"}}')
        toBson(addFields(new Field('this', '$$CURRENT'))) == parse('{$addFields: {this: "$$CURRENT"}}')
        toBson(addFields(new Field('myNewField', new Document('c', 3)
                .append('d', 4)))) == parse('{$addFields: {myNewField: {c: 3, d: 4}}}')
        toBson(addFields(new Field('alt3', new Document('$lt', asList('$a', 3))))) == parse(
                '{$addFields: {alt3: {$lt: ["$a", 3]}}}')
        toBson(addFields(new Field('b', 3), new Field('c', 5))) == parse('{$addFields: {b: 3, c: 5}}')
        toBson(addFields(asList(new Field('b', 3), new Field('c', 5)))) == parse('{$addFields: {b: 3, c: 5}}')
    }

    def 'should render $bucket'() {
        expect:
        toBson(bucket('$screenSize', [0, 24, 32, 50, 100000])) == parse('''{
            $bucket: {
              groupBy: "$screenSize",
              boundaries: [0, 24, 32, 50, 100000]
            }
          }''')
        toBson(bucket('$screenSize', [0, 24, 32, 50, 100000],
                      new BucketOptions()
                              .defaultBucket('other'))) == parse('''{
            $bucket: {
              groupBy: "$screenSize",
              boundaries: [0, 24, 32, 50, 100000],
              default: "other"
            }
          }''')
        toBson(bucket('$screenSize', [0, 24, 32, 50, 100000],
                      new BucketOptions()
                              .defaultBucket('other')
                              .output(sum('count', 1), push('matches', '$screenSize')))) == parse('''{
            $bucket: {
                groupBy: "$screenSize",
                boundaries: [0, 24, 32, 50, 100000],
                default: "other",
                output: {
                    count: {$sum: 1},
                    matches: {$push: "$screenSize"}
                }
            }
        }''')
    }

    def 'should render $bucketAuto'() {
        expect:
        toBson(bucketAuto('$price', 4)) == parse('''{
            $bucketAuto: {
              groupBy: "$price",
              buckets: 4
            }
          }''')
        toBson(bucketAuto('$price', 4, new BucketAutoOptions()
                .output(sum('count', 1),
                        avg('avgPrice', '$price')))) == parse('''{
                                              $bucketAuto: {
                                                groupBy: "$price",
                                                buckets: 4,
                                                output: {
                                                  count: {$sum: 1},
                                                  avgPrice: {$avg: "$price"},
                                                }
                                              }
                                            }''')
        toBson(bucketAuto('$price', 4, new BucketAutoOptions()
                .granularity(R5)
                .output(sum('count', 1),
                        avg('avgPrice', '$price')))) == parse('''{
                                              $bucketAuto: {
                                                groupBy: "$price",
                                                buckets: 4,
                                                output: {
                                                  count: {$sum: 1},
                                                  avgPrice: {$avg: "$price"},
                                                },
                                                granularity: "R5"
                                              }
                                            }''')
    }

    def 'should render $count'() {
        expect:
        toBson(count()) == parse('{$count: "count"}')
        toBson(count('count')) == parse('{$count: "count"}')
        toBson(count('total')) == parse('{$count: "total"}')
    }

    def 'should render $match'() {
        expect:
        toBson(match(eq('author', 'dave'))) == parse('{ $match : { author : "dave" } }')
    }

    def 'should render $project'() {
        expect:
        toBson(project(fields(include('title', 'author'), computed('lastName', '$author.last')))) ==
        parse('{ $project : { title : 1 , author : 1, lastName : "$author.last" } }')
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) })
    def 'should render $replaceRoot'() {
        expect:
        toBson(replaceRoot('$a1')) == parse('{$replaceRoot: {newRoot: "$a1"}}')
        toBson(replaceRoot('$a1.b')) == parse('{$replaceRoot: {newRoot: "$a1.b"}}')
        toBson(replaceRoot('$a1')) == parse('{$replaceRoot: {newRoot: "$a1"}}')
    }

    def 'should render $sort'() {
        expect:
        toBson(sort(ascending('title', 'author'))) == parse('{ $sort : { title : 1 , author : 1 } }')
    }

    def 'should render $sortByCount'() {
        expect:
        toBson(sortByCount('someField')) == parse('{$sortByCount: "someField"}')
        toBson(sortByCount(new Document('$floor', '$x'))) == parse('{$sortByCount: {$floor: "$x"}}')
    }

    def 'should render $limit'() {
        expect:
        toBson(limit(5)) == parse('{ $limit : 5 }')
    }

    def 'should render $lookup'() {
        expect:
        toBson(lookup('from', 'localField', 'foreignField', 'as')) == parse('''{ $lookup : { from: "from", localField: "localField",
            foreignField: "foreignField", as: "as" } }''')
    }

    def 'should render $facet'() {
        expect:
        toBson(facet(
                new Facet('Screen Sizes',
                               unwind('$attributes'),
                               match(eq('attributes.name', 'screen size')),
                               group(null, sum('count', 1 ))),
                new Facet('Manufacturer',
                          match(eq('attributes.name', 'manufacturer')),
                          group('$attributes.value', sum('count', 1)),
                          sort(descending('count')),
                          limit(5)))) ==
        parse('''{$facet: {
          "Screen Sizes": [
             {$unwind: "$attributes"},
             {$match: {"attributes.name": "screen size"}},
             {$group: {
                 _id: null,
                 count: {$sum: 1}
             }}
           ],

           "Manufacturer": [
             {$match: {"attributes.name": "manufacturer"}},
             {$group: {_id: "$attributes.value", count: {$sum: 1}}},
             {$sort: {count: -1}}
             {$limit: 5}
           ]
        }} ''')
    }

    def 'should render $graphLookup'() {
        expect:
        //without options
        toBson(graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork')) ==
        parse('''{ $graphLookup: { from: "contacts", startWith: "$friends", connectFromField: "friends", connectToField: "name",
            as: "socialNetwork" } }''')

        //with maxDepth
        toBson(graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions().maxDepth(1))) ==
        parse('''{ $graphLookup: { from: "contacts", startWith: "$friends", connectFromField: "friends", connectToField: "name",
            as: "socialNetwork", maxDepth: 1 } }''')

        // with depthField
        toBson(graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions().depthField('master'))) ==
                parse('''{ $graphLookup: { from: "contacts", startWith: "$friends", connectFromField: "friends", connectToField: "name",
            as: "socialNetwork", depthField: "master" } }''')

        // with restrictSearchWithMatch
        toBson(graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .restrictSearchWithMatch(Filters.eq('hobbies', 'golf')))) ==
                parse('''{ $graphLookup: { from: "contacts", startWith: "$friends", connectFromField: "friends", connectToField: "name",
            as: "socialNetwork", restrictSearchWithMatch : { "hobbies" : "golf" } } }''')

        // with maxDepth and depthField
        toBson(graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .maxDepth(1).depthField('master'))) ==
        parse('''{ $graphLookup: { from: "contacts", startWith: "$friends", connectFromField: "friends", connectToField: "name",
            as: "socialNetwork", maxDepth: 1, depthField: "master" } }''')

        // with all options
        toBson(graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .maxDepth(1).depthField('master').restrictSearchWithMatch(Filters.eq('hobbies', 'golf')))) ==
                parse('''{ $graphLookup: { from: "contacts", startWith: "$friends", connectFromField: "friends", connectToField: "name",
            as: "socialNetwork", maxDepth: 1, depthField: "master", restrictSearchWithMatch : { "hobbies" : "golf" } } }''')
    }

    def 'should render $skip'() {
        expect:
        toBson(skip(5)) == parse('{ $skip : 5 }')
    }

    def 'should render $unwind'() {
        expect:
        toBson(unwind('$sizes')) == parse('{ $unwind : "$sizes" }')
        toBson(unwind('$sizes', new UnwindOptions().preserveNullAndEmptyArrays(null))) == parse('{ $unwind : { path : "$sizes" } }')
        toBson(unwind('$sizes', new UnwindOptions().preserveNullAndEmptyArrays(false))) == parse('''
            { $unwind : { path : "$sizes", preserveNullAndEmptyArrays : false } }''')
        toBson(unwind('$sizes', new UnwindOptions().preserveNullAndEmptyArrays(true))) == parse('''
            { $unwind : { path : "$sizes", preserveNullAndEmptyArrays : true } }''')
        toBson(unwind('$sizes', new UnwindOptions().includeArrayIndex(null))) == parse('{ $unwind : { path : "$sizes" } }')
        toBson(unwind('$sizes', new UnwindOptions().includeArrayIndex('$a'))) == parse('''
            { $unwind : { path : "$sizes", includeArrayIndex : "$a" } }''')
        toBson(unwind('$sizes', new UnwindOptions().preserveNullAndEmptyArrays(true).includeArrayIndex('$a'))) == parse('''
            { $unwind : { path : "$sizes", preserveNullAndEmptyArrays : true, includeArrayIndex : "$a" } }''')
    }

    def 'should render $out'() {
        expect:
        toBson(out('authors')) == parse('{ $out : "authors" }')
    }

    def 'should render $group'() {
        expect:
        toBson(group('$customerId')) == parse('{ $group : { _id : "$customerId" } }')
        toBson(group(null)) == parse('{ $group : { _id : null } }')

        toBson(group(parse('{ month: { $month: "$date" }, day: { $dayOfMonth: "$date" }, year: { $year: "$date" } }'))) ==
        parse('{ $group : { _id : { month: { $month: "$date" }, day: { $dayOfMonth: "$date" }, year: { $year: "$date" } } } }')


        def groupDocument = parse('''{
                            $group : {
                                      _id : null,
                                      sum: { $sum: { $multiply: [ "$price", "$quantity" ] } },
                                      avg: { $avg: "$quantity" },
                                      min: { $min: "$quantity" },
                                      max: { $max: "$quantity" },
                                      first: { $first: "$quantity" },
                                      last: { $last: "$quantity" },
                                      all: { $push: "$quantity" },
                                      unique: { $addToSet: "$quantity" },
                                      stdDevPop: { $stdDevPop: "$quantity" },
                                      stdDevSamp: { $stdDevSamp: "$quantity" }
                                     }
                                  }''')
        toBson(group(null,
                     sum('sum', parse('{ $multiply: [ "$price", "$quantity" ] }')),
                     avg('avg', '$quantity'),
                     min('min', '$quantity'),
                     max('max', '$quantity'),
                     first('first', '$quantity'),
                     last('last', '$quantity'),
                     push('all', '$quantity'),
                     addToSet('unique', '$quantity'),
                     stdDevPop('stdDevPop', '$quantity'),
                     stdDevSamp('stdDevSamp', '$quantity')
        )) == groupDocument
    }

    def 'should create string representation for simple stages'() {
        expect:
        match(new BsonDocument('x', new BsonInt32(1))).toString() == 'Stage{name=\'$match\', value={ "x" : 1 }}'
    }

    def 'should create string representation for group stage'() {
        expect:
        group('_id', avg('avg', '$quantity')).toString() ==
                'Stage{name=\'$group\', id=_id, ' +
                'fieldAccumulators=[' +
                'Field{name=\'avg\', value=Expression{name=\'$avg\', expression=$quantity}}]}'
        group(null, avg('avg', '$quantity')).toString() ==
                'Stage{name=\'$group\', id=null, ' +
                'fieldAccumulators=[' +
                'Field{name=\'avg\', value=Expression{name=\'$avg\', expression=$quantity}}]}'
    }

    def 'should render $sample'() {
        expect:
        toBson(sample(5)) == parse('{ $sample : { size: 5} }')
    }

    def toBson(Bson bson) {
        bson.toBsonDocument(BsonDocument, registry)
    }
}
