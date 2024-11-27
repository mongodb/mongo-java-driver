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

import com.mongodb.MongoNamespace
import com.mongodb.client.model.fill.FillOutputField
import com.mongodb.client.model.search.SearchCollector
import com.mongodb.client.model.search.SearchOperator
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.Document
import org.bson.Vector
import org.bson.conversions.Bson
import spock.lang.IgnoreIf
import spock.lang.Specification

import static BucketGranularity.R5
import static MongoTimeUnit.DAY
import static com.mongodb.ClusterFixture.serverVersionLessThan
import static com.mongodb.client.model.Accumulators.accumulator
import static com.mongodb.client.model.Accumulators.addToSet
import static com.mongodb.client.model.Accumulators.avg
import static com.mongodb.client.model.Accumulators.bottom
import static com.mongodb.client.model.Accumulators.bottomN
import static com.mongodb.client.model.Accumulators.first
import static com.mongodb.client.model.Accumulators.firstN
import static com.mongodb.client.model.Accumulators.last
import static com.mongodb.client.model.Accumulators.lastN
import static com.mongodb.client.model.Accumulators.max
import static com.mongodb.client.model.Accumulators.maxN
import static com.mongodb.client.model.Accumulators.mergeObjects
import static com.mongodb.client.model.Accumulators.min
import static com.mongodb.client.model.Accumulators.minN
import static com.mongodb.client.model.Accumulators.push
import static com.mongodb.client.model.Accumulators.stdDevPop
import static com.mongodb.client.model.Accumulators.stdDevSamp
import static com.mongodb.client.model.Accumulators.sum
import static com.mongodb.client.model.Accumulators.top
import static com.mongodb.client.model.Accumulators.topN
import static com.mongodb.client.model.Aggregates.addFields
import static com.mongodb.client.model.Aggregates.bucket
import static com.mongodb.client.model.Aggregates.bucketAuto
import static com.mongodb.client.model.Aggregates.count
import static com.mongodb.client.model.Aggregates.densify
import static com.mongodb.client.model.Aggregates.fill
import static com.mongodb.client.model.Aggregates.graphLookup
import static com.mongodb.client.model.Aggregates.group
import static com.mongodb.client.model.Aggregates.limit
import static com.mongodb.client.model.Aggregates.lookup
import static com.mongodb.client.model.Aggregates.match
import static com.mongodb.client.model.Aggregates.merge
import static com.mongodb.client.model.Aggregates.out
import static com.mongodb.client.model.Aggregates.project
import static com.mongodb.client.model.Aggregates.replaceRoot
import static com.mongodb.client.model.Aggregates.replaceWith
import static com.mongodb.client.model.Aggregates.sample
import static com.mongodb.client.model.Aggregates.search
import static com.mongodb.client.model.Aggregates.searchMeta
import static com.mongodb.client.model.Aggregates.set
import static com.mongodb.client.model.Aggregates.setWindowFields
import static com.mongodb.client.model.Aggregates.skip
import static com.mongodb.client.model.Aggregates.sort
import static com.mongodb.client.model.Aggregates.sortByCount
import static com.mongodb.client.model.Aggregates.unionWith
import static com.mongodb.client.model.Aggregates.unwind
import static com.mongodb.client.model.Aggregates.vectorSearch
import static com.mongodb.client.model.BsonHelper.toBson
import static com.mongodb.client.model.Filters.eq
import static com.mongodb.client.model.Filters.expr
import static com.mongodb.client.model.Projections.computed
import static com.mongodb.client.model.Projections.fields
import static com.mongodb.client.model.Projections.include
import static com.mongodb.client.model.Sorts.ascending
import static com.mongodb.client.model.Sorts.descending
import static com.mongodb.client.model.Windows.Bound.CURRENT
import static com.mongodb.client.model.Windows.Bound.UNBOUNDED
import static com.mongodb.client.model.Windows.documents
import static com.mongodb.client.model.densify.DensifyRange.fullRangeWithStep
import static com.mongodb.client.model.fill.FillOptions.fillOptions
import static com.mongodb.client.model.search.SearchCollector.facet
import static com.mongodb.client.model.search.SearchCount.total
import static com.mongodb.client.model.search.SearchFacet.stringFacet
import static com.mongodb.client.model.search.SearchHighlight.paths
import static com.mongodb.client.model.search.SearchOperator.exists
import static com.mongodb.client.model.search.SearchOptions.searchOptions
import static com.mongodb.client.model.search.SearchPath.fieldPath
import static com.mongodb.client.model.search.SearchPath.wildcardPath
import static com.mongodb.client.model.search.VectorSearchOptions.approximateVectorSearchOptions
import static com.mongodb.client.model.search.VectorSearchOptions.exactVectorSearchOptions
import static java.util.Arrays.asList
import static org.bson.BsonDocument.parse

class AggregatesSpecification extends Specification {

    @IgnoreIf({ serverVersionLessThan(4, 4) })
    def 'should render $accumulator'() {
        given:
        def initFunction = 'function() { return { count : 0, sum : 0 } }'
        def initFunctionWithArgs = 'function(initCount, initSun) { return { count : parseInt(initCount), sum : parseInt(initSun) } }'
        def accumulateFunction = 'function(state, numCopies) { return { count : state.count + 1, sum : state.sum + numCopies } }'
        def mergeFunction = 'function(state1, state2) { return { count : state1.count + state2.count, sum : state1.sum + state2.sum } }'
        def finalizeFunction = 'function(state) { return (state.sum / state.count) }'

        expect:
        toBson(group(null, accumulator('test', initFunction, accumulateFunction, mergeFunction))) ==
                parse('{$group: {_id: null, test: {$accumulator: {init: "' + initFunction + '", initArgs: [], accumulate: "' +
                        accumulateFunction + '", accumulateArgs: [], merge: "' + mergeFunction + '", lang: "js"}}}}')
        toBson(group(null, accumulator('test', initFunction, accumulateFunction, mergeFunction, finalizeFunction))) ==
                parse('{$group: {_id: null, test: {$accumulator: {init: "' + initFunction + '", initArgs: [], accumulate: "' +
                        accumulateFunction + '", accumulateArgs: [], merge: "' + mergeFunction + '", finalize: "' + finalizeFunction +
                        '", lang: "js"}}}}')
        toBson(group(null, accumulator('test', initFunctionWithArgs, ['0', '0'], accumulateFunction, [ '$copies' ], mergeFunction,
                finalizeFunction))) ==
                parse('{$group: {_id: null, test: {$accumulator: {init: "' + initFunctionWithArgs +
                        '", initArgs: [ "0", "0" ], accumulate: "' + accumulateFunction +
                        '", accumulateArgs: [ "$copies" ], merge: "' + mergeFunction +
                        '", finalize: "' + finalizeFunction + '", lang: "js"}}}}')
        toBson(group(null, accumulator('test', initFunction, accumulateFunction, mergeFunction, finalizeFunction, 'lang'))) ==
                parse('{$group: {_id: null, test: {$accumulator: {init: "' + initFunction + '", initArgs: [], accumulate: "' +
                        accumulateFunction + '", accumulateArgs: [], merge: "' + mergeFunction + '", finalize: "' + finalizeFunction +
                        '", lang: "lang"}}}}')
        toBson(group(null, accumulator('test', initFunctionWithArgs, ['0', '0'], accumulateFunction, [ '$copies' ], mergeFunction,
                finalizeFunction, 'js'))) ==
                parse('{$group: {_id: null, test: {$accumulator: {init: "' + initFunctionWithArgs +
                        '", initArgs: [ "0", "0" ], accumulate: "' + accumulateFunction +
                        '", accumulateArgs: [ "$copies" ], merge: "' + mergeFunction +
                        '", finalize: "' + finalizeFunction + '", lang: "js"}}}}')
    }

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

    def 'should render $set'() {
        expect:
        toBson(set(new Field('newField', null))) == parse('{$set: {newField: null}}')
        toBson(set(new Field('newField', 'hello'))) == parse('{$set: {newField: "hello"}}')
        toBson(set(new Field('this', '$$CURRENT'))) == parse('{$set: {this: "$$CURRENT"}}')
        toBson(set(new Field('myNewField', new Document('c', 3)
                .append('d', 4)))) == parse('{$set: {myNewField: {c: 3, d: 4}}}')
        toBson(set(new Field('alt3', new Document('$lt', asList('$a', 3))))) == parse(
                '{$set: {alt3: {$lt: ["$a", 3]}}}')
        toBson(set(new Field('b', 3), new Field('c', 5))) == parse('{$set: {b: 3, c: 5}}')
        toBson(set(asList(new Field('b', 3), new Field('c', 5)))) == parse('{$set: {b: 3, c: 5}}')
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

    def 'should render $replaceRoot'() {
        expect:
        toBson(replaceRoot('$a1')) == parse('{$replaceRoot: {newRoot: "$a1"}}')
        toBson(replaceRoot('$a1.b')) == parse('{$replaceRoot: {newRoot: "$a1.b"}}')
        toBson(replaceRoot('$a1')) == parse('{$replaceRoot: {newRoot: "$a1"}}')
    }

    def 'should render $replaceWith'() {
        expect:
        toBson(replaceWith('$a1')) == parse('{$replaceWith: "$a1"}')
        toBson(replaceWith('$a1.b')) == parse('{$replaceWith: "$a1.b"}')
        toBson(replaceWith('$a1')) == parse('{$replaceWith: "$a1"}')
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

        List<Bson> pipeline = asList(match(expr(new Document('$eq', asList('x', '1')))))
        toBson(lookup('from', asList(new Variable('var1', 'expression1')), pipeline, 'as')) ==
                parse('''{ $lookup : { from: "from",
                                            let: { var1: "expression1" },
                                            pipeline : [{ $match : { $expr: { $eq : [ "x" , "1" ]}}}],
                                            as: "as" }}''')

        // without variables
        toBson(lookup('from', pipeline, 'as')) ==
                parse('''{ $lookup : { from: "from",
                                            pipeline : [{ $match : { $expr: { $eq : [ "x" , "1" ]}}}],
                                            as: "as" }}''')
    }

    def 'should render $facet'() {
        expect:
        toBson(Aggregates.facet(
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
        toBson(graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions().depthField('depth'))) ==
                parse('''{ $graphLookup: { from: "contacts", startWith: "$friends", connectFromField: "friends", connectToField: "name",
            as: "socialNetwork", depthField: "depth" } }''')

        // with restrictSearchWithMatch
        toBson(graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .restrictSearchWithMatch(eq('hobbies', 'golf')))) ==
                parse('''{ $graphLookup: { from: "contacts", startWith: "$friends", connectFromField: "friends", connectToField: "name",
            as: "socialNetwork", restrictSearchWithMatch : { "hobbies" : "golf" } } }''')

        // with maxDepth and depthField
        toBson(graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .maxDepth(1).depthField('depth'))) ==
        parse('''{ $graphLookup: { from: "contacts", startWith: "$friends", connectFromField: "friends", connectToField: "name",
            as: "socialNetwork", maxDepth: 1, depthField: "depth" } }''')

        // with all options
        toBson(graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .maxDepth(1).depthField('depth').restrictSearchWithMatch(eq('hobbies', 'golf')))) ==
                parse('''{ $graphLookup: { from: "contacts", startWith: "$friends", connectFromField: "friends", connectToField: "name",
            as: "socialNetwork", maxDepth: 1, depthField: "depth", restrictSearchWithMatch : { "hobbies" : "golf" } } }''')
    }

    def 'should render $skip'() {
        expect:
        toBson(skip(5)) == parse('{ $skip : 5 }')
    }

    def 'should render $unionWith'() {
        expect:
        List<Bson> pipeline = asList(match(expr(new Document('$eq', asList('x', '1')))))
        toBson(unionWith('with', pipeline)) ==
                parse('''{ $unionWith : { coll: "with", pipeline : [{ $match : { $expr: { $eq : [ "x" , "1" ]}}}] }}''')
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
        toBson(out(Document.parse('{ s3: "s3://bucket/path/to/file…?format=json&maxFileSize=100MiB"}'))) ==
                parse('{ $out : { s3: "s3://bucket/path/to/file…?format=json&maxFileSize=100MiB"} }')
        toBson(out('authorsDB', 'books')) == parse('{ $out : { db: "authorsDB", coll: "books" } }')
    }

    def 'should render merge'() {
        expect:
        toBson(merge('authors')) == parse('{ $merge : {into: "authors" }}')
        toBson(merge(new MongoNamespace('db1', 'authors'))) ==
                parse('{ $merge : {into: {db: "db1", coll: "authors" }}}')

        toBson(merge('authors',
                new MergeOptions().uniqueIdentifier('ssn'))) ==
                parse('{ $merge : {into: "authors", on: "ssn" }}')

        toBson(merge('authors',
                new MergeOptions().uniqueIdentifier(['ssn', 'otherId']))) ==
                parse('{ $merge : {into: "authors", on: ["ssn", "otherId"] }}')

        toBson(merge('authors',
                new MergeOptions().whenMatched(MergeOptions.WhenMatched.REPLACE))) ==
                parse('{ $merge : {into: "authors", whenMatched: "replace" }}')
        toBson(merge('authors',
                new MergeOptions().whenMatched(MergeOptions.WhenMatched.KEEP_EXISTING))) ==
                parse('{ $merge : {into: "authors", whenMatched: "keepExisting" }}')
        toBson(merge('authors',
                new MergeOptions().whenMatched(MergeOptions.WhenMatched.MERGE))) ==
                parse('{ $merge : {into: "authors", whenMatched: "merge" }}')
        toBson(merge('authors',
                new MergeOptions().whenMatched(MergeOptions.WhenMatched.FAIL))) ==
                parse('{ $merge : {into: "authors", whenMatched: "fail" }}')

        toBson(merge('authors',
                new MergeOptions().whenNotMatched(MergeOptions.WhenNotMatched.INSERT))) ==
                parse('{ $merge : {into: "authors", whenNotMatched: "insert" }}')
        toBson(merge('authors',
                new MergeOptions().whenNotMatched(MergeOptions.WhenNotMatched.DISCARD))) ==
                parse('{ $merge : {into: "authors", whenNotMatched: "discard" }}')
        toBson(merge('authors',
                new MergeOptions().whenNotMatched(MergeOptions.WhenNotMatched.FAIL))) ==
                parse('{ $merge : {into: "authors", whenNotMatched: "fail" }}')

        toBson(merge('authors',
                new MergeOptions().whenMatched(MergeOptions.WhenMatched.PIPELINE)
                .variables([new Variable<Integer>('y', 2), new Variable<Integer>('z', 3)])
                .whenMatchedPipeline([addFields([new Field('x', 1)])]))) ==
                parse('{ $merge : {into: "authors", let: {y: 2, z: 3}, whenMatched: [{$addFields: {x: 1}}]}}')
    }

    def 'should render $group'() {
        expect:
        toBson(group('$customerId')) == parse('{ $group : { _id : "$customerId" } }')
        toBson(group(null)) == parse('{ $group : { _id : null } }')

        toBson(group(parse('{ month: { $month: "$date" }, day: { $dayOfMonth: "$date" }, year: { $year: "$date" } }'))) ==
        parse('{ $group : { _id : { month: { $month: "$date" }, day: { $dayOfMonth: "$date" }, year: { $year: "$date" } } } }')


        def groupDocument = parse('''{
                            $group : {
                                      _id : { gid: "$groupByField"},
                                      sum: { $sum: { $multiply: [ "$price", "$quantity" ] } },
                                      avg: { $avg: "$quantity" },
                                      min: { $min: "$quantity" },
                                      minN: { $minN: { input: "$quantity",
                                        n: { $cond: { if: { $eq: ["$gid", true] }, then: 2, else: 1 } } } },
                                      max: { $max: "$quantity" },
                                      maxN: { $maxN: { input: "$quantity", n: 2 } },
                                      first: { $first: "$quantity" },
                                      firstN: { $firstN: { input: "$quantity", n: 2 } },
                                      top: { $top: { sortBy: { quantity: 1 }, output: "$quantity" } },
                                      topN: { $topN: { sortBy: { quantity: 1 }, output: "$quantity", n: 2 } },
                                      last: { $last: "$quantity" },
                                      lastN: { $lastN: { input: "$quantity", n: 2 } },
                                      bottom: { $bottom: { sortBy: { quantity: 1 }, output: ["$quantity", "$quality"] } },
                                      bottomN: { $bottomN: { sortBy: { quantity: 1 }, output: ["$quantity", "$quality"],
                                        n: { $cond: { if: { $eq: ["$gid", true] }, then: 2, else: 1 } } } },
                                      all: { $push: "$quantity" },
                                      merged: { $mergeObjects: "$quantity" },
                                      unique: { $addToSet: "$quantity" },
                                      stdDevPop: { $stdDevPop: "$quantity" },
                                      stdDevSamp: { $stdDevSamp: "$quantity" }
                                     }
                                  }''')
        toBson(group(new Document('gid', '$groupByField'),
                     sum('sum', parse('{ $multiply: [ "$price", "$quantity" ] }')),
                     avg('avg', '$quantity'),
                     min('min', '$quantity'),
                     minN('minN', '$quantity',
                             new Document('$cond', new Document('if', new Document('$eq', asList('$gid', true)))
                                     .append('then', 2).append('else', 1))),
                     max('max', '$quantity'),
                     maxN('maxN', '$quantity', 2),
                     first('first', '$quantity'),
                     firstN('firstN', '$quantity', 2),
                     top('top', ascending('quantity'), '$quantity'),
                     topN('topN', ascending('quantity'), '$quantity', 2),
                     last('last', '$quantity'),
                     lastN('lastN', '$quantity', 2),
                     bottom('bottom', ascending('quantity'), ['$quantity', '$quality']),
                     bottomN('bottomN', ascending('quantity'), ['$quantity', '$quality'],
                             new Document('$cond', new Document('if', new Document('$eq', asList('$gid', true)))
                                     .append('then', 2).append('else', 1))),
                     push('all', '$quantity'),
                     mergeObjects('merged', '$quantity'),
                     addToSet('unique', '$quantity'),
                     stdDevPop('stdDevPop', '$quantity'),
                     stdDevSamp('stdDevSamp', '$quantity')
        )) == groupDocument
    }

    def 'should render $setWindowFields'() {
        given:
        Window window = documents(1, 2)
        BsonDocument setWindowFieldsBson = toBson(setWindowFields(
                new Document('gid', '$partitionByField'), ascending('sortByField'), asList(
                WindowOutputFields.of(new BsonField('newField00', new Document('$sum', '$field00')
                        .append('window', Windows.of(new Document('range', asList(1, 'current')))))),
                WindowOutputFields.sum('newField01', '$field01', Windows.range(1, CURRENT)),
                WindowOutputFields.avg('newField02', '$field02', Windows.range(UNBOUNDED, 1)),
                WindowOutputFields.stdDevSamp('newField03', '$field03', window),
                WindowOutputFields.stdDevPop('newField04', '$field04', window),
                WindowOutputFields.min('newField05', '$field05', window),
                WindowOutputFields.minN('newField05N', '$field05N',
                        new Document('$cond', new Document('if', new Document('$eq', asList('$gid', true)))
                                .append('then', 2).append('else', 1)),
                        window),
                WindowOutputFields.max('newField06', '$field06', window),
                WindowOutputFields.maxN('newField06N', '$field06N', 2, window),
                WindowOutputFields.count('newField07', window),
                WindowOutputFields.derivative('newField08', '$field08', window),
                WindowOutputFields.timeDerivative('newField09', '$field09', window, DAY),
                WindowOutputFields.integral('newField10', '$field10', window),
                WindowOutputFields.timeIntegral('newField11', '$field11', window, DAY),
                WindowOutputFields.timeIntegral('newField11', '$field11', window, DAY),
                WindowOutputFields.covarianceSamp('newField12', '$field12_1', '$field12_2', window),
                WindowOutputFields.covariancePop('newField13', '$field13_1', '$field13_2', window),
                WindowOutputFields.expMovingAvg('newField14', '$field14', 3),
                WindowOutputFields.expMovingAvg('newField15', '$field15', 0.5),
                WindowOutputFields.push('newField16', '$field16', window),
                WindowOutputFields.addToSet('newField17', '$field17', window),
                WindowOutputFields.first('newField18', '$field18', window),
                WindowOutputFields.firstN('newField18N', '$field18N', 2, window),
                WindowOutputFields.last('newField19', '$field19', window),
                WindowOutputFields.lastN('newField19N', '$field19N', 2, window),
                WindowOutputFields.shift('newField20', '$field20', 'defaultConstantValue', -3),
                WindowOutputFields.documentNumber('newField21'),
                WindowOutputFields.rank('newField22'),
                WindowOutputFields.denseRank('newField23'),
                WindowOutputFields.bottom('newField24', descending('sortByField'), '$field24', window),
                WindowOutputFields.bottomN('newField24N', descending('sortByField'), '$field24N',
                        new Document('$cond', new Document('if', new Document('$eq', asList('$gid', true)))
                                .append('then', 2).append('else', 1)),
                        window),
                WindowOutputFields.top('newField25', ascending('sortByField'), '$field25', window),
                WindowOutputFields.topN('newField25N', ascending('sortByField'), '$field25N', 2, window),
                WindowOutputFields.locf('newField26', '$field26'),
                WindowOutputFields.linearFill('newField27', '$field27')
        )))

        expect:
        setWindowFieldsBson == parse('''{
                "$setWindowFields": {
                    "partitionBy": { "gid": "$partitionByField" },
                    "sortBy": { "sortByField" : 1 },
                    "output": {
                        "newField00": { "$sum": "$field00", "window": { "range": [{"$numberInt": "1"}, "current"] } },
                        "newField01": { "$sum": "$field01", "window": { "range": [{"$numberLong": "1"}, "current"] } },
                        "newField02": { "$avg": "$field02", "window": { "range": ["unbounded", {"$numberLong": "1"}] } },
                        "newField03": { "$stdDevSamp": "$field03", "window": { "documents": [1, 2] } },
                        "newField04": { "$stdDevPop": "$field04", "window": { "documents": [1, 2] } },
                        "newField05": { "$min": "$field05", "window": { "documents": [1, 2] } },
                        "newField05N": {
                            "$minN": { "input": "$field05N", "n": { "$cond": { "if": { "$eq": ["$gid", true] }, "then": 2, "else": 1 } } },
                            "window": { "documents": [1, 2] } },
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
                        "newField24": {
                            "$bottom": { "sortBy": { "sortByField": -1 }, "output": "$field24"},
                            "window": { "documents": [1, 2] } },
                        "newField24N": {
                            "$bottomN": { "sortBy": { "sortByField": -1 }, "output": "$field24N",
                                "n": { "$cond": { "if": { "$eq": ["$gid", true] }, "then": 2, "else": 1 } } },
                            "window": { "documents": [1, 2] } },
                        "newField25": {
                            "$top": { "sortBy": { "sortByField": 1 }, "output": "$field25"},
                            "window": { "documents": [1, 2] } },
                        "newField25N": {
                            "$topN": { "sortBy": { "sortByField": 1 }, "output": "$field25N", "n": 2 },
                            "window": { "documents": [1, 2] } },
                        "newField26": { "$locf": "$field26" },
                        "newField27": { "$linearFill": "$field27" }
                    }
                }
        }''')
    }

    def 'should render $setWindowFields with no partitionBy/sortBy'() {
        given:
        BsonDocument setWindowFields = toBson(setWindowFields(null, null, asList(
                WindowOutputFields.sum('newField01', '$field01', documents(1, 2)))
        ))

        expect:
        setWindowFields == parse('''{
                "$setWindowFields": {
                    "output": {
                        "newField01": { "$sum": "$field01", "window": { "documents": [1, 2] } }
                    }
                }
        }''')
    }

    def 'should render $densify'() {
        when:
        BsonDocument densifyDoc = toBson(
                densify(
                        'fieldName',
                        fullRangeWithStep(1))
        )

        then:
        densifyDoc == parse('''{
                "$densify": {
                    "field": "fieldName",
                    "range": { "bounds": "full", "step": 1 }
                }
        }''')
    }

    def 'should render $fill'() {
        when:
        BsonDocument fillDoc = toBson(
                fill(fillOptions().sortBy(ascending('fieldName3')),
                        FillOutputField.linear('fieldName1'),
                        FillOutputField.locf('fieldName2'))
        )

        then:
        fillDoc == parse('''{
                "$fill": {
                    "output": {
                        "fieldName1": { "method" : "linear" }
                        "fieldName2": { "method" : "locf" }
                    }
                    "sortBy": { "fieldName3": 1 }
                }
        }''')
    }

    def 'should render $search'() {
        when:
        BsonDocument searchDoc = toBson(
                search(
                        (SearchOperator) exists(fieldPath('fieldName')),
                        searchOptions()
                )
        )

        then:
        searchDoc == parse('''{
                "$search": {
                    "exists": { "path": "fieldName" }
                }
        }''')

        when:
        searchDoc = toBson(
                search(
                        (SearchCollector) facet(
                                exists(fieldPath('fieldName')),
                                [stringFacet('stringFacetName', fieldPath('fieldName1'))]),
                        searchOptions()
                                .index('indexName')
                                .count(total())
                                .highlight(paths(
                                        fieldPath('fieldName1'),
                                        fieldPath('fieldName2').multi('analyzerName'),
                                        wildcardPath('field.name*')))
                )
        )

        then:
        searchDoc == parse('''{
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
        }''')
    }

    def 'should render $search with no options'() {
        when:
        BsonDocument searchDoc = toBson(
                search(
                        (SearchOperator) exists(fieldPath('fieldName'))
                )
        )

        then:
        searchDoc == parse('''{
                "$search": {
                    "exists": { "path": "fieldName" }
                }
        }''')

        when:
        searchDoc = toBson(
                search(
                        (SearchCollector) facet(
                                exists(fieldPath('fieldName')),
                                [stringFacet('facetName', fieldPath('fieldName')).numBuckets(3)])
                )
        )

        then:
        searchDoc == parse('''{
                "$search": {
                    "facet": {
                        "operator": { "exists": { "path": "fieldName" } },
                        "facets": {
                          "facetName": { "type": "string", "path": "fieldName", "numBuckets": 3 }
                        }
                    }
                }
        }''')
    }

    def 'should render $searchMeta'() {
        when:
        BsonDocument searchDoc = toBson(
                searchMeta(
                        (SearchOperator) exists(fieldPath('fieldName')),
                        searchOptions()
                )
        )

        then:
        searchDoc == parse('''{
                "$searchMeta": {
                    "exists": { "path": "fieldName" }
                }
        }''')

        when:
        searchDoc = toBson(
                searchMeta(
                        (SearchCollector) facet(
                                exists(fieldPath('fieldName')),
                                [stringFacet('stringFacetName', fieldPath('fieldName1'))]),
                        searchOptions()
                                .index('indexName')
                                .count(total())
                                .highlight(paths(
                                        fieldPath('fieldName1'),
                                        fieldPath('fieldName2').multi('analyzerName'),
                                        wildcardPath('field.name*')))
                )
        )

        then:
        searchDoc == parse('''{
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
        }''')
    }

    def 'should render $searchMeta with no options'() {
        when:
        BsonDocument searchDoc = toBson(
                searchMeta(
                        (SearchOperator) exists(fieldPath('fieldName'))
                )
        )

        then:
        searchDoc == parse('''{
                "$searchMeta": {
                    "exists": { "path": "fieldName" }
                }
        }''')

        when:
        searchDoc = toBson(
                searchMeta(
                        (SearchCollector) facet(
                                exists(fieldPath('fieldName')),
                                [stringFacet('facetName', fieldPath('fieldName')).numBuckets(3)])
                )
        )

        then:
        searchDoc == parse('''{
                "$searchMeta": {
                    "facet": {
                        "operator": { "exists": { "path": "fieldName" } },
                        "facets": {
                          "facetName": { "type": "string", "path": "fieldName", "numBuckets": 3 }
                        }
                    }
                }
        }''')
    }

    def 'should render approximate $vectorSearch'() {
        when:
        BsonDocument vectorSearchDoc = toBson(
                vectorSearch(
                        fieldPath('fieldName').multi('ignored'),
                        vector,
                        'indexName',
                        1,
                        approximateVectorSearchOptions(2)
                                .filter(Filters.ne("fieldName", "fieldValue"))

                )
        )

        then:
        vectorSearchDoc == parse('''{
                "$vectorSearch": {
                    "path": "fieldName",
                    "queryVector": ''' + queryVector + ''',
                    "index": "indexName",
                    "numCandidates": {"$numberLong": "2"},
                    "limit": {"$numberLong": "1"},
                    "filter": {"fieldName": {"$ne": "fieldValue"}}
                }
        }''')

        where:
        vector                                               | queryVector
        Vector.int8Vector([127, 7] as byte[])                | '{"$binary": {"base64": "AwB/Bw==", "subType": "09"}}'
        Vector.floatVector([127.0f, 7.0f] as float[])        | '{"$binary": {"base64": "JwAAAP5CAADgQA==", "subType": "09"}}'
        Vector.packedBitVector([127, 7] as byte[], (byte) 0) | '{"$binary": {"base64": "EAB/Bw==", "subType": "09"}}'
        [1.0d, 2.0d]                                         | "[1.0, 2.0]"
    }

    def 'should render exact $vectorSearch'() {
        when:
        BsonDocument vectorSearchDoc = toBson(
                vectorSearch(
                        fieldPath('fieldName').multi('ignored'),
                        vector,
                        'indexName',
                        1,
                        exactVectorSearchOptions()
                                .filter(Filters.ne("fieldName", "fieldValue"))

                )
        )

        then:
        vectorSearchDoc == parse('''{
                "$vectorSearch": {
                    "path": "fieldName",
                     "queryVector": ''' + queryVector + ''',
                    "index": "indexName",
                    "exact": true,
                    "limit": {"$numberLong": "1"},
                    "filter": {"fieldName": {"$ne": "fieldValue"}}
                }
        }''')

        where:
        vector                                        | queryVector
        Vector.int8Vector([127, 7] as byte[])         | '{"$binary": {"base64": "AwB/Bw==", "subType": "09"}}'
        Vector.floatVector([127.0f, 7.0f] as float[]) | '{"$binary": {"base64": "JwAAAP5CAADgQA==", "subType": "09"}}'
        [1.0d, 2.0d]                                  | "[1.0, 2.0]"
    }

    def 'should create string representation for simple stages'() {
        expect:
        match(new BsonDocument('x', new BsonInt32(1))).toString() == 'Stage{name=\'$match\', value={"x": 1}}'
    }

    def 'should create string representation for group stage'() {
        expect:
        group('_id', avg('avg', '$quantity')).toString() ==
                'Stage{name=\'$group\', id=_id, ' +
                'fieldAccumulators=[' +
                'BsonField{name=\'avg\', value=Expression{name=\'$avg\', expression=$quantity}}]}'
        group(null, avg('avg', '$quantity')).toString() ==
                'Stage{name=\'$group\', id=null, ' +
                'fieldAccumulators=[' +
                'BsonField{name=\'avg\', value=Expression{name=\'$avg\', expression=$quantity}}]}'
    }

    def 'should render $sample'() {
        expect:
        toBson(sample(5)) == parse('{ $sample : { size: 5} }')
    }

    def 'should test equals for SimplePipelineStage'() {
        expect:
        match(eq('author', 'dave')).equals(match(eq('author', 'dave')))
        project(fields(include('title', 'author'), computed('lastName', '$author.last')))
                .equals(project(fields(include('title', 'author'), computed('lastName', '$author.last'))))
        sort(ascending('title', 'author')).equals(sort(ascending('title', 'author')))
        !sort(ascending('title', 'author')).equals(sort(descending('title', 'author')))
    }

    def 'should test hashCode for SimplePipelineStage'() {
        expect:
        match(eq('author', 'dave')).hashCode() == match(eq('author', 'dave')).hashCode()
        project(fields(include('title', 'author'), computed('lastName', '$author.last'))).hashCode() ==
                project(fields(include('title', 'author'), computed('lastName', '$author.last'))).hashCode()
        sort(ascending('title', 'author')).hashCode() == sort(ascending('title', 'author')).hashCode()
        sort(ascending('title', 'author')).hashCode() != sort(descending('title', 'author')).hashCode()
    }

    def 'should test equals for BucketStage'() {
        expect:
        bucket('$screenSize', [0, 24, 32, 50, 100000]).equals(bucket('$screenSize', [0, 24, 32, 50, 100000]))
    }

    def 'should test hashCode for BucketStage'() {
        expect:
        bucket('$screenSize', [0, 24, 32, 50, 100000]).hashCode() == bucket('$screenSize', [0, 24, 32, 50, 100000]).hashCode()
        bucket('$screenSize', [0, 24, 32, 50, 100000]).hashCode() != bucket('$screenSize', [0, 24, 32, 50, 10000]).hashCode()
    }

    def 'should test equals for BucketAutoStage'() {
        expect:
        bucketAuto('$price', 4).equals(bucketAuto('$price', 4))
        bucketAuto('$price', 4, new BucketAutoOptions()
                .output(sum('count', 1),
                        avg('avgPrice', '$price')))
                .equals(bucketAuto('$price', 4, new BucketAutoOptions()
                .output(sum('count', 1),
                avg('avgPrice', '$price'))))
        bucketAuto('$price', 4, new BucketAutoOptions()
                .granularity(R5)
                .output(sum('count', 1),
                avg('avgPrice', '$price')))
                .equals(bucketAuto('$price', 4, new BucketAutoOptions()
                .granularity(R5)
                .output(sum('count', 1),
                avg('avgPrice', '$price'))))
    }

    def 'should test hashCode for BucketAutoStage'() {
        expect:
        bucketAuto('$price', 4).hashCode() == bucketAuto('$price', 4).hashCode()
        bucketAuto('$price', 4, new BucketAutoOptions()
                .output(sum('count', 1),
                avg('avgPrice', '$price'))).hashCode() ==
                bucketAuto('$price', 4, new BucketAutoOptions()
                .output(sum('count', 1),
                avg('avgPrice', '$price'))).hashCode()
        bucketAuto('$price', 4, new BucketAutoOptions()
                .granularity(R5)
                .output(sum('count', 1),
                avg('avgPrice', '$price'))).hashCode() ==
                bucketAuto('$price', 4, new BucketAutoOptions()
                .granularity(R5)
                .output(sum('count', 1),
                avg('avgPrice', '$price'))).hashCode()
    }

    def 'should test equals for LookupStage'() {
        expect:
        lookup('from', 'localField', 'foreignField', 'as')
                .equals(lookup('from', 'localField', 'foreignField', 'as'))

        List<Bson> pipeline = asList(match(expr(new Document('$eq', asList('x', '1')))))
        lookup('from', asList(new Variable('var1', 'expression1')), pipeline, 'as')
                .equals(lookup('from', asList(new Variable('var1', 'expression1')), pipeline, 'as'))

        lookup('from', pipeline, 'as').equals(lookup('from', pipeline, 'as'))
    }

    def 'should test hashCode for LookupStage'() {
        expect:
        lookup('from', 'localField', 'foreignField', 'as').hashCode() ==
                lookup('from', 'localField', 'foreignField', 'as').hashCode()

        List<Bson> pipeline = asList(match(expr(new Document('$eq', asList('x', '1')))))
        lookup('from', asList(new Variable('var1', 'expression1')), pipeline, 'as').hashCode() ==
                lookup('from', asList(new Variable('var1', 'expression1')), pipeline, 'as').hashCode()

        lookup('from', pipeline, 'as').hashCode() == lookup('from', pipeline, 'as').hashCode()
    }

    def 'should test equals for GraphLookupStage'() {
        expect:
        graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork')
                .equals(graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork'))

        graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions().maxDepth(1))
                .equals(graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork',
                new GraphLookupOptions().maxDepth(1)))

        graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions().depthField('depth'))
                .equals(graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork',
                new GraphLookupOptions().depthField('depth')))

        graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .restrictSearchWithMatch(eq('hobbies', 'golf')))
                .equals(graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .restrictSearchWithMatch(eq('hobbies', 'golf'))))

        graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .maxDepth(1).depthField('depth'))
                .equals(graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .maxDepth(1).depthField('depth')))

        graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .maxDepth(1).depthField('depth').restrictSearchWithMatch(eq('hobbies', 'golf')))
                .equals(graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .maxDepth(1).depthField('depth').restrictSearchWithMatch(eq('hobbies', 'golf'))))
    }

    def 'should test hashCode for GraphLookupStage'() {
        expect:
        graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork').hashCode() ==
                graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork').hashCode()

        graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions().maxDepth(1))
                .hashCode() ==
                graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork',
                new GraphLookupOptions().maxDepth(1)).hashCode()

        graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions().depthField('depth'))
                .hashCode() ==
                graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork',
                new GraphLookupOptions().depthField('depth')).hashCode()

        graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .restrictSearchWithMatch(eq('hobbies', 'golf'))).hashCode() ==
                graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .restrictSearchWithMatch(eq('hobbies', 'golf'))).hashCode()

        graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .maxDepth(1).depthField('depth')).hashCode() ==
                graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .maxDepth(1).depthField('depth')).hashCode()

        graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .maxDepth(1).depthField('depth').restrictSearchWithMatch(eq('hobbies', 'golf'))).hashCode() ==
                graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .maxDepth(1).depthField('depth').restrictSearchWithMatch(eq('hobbies', 'golf'))).hashCode()
    }

    def 'should test equals for GroupStage'() {
        expect:
        group('$customerId').equals(group('$customerId'))
        group(null).equals(group(null))

        group(parse('{ month: { $month: "$date" }, day: { $dayOfMonth: "$date" }, year: { $year: "$date" } }'))
                .equals(group(parse('{ month: { $month: "$date" }, day: { $dayOfMonth: "$date" }, year: { $year: "$date" } }')))

        group(null,
                sum('sum', parse('{ $multiply: [ "$price", "$quantity" ] }')),
                avg('avg', '$quantity'),
                min('min', '$quantity'),
                minN('minN', '$quantity',
                        new Document('$cond', new Document('if', new Document('$eq', asList('$gid', true)))
                                .append('then', 2).append('else', 1))),
                max('max', '$quantity'),
                maxN('maxN', '$quantity', 2),
                first('first', '$quantity'),
                firstN('firstN', '$quantity', 2),
                top('top', ascending('quantity'), '$quantity'),
                topN('topN', ascending('quantity'), '$quantity', 2),
                last('last', '$quantity'),
                lastN('lastN', '$quantity', 2),
                bottom('bottom', ascending('quantity'), ['$quantity', '$quality']),
                bottomN('bottomN', ascending('quantity'), ['$quantity', '$quality'],
                        new Document('$cond', new Document('if', new Document('$eq', asList('$gid', true)))
                                .append('then', 2).append('else', 1))),
                push('all', '$quantity'),
                mergeObjects('merged', '$quantity'),
                addToSet('unique', '$quantity'),
                stdDevPop('stdDevPop', '$quantity'),
                stdDevSamp('stdDevSamp', '$quantity')
        ).equals(group(null,
                sum('sum', parse('{ $multiply: [ "$price", "$quantity" ] }')),
                avg('avg', '$quantity'),
                min('min', '$quantity'),
                minN('minN', '$quantity',
                        new Document('$cond', new Document('if', new Document('$eq', asList('$gid', true)))
                                .append('then', 2).append('else', 1))),
                max('max', '$quantity'),
                maxN('maxN', '$quantity', 2),
                first('first', '$quantity'),
                firstN('firstN', '$quantity', 2),
                top('top', ascending('quantity'), '$quantity'),
                topN('topN', ascending('quantity'), '$quantity', 2),
                last('last', '$quantity'),
                lastN('lastN', '$quantity', 2),
                bottom('bottom', ascending('quantity'), ['$quantity', '$quality']),
                bottomN('bottomN', ascending('quantity'), ['$quantity', '$quality'],
                        new Document('$cond', new Document('if', new Document('$eq', asList('$gid', true)))
                                .append('then', 2).append('else', 1))),
                push('all', '$quantity'),
                mergeObjects('merged', '$quantity'),
                addToSet('unique', '$quantity'),
                stdDevPop('stdDevPop', '$quantity'),
                stdDevSamp('stdDevSamp', '$quantity')
        ))
    }

    def 'should test hashCode for GroupStage'() {
        expect:
        group('$customerId').hashCode() == group('$customerId').hashCode()
        group(null).hashCode() == group(null).hashCode()

        group(parse('{ month: { $month: "$date" }, day: { $dayOfMonth: "$date" }, year: { $year: "$date" } }')).hashCode() ==
                group(parse('{ month: { $month: "$date" }, day: { $dayOfMonth: "$date" }, year: { $year: "$date" } }')).hashCode()

        group(null,
                sum('sum', parse('{ $multiply: [ "$price", "$quantity" ] }')),
                avg('avg', '$quantity'),
                min('min', '$quantity'),
                minN('minN', '$quantity',
                        new Document('$cond', new Document('if', new Document('$eq', asList('$gid', true)))
                                .append('then', 2).append('else', 1))),
                max('max', '$quantity'),
                maxN('maxN', '$quantity', 2),
                first('first', '$quantity'),
                firstN('firstN', '$quantity', 2),
                top('top', ascending('quantity'), '$quantity'),
                topN('topN', ascending('quantity'), '$quantity', 2),
                last('last', '$quantity'),
                lastN('lastN', '$quantity', 2),
                bottom('bottom', ascending('quantity'), ['$quantity', '$quality']),
                bottomN('bottomN', ascending('quantity'), ['$quantity', '$quality'],
                        new Document('$cond', new Document('if', new Document('$eq', asList('$gid', true)))
                                .append('then', 2).append('else', 1))),
                push('all', '$quantity'),
                mergeObjects('merged', '$quantity'),
                addToSet('unique', '$quantity'),
                stdDevPop('stdDevPop', '$quantity'),
                stdDevSamp('stdDevSamp', '$quantity')
        ).hashCode() ==
                group(null,
                sum('sum', parse('{ $multiply: [ "$price", "$quantity" ] }')),
                avg('avg', '$quantity'),
                min('min', '$quantity'),
                minN('minN', '$quantity',
                        new Document('$cond', new Document('if', new Document('$eq', asList('$gid', true)))
                                .append('then', 2).append('else', 1))),
                max('max', '$quantity'),
                maxN('maxN', '$quantity', 2),
                first('first', '$quantity'),
                firstN('firstN', '$quantity', 2),
                top('top', ascending('quantity'), '$quantity'),
                topN('topN', ascending('quantity'), '$quantity', 2),
                last('last', '$quantity'),
                lastN('lastN', '$quantity', 2),
                bottom('bottom', ascending('quantity'), ['$quantity', '$quality']),
                bottomN('bottomN', ascending('quantity'), ['$quantity', '$quality'],
                        new Document('$cond', new Document('if', new Document('$eq', asList('$gid', true)))
                                .append('then', 2).append('else', 1))),
                push('all', '$quantity'),
                mergeObjects('merged', '$quantity'),
                addToSet('unique', '$quantity'),
                stdDevPop('stdDevPop', '$quantity'),
                stdDevSamp('stdDevSamp', '$quantity')).hashCode()
    }

    def 'should test equals for SortByCountStage'() {
        expect:
        sortByCount('someField').equals(sortByCount('someField'))
        sortByCount(new Document('$floor', '$x')).equals(sortByCount(new Document('$floor', '$x')))
    }

    def 'should test hashCode for SortByCountStage'() {
        expect:
        sortByCount('someField').hashCode() == sortByCount('someField').hashCode()
        sortByCount(new Document('$floor', '$x')).hashCode() == sortByCount(new Document('$floor', '$x')).hashCode()
    }

    def 'should test equals for FacetStage'() {
        expect:
        Aggregates.facet(
                new Facet('Screen Sizes',
                        unwind('$attributes'),
                        match(eq('attributes.name', 'screen size')),
                        group(null, sum('count', 1 ))),
                new Facet('Manufacturer',
                        match(eq('attributes.name', 'manufacturer')),
                        group('$attributes.value', sum('count', 1)),
                        sort(descending('count')),
                        limit(5)))
                .equals(Aggregates.facet(
                new Facet('Screen Sizes',
                        unwind('$attributes'),
                        match(eq('attributes.name', 'screen size')),
                        group(null, sum('count', 1 ))),
                new Facet('Manufacturer',
                        match(eq('attributes.name', 'manufacturer')),
                        group('$attributes.value', sum('count', 1)),
                        sort(descending('count')),
                        limit(5))))
    }

    def 'should test hashCode for FacetStage'() {
        expect:
        Aggregates.facet(
                new Facet('Screen Sizes',
                        unwind('$attributes'),
                        match(eq('attributes.name', 'screen size')),
                        group(null, sum('count', 1 ))),
                new Facet('Manufacturer',
                        match(eq('attributes.name', 'manufacturer')),
                        group('$attributes.value', sum('count', 1)),
                        sort(descending('count')),
                        limit(5))).hashCode() ==
                Aggregates.facet(
                new Facet('Screen Sizes',
                        unwind('$attributes'),
                        match(eq('attributes.name', 'screen size')),
                        group(null, sum('count', 1 ))),
                new Facet('Manufacturer',
                        match(eq('attributes.name', 'manufacturer')),
                        group('$attributes.value', sum('count', 1)),
                        sort(descending('count')),
                        limit(5))).hashCode()
    }

    def 'should test equals for AddFieldsStage'() {
        expect:
        addFields(new Field('newField', null)).equals(addFields(new Field('newField', null)))
        addFields(new Field('newField', 'hello')).equals(addFields(new Field('newField', 'hello')))
        addFields(new Field('this', '$$CURRENT')).equals(addFields(new Field('this', '$$CURRENT')))
        addFields(new Field('myNewField', new Document('c', 3).append('d', 4)))
                .equals(addFields(new Field('myNewField', new Document('c', 3).append('d', 4))))
        addFields(new Field('alt3', new Document('$lt', asList('$a', 3))))
                .equals(addFields(new Field('alt3', new Document('$lt', asList('$a', 3)))))
        addFields(new Field('b', 3), new Field('c', 5))
                .equals(addFields(new Field('b', 3), new Field('c', 5)))
        addFields(asList(new Field('b', 3), new Field('c', 5)))
                .equals(addFields(asList(new Field('b', 3), new Field('c', 5))))
    }

    def 'should test hashCode for AddFieldsStage'() {
        expect:
        addFields(new Field('newField', null)).hashCode() == addFields(new Field('newField', null)).hashCode()
        addFields(new Field('newField', 'hello')).hashCode() == addFields(new Field('newField', 'hello')).hashCode()
        addFields(new Field('this', '$$CURRENT')).hashCode() == addFields(new Field('this', '$$CURRENT')).hashCode()
        addFields(new Field('myNewField', new Document('c', 3).append('d', 4))).hashCode() ==
                addFields(new Field('myNewField', new Document('c', 3).append('d', 4))).hashCode()
        addFields(new Field('alt3', new Document('$lt', asList('$a', 3)))).hashCode() ==
                addFields(new Field('alt3', new Document('$lt', asList('$a', 3)))).hashCode()
        addFields(new Field('b', 3), new Field('c', 5)).hashCode() ==
                addFields(new Field('b', 3), new Field('c', 5)).hashCode()
        addFields(asList(new Field('b', 3), new Field('c', 5))).hashCode() ==
                addFields(asList(new Field('b', 3), new Field('c', 5))).hashCode()
    }

    @IgnoreIf({ serverVersionLessThan(4, 4) })
    def 'should test equals for accumulator operator'() {
        given:
        def initFunction = 'function() { return { count : 0, sum : 0 } }'
        def initFunctionWithArgs = 'function(initCount, initSun) { return { count : parseInt(initCount), sum : parseInt(initSun) } }'
        def initArgs = ['0', '0']
        def accumulateFunction = 'function(state, numCopies) { return { count : state.count + 1, sum : state.sum + numCopies } }'
        def mergeFunction = 'function(state1, state2) { return { count : state1.count + state2.count, sum : state1.sum + state2.sum } }'
        def finalizeFunction = 'function(state) { return (state.sum / state.count) }'

        expect:
        accumulator('test', initFunction, accumulateFunction, mergeFunction)
                .equals(accumulator('test', initFunction, null, accumulateFunction, null, mergeFunction,
                        null, 'js'))
        accumulator('test', initFunction, accumulateFunction, mergeFunction, finalizeFunction)
                .equals(accumulator('test', initFunction, null, accumulateFunction, null, mergeFunction,
                        finalizeFunction, 'js'))
        accumulator('test', initFunction, accumulateFunction, mergeFunction, finalizeFunction, 'lang')
                .equals(accumulator('test', initFunction, null, accumulateFunction, null, mergeFunction,
                        finalizeFunction, 'lang'))
        accumulator('test', initFunctionWithArgs, initArgs, accumulateFunction, [ '$copies' ], mergeFunction, finalizeFunction)
                .equals(accumulator('test', initFunctionWithArgs, initArgs, accumulateFunction, [ '$copies' ], mergeFunction,
                        finalizeFunction, 'js'))
    }

    @IgnoreIf({ serverVersionLessThan(4, 4) })
    def 'should test hashCode for accumulator operator'() {
        given:
        def initFunction = 'function() { return { count : 0, sum : 0 } }'
        def initFunctionWithArgs = 'function(initCount, initSun) { return { count : parseInt(initCount), sum : parseInt(initSun) } }'
        def initArgs = ['0', '0']
        def accumulateFunction = 'function(state, numCopies) { return { count : state.count + 1, sum : state.sum + numCopies } }'
        def mergeFunction = 'function(state1, state2) { return { count : state1.count + state2.count, sum : state1.sum + state2.sum } }'
        def finalizeFunction = 'function(state) { return (state.sum / state.count) }'

        expect:
        accumulator('test', initFunction, accumulateFunction, mergeFunction).hashCode() ==
                accumulator('test', initFunction, null, accumulateFunction, null,
                        mergeFunction, null, 'js').hashCode()
        accumulator('test', initFunction, accumulateFunction, mergeFunction, finalizeFunction).hashCode() ==
                accumulator('test', initFunction, null, accumulateFunction, null,
                        mergeFunction, finalizeFunction, 'js').hashCode()
        accumulator('test', initFunction, accumulateFunction, mergeFunction, finalizeFunction, 'lang').hashCode() ==
                accumulator('test', initFunction, null, accumulateFunction, null, mergeFunction,
                        finalizeFunction, 'lang').hashCode()
        accumulator('test', initFunctionWithArgs, initArgs, accumulateFunction, [ '$copies' ], mergeFunction,
                finalizeFunction).hashCode() == accumulator('test', initFunctionWithArgs, initArgs, accumulateFunction,
                [ '$copies' ], mergeFunction, finalizeFunction, 'js').hashCode()
    }

    def 'should test equals for ReplaceRootStage'() {
        expect:
        replaceRoot('$a1').equals(replaceRoot('$a1'))
        replaceRoot('$a1.b').equals(replaceRoot('$a1.b'))
        replaceRoot('$a1').equals(replaceRoot('$a1'))
    }

    def 'should test hashCode for ReplaceRootStage'() {
        expect:
        replaceRoot('$a1').hashCode() == replaceRoot('$a1').hashCode()
        replaceRoot('$a1.b').hashCode() == replaceRoot('$a1.b').hashCode()
        replaceRoot('$a1').hashCode() == replaceRoot('$a1').hashCode()
    }
}
