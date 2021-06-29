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
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.Document
import org.bson.conversions.Bson
import spock.lang.IgnoreIf
import spock.lang.Specification

import static BucketGranularity.R5
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.client.model.Accumulators.accumulator
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
import static com.mongodb.client.model.Aggregates.merge
import static com.mongodb.client.model.Aggregates.out
import static com.mongodb.client.model.Aggregates.project
import static com.mongodb.client.model.Aggregates.replaceRoot
import static com.mongodb.client.model.Aggregates.replaceWith
import static com.mongodb.client.model.Aggregates.sample
import static com.mongodb.client.model.Aggregates.set
import static com.mongodb.client.model.Aggregates.setWindowFields
import static com.mongodb.client.model.Aggregates.skip
import static com.mongodb.client.model.Aggregates.sort
import static com.mongodb.client.model.Aggregates.sortByCount
import static com.mongodb.client.model.Aggregates.unionWith
import static com.mongodb.client.model.Aggregates.unwind
import static com.mongodb.client.model.BsonHelper.toBson
import static com.mongodb.client.model.Filters.eq
import static com.mongodb.client.model.Filters.expr
import static com.mongodb.client.model.Projections.computed
import static com.mongodb.client.model.Projections.fields
import static com.mongodb.client.model.Projections.include
import static com.mongodb.client.model.Sorts.ascending
import static com.mongodb.client.model.Sorts.descending
import static MongoTimeUnit.DAY
import static com.mongodb.client.model.Windows.Bound.CURRENT
import static com.mongodb.client.model.Windows.Bound.UNBOUNDED
import static com.mongodb.client.model.Windows.documents
import static java.util.Arrays.asList
import static org.bson.BsonDocument.parse

class AggregatesSpecification extends Specification {

    @IgnoreIf({ !serverVersionAtLeast(4, 3) })
    def 'should render $accumulator'() {
        given:
        def initFunction = 'function() { return { count : 0, sum : 0 } }';
        def initFunctionWithArgs = 'function(initCount, initSun) { return { count : parseInt(initCount), sum : parseInt(initSun) } }';
        def accumulateFunction = 'function(state, numCopies) { return { count : state.count + 1, sum : state.sum + numCopies } }';
        def mergeFunction = 'function(state1, state2) { return { count : state1.count + state2.count, sum : state1.sum + state2.sum } }';
        def finalizeFunction = 'function(state) { return (state.sum / state.count) }';

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
                .restrictSearchWithMatch(eq('hobbies', 'golf')))) ==
                parse('''{ $graphLookup: { from: "contacts", startWith: "$friends", connectFromField: "friends", connectToField: "name",
            as: "socialNetwork", restrictSearchWithMatch : { "hobbies" : "golf" } } }''')

        // with maxDepth and depthField
        toBson(graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .maxDepth(1).depthField('master'))) ==
        parse('''{ $graphLookup: { from: "contacts", startWith: "$friends", connectFromField: "friends", connectToField: "name",
            as: "socialNetwork", maxDepth: 1, depthField: "master" } }''')

        // with all options
        toBson(graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .maxDepth(1).depthField('master').restrictSearchWithMatch(eq('hobbies', 'golf')))) ==
                parse('''{ $graphLookup: { from: "contacts", startWith: "$friends", connectFromField: "friends", connectToField: "name",
            as: "socialNetwork", maxDepth: 1, depthField: "master", restrictSearchWithMatch : { "hobbies" : "golf" } } }''')
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

    def 'should render $setWindowFields'() {
        given:
        Window window = documents(1, 2)
        BsonDocument setWindowFieldsBson = toBson(setWindowFields('$partitionByField', ascending('sortByField'), asList(
                WindowedComputations.sum('newField01', '$field01', Windows.range(1, CURRENT)),
                WindowedComputations.avg('newField02', '$field02', Windows.range(UNBOUNDED, 1)),
                WindowedComputations.stdDevSamp('newField03', '$field03', window),
                WindowedComputations.stdDevPop('newField04', '$field04', window),
                WindowedComputations.min('newField05', '$field05', window),
                WindowedComputations.max('newField06', '$field06', window),
                WindowedComputations.count('newField07', window),
                WindowedComputations.derivative('newField08', '$field08', window),
                WindowedComputations.timeDerivative('newField09', '$field09', window, DAY),
                WindowedComputations.integral('newField10', '$field10', window),
                WindowedComputations.timeIntegral('newField11', '$field11', window, DAY),
                WindowedComputations.timeIntegral('newField11', '$field11', window, DAY),
                WindowedComputations.covarianceSamp('newField12', '$field12_1', '$field12_2', window),
                WindowedComputations.covariancePop('newField13', '$field13_1', '$field13_2', window),
                WindowedComputations.expMovingAvg('newField14', '$field14', 3),
                WindowedComputations.expMovingAvg('newField15', '$field15', 0.5),
                WindowedComputations.push('newField16', '$field16', window),
                WindowedComputations.addToSet('newField17', '$field17', window),
                WindowedComputations.first('newField18', '$field18', window),
                WindowedComputations.last('newField19', '$field19', window),
                WindowedComputations.shift('newField20', '$field20', 'defaultConstantValue', -3),
                WindowedComputations.documentNumber('newField21'),
                WindowedComputations.rank('newField22'),
                WindowedComputations.denseRank('newField23'))
        ))

        expect:
        setWindowFieldsBson == parse('''{
                "$setWindowFields": {
                    "partitionBy": "$partitionByField",
                    "sortBy": { "sortByField" : 1 },
                    "output": {
                        "newField01": { "$sum": "$field01", "window": { "range": [{"$numberLong": "1"}, "current"] } },
                        "newField02": { "$avg": "$field02", "window": { "range": ["unbounded", {"$numberLong": "1"}] } },
                        "newField03": { "$stdDevSamp": "$field03", "window": { "documents": [1, 2] } },
                        "newField04": { "$stdDevPop": "$field04", "window": { "documents": [1, 2] } },
                        "newField05": { "$min": "$field05", "window": { "documents": [1, 2] } },
                        "newField06": { "$max": "$field06", "window": { "documents": [1, 2] } },
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
                        "newField19": { "$last": "$field19", "window": { "documents": [1, 2] } },
                        "newField20": { "$shift": { "output": "$field20", "by": -3, "default": "defaultConstantValue" } },
                        "newField21": { "$documentNumber": {} },
                        "newField22": { "$rank": {} },
                        "newField23": { "$denseRank": {} }
                    }
                }
        }''')
    }

    def 'should render $setWindowFields with no partitionBy/sortBy'() {
        given:
        BsonDocument setWindowFields = toBson(setWindowFields(null, null, asList(
                WindowedComputations.sum('newField01', '$field01', documents(1, 2)))
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

        graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions().depthField('master'))
                .equals(graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork',
                new GraphLookupOptions().depthField('master')))

        graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .restrictSearchWithMatch(eq('hobbies', 'golf')))
                .equals(graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .restrictSearchWithMatch(eq('hobbies', 'golf'))))

        graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .maxDepth(1).depthField('master'))
                .equals(graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .maxDepth(1).depthField('master')))

        graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .maxDepth(1).depthField('master').restrictSearchWithMatch(eq('hobbies', 'golf')))
                .equals(graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .maxDepth(1).depthField('master').restrictSearchWithMatch(eq('hobbies', 'golf'))))
    }

    def 'should test hashCode for GraphLookupStage'() {
        expect:
        graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork').hashCode() ==
                graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork').hashCode()

        graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions().maxDepth(1))
                .hashCode() ==
                graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork',
                new GraphLookupOptions().maxDepth(1)).hashCode()

        graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions().depthField('master'))
                .hashCode() ==
                graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork',
                new GraphLookupOptions().depthField('master')).hashCode()

        graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .restrictSearchWithMatch(eq('hobbies', 'golf'))).hashCode() ==
                graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .restrictSearchWithMatch(eq('hobbies', 'golf'))).hashCode()

        graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .maxDepth(1).depthField('master')).hashCode() ==
                graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .maxDepth(1).depthField('master')).hashCode()

        graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .maxDepth(1).depthField('master').restrictSearchWithMatch(eq('hobbies', 'golf'))).hashCode() ==
                graphLookup('contacts', '$friends', 'friends', 'name', 'socialNetwork', new GraphLookupOptions()
                .maxDepth(1).depthField('master').restrictSearchWithMatch(eq('hobbies', 'golf'))).hashCode()
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
                max('max', '$quantity'),
                first('first', '$quantity'),
                last('last', '$quantity'),
                push('all', '$quantity'),
                addToSet('unique', '$quantity'),
                stdDevPop('stdDevPop', '$quantity'),
                stdDevSamp('stdDevSamp', '$quantity')
        ).equals(group(null,
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
                max('max', '$quantity'),
                first('first', '$quantity'),
                last('last', '$quantity'),
                push('all', '$quantity'),
                addToSet('unique', '$quantity'),
                stdDevPop('stdDevPop', '$quantity'),
                stdDevSamp('stdDevSamp', '$quantity')
        ).hashCode() ==
                group(null,
                sum('sum', parse('{ $multiply: [ "$price", "$quantity" ] }')),
                avg('avg', '$quantity'),
                min('min', '$quantity'),
                max('max', '$quantity'),
                first('first', '$quantity'),
                last('last', '$quantity'),
                push('all', '$quantity'),
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
        facet(
                new Facet('Screen Sizes',
                        unwind('$attributes'),
                        match(eq('attributes.name', 'screen size')),
                        group(null, sum('count', 1 ))),
                new Facet('Manufacturer',
                        match(eq('attributes.name', 'manufacturer')),
                        group('$attributes.value', sum('count', 1)),
                        sort(descending('count')),
                        limit(5)))
                .equals(facet(
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
        facet(
                new Facet('Screen Sizes',
                        unwind('$attributes'),
                        match(eq('attributes.name', 'screen size')),
                        group(null, sum('count', 1 ))),
                new Facet('Manufacturer',
                        match(eq('attributes.name', 'manufacturer')),
                        group('$attributes.value', sum('count', 1)),
                        sort(descending('count')),
                        limit(5))).hashCode() ==
                facet(
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

    @IgnoreIf({ !serverVersionAtLeast(4, 3) })
    def 'should test equals for accumulator operator'() {
        given:
        def initFunction = 'function() { return { count : 0, sum : 0 } }';
        def initFunctionWithArgs = 'function(initCount, initSun) { return { count : parseInt(initCount), sum : parseInt(initSun) } }';
        def initArgs = ['0', '0']
        def accumulateFunction = 'function(state, numCopies) { return { count : state.count + 1, sum : state.sum + numCopies } }';
        def mergeFunction = 'function(state1, state2) { return { count : state1.count + state2.count, sum : state1.sum + state2.sum } }';
        def finalizeFunction = 'function(state) { return (state.sum / state.count) }';

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

    @IgnoreIf({ !serverVersionAtLeast(4, 3) })
    def 'should test hashCode for accumulator operator'() {
        given:
        def initFunction = 'function() { return { count : 0, sum : 0 } }';
        def initFunctionWithArgs = 'function(initCount, initSun) { return { count : parseInt(initCount), sum : parseInt(initSun) } }';
        def initArgs = ['0', '0']
        def accumulateFunction = 'function(state, numCopies) { return { count : state.count + 1, sum : state.sum + numCopies } }';
        def mergeFunction = 'function(state1, state2) { return { count : state1.count + state2.count, sum : state1.sum + state2.sum } }';
        def finalizeFunction = 'function(state) { return (state.sum / state.count) }';

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
