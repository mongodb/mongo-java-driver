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

import com.mongodb.MongoNamespace
import com.mongodb.OperationFunctionalSpecification
import org.bson.BsonString
import org.bson.Document
import org.bson.conversions.Bson
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.isEnterpriseServer
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
import static com.mongodb.client.model.Aggregates.graphLookup
import static com.mongodb.client.model.Aggregates.group
import static com.mongodb.client.model.Aggregates.limit
import static com.mongodb.client.model.Aggregates.lookup
import static com.mongodb.client.model.Aggregates.match
import static com.mongodb.client.model.Aggregates.out
import static com.mongodb.client.model.Aggregates.project
import static com.mongodb.client.model.Aggregates.sample
import static com.mongodb.client.model.Aggregates.skip
import static com.mongodb.client.model.Aggregates.sort
import static com.mongodb.client.model.Aggregates.unwind
import static com.mongodb.client.model.Filters.exists
import static com.mongodb.client.model.Projections.computed
import static com.mongodb.client.model.Projections.exclude
import static com.mongodb.client.model.Projections.excludeId
import static com.mongodb.client.model.Projections.fields
import static com.mongodb.client.model.Projections.include
import static com.mongodb.client.model.Sorts.descending
import static java.util.Arrays.asList
import static org.spockframework.util.CollectionUtil.containsAny

class AggregatesFunctionalSpecification extends OperationFunctionalSpecification {

    def a = new Document('_id', 1).append('x', 1)
                                  .append('y', 'a')
                                  .append('z', false)
                                  .append('a', [1, 2, 3])
                                  .append('a1', [new Document('c', 1).append('d', 2), new Document('c', 2).append('d', 3)])

    def b = new Document('_id', 2).append('x', 2)
                                  .append('y', 'b')
                                  .append('z', true)
                                  .append('a', [3, 4, 5, 6])
                                  .append('a1', [new Document('c', 2).append('d', 3), new Document('c', 3).append('d', 4)])

    def c = new Document('_id', 3).append('x', 3)
                                  .append('y', 'c')
                                  .append('z', true)

    def setup() {
        getCollectionHelper().insertDocuments(a, b, c)
    }


    def aggregate(List<Bson> pipeline) {
        getCollectionHelper().aggregate(pipeline)
    }

    def '$match'() {
        expect:
        aggregate([match(exists('a1'))]) == [a, b]
    }

    def '$project'() {
        expect:
        aggregate([project(fields(include('x'), computed('c', '$y')))]) == [new Document('_id', 1).append('x', 1).append('c', 'a'),
                                                                            new Document('_id', 2).append('x', 2).append('c', 'b'),
                                                                            new Document('_id', 3).append('x', 3).append('c', 'c')]
    }

    @IgnoreIf({ !serverVersionAtLeast([3, 3, 10]) })
    def '$project an exclusion'() {
        expect:
        aggregate([project(exclude('a', 'a1', 'z'))]) == [new Document('_id', 1).append('x', 1).append('y', 'a'),
                                                          new Document('_id', 2).append('x', 2).append('y', 'b'),
                                                          new Document('_id', 3).append('x', 3).append('y', 'c')]
    }

    def '$sort'() {
        expect:
        aggregate([sort(descending('x'))]) == [c, b, a]
    }

    def '$skip'() {
        expect:
        aggregate([skip(1)]) == [b, c]
    }

    def '$limit'() {
        expect:
        aggregate([limit(2)]) == [a, b]
    }

    def '$unwind'() {
        expect:
        aggregate([project(fields(include('a'), excludeId())), unwind('$a')]) == [new Document('a', 1),
                                                                                  new Document('a', 2),
                                                                                  new Document('a', 3),
                                                                                  new Document('a', 3),
                                                                                  new Document('a', 4),
                                                                                  new Document('a', 5),
                                                                                  new Document('a', 6)]
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(3, 1, 10)) })
    def '$unwind with UnwindOptions'() {
        given:
        getCollectionHelper().drop()
        getCollectionHelper().insertDocuments(new Document('a', [1]), new Document('a', null), new Document('a', []))

        when:
        def results = aggregate([project(fields(include('a'), excludeId())), unwind('$a', options)])

        then:
        results == expectedResults

        where:
        options                                                  | expectedResults
        new UnwindOptions()                                      | [Document.parse('{a: 1}')]
        new UnwindOptions().preserveNullAndEmptyArrays(true)     | [Document.parse('{a: 1}'), Document.parse('{a: null}'),
                                                                    Document.parse('{}')]
        new UnwindOptions()
                .preserveNullAndEmptyArrays(true)
                .includeArrayIndex('b')                          | [Document.parse('{a: 1, b: 0}'), Document.parse('{a: null, b: null}'),
                                                                    Document.parse('{b: null}')]
    }

    def '$group'() {
        expect:
        aggregate([group(null)]) == [new Document('_id', null)]

        aggregate([group('$z')]).containsAll([new Document('_id', true), new Document('_id', false)])

        aggregate([group(null, sum('acc', '$x'))]) == [new Document('_id', null).append('acc', 6)]

        aggregate([group(null, avg('acc', '$x'))]) == [new Document('_id', null).append('acc', 2)]

        aggregate([group(null, first('acc', '$x'))]) == [new Document('_id', null).append('acc', 1)]

        aggregate([group(null, last('acc', '$x'))]) == [new Document('_id', null).append('acc', 3)]

        aggregate([group(null, max('acc', '$x'))]) == [new Document('_id', null).append('acc', 3)]

        aggregate([group(null, min('acc', '$x'))]) == [new Document('_id', null).append('acc', 1)]

        aggregate([group('$z', push('acc', '$z'))]).containsAll([new Document('_id', true).append('acc', [true, true]),
                                                                 new Document('_id', false).append('acc', [false])])

        aggregate([group('$z', addToSet('acc', '$z'))]).containsAll([new Document('_id', true).append('acc', [true]),
                                                                     new Document('_id', false).append('acc', [false])])
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(2, 6, 0)) })
    def '$out'() {
        given:
        def outCollectionName = getCollectionName() + '.out'

        when:
        aggregate([out(outCollectionName)])

        then:
        getCollectionHelper(new MongoNamespace(getDatabaseName(), outCollectionName)).find() == [a, b, c]
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(3, 1, 8)) })
    def '$stdDev'() {
        when:
        def results = aggregate([group(null, stdDevPop('stdDevPop', '$x'), stdDevSamp('stdDevSamp', '$x'))]).first()

        then:
        results.keySet().containsAll(['_id', 'stdDevPop', 'stdDevSamp'])
        results.get('_id') == null
        results.getDouble('stdDevPop').round(10) == new Double(Math.sqrt(2 / 3)).round(10)
        results.get('stdDevSamp') == 1.0
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(3, 1, 8)) })
    def '$sample'() {
        expect:
        containsAny([a, b, c], aggregate([sample(1)]).first())
    }


    @IgnoreIf({ !serverVersionAtLeast(asList(3, 1, 8)) || !isEnterpriseServer() })
    def '$lookup'() {
        given:
        def fromCollectionName = 'lookupCollection'
        def fromHelper = getCollectionHelper(new MongoNamespace(getDatabaseName(), fromCollectionName))

        getCollectionHelper().drop()
        fromHelper.drop()

        getCollectionHelper().insertDocuments(new Document('_id', 0).append('a', 1),
                new Document('_id', 1).append('a', null), new Document('_id', 2))
        fromHelper.insertDocuments(new Document('_id', 0).append('b', 1), new Document('_id', 1).append('b', null), new Document('_id', 2))
        def lookupDoc = lookup(fromCollectionName, 'a', 'b', 'same')

        when:
        def results = aggregate([lookupDoc])

        then:
        results == [
            Document.parse('{_id: 0, a: 1, "same": [{_id: 0, b: 1}]}'),
            Document.parse('{_id: 1, a: null, "same": [{_id: 1, b: null}, {_id: 2}]}'),
            Document.parse('{_id: 2, "same": [{_id: 1, b: null}, {_id: 2}]}')
        ]

        cleanup:
        fromHelper?.drop()
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(3, 3, 9)) })
    def '$graphLookup'() {
        given:
        def fromCollectionName = 'contacts'
        def fromHelper = getCollectionHelper(new MongoNamespace(getDatabaseName(), fromCollectionName))

        fromHelper.drop()

        fromHelper.insertDocuments(Document.parse('{ _id: 0, name: "Bob Smith", friends: ["Anna Jones", "Chris Green"] }'))
        fromHelper.insertDocuments(Document.parse('{ _id: 1, name: "Anna Jones", friends: ["Bob Smith", "Chris Green", "Joe Lee"] }'))
        fromHelper.insertDocuments(Document.parse('{ _id: 2, name: "Chris Green", friends: ["Anna Jones", "Bob Smith"] }'))
        fromHelper.insertDocuments(Document.parse('{ _id: 3, name: "Joe Lee", friends: ["Anna Jones", "Fred Brown"] }'))
        fromHelper.insertDocuments(Document.parse('{ _id: 4, name: "Fred Brown", friends: ["Joe Lee"] }'))

        def lookupDoc = graphLookup('contacts', new BsonString('$friends'), 'friends', 'name', 'socialNetwork')

        when:
        def results = fromHelper.aggregate([lookupDoc])

        then:
        results[0] ==
            Document.parse('''{
                _id: 0,
                name: "Bob Smith",
                friends: ["Anna Jones", "Chris Green"],
                socialNetwork: [
                    { _id: 3, name: "Joe Lee", friends: ["Anna Jones", "Fred Brown" ] },
                    { _id: 4, name: "Fred Brown", friends: ["Joe Lee"] },
                    { _id: 0, name: "Bob Smith", friends: ["Anna Jones", "Chris Green"] },
                    { _id: 2, name: "Chris Green", friends: ["Anna Jones", "Bob Smith"] },
                    { _id: 1, name: "Anna Jones", friends: ["Bob Smith", "Chris Green", "Joe Lee"] }
                ]
            }''')

        cleanup:
        fromHelper?.drop()
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(3, 3, 9)) })
    def '$graphLookup with options'() {
        given:
        def fromCollectionName = 'contacts'
        def fromHelper = getCollectionHelper(new MongoNamespace(getDatabaseName(), fromCollectionName))

        fromHelper.drop()

        fromHelper.insertDocuments(Document.parse('{ _id: 0, name: "Bob Smith", friends: ["Anna Jones", "Chris Green"] }'))
        fromHelper.insertDocuments(Document.parse('{ _id: 1, name: "Anna Jones", friends: ["Bob Smith", "Chris Green", "Joe Lee"] }'))
        fromHelper.insertDocuments(Document.parse('{ _id: 2, name: "Chris Green", friends: ["Anna Jones", "Bob Smith"] }'))
        fromHelper.insertDocuments(Document.parse('{ _id: 3, name: "Joe Lee", friends: ["Anna Jones", "Fred Brown"] }'))
        fromHelper.insertDocuments(Document.parse('{ _id: 4, name: "Fred Brown", friends: ["Joe Lee"] }'))

        def lookupDoc = graphLookup('contacts', new BsonString('$friends'), 'friends', 'name', 'socialNetwork',
                                    new GraphLookupOptions()
                                        .maxDepth(1)
                                        .depthField('depth'))

        when:
        def results = fromHelper.aggregate([lookupDoc])

        then:
        results[0] ==
            Document.parse('''{
                _id: 0,
                name: "Bob Smith",
                friends: ["Anna Jones", "Chris Green"],
                socialNetwork: [
                    { _id: 3, name: "Joe Lee", friends: ["Anna Jones", "Fred Brown" ], depth:1 },
                    { _id: 0, name: "Bob Smith", friends: ["Anna Jones", "Chris Green"], depth:1 },
                    { _id: 2, name: "Chris Green", friends: ["Anna Jones", "Bob Smith"], depth:0 },
                    { _id: 1, name: "Anna Jones", friends: ["Bob Smith", "Chris Green", "Joe Lee"], depth:0 }
                ]
            }''')

        cleanup:
        fromHelper?.drop()
    }
}
