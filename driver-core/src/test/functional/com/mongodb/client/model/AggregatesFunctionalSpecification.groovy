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

import java.security.SecureRandom

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
import static com.mongodb.client.model.Filters.exists
import static com.mongodb.client.model.Projections.computed
import static com.mongodb.client.model.Projections.exclude
import static com.mongodb.client.model.Projections.excludeId
import static com.mongodb.client.model.Projections.fields
import static com.mongodb.client.model.Projections.include
import static com.mongodb.client.model.Sorts.ascending
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

    @IgnoreIf({ !serverVersionAtLeast(3, 4) })
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

    @IgnoreIf({ !serverVersionAtLeast(3, 2) })
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

    def '$out'() {
        given:
        def outCollectionName = getCollectionName() + '.out'

        when:
        aggregate([out(outCollectionName)])

        then:
        getCollectionHelper(new MongoNamespace(getDatabaseName(), outCollectionName)).find() == [a, b, c]
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 2) })
    def '$stdDev'() {
        when:
        def results = aggregate([group(null, stdDevPop('stdDevPop', '$x'), stdDevSamp('stdDevSamp', '$x'))]).first()

        then:
        results.keySet().containsAll(['_id', 'stdDevPop', 'stdDevSamp'])
        results.get('_id') == null
        results.getDouble('stdDevPop').round(10) == new Double(Math.sqrt(2 / 3)).round(10)
        results.get('stdDevSamp') == 1.0
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 2) })
    def '$sample'() {
        expect:
        containsAny([a, b, c], aggregate([sample(1)]).first())
    }


    @IgnoreIf({ !serverVersionAtLeast(3, 2) })
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

    @IgnoreIf({ !serverVersionAtLeast(3, 4) })
    def '$facet'() {
        given:
        def helper = getCollectionHelper()

        helper.drop()

        (0..50).each {
            def size = (35 + it)
            def manufacturer = ['Sony', 'Samsung', 'Vizio'][it % 3]
            helper.insertDocuments(Document.parse("""    {
                  title: "${manufacturer} ${size} inch HDTV",
                  attributes: {
                    "type": "HD",
                    "screen_size": ${size},
                    "manufacturer": "${manufacturer}",
                  }
                }"""))
        }
        def stage = facet(
                new Facet('Manufacturer',
                        sortByCount('$attributes.manufacturer'),
                        limit(5)),
                new Facet('Screen Sizes',
                          unwind('$attributes'),
                          bucketAuto('$attributes.screen_size', 5, new BucketAutoOptions()
                                  .output(sum('count', 1)))))

        when:
        def results = aggregate([stage,
                                 unwind('$Manufacturer'),
                                 sort(ascending('Manufacturer')),
                                 group('_id', push('Manufacturer', '$Manufacturer'),
                                         first('Screen Sizes', '$Screen Sizes')),
                                 project(excludeId())])

        then:
        results == [
            Document.parse(
                    '''{ 'Manufacturer': [
                        {'_id': "Samsung", 'count': 17},
                        {'_id': "Sony", 'count': 17},
                        {'_id': "Vizio", 'count': 17}
                    ], 'Screen Sizes': [
                        {'_id': {'min': 35, 'max': 45}, 'count': 10},
                        {'_id': {'min': 45, 'max': 55}, 'count': 10},
                        {'_id': {'min': 55, 'max': 65}, 'count': 10},
                        {'_id': {'min': 65, 'max': 75}, 'count': 10},
                        {'_id': {'min': 75, 'max': 85}, 'count': 11}
                    ]}
                    '''),
        ]

        cleanup:
        helper?.drop()
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) })
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
        def results = fromHelper.aggregate([lookupDoc,
                                            unwind('$socialNetwork'),
                                            sort(new Document('_id', 1).append('socialNetwork._id', 1))])

        then:
        results.subList(0, 5) == [
                Document.parse('''{ _id: 0, name: "Bob Smith", friends: ["Anna Jones", "Chris Green"], socialNetwork: {
                    _id: 0, name: "Bob Smith", friends: ["Anna Jones", "Chris Green"] } }'''),
                Document.parse('''{ _id: 0, name: "Bob Smith", friends: ["Anna Jones", "Chris Green"], socialNetwork: {
                    _id: 1, name: "Anna Jones", friends: ["Bob Smith", "Chris Green", "Joe Lee"] } }'''),
                Document.parse('''{ _id: 0, name: "Bob Smith", friends: ["Anna Jones", "Chris Green"], socialNetwork: {
                    _id: 2, name: "Chris Green", friends: ["Anna Jones", "Bob Smith"] } }'''),
                Document.parse('''{ _id: 0, name: "Bob Smith", friends: ["Anna Jones", "Chris Green"], socialNetwork: {
                    _id: 3, name: "Joe Lee", friends: ["Anna Jones", "Fred Brown" ] } }'''),
                Document.parse('''{ _id: 0, name: "Bob Smith", friends: ["Anna Jones", "Chris Green"], socialNetwork: {
                    _id: 4, name: "Fred Brown", friends: ["Joe Lee"] } }''')
        ]

        cleanup:
        fromHelper?.drop()
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) })
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
        def results = fromHelper.aggregate([lookupDoc,
                                            unwind('$socialNetwork'),
                                            sort(new Document('_id', 1)
                                                         .append('socialNetwork._id', 1))])

        then:
        results.subList(0, 4) == [
                Document.parse('''{ _id: 0, name: "Bob Smith", friends: ["Anna Jones", "Chris Green"], socialNetwork: {
                    _id: 0, name: "Bob Smith", friends: ["Anna Jones", "Chris Green"], depth:1 } }'''),
                Document.parse('''{ _id: 0, name: "Bob Smith", friends: ["Anna Jones", "Chris Green"], socialNetwork: {
                    _id: 1, name: "Anna Jones", friends: ["Bob Smith", "Chris Green", "Joe Lee"], depth:0 } }'''),
                Document.parse('''{ _id: 0, name: "Bob Smith", friends: ["Anna Jones", "Chris Green"], socialNetwork: {
                    _id: 2, name: "Chris Green", friends: ["Anna Jones", "Bob Smith"], depth:0 } }'''),
                Document.parse('''{ _id: 0, name: "Bob Smith", friends: ["Anna Jones", "Chris Green"], socialNetwork: {
                    _id: 3, name: "Joe Lee", friends: ["Anna Jones", "Fred Brown" ], depth:1 } }''')
        ]

        cleanup:
        fromHelper?.drop()
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) })
    def '$bucket'() {
        given:
        def helper = getCollectionHelper()

        helper.drop()

        helper.insertDocuments(Document.parse('{screenSize: 30}'))
        helper.insertDocuments(Document.parse('{screenSize: 24}'))
        helper.insertDocuments(Document.parse('{screenSize: 42}'))
        helper.insertDocuments(Document.parse('{screenSize: 22}'))
        helper.insertDocuments(Document.parse('{screenSize: 55}'))
        helper.insertDocuments(Document.parse('{screenSize: 155}'))
        helper.insertDocuments(Document.parse('{screenSize: 75}'))

        def bucket = bucket('$screenSize', [0, 24, 32, 50, 70], new BucketOptions()
                .defaultBucket('monster')
                .output(sum('count', 1), push('matches', '$screenSize')))

        when:
        def results = helper.aggregate([sort(new Document('screenSize', 1)), bucket])

        then:
        results == [
                Document.parse('{_id: 0, count: 1, matches: [22]}'),
                Document.parse('{_id: 24, count: 2, matches: [24, 30]}'),
                Document.parse('{_id: 32, count: 1, matches: [42]}'),
                Document.parse('{_id: 50, count: 1, matches: [55]}'),
                Document.parse('{_id: "monster", count: 2, matches: [75, 155]}')
        ]
        cleanup:
        helper?.drop()
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) })
    def '$bucketAuto'() {
        given:
        def helper = getCollectionHelper()

        helper.drop()

        (1..100).each {
            helper.insertDocuments(Document.parse("{price: ${it * 2}}"))
        }

        when:
        def results = helper.aggregate([bucketAuto('$price', 10)])

        then:
        results[0]._id.min == 2
        results[0].count == 10

        results[-1]._id.max == 200

        when:
        results = helper.aggregate([bucketAuto('$price', 7)])

        then:
        results[0]._id.min == 2
        results[0].count == 14

        results[-1]._id.max == 200
        results[-1].count == 16

        cleanup:
        helper?.drop()
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) })
    def '$bucketAuto with options'() {
        given:
        def helper = getCollectionHelper()

        helper.drop()

        (0..2000).each {
            def document = new Document('price', it * 5.01D)
            helper.insertDocuments(document)
        }

        when:
        def results = helper.aggregate([bucketAuto('$price', 10, new BucketAutoOptions()
            .granularity(BucketGranularity.POWERSOF2)
            .output(sum('count', 1), avg('avgPrice', '$price')))])

        then:
        results.size() == 5
        results[0].count != null
        results[0].avgPrice != null

        cleanup:
        helper?.drop()
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) })
    def '$count'() {
        given:
        def helper = getCollectionHelper()

        helper.drop()

        def random = new SecureRandom()
        def total = random.nextInt(2000)
        (1..total).each {
            helper.insertDocuments(new Document('price', random.nextDouble() * 5000D + 5.01D))
        }

        when:
        def results = helper.aggregate([count()])

        then:
        results[0].count == total

        when:
        results = helper.aggregate([count('count')])

        then:
        results[0].count == total

        when:
        results = helper.aggregate([count('total')])

        then:
        results[0].total == total

        cleanup:
        helper?.drop()
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) })
    def '$sortByCount'() {
        given:
        def helper = getCollectionHelper()

        when:
        helper.drop()

        helper.insertDocuments(Document.parse('{_id: 0, x: 1}'))
        helper.insertDocuments(Document.parse('{_id: 1, x: 2}'))
        helper.insertDocuments(Document.parse('{_id: 2, x: 1}'))
        helper.insertDocuments(Document.parse('{_id: 3, x: 0}'))

        def results = helper.aggregate([sortByCount('$x')])

        then:
        results == [Document.parse('{_id: 1, count: 2}'),
                    Document.parse('{_id: 0, count: 1}'),
                    Document.parse('{_id: 2, count: 1}')]

        when:
        helper.drop()

        helper.insertDocuments(Document.parse('{_id: 0, x: 1.4}'))
        helper.insertDocuments(Document.parse('{_id: 1, x: 2.3}'))
        helper.insertDocuments(Document.parse('{_id: 2, x: 1.1}'))
        helper.insertDocuments(Document.parse('{_id: 3, x: 0.5}'))

        results = helper.aggregate([sortByCount(new Document('$floor', '$x'))])

        then:
        results == [Document.parse('{_id: 1, count: 2}'),
                    Document.parse('{_id: 0, count: 1}'),
                    Document.parse('{_id: 2, count: 1}')]

        cleanup:
        helper?.drop()
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) })
    def '$addFields'() {
        given:
        def helper = getCollectionHelper()

        when:
        helper.drop()
        helper.insertDocuments(Document.parse('{_id: 0, a: 1}'))
        def results = helper.aggregate([addFields(new Field('newField', null))])

        then:
        results == [Document.parse('{_id: 0, a: 1, newField: null}')]

        when:
        helper.drop()
        helper.insertDocuments(Document.parse('{_id: 0, a: 1}'))
        results = helper.aggregate([addFields(new Field('newField', 'hello'))])

        then:
        results == [Document.parse('{_id: 0, a: 1, newField: "hello"}')]

        when:
        helper.drop()
        helper.insertDocuments(Document.parse('{_id: 0, a: 1}'))
        results = helper.aggregate([addFields(new Field('b', '$a'))])

        then:
        results == [Document.parse('{_id: 0, a: 1, b: 1}')]

        when:
        helper.drop()
        helper.insertDocuments(Document.parse('{_id: 0, a: 1}'))
        results = helper.aggregate([addFields(new Field('this', '$$CURRENT'))])

        then:
        results == [Document.parse('{_id: 0, a: 1, this: {_id: 0, a: 1}}')]

        when:
        helper.drop()
        helper.insertDocuments(Document.parse('{_id: 0, a: 1}'))
        results = helper.aggregate([addFields(new Field('myNewField',
                                                        new Document('c', 3).append('d', 4)))])

        then:
        results == [Document.parse('{_id: 0, a: 1, myNewField: {c: 3, d: 4}}')]

        when:
        helper.drop()
        helper.insertDocuments(Document.parse('{_id: 0, a: 1}'))
        results = helper.aggregate([addFields(new Field('alt3', new Document('$lt', asList('$a', 3))))])

        then:
        results == [Document.parse('{_id: 0, a: 1, alt3: true}')]

        when:
        helper.drop()
        helper.insertDocuments(Document.parse('{_id: 0, a: 1}'))
        results = helper.aggregate([addFields(new Field('b', 3), new Field('c', 5))])

        then:
        results == [Document.parse('{_id: 0, a: 1, b: 3, c: 5}')]

        when:
        helper.drop()
        helper.insertDocuments(Document.parse('{_id: 0, a: 1}'))
        results = helper.aggregate([addFields(new Field('a', [1, 2, 3]))])

        then:
        results == [Document.parse('{_id: 0, a: [1, 2, 3]}')]

        cleanup:
        helper?.drop()
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) })
    def '$replaceRoot'() {
        given:
        def helper = getCollectionHelper()
        def results = []

        when:
        helper.drop()
        helper.insertDocuments(Document.parse('{_id: 0, a1: {b: 1}, a2: 2}'))
        results = helper.aggregate([replaceRoot('$a1')])

        then:
        results == [Document.parse('{b: 1}')]

        when:
        helper.drop()
        helper.insertDocuments(Document.parse('{_id: 0, a1: {b: {c1: 4, c2: 5}}, a2: 2}'))
        results = helper.aggregate([replaceRoot('$a1.b')])

        then:
        results == [Document.parse('{c1: 4, c2: 5}')]

        when:
        helper.drop()
        helper.insertDocuments(Document.parse('{_id: 0, a1: {b: 1, _id: 7}, a2: 2}'))
        results = helper.aggregate([replaceRoot('$a1')])

        then:
        results == [Document.parse('{b: 1, _id: 7}')]
    }
}
