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

import com.mongodb.MongoCommandException
import com.mongodb.MongoNamespace
import com.mongodb.OperationFunctionalSpecification
import org.bson.BsonDocument
import org.bson.BsonString
import org.bson.Document
import org.bson.conversions.Bson
import spock.lang.IgnoreIf

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
import static com.mongodb.client.model.Aggregates.skip
import static com.mongodb.client.model.Aggregates.sort
import static com.mongodb.client.model.Aggregates.sortByCount
import static com.mongodb.client.model.Aggregates.unionWith
import static com.mongodb.client.model.Aggregates.unwind
import static com.mongodb.client.model.Filters.eq
import static com.mongodb.client.model.Filters.exists
import static com.mongodb.client.model.Filters.expr
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

    @IgnoreIf({ !serverVersionAtLeast(5, 0) })
    def '$group with $count'() {
        expect:
        aggregate([group(null, Accumulators.count('acc'))]) == [new Document('_id', null).append('acc', 3)]
    }

    def '$out'() {
        given:
        def outCollectionName = getCollectionName() + '.out'

        when:
        aggregate([out(outCollectionName)])

        then:
        getCollectionHelper(new MongoNamespace(getDatabaseName(), outCollectionName)).find() == [a, b, c]
    }

    @IgnoreIf({ !serverVersionAtLeast(4, 3) })
    def '$out to specified database'() {
        given:
        def outDatabaseName = getDatabaseName() + '_out'
        def outCollectionName = getCollectionName() + '.out'
        getCollectionHelper(new MongoNamespace(outDatabaseName, outCollectionName)).create()

        when:
        aggregate([out(outDatabaseName, outCollectionName)])

        then:
        getCollectionHelper(new MongoNamespace(outDatabaseName, outCollectionName)).find() == [a, b, c]
    }

    @IgnoreIf({ !serverVersionAtLeast(4, 2) })
    def '$merge'() {
        given:
        def outCollectionName = getCollectionName() + '.out'
        getCollectionHelper(new MongoNamespace(getDatabaseName(), outCollectionName))
                .createUniqueIndex(new Document('x', 1))
        getCollectionHelper(new MongoNamespace('db1', outCollectionName)).create()

        when:
        aggregate([merge(outCollectionName)])

        then:
        getCollectionHelper(new MongoNamespace(getDatabaseName(), outCollectionName)).find() == [a, b, c]

        when:
        aggregate([merge(new MongoNamespace('db1', outCollectionName))])

        then:
        getCollectionHelper(new MongoNamespace('db1', outCollectionName)).find() == [a, b, c]

        when:
        aggregate([merge(outCollectionName, new MergeOptions()
                .uniqueIdentifier('x')
                .whenMatched(MergeOptions.WhenMatched.REPLACE)
                .whenNotMatched(MergeOptions.WhenNotMatched.FAIL))])

        then:
        getCollectionHelper(new MongoNamespace(getDatabaseName(), outCollectionName)).find() == [a, b, c]

        when:
        aggregate([merge(outCollectionName, new MergeOptions()
                .uniqueIdentifier('x')
                .whenMatched(MergeOptions.WhenMatched.KEEP_EXISTING))])

        then:
        getCollectionHelper(new MongoNamespace(getDatabaseName(), outCollectionName)).find() == [a, b, c]

        when:
        aggregate([merge(outCollectionName, new MergeOptions()
                .uniqueIdentifier('x')
                .whenMatched(MergeOptions.WhenMatched.MERGE))])

        then:
        getCollectionHelper(new MongoNamespace(getDatabaseName(), outCollectionName)).find() == [a, b, c]

        when:
        aggregate([merge(outCollectionName, new MergeOptions()
                .uniqueIdentifier('x')
                .whenMatched(MergeOptions.WhenMatched.FAIL))])

        then:
        thrown(MongoCommandException)

        when:
        aggregate([merge(outCollectionName, new MergeOptions()
                .uniqueIdentifier('x')
                .whenMatched(MergeOptions.WhenMatched.REPLACE)
                .whenNotMatched(MergeOptions.WhenNotMatched.DISCARD))])

        then:
        getCollectionHelper(new MongoNamespace(getDatabaseName(), outCollectionName)).find() == [a, b, c]

        when:
        aggregate([merge(outCollectionName, new MergeOptions()
                .uniqueIdentifier('x')
                .whenMatched(MergeOptions.WhenMatched.REPLACE)
                .whenNotMatched(MergeOptions.WhenNotMatched.INSERT))])

        then:
        getCollectionHelper(new MongoNamespace(getDatabaseName(), outCollectionName)).find() == [a, b, c]

        when:
        aggregate([merge(outCollectionName, new MergeOptions()
                .uniqueIdentifier('x')
                .whenMatched(MergeOptions.WhenMatched.PIPELINE)
                .variables([new Variable<Integer>('b', 1)])
                .whenMatchedPipeline([addFields([new Field<String>('b', '$$b')])])
                .whenNotMatched(MergeOptions.WhenNotMatched.FAIL))])

        then:
        a.append('b', 1)
        b.append('b', 1)
        c.append('b', 1)
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

    @IgnoreIf({ !serverVersionAtLeast(3, 6) })
    def '$lookup with pipeline'() {
        given:
        def fromCollectionName = 'warehouses'
        def fromHelper = getCollectionHelper(new MongoNamespace(getDatabaseName(), fromCollectionName))
        def collection = getCollectionHelper()

        collection.drop()
        fromHelper.drop()

        fromHelper.insertDocuments(
                Document.parse('{ "_id" : 1, "stock_item" : "abc", warehouse: "A", "instock" : 120 }'),
                Document.parse('{ "_id" : 2, "stock_item" : "abc", warehouse: "B", "instock" : 60 }'),
                Document.parse('{ "_id" : 3, "stock_item" : "xyz", warehouse: "B", "instock" : 40 }'),
                Document.parse('{ "_id" : 4, "stock_item" : "xyz", warehouse: "A", "instock" : 80 }'))

        collection.insertDocuments(
                Document.parse('{ "_id" : 1, "item" : "abc", "price" : 12, "ordered" : 2 }'),
                Document.parse('{ "_id" : 2, "item" : "xyz", "price" : 10, "ordered" : 60 }')
        )

        def let = asList(new Variable('order_item', '$item'), new Variable('order_qty', '$ordered'))

        def  pipeline = asList(
                match(expr(new Document('$and',
                        asList( new Document('$eq', asList('$stock_item', '$$order_item')),
                                new Document('$gte', asList('$instock', '$$order_qty')))))),
                project(fields(exclude('stock_item'), excludeId())))

        def lookupDoc = lookup(fromCollectionName, let, pipeline, 'stockdata')

        when:
        def results = aggregate([lookupDoc])

        then:
        results == [
                Document.parse('{ "_id" : 1.0, "item" : "abc", "price" : 12.0, "ordered" : 2.0, ' +
                        '"stockdata" : [ { "warehouse" : "A", "instock" : 120.0 }, { "warehouse" : "B", "instock" : 60.0 } ] }'),
                Document.parse('{ "_id" : 2.0, "item" : "xyz", "price" : 10.0, "ordered" : 60.0, ' +
                        '"stockdata" : [ { "warehouse" : "A", "instock" : 80.0 } ] }') ]

        cleanup:
        fromHelper?.drop()
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 6) })
    def '$lookup with pipeline without variables'() {
        given:
        def fromCollectionName = 'holidays'
        def fromCollection = getCollectionHelper(new MongoNamespace(getDatabaseName(), fromCollectionName))
        def collection = getCollectionHelper()

        collection.drop()
        fromCollection.drop()

        fromCollection.insertDocuments(
                Document.parse('{ "_id" : 1, year: 2018, name: "New Years", date: { $date : "2018-01-01T00:00:00Z"} }'),
                Document.parse('{ "_id" : 2, year: 2018, name: "Pi Day", date: { $date : "2018-03-14T00:00:00Z" } }'),
                Document.parse('{ "_id" : 3, year: 2018, name: "Ice Cream Day", date: { $date : "2018-07-15T00:00:00Z"} }'),
                Document.parse('{ "_id" : 4, year: 2017, name: "New Years", date: { $date : "2017-01-01T00:00:00Z" } }'),
                Document.parse('{ "_id" : 5, year: 2017, name: "Ice Cream Day", date: { $date : "2017-07-16T00:00:00Z" } }')
        )

        collection.insertDocuments(
                Document.parse('''{ "_id" : 1, "student" : "Ann Aardvark",
                            sickdays: [ { $date : "2018-05-01T00:00:00Z" }, { $date : "2018-08-23T00:00:00Z" } ] }'''),
                Document.parse('''{ "_id" : 2, "student" : "Zoe Zebra",
                            sickdays: [ { $date : "2018-02-01T00:00:00Z" }, { $date : "2018-05-23T00:00:00Z" } ] }''')
        )

        def  pipeline = asList(
                match(eq('year', 2018)),
                project(fields(excludeId(), computed('date', fields(computed('name', '$name'), computed('date', '$date'))))),
                replaceRoot('$date')
        )

        def lookupDoc = lookup(fromCollectionName, pipeline, 'holidays')

        when:
        def results = aggregate([lookupDoc])

        then:
        results == [
                Document.parse(
                        '''{ '_id' : 1, 'student' : "Ann Aardvark",
                        'sickdays' : [ ISODate("2018-05-01T00:00:00Z"), ISODate("2018-08-23T00:00:00Z") ],
                        'holidays' : [  { 'name' : "New Years", 'date' : ISODate ("2018-01-01T00:00:00Z") },
                                        { 'name' : "Pi Day", 'date' : ISODate("2018-03-14T00:00:00Z") },
                                        { 'name' : "Ice Cream Day", 'date' : ISODate("2018-07-15T00:00:00Z") } ] }'''),
                Document.parse(
                        '''{ '_id' : 2, 'student' : "Zoe Zebra",
                        'sickdays' : [ ISODate("2018-02-01T00:00:00Z"), ISODate("2018-05-23T00:00:00Z") ],
                        'holidays' : [  { 'name' : "New Years", 'date' : ISODate("2018-01-01T00:00:00Z") },
                                        { 'name' : "Pi Day", 'date' : ISODate("2018-03-14T00:00:00Z") },
                                        { 'name' : "Ice Cream Day", 'date' : ISODate("2018-07-15T00:00:00Z") } ] }''') ]

        cleanup:
        fromCollection?.drop()
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
    def '$graphLookup with depth options'() {
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
    def '$graphLookup with query filter option'() {
        given:
        def fromCollectionName = 'contacts'
        def fromHelper = getCollectionHelper(new MongoNamespace(getDatabaseName(), fromCollectionName))

        fromHelper.drop()

        fromHelper.insertDocuments(
            Document.parse('''{ _id: 0, name: "Bob Smith", friends: ["Anna Jones", "Chris Green"],
                hobbies : ["tennis", "unicycling", "golf"] }'''),
            Document.parse('''{ _id: 1, name: "Anna Jones", friends: ["Bob Smith", "Chris Green", "Joe Lee"],
                 hobbies : ["archery", "golf", "woodworking"] }'''),
            Document.parse('''{ _id: 2, name: "Chris Green", friends: ["Anna Jones", "Bob Smith"],
                hobbies : ["knitting", "frisbee"] }'''),
            Document.parse('''{ _id: 3, name: "Joe Lee", friends: ["Anna Jones", "Fred Brown"],
                hobbies : [ "tennis", "golf", "topiary" ] }'''),
            Document.parse('''{ _id: 4, name: "Fred Brown", friends: ["Joe Lee"],
                hobbies : [ "travel", "ceramics", "golf" ] }'''))


        def lookupDoc = graphLookup('contacts', new BsonString('$friends'), 'friends', 'name', 'golfers',
                new GraphLookupOptions()
                        .restrictSearchWithMatch(eq('hobbies', 'golf')))

        when:
        def results = fromHelper.aggregate([lookupDoc,
                                            unwind('$golfers'),
                                            sort(new Document('_id', 1)
                                                    .append('golfers._id', 1))])

        then:
        results.subList(0, 4) == [
                Document.parse('''{ _id: 0, name: "Bob Smith", friends: ["Anna Jones", "Chris Green"],
                        hobbies : ["tennis", "unicycling", "golf"], golfers: {_id: 0, name: "Bob Smith",
                        friends: ["Anna Jones", "Chris Green"], hobbies : ["tennis", "unicycling", "golf"] } }'''),
                Document.parse('''{ _id: 0, name: "Bob Smith", friends: ["Anna Jones", "Chris Green"],
                       hobbies: ["tennis", "unicycling", "golf"], golfers:{ _id: 1, name: "Anna Jones",
                       friends: ["Bob Smith", "Chris Green", "Joe Lee"], hobbies : ["archery", "golf", "woodworking"] } } }'''),
                Document.parse('''{ _id: 0, name: "Bob Smith", friends: ["Anna Jones", "Chris Green"],
                       hobbies: ["tennis", "unicycling", "golf"], golfers: { _id: 3, name: "Joe Lee",
                       friends: ["Anna Jones", "Fred Brown"], hobbies : [ "tennis", "golf", "topiary" ] } }'''),
                Document.parse('''{ _id: 0, name: "Bob Smith", friends: ["Anna Jones", "Chris Green"],
                       hobbies: ["tennis", "unicycling", "golf"], golfers:{ _id: 4, name: "Fred Brown", friends: ["Joe Lee"],
                       hobbies : [ "travel", "ceramics", "golf" ] } }''')
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

        def total = 3
        def documents = []
        (1..total).each {
            documents.add(new BsonDocument())
        }
        helper.insertDocuments(documents)

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
        helper.insertDocuments(Document.parse('{_id: 2, x: 1}'))
        helper.insertDocuments(Document.parse('{_id: 3, x: 0}'))

        def results = helper.aggregate([sortByCount('$x')])

        then:
        results == [Document.parse('{_id: 1, count: 2}'),
                    Document.parse('{_id: 0, count: 1}')]

        when:
        helper.drop()

        helper.insertDocuments(Document.parse('{_id: 0, x: 1.4}'))
        helper.insertDocuments(Document.parse('{_id: 2, x: 1.1}'))
        helper.insertDocuments(Document.parse('{_id: 3, x: 0.5}'))

        results = helper.aggregate([sortByCount(new Document('$floor', '$x'))])

        then:
        results == [Document.parse('{_id: 1, count: 2}'),
                    Document.parse('{_id: 0, count: 1}')]

        cleanup:
        helper?.drop()
    }

    @IgnoreIf({ !serverVersionAtLeast(4, 3) })
    def '$accumulator'() {
        given:
        def helper = getCollectionHelper()

        when:
        helper.drop()
        helper.insertDocuments(Document.parse('{_id: 1, x: "string"}'))
        def init = 'function() { return { x: "test string" } }'
        def accumulate = 'function(state) { return state }'
        def merge = 'function(state1, state2) { return state1 }'
        def accumulatorExpr = accumulator('testString', init, accumulate, merge);
        def results1 = helper.aggregate([group('$x', asList(accumulatorExpr))])

        then:
        results1.size() == 1
        results1.contains(Document.parse('{ _id: "string", testString: { x: "test string" } }'))

        when:
        helper.drop()
        helper.insertDocuments(Document.parse('{_id: 8751, title: "The Banquet", author: "Dante", copies: 2}'),
                Document.parse('{_id: 8752, title: "Divine Comedy", author: "Dante", copies: 1}'),
                Document.parse('{_id: 8645, title: "Eclogues", author: "Dante", copies: 2}'),
                Document.parse('{_id: 7000, title: "The Odyssey", author: "Homer", copies: 10}'),
                Document.parse('{_id: 7020, title: "Iliad", author: "Homer", copies: 10}'))
        def initFunction = 'function(initCount, initSum) { return { count: parseInt(initCount), sum: parseInt(initSum) } }';
        def accumulateFunction = 'function(state, numCopies) { return { count : state.count + 1, sum : state.sum + numCopies } }';
        def mergeFunction = 'function(state1, state2) { return { count : state1.count + state2.count, sum : state1.sum + state2.sum } }';
        def finalizeFunction = 'function(state) { return (state.sum / state.count) }';
        def accumulatorExpression = accumulator('avgCopies', initFunction, [ '0', '0' ], accumulateFunction,
                [ '$copies' ], mergeFunction, finalizeFunction)
        def results2 = helper.aggregate([group('$author', asList(
                new BsonField('minCopies', new Document('$min', '$copies')), accumulatorExpression,
                new BsonField('maxCopies', new Document('$max', '$copies'))))])

        then:
        results2.size() == 2
        results2.contains(Document.parse('{_id: "Dante", minCopies: 1, avgCopies: 1.6666666666666667, maxCopies : 2}'))
        results2.contains(Document.parse('{_id: "Homer", minCopies: 10, avgCopies: 10.0, maxCopies : 10}'))

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

    @IgnoreIf({ !serverVersionAtLeast(4, 2) })
    def '$set'() {
        expect:
        aggregate([set(new Field('c', '$y'))]) == [new Document(a).append('c', 'a'),
                                                   new Document(b).append('c', 'b'),
                                                   new Document(c).append('c', 'c')]
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

    @IgnoreIf({ !serverVersionAtLeast(4, 2) })
    def '$replaceWith'() {
        given:
        def helper = getCollectionHelper()
        def results = []

        when:
        helper.drop()
        helper.insertDocuments(Document.parse('{_id: 0, a1: {b: 1}, a2: 2}'))
        results = helper.aggregate([replaceWith('$a1')])

        then:
        results == [Document.parse('{b: 1}')]

        when:
        helper.drop()
        helper.insertDocuments(Document.parse('{_id: 0, a1: {b: {c1: 4, c2: 5}}, a2: 2}'))
        results = helper.aggregate([replaceWith('$a1.b')])

        then:
        results == [Document.parse('{c1: 4, c2: 5}')]

        when:
        helper.drop()
        helper.insertDocuments(Document.parse('{_id: 0, a1: {b: 1, _id: 7}, a2: 2}'))
        results = helper.aggregate([replaceWith('$a1')])

        then:
        results == [Document.parse('{b: 1, _id: 7}')]
    }

    @IgnoreIf({ !serverVersionAtLeast(4, 3) })
    def '$unionWith'() {
        given:
        def coll1Helper = getCollectionHelper(new MongoNamespace(getDatabaseName(), 'coll1'))
        def coll2Helper = getCollectionHelper(new MongoNamespace(getDatabaseName(), 'coll2'))

        coll1Helper.drop()
        coll2Helper.drop()

        coll1Helper.insertDocuments(
                Document.parse('{ "name1" : "almonds" }'),
                Document.parse('{ "name1" : "cookies" }'))

        coll2Helper.insertDocuments(
                Document.parse('{ "name2" : "cookies" }'),
                Document.parse('{ "name2" : "cookies" }'),
                Document.parse('{ "name2" : "pecans" }'))

        def pipeline = asList(match(eq('name2', 'cookies')), project(fields(excludeId(), computed('name', '$name2'))),
                sort(ascending('name')))

        when:
        def results = coll1Helper.aggregate([project(fields(excludeId(), computed('name', '$name1'))),
                                             unionWith('coll2', pipeline)])

        then:
        results == [
                Document.parse('{ name: "almonds" }'),
                Document.parse('{ name: "cookies" }'),
                Document.parse('{ name: "cookies" }'),
                Document.parse('{ name: "cookies" }') ]

        cleanup:
        coll1Helper?.drop()
        coll2Helper?.drop()
    }
}
