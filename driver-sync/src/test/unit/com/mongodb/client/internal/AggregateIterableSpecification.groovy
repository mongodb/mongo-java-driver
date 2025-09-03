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

package com.mongodb.client.internal

import com.mongodb.Function
import com.mongodb.MongoException
import com.mongodb.MongoNamespace
import com.mongodb.ReadConcern
import com.mongodb.WriteConcern
import com.mongodb.client.ClientSession
import com.mongodb.client.model.Collation
import com.mongodb.internal.client.model.AggregationLevel
import com.mongodb.internal.operation.AggregateOperation
import com.mongodb.internal.operation.AggregateToCollectionOperation
import com.mongodb.internal.operation.BatchCursor
import com.mongodb.internal.operation.FindOperation
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.DocumentCodec
import org.bson.codecs.DocumentCodecProvider
import org.bson.codecs.ValueCodecProvider
import org.bson.codecs.configuration.CodecConfigurationException
import spock.lang.Specification

import java.util.function.Consumer

import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS
import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.ReadPreference.secondary
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static org.bson.codecs.configuration.CodecRegistries.fromProviders
import static spock.util.matcher.HamcrestSupport.expect

class AggregateIterableSpecification extends Specification {

    def namespace = new MongoNamespace('db', 'coll')
    def codecRegistry = fromProviders([new ValueCodecProvider(), new DocumentCodecProvider(), new BsonValueCodecProvider()])
    def readPreference = secondary()
    def readConcern = ReadConcern.MAJORITY
    def writeConcern = WriteConcern.MAJORITY
    def collation = Collation.builder().locale('en').build()

    def 'should build the expected AggregationOperation'() {
        given:
        def executor = new TestOperationExecutor([null, null, null, null, null])
        def pipeline = [new Document('$match', 1)]
        def aggregationIterable = new AggregateIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference,
                readConcern, writeConcern, executor, pipeline, AggregationLevel.COLLECTION,
                true, TIMEOUT_SETTINGS)

        when: 'default input should be as expected'
        aggregationIterable.iterator()

        def operation = executor.getReadOperation() as AggregateOperation<Document>
        def readPreference = executor.getReadPreference()

        then:
        expect operation, isTheSameAs(new AggregateOperation<Document>(namespace,
                [new BsonDocument('$match', new BsonInt32(1))], new DocumentCodec())
                .retryReads(true))
        readPreference == secondary()

        when: 'overriding initial options'
        aggregationIterable
                .maxAwaitTime(1001, MILLISECONDS)
                .maxTime(101, MILLISECONDS)
                .collation(collation)
                .hint(new Document('a', 1))
                .comment('this is a comment')
                .iterator()

        operation = executor.getReadOperation() as AggregateOperation<Document>

        then: 'should use the overrides'
        expect operation, isTheSameAs(new AggregateOperation<Document>(namespace,
                [new BsonDocument('$match', new BsonInt32(1))], new DocumentCodec())
                .retryReads(true)
                .collation(collation)
                .hint(new BsonDocument('a', new BsonInt32(1)))
                .comment(new BsonString('this is a comment')))

        when: 'both hint and hint string are set'
        aggregationIterable = new AggregateIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference,
                readConcern, writeConcern, executor, pipeline, AggregationLevel.COLLECTION, false, TIMEOUT_SETTINGS)

        aggregationIterable
                .hint(new Document('a', 1))
                .hintString('a_1')
                .iterator()

        operation = executor.getReadOperation() as AggregateOperation<Document>

        then: 'should use hint not hint string'
        expect operation, isTheSameAs(new AggregateOperation<Document>(namespace,
                [new BsonDocument('$match', new BsonInt32(1))], new DocumentCodec())
                .hint(new BsonDocument('a', new BsonInt32(1))))
    }

    def 'should build the expected AggregateToCollectionOperation for $out'() {
        given:
        def executor = new TestOperationExecutor([null, null, null, null, null])
        def collectionName = 'collectionName'
        def collectionNamespace = new MongoNamespace(namespace.getDatabaseName(), collectionName)
        def pipeline = [new Document('$match', 1), new Document('$out', collectionName)]

        when: 'aggregation includes $out'
        new AggregateIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference, readConcern, writeConcern, executor,
                pipeline, AggregationLevel.COLLECTION, false, TIMEOUT_SETTINGS)
                .batchSize(99)
                .allowDiskUse(true)
                .collation(collation)
                .hint(new Document('a', 1))
                .comment(new BsonString('this is a comment'))
                .iterator()

        def operation = executor.getReadOperation() as AggregateToCollectionOperation

        then: 'should use the overrides'
        expect operation, isTheSameAs(new AggregateToCollectionOperation(namespace,
                [new BsonDocument('$match', new BsonInt32(1)), new BsonDocument('$out', new BsonString(collectionName))],
                readConcern, writeConcern, AggregationLevel.COLLECTION)
                .allowDiskUse(true)
                .collation(collation)
                .hint(new BsonDocument('a', new BsonInt32(1)))
                .comment(new BsonString('this is a comment'))
        )

        when: 'the subsequent read should have the batchSize set'
        operation = executor.getReadOperation() as FindOperation<Document>

        then: 'should use the correct settings'
        operation.getNamespace() == collectionNamespace
        operation.getBatchSize() == 99
        operation.getCollation() == collation

        when: 'aggregation includes $out and is at the database level'
        new AggregateIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference, readConcern, writeConcern, executor,
                pipeline, AggregationLevel.DATABASE, false, TIMEOUT_SETTINGS)
                .batchSize(99)
                .maxTime(100, MILLISECONDS)
                .allowDiskUse(true)
                .collation(collation)
                .hint(new Document('a', 1))
                .comment(new BsonString('this is a comment'))
                .iterator()

        operation = executor.getReadOperation() as AggregateToCollectionOperation

        then: 'should use the overrides'
        expect operation, isTheSameAs(new AggregateToCollectionOperation(namespace,
                [new BsonDocument('$match', new BsonInt32(1)), new BsonDocument('$out', new BsonString(collectionName))],
                readConcern, writeConcern,
                AggregationLevel.DATABASE)
                .allowDiskUse(true)
                .collation(collation)
                .hint(new BsonDocument('a', new BsonInt32(1)))
                .comment(new BsonString('this is a comment'))
        )

        when: 'the subsequent read should have the batchSize set'
        operation = executor.getReadOperation() as FindOperation<Document>

        then: 'should use the correct settings'
        operation.getNamespace() == collectionNamespace
        operation.getBatchSize() == 99
        operation.getCollation() == collation
        operation.isAllowDiskUse() == null

        when: 'toCollection should work as expected'
        new AggregateIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference, readConcern, writeConcern, executor,
                pipeline, AggregationLevel.COLLECTION, false, TIMEOUT_SETTINGS)
                .allowDiskUse(true)
                .collation(collation)
                .hint(new Document('a', 1))
                .comment(new BsonString('this is a comment'))
                .toCollection()

        operation = executor.getReadOperation() as AggregateToCollectionOperation

        then:
        expect operation, isTheSameAs(new AggregateToCollectionOperation(namespace,
                [new BsonDocument('$match', new BsonInt32(1)), new BsonDocument('$out', new BsonString(collectionName))],
                readConcern, writeConcern)
                .allowDiskUse(true)
                .collation(collation)
                .hint(new BsonDocument('a', new BsonInt32(1)))
                .comment(new BsonString('this is a comment')))
    }

    def 'should build the expected AggregateToCollectionOperation for $out with hint string'() {
        given:
        def executor = new TestOperationExecutor([null, null, null, null, null])
        def collectionName = 'collectionName'
        def pipeline = [new Document('$match', 1), new Document('$out', collectionName)]

        when: 'aggregation includes $out and hint string'
        new AggregateIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference, readConcern, writeConcern, executor,
                pipeline, AggregationLevel.COLLECTION, false, TIMEOUT_SETTINGS)
                .hintString('x_1').iterator()

        def operation = executor.getReadOperation() as AggregateToCollectionOperation

        then: 'should use the overrides'
        expect operation, isTheSameAs(new AggregateToCollectionOperation(namespace,
                [new BsonDocument('$match', new BsonInt32(1)), new BsonDocument('$out', new BsonString(collectionName))],
                readConcern, writeConcern, AggregationLevel.COLLECTION)
                .hint(new BsonString('x_1')))

        when: 'aggregation includes $out and hint and hint string'
        executor = new TestOperationExecutor([null, null, null, null, null])
        new AggregateIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference, readConcern, writeConcern, executor,
                pipeline, AggregationLevel.COLLECTION, false, TIMEOUT_SETTINGS)
                .hint(new BsonDocument('x', new BsonInt32(1)))
                .hintString('x_1').iterator()

        operation = executor.getReadOperation() as AggregateToCollectionOperation

        then: 'should use the hint not the hint string'
        expect operation, isTheSameAs(new AggregateToCollectionOperation(namespace,
                [new BsonDocument('$match', new BsonInt32(1)), new BsonDocument('$out', new BsonString(collectionName))],
                readConcern, writeConcern, AggregationLevel.COLLECTION)
                .hint(new BsonDocument('x', new BsonInt32(1))))
    }

    def 'should build the expected AggregateToCollectionOperation for $merge document'() {
        given:
        def executor = new TestOperationExecutor([null, null, null, null, null, null, null])
        def collectionName = 'collectionName'
        def collectionNamespace = new MongoNamespace(namespace.getDatabaseName(), collectionName)
        def pipeline = [new Document('$match', 1), new Document('$merge', new Document('into', collectionName))]
        def pipelineWithIntoDocument = [new Document('$match', 1), new Document('$merge',
                new Document('into', new Document('db', 'db2').append('coll', collectionName)))]

        when: 'aggregation includes $merge'
        new AggregateIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference, readConcern, writeConcern, executor,
                pipeline, AggregationLevel.COLLECTION, false, TIMEOUT_SETTINGS)
                .batchSize(99)
                .allowDiskUse(true)
                .collation(collation)
                .hint(new Document('a', 1))
                .comment(new BsonString('this is a comment')).iterator()

        def operation = executor.getReadOperation() as AggregateToCollectionOperation

        then: 'should use the overrides'
        expect operation, isTheSameAs(new AggregateToCollectionOperation(namespace,
                [new BsonDocument('$match', new BsonInt32(1)),
                 new BsonDocument('$merge', new BsonDocument('into', new BsonString(collectionName)))],
                readConcern, writeConcern,
                AggregationLevel.COLLECTION)
                .allowDiskUse(true)
                .collation(collation)
                .hint(new BsonDocument('a', new BsonInt32(1)))
                .comment(new BsonString('this is a comment'))
        )

        when: 'the subsequent read should have the batchSize set'
        operation = executor.getReadOperation() as FindOperation<Document>

        then: 'should use the correct settings'
        operation.getNamespace() == collectionNamespace
        operation.getBatchSize() == 99
        operation.getCollation() == collation

        when: 'aggregation includes $merge into a different database'
        new AggregateIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference, readConcern, writeConcern, executor,
                pipelineWithIntoDocument, AggregationLevel.COLLECTION, false, TIMEOUT_SETTINGS)
                .batchSize(99)
                .maxTime(100, MILLISECONDS)
                .allowDiskUse(true)
                .collation(collation)
                .hint(new Document('a', 1))
                .comment(new BsonString('this is a comment')).iterator()

        operation = executor.getReadOperation() as AggregateToCollectionOperation

        then: 'should use the overrides'
        expect operation, isTheSameAs(new AggregateToCollectionOperation(namespace,
                [new BsonDocument('$match', new BsonInt32(1)),
                 new BsonDocument('$merge', new BsonDocument('into',
                         new BsonDocument('db', new BsonString('db2')).append('coll', new BsonString(collectionName))))],
                readConcern, writeConcern,
                AggregationLevel.COLLECTION)
                .allowDiskUse(true)
                .collation(collation)
                .hint(new BsonDocument('a', new BsonInt32(1)))
                .comment(new BsonString('this is a comment'))
        )

        when: 'the subsequent read should have the batchSize set'
        operation = executor.getReadOperation() as FindOperation<Document>

        then: 'should use the correct settings'
        operation.getNamespace() == new MongoNamespace('db2', collectionName)
        operation.getBatchSize() == 99
        operation.getCollation() == collation

        when: 'aggregation includes $merge and is at the database level'
        new AggregateIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference, readConcern, writeConcern, executor,
                pipeline, AggregationLevel.DATABASE, false, TIMEOUT_SETTINGS)
                .batchSize(99)
                .maxTime(100, MILLISECONDS)
                .allowDiskUse(true)
                .collation(collation)
                .hint(new Document('a', 1))
                .comment(new BsonString('this is a comment')).iterator()

        operation = executor.getReadOperation() as AggregateToCollectionOperation

        then: 'should use the overrides'
        expect operation, isTheSameAs(new AggregateToCollectionOperation(namespace,
                [new BsonDocument('$match', new BsonInt32(1)),
                 new BsonDocument('$merge', new BsonDocument('into', new BsonString(collectionName)))],
                readConcern, writeConcern,
                AggregationLevel.DATABASE)
                .allowDiskUse(true)
                .collation(collation)
                .hint(new BsonDocument('a', new BsonInt32(1)))
                .comment(new BsonString('this is a comment'))
        )

        when: 'the subsequent read should have the batchSize set'
        operation = executor.getReadOperation() as FindOperation<Document>

        then: 'should use the correct settings'
        operation.getNamespace() == collectionNamespace
        operation.getBatchSize() == 99
        operation.getCollation() == collation

        when: 'toCollection should work as expected'
        new AggregateIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference, readConcern, writeConcern, executor,
                pipeline, AggregationLevel.COLLECTION, false, TIMEOUT_SETTINGS)
                .allowDiskUse(true)
                .collation(collation)
                .hint(new Document('a', 1))
                .comment(new BsonString('this is a comment'))
                .toCollection()

        operation = executor.getReadOperation() as AggregateToCollectionOperation

        then:
        expect operation, isTheSameAs(new AggregateToCollectionOperation(namespace,
                [new BsonDocument('$match', new BsonInt32(1)),
                 new BsonDocument('$merge', new BsonDocument('into', new BsonString(collectionName)))],
                readConcern, writeConcern)
                .allowDiskUse(true)
                .collation(collation)
                .hint(new BsonDocument('a', new BsonInt32(1)))
                .comment(new BsonString('this is a comment')))
    }

    def 'should build the expected AggregateToCollectionOperation for $merge string'() {
        given:
        def executor = new TestOperationExecutor([null, null, null, null, null, null, null])
        def collectionName = 'collectionName'
        def collectionNamespace = new MongoNamespace(namespace.getDatabaseName(), collectionName)
        def pipeline = [new BsonDocument('$match', new BsonDocument()), new BsonDocument('$merge', new BsonString(collectionName))]

        when:
        new AggregateIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference, readConcern, writeConcern, executor,
                pipeline, AggregationLevel.COLLECTION, false, TIMEOUT_SETTINGS)
                .iterator()

        def operation = executor.getReadOperation() as AggregateToCollectionOperation

        then:
        expect operation, isTheSameAs(new AggregateToCollectionOperation(namespace, pipeline, readConcern,
                writeConcern, AggregationLevel.COLLECTION))

        when:
        operation = executor.getReadOperation() as FindOperation<Document>

        then:
        operation.getNamespace() == collectionNamespace
    }

    def 'should build the expected AggregateToCollectionOperation for $out as a document'() {
        given:
        def cannedResults = [new Document('_id', 1), new Document('_id', 2), new Document('_id', 3)]
        def cursor = {
            Stub(BatchCursor) {
                def count = 0
                def results
                def getResult = {
                    count++
                    results = count == 1 ? cannedResults : null
                    results
                }
                next() >> {
                    getResult()
                }
                hasNext() >> {
                    count == 0
                }
            }
        }
        def executor = new TestOperationExecutor([cursor(), cursor(), cursor(), cursor(), cursor(), cursor()])
        def pipeline = [new Document('$match', 1), new Document('$out', new Document('s3', true))]
        def outWithDBpipeline = [new Document('$match', 1),
                                 new Document('$out', new Document('db', 'testDB').append('coll', 'testCollection'))]

        when: 'aggregation includes $out'
        def aggregateIterable = new AggregateIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference,
                readConcern, writeConcern, executor, pipeline, AggregationLevel.COLLECTION, false, TIMEOUT_SETTINGS)

        aggregateIterable.toCollection()
        def operation = executor.getReadOperation() as AggregateToCollectionOperation

        then:
        expect operation, isTheSameAs(new AggregateToCollectionOperation(namespace,
                [new BsonDocument('$match', new BsonInt32(1)), BsonDocument.parse('{$out: {s3: true}}')],
                readConcern, writeConcern, AggregationLevel.COLLECTION)
        )

        when: 'Trying to iterate it should fail'
        aggregateIterable.iterator()

        then:
        thrown(IllegalStateException)

        when: 'aggregation includes $out and is at the database level'
        aggregateIterable = new AggregateIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference,
                readConcern, writeConcern, executor, pipeline, AggregationLevel.DATABASE, false, TIMEOUT_SETTINGS)
        aggregateIterable.toCollection()

        operation = executor.getReadOperation() as AggregateToCollectionOperation

        then:
        expect operation, isTheSameAs(new AggregateToCollectionOperation(namespace,
                [new BsonDocument('$match', new BsonInt32(1)), BsonDocument.parse('{$out: {s3: true}}')],
                readConcern, writeConcern, AggregationLevel.DATABASE)
        )

        when: 'Trying to iterate it should fail'
        aggregateIterable.iterator()

        then:
        thrown(IllegalStateException)

        when: 'toCollection should work as expected'
        aggregateIterable = new AggregateIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference,
                readConcern, writeConcern, executor, pipeline, AggregationLevel.COLLECTION, false, TIMEOUT_SETTINGS)
        aggregateIterable.toCollection()

        operation = executor.getReadOperation() as AggregateToCollectionOperation

        then:
        expect operation, isTheSameAs(new AggregateToCollectionOperation(namespace,
                [new BsonDocument('$match', new BsonInt32(1)), BsonDocument.parse('{$out: {s3: true}}')],
                readConcern, writeConcern))

        when: 'Trying to iterate it should fail'
        aggregateIterable.iterator()

        then:
        thrown(IllegalStateException)

        when: 'aggregation includes $out with namespace'
        aggregateIterable = new AggregateIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference,
                readConcern, writeConcern, executor, outWithDBpipeline, AggregationLevel.COLLECTION, false, TIMEOUT_SETTINGS)
        aggregateIterable.toCollection()

        operation = executor.getReadOperation() as AggregateToCollectionOperation

        then:
        expect operation, isTheSameAs(new AggregateToCollectionOperation(namespace,
                [new BsonDocument('$match', new BsonInt32(1)),
                 BsonDocument.parse('{$out: {db: "testDB", coll: "testCollection"}}')], readConcern, writeConcern))

        when: 'Trying to iterate it should succeed'
        def results = []
        aggregateIterable.into(results)

        then:
        results == cannedResults
    }


    def 'should use ClientSession for AggregationOperation'() {
        given:
        def batchCursor = Stub(BatchCursor) {
            _ * hasNext() >> { false }
        }
        def executor = new TestOperationExecutor([batchCursor, batchCursor])
        def pipeline = [new Document('$match', 1)]
        def aggregationIterable = new AggregateIterableImpl(clientSession, namespace, Document, Document, codecRegistry, readPreference,
                readConcern, writeConcern, executor, pipeline, AggregationLevel.COLLECTION, false, TIMEOUT_SETTINGS)

        when:
        aggregationIterable.first()

        then:
        executor.getClientSession() == clientSession

        when:
        aggregationIterable.iterator()

        then:
        executor.getClientSession() == clientSession

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should use ClientSession for AggregateToCollectionOperation'() {
        given:
        def batchCursor = Stub(BatchCursor) {
            _ * hasNext() >> { false }
        }
        def executor = new TestOperationExecutor([null, batchCursor, null, batchCursor, null])
        def pipeline = [new Document('$match', 1), new Document('$out', 'collName')]
        def aggregationIterable = new AggregateIterableImpl(clientSession, namespace, Document, Document, codecRegistry, readPreference,
                readConcern, writeConcern, executor, pipeline, AggregationLevel.COLLECTION, false, TIMEOUT_SETTINGS)

        when:
        aggregationIterable.first()

        then:
        executor.getClientSession() == clientSession

        when:
        aggregationIterable.iterator()

        then:
        executor.getClientSession() == clientSession
        executor.getClientSession() == clientSession

        when:
        aggregationIterable.toCollection()

        then:
        executor.getClientSession() == clientSession

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should handle exceptions correctly'() {
        given:
        def codecRegistry = fromProviders([new ValueCodecProvider(), new BsonValueCodecProvider()])
        def executor = new TestOperationExecutor([new MongoException('failure')])
        def pipeline = [new BsonDocument('$match', new BsonInt32(1))]
        def aggregationIterable = new AggregateIterableImpl(null, namespace, BsonDocument, BsonDocument, codecRegistry, readPreference,
                readConcern, writeConcern, executor, pipeline, AggregationLevel.COLLECTION, false, TIMEOUT_SETTINGS)

        when: 'The operation fails with an exception'
        aggregationIterable.iterator()

        then: 'the future should handle the exception'
        thrown(MongoException)

        when: 'toCollection should throw IllegalStateException when last state is not $out'
        aggregationIterable.toCollection()

        then:
        thrown(IllegalStateException)

        when: 'a codec is missing'
        new AggregateIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference, readConcern, writeConcern, executor,
                pipeline, AggregationLevel.COLLECTION, false, TIMEOUT_SETTINGS).iterator()

        then:
        thrown(CodecConfigurationException)

        when: 'pipeline contains null'
        new AggregateIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference, readConcern, writeConcern, executor,
                [null], AggregationLevel.COLLECTION, false, TIMEOUT_SETTINGS).iterator()

        then:
        thrown(IllegalArgumentException)
    }

    def 'should follow the MongoIterable interface as expected'() {
        given:
        def cannedResults = [new Document('_id', 1), new Document('_id', 2), new Document('_id', 3)]
        def cursor = {
            Stub(BatchCursor) {
                def count = 0
                def results
                def getResult = {
                    count++
                    results = count == 1 ? cannedResults : null
                    results
                }
                next() >> {
                    getResult()
                }
                hasNext() >> {
                    count == 0
                }
            }
        }
        def executor = new TestOperationExecutor([cursor(), cursor(), cursor(), cursor()])
        def mongoIterable = new AggregateIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference,
                readConcern, writeConcern, executor, [new Document('$match', 1)], AggregationLevel.COLLECTION, false,
                TIMEOUT_SETTINGS)

        when:
        def results = mongoIterable.first()

        then:
        results == cannedResults[0]

        when:
        def count = 0
        mongoIterable.forEach(new Consumer<Document>() {
            @Override
            void accept(Document document) {
                count++
            }
        })

        then:
        count == 3

        when:
        def target = []
        mongoIterable.into(target)

        then:
        target == cannedResults

        when:
        target = []
        mongoIterable.map(new Function<Document, Integer>() {
            @Override
            Integer apply(Document document) {
                document.getInteger('_id')
            }
        }).into(target)

        then:
        target == [1, 2, 3]
    }

    def 'should get and set batchSize as expected'() {
        when:
        def batchSize = 5
        def mongoIterable = new AggregateIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference,
                readConcern, writeConcern, Stub(OperationExecutor), [new Document('$match', 1)], AggregationLevel.COLLECTION,
                false, TIMEOUT_SETTINGS)

        then:
        mongoIterable.getBatchSize() == null

        when:
        mongoIterable.batchSize(batchSize)

        then:
        mongoIterable.getBatchSize() == batchSize
    }

}
