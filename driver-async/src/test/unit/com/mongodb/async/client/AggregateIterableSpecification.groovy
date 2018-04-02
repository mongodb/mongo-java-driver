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

package com.mongodb.async.client

import com.mongodb.Block
import com.mongodb.Function
import com.mongodb.MongoException
import com.mongodb.MongoNamespace
import com.mongodb.ReadConcern
import com.mongodb.WriteConcern
import com.mongodb.async.AsyncBatchCursor
import com.mongodb.async.FutureResultCallback
import com.mongodb.async.SingleResultCallback
import com.mongodb.client.model.Collation
import com.mongodb.operation.AggregateOperation
import com.mongodb.operation.AggregateToCollectionOperation
import com.mongodb.operation.FindOperation
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

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.ReadPreference.secondary
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static org.bson.codecs.configuration.CodecRegistries.fromProviders
import static spock.util.matcher.HamcrestSupport.expect

class AggregateIterableSpecification extends Specification {

    def namespace = new MongoNamespace('db', 'coll')
    def codecRegistry = fromProviders([new ValueCodecProvider(), new DocumentCodecProvider(), new BsonValueCodecProvider()])
    def readPreference = secondary()
    def readConcern = ReadConcern.DEFAULT
    def writeConcern = WriteConcern.MAJORITY
    def collation = Collation.builder().locale('en').build()

    def 'should build the expected AggregationOperation'() {
        given:
        def cursor = Stub(AsyncBatchCursor) {
            next(_) >> {
                it[0].onResult(null, null)
            }
        }
        def executor = new TestOperationExecutor([cursor, cursor, cursor, cursor, cursor]);
        def pipeline = [new Document('$match', 1)]
        def aggregationIterable = new AggregateIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference, readConcern,
                writeConcern, executor, pipeline)

        when: 'default input should be as expected'
        aggregationIterable.into([]) { result, t -> }

        def operation = executor.getReadOperation() as AggregateOperation<Document>
        def readPreference = executor.getReadPreference()

        then:
        expect operation, isTheSameAs(new AggregateOperation<Document>(namespace, [new BsonDocument('$match', new BsonInt32(1))],
                new DocumentCodec()));
        readPreference == secondary()

        when: 'overriding initial options'
        aggregationIterable
                .maxAwaitTime(99, MILLISECONDS)
                .maxTime(999, MILLISECONDS)
                .collation(collation)
                .hint(new Document('a', 1))
                .comment('this is a comment')
                .useCursor(true)
                .into([]) { result, t -> }

        operation = executor.getReadOperation() as AggregateOperation<Document>

        then: 'should use the overrides'
        expect operation, isTheSameAs(new AggregateOperation<Document>(namespace, [new BsonDocument('$match', new BsonInt32(1))],
                new DocumentCodec())
                .maxAwaitTime(99, MILLISECONDS)
                .maxTime(999, MILLISECONDS)
                .useCursor(true)
                .collation(collation)
                .hint(new BsonDocument('a', new BsonInt32(1)))
                .comment('this is a comment')
        )
    }

    def 'should build the expected AggregateToCollectionOperation'() {
        given:
        def cursor = Stub(AsyncBatchCursor) {
            next(_) >> {
                it[0].onResult(null, null)
            }
        }
        def executor = new TestOperationExecutor([cursor, cursor, cursor, cursor, cursor]);
        def collectionName = 'collectionName'
        def collectionNamespace = new MongoNamespace(namespace.getDatabaseName(), collectionName)
        def pipeline = [new Document('$match', 1), new Document('$out', collectionName)]

        when: 'aggregation includes $out'
        new AggregateIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference, readConcern, writeConcern, executor,
                pipeline)
                .batchSize(99)
                .maxAwaitTime(99, MILLISECONDS)
                .maxTime(999, MILLISECONDS)
                .allowDiskUse(true)
                .useCursor(true)
                .collation(collation)
                .hint(new Document('a', 1))
                .comment('this is a comment')
                .into([]) { result, t -> }

        def operation = executor.getReadOperation() as WriteOperationThenCursorReadOperation

        then: 'should use the overrides'
        expect operation.getAggregateToCollectionOperation(), isTheSameAs(new AggregateToCollectionOperation(namespace,
                [new BsonDocument('$match', new BsonInt32(1)), new BsonDocument('$out', new BsonString(collectionName))], writeConcern)
                .maxTime(999, MILLISECONDS)
                .allowDiskUse(true)
                .collation(collation)
                .hint(new BsonDocument('a', new BsonInt32(1)))
                .comment('this is a comment'))

        when: 'the subsequent read should have the batchSize set'
        operation = operation.getReadOperation() as FindOperation

        then: 'should use the correct settings'
        operation.getNamespace() == collectionNamespace
        operation.getCollation() == collation
        operation.getBatchSize() == 99
        operation.getMaxAwaitTime(MILLISECONDS) == 0
        operation.getMaxTime(MILLISECONDS) == 0

        when: 'toCollection should work as expected'
        def futureResultCallback = new FutureResultCallback()
        new AggregateIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference, readConcern, writeConcern, executor,
                pipeline)
                .allowDiskUse(true)
                .collation(collation)
                .hint(new Document('a', 1))
                .comment('this is a comment')
                .toCollection(futureResultCallback);
        futureResultCallback.get()

        operation = executor.getWriteOperation() as AggregateToCollectionOperation

        then:
        expect operation, isTheSameAs(new AggregateToCollectionOperation(namespace,
                [new BsonDocument('$match', new BsonInt32(1)), new BsonDocument('$out', new BsonString(collectionName))], writeConcern)
                .allowDiskUse(true)
                .collation(collation)
                .hint(new BsonDocument('a', new BsonInt32(1)))
                .comment('this is a comment'))
    }

    def 'should handle exceptions correctly'() {
        given:
        def codecRegistry = fromProviders([new ValueCodecProvider(), new BsonValueCodecProvider()])
        def executor = new TestOperationExecutor([new MongoException('failure')])
        def pipeline = [new BsonDocument('$match', new BsonInt32(1))]
        def aggregationIterable = new AggregateIterableImpl(null, namespace, Document, BsonDocument, codecRegistry, readPreference,
                readConcern, writeConcern, executor, pipeline)

        def futureResultCallback = new FutureResultCallback<List<BsonDocument>>()

        when: 'The operation fails with an exception'
        aggregationIterable.into([], futureResultCallback)
        futureResultCallback.get()

        then: 'the future should handle the exception'
        thrown(MongoException)

        when: 'toCollection should throw IllegalStateException when last state is not $out'
        aggregationIterable.toCollection(new FutureResultCallback())

        then:
        thrown(IllegalStateException)

        when: 'a codec is missing'
        futureResultCallback = new FutureResultCallback<List<BsonDocument>>()
        new AggregateIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference,  readConcern, writeConcern, executor,
                pipeline)
                .into([], futureResultCallback)
        futureResultCallback.get()

        then:
        thrown(CodecConfigurationException)
    }

    def 'should follow the MongoIterable interface as expected'() {
        given:
        def cannedResults = [new Document('_id', 1), new Document('_id', 1), new Document('_id', 1)]
        def cursor = {
            Stub(AsyncBatchCursor) {
                def count = 0
                def results;
                def getResult = {
                    count++
                    results = count == 1 ? cannedResults : null
                    results
                }
                next(_) >> {
                    it[0].onResult(getResult(), null)
                }
                isClosed() >> { count >= 1 }
            }
        }
        def executor = new TestOperationExecutor([cursor(), cursor(), cursor(), cursor(), cursor()]);
        def mongoIterable = new AggregateIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference,
                readConcern, writeConcern, executor, [new BsonDocument('$match', new BsonInt32(1))])

        when:
        def results = new FutureResultCallback()
        mongoIterable.first(results)

        then:
        results.get() == cannedResults[0]

        when:
        def count = 0
        results = new FutureResultCallback()
        mongoIterable.forEach(new Block<Document>() {
            @Override
            void apply(Document document) {
                count++
            }
        }, results)
        results.get()

        then:
        count == 3

        when:
        def target = []
        results = new FutureResultCallback()
        mongoIterable.into(target, results)

        then:
        results.get() == cannedResults

        when:
        target = []
        results = new FutureResultCallback()
        mongoIterable.map(new Function<Document, Integer>() {
            @Override
            Integer apply(Document document) {
                document.getInteger('_id')
            }
        }).into(target, results)
        then:
        results.get() == [1, 1, 1]

        when:
        results = new FutureResultCallback()
        mongoIterable.batchCursor(results)
        def batchCursor = results.get()

        then:
        !batchCursor.isClosed()

        when:
        results = new FutureResultCallback()
        batchCursor.next(results)

        then:
        results.get() == cannedResults
        batchCursor.isClosed()
    }

    def 'should check variables using notNull'() {
        given:
        def mongoIterable = new AggregateIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference,
                readConcern, writeConcern, Stub(OperationExecutor), [Document.parse('{$match: 1}')])
        def callback = Stub(SingleResultCallback)
        def block = Stub(Block)
        def target = Stub(List)

        when:
        mongoIterable.first(null)

        then:
        thrown(IllegalArgumentException)

        when:
        mongoIterable.into(null, callback)

        then:
        thrown(IllegalArgumentException)

        when:
        mongoIterable.into(target, null)

        then:
        thrown(IllegalArgumentException)

        when:
        mongoIterable.forEach(null, callback)

        then:
        thrown(IllegalArgumentException)

        when:
        mongoIterable.forEach(block, null)

        then:
        thrown(IllegalArgumentException)

        when:
        mongoIterable.map()

        then:
        thrown(IllegalArgumentException)

        when:
        def results = new FutureResultCallback()
        mongoIterable = new AggregateIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference,
                readConcern, writeConcern, Stub(OperationExecutor), [null])
        mongoIterable.into(target, results)
        results.get()

        then:
        thrown(IllegalArgumentException)
    }

    def 'should get and set batchSize as expected'() {
        when:
        def batchSize = 5
        def mongoIterable = new AggregateIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference,
                readConcern, writeConcern, Stub(OperationExecutor), [null])

        then:
        mongoIterable.getBatchSize() == null

        when:
        mongoIterable.batchSize(batchSize)

        then:
        mongoIterable.getBatchSize() == batchSize
    }

}
