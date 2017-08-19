/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import com.mongodb.client.model.FullDocument
import com.mongodb.operation.AsyncOperationExecutor
import com.mongodb.operation.ChangeStreamOperation
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.Document
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.DocumentCodecProvider
import org.bson.codecs.ValueCodecProvider
import org.bson.codecs.configuration.CodecConfigurationException
import spock.lang.Specification

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.ReadPreference.secondary
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static org.bson.codecs.configuration.CodecRegistries.fromProviders
import static spock.util.matcher.HamcrestSupport.expect

class ChangeStreamIterableSpecification extends Specification {

    def namespace = new MongoNamespace('db', 'coll')
    def codecRegistry = fromProviders([new ValueCodecProvider(), new DocumentCodecProvider(), new BsonValueCodecProvider()])
    def readPreference = secondary()
    def readConcern = ReadConcern.DEFAULT
    def writeConcern = WriteConcern.MAJORITY
    def collation = Collation.builder().locale('en').build()

    def 'should build the expected ChangeStreamOperation'() {
        given:
        def cursor = Stub(AsyncBatchCursor) {
            next(_) >> {
                it[0].onResult(null, null)
            }
        }
        def executor = new TestOperationExecutor([cursor, cursor, cursor, cursor, cursor])
        def pipeline = [new Document('$match', 1)]
        def changeStreamIterable = new ChangeStreamIterableImpl(namespace, codecRegistry, readPreference, readConcern, executor, pipeline,
                Document)

        when: 'default input should be as expected'
        changeStreamIterable.into([]) { result, t -> }

        def operation = executor.getReadOperation() as ChangeStreamOperation<Document>
        def readPreference = executor.getReadPreference()

        then:
        expect operation, isTheSameAs(new ChangeStreamOperation<Document>(namespace, FullDocument.DEFAULT,
                [BsonDocument.parse('{$match: 1}')], codecRegistry.get(Document)))
        readPreference == secondary()

        when: 'overriding initial options'
        def resumeToken = BsonDocument.parse('{_id: {a: 1}}')
        changeStreamIterable.collation(collation).maxAwaitTime(99, MILLISECONDS)
                .fullDocument(FullDocument.UPDATE_LOOKUP).resumeAfter(resumeToken).into([]) { result, t -> }

        operation = executor.getReadOperation() as ChangeStreamOperation<Document>

        then: 'should use the overrides'
        expect operation, isTheSameAs(new ChangeStreamOperation<Document>(namespace, FullDocument.UPDATE_LOOKUP,
                [BsonDocument.parse('{$match: 1}')], codecRegistry.get(Document))
                .collation(collation).maxAwaitTime(99, MILLISECONDS)
                .resumeAfter(resumeToken))
    }

    def 'should handle exceptions correctly'() {
        given:
        def codecRegistry = fromProviders([new ValueCodecProvider(), new BsonValueCodecProvider()])
        def executor = new TestOperationExecutor([new MongoException('failure')])
        def pipeline = [new BsonDocument('$match', new BsonInt32(1))]
        def changeStreamIterable = new ChangeStreamIterableImpl(namespace, codecRegistry, readPreference, readConcern, executor, pipeline,
                BsonDocument)
        def futureResultCallback = new FutureResultCallback<List<BsonDocument>>()

        when: 'The operation fails with an exception'
        changeStreamIterable.into([], futureResultCallback)
        futureResultCallback.get()

        then:
        thrown(MongoException)

        when: 'a codec is missing'
        futureResultCallback = new FutureResultCallback<List<BsonDocument>>()
        new ChangeStreamIterableImpl(namespace, codecRegistry, readPreference, readConcern, executor, pipeline, Document)
                .into([], futureResultCallback)
        futureResultCallback.get()

        then:
        thrown(CodecConfigurationException)

        when: 'pipeline contains null'
        futureResultCallback = new FutureResultCallback<List<BsonDocument>>()
        new ChangeStreamIterableImpl(namespace, codecRegistry, readPreference, readConcern, executor, [null], Document)
                .into([], futureResultCallback)
        futureResultCallback.get()

        then:
        thrown(IllegalArgumentException)
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
        def mongoIterable = new ChangeStreamIterableImpl(namespace, codecRegistry, readPreference, readConcern, executor,
                [new BsonDocument('$match', new BsonInt32(1))], BsonDocument)

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
        def mongoIterable = new ChangeStreamIterableImpl(namespace, codecRegistry, readPreference, readConcern,
                Stub(AsyncOperationExecutor), [new BsonDocument('$match', new BsonInt32(1))], BsonDocument)
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
        mongoIterable = new ChangeStreamIterableImpl(namespace, codecRegistry, readPreference, readConcern,
                Stub(AsyncOperationExecutor), [null], BsonDocument)
        mongoIterable.into(target, results)
        results.get()

        then:
        thrown(IllegalArgumentException)
    }

}
