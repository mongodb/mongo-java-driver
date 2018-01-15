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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.async.client

import com.mongodb.Block
import com.mongodb.CursorType
import com.mongodb.Function
import com.mongodb.MongoNamespace
import com.mongodb.ReadConcern
import com.mongodb.async.AsyncBatchCursor
import com.mongodb.async.FutureResultCallback
import com.mongodb.async.SingleResultCallback
import com.mongodb.client.model.Collation
import com.mongodb.client.model.FindOptions
import com.mongodb.operation.AsyncOperationExecutor
import com.mongodb.operation.FindOperation
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.Document
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.DocumentCodec
import org.bson.codecs.DocumentCodecProvider
import org.bson.codecs.ValueCodecProvider
import spock.lang.Specification

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.ReadPreference.secondary
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.SECONDS
import static org.bson.codecs.configuration.CodecRegistries.fromProviders
import static spock.util.matcher.HamcrestSupport.expect

class FindIterableSpecification extends Specification {

    def namespace = new MongoNamespace('db', 'coll')
    def codecRegistry = fromProviders([new ValueCodecProvider(), new DocumentCodecProvider(), new BsonValueCodecProvider()])
    def readPreference = secondary()
    def readConcern = ReadConcern.DEFAULT
    def collation = Collation.builder().locale('en').build()

    @SuppressWarnings('deprecation')
    def 'should build the expected findOperation'() {
        given:
        def cursor = Stub(AsyncBatchCursor) {
            next(_) >> {
                it[0].onResult(null, null)
            }
        }
        def executor = new TestOperationExecutor([cursor, cursor]);
        def findOptions = new FindOptions().sort(new Document('sort', 1))
                .modifiers(new Document('modifier', 1))
                .projection(new Document('projection', 1))
                .maxTime(10, SECONDS)
                .maxAwaitTime(20, SECONDS)
                .batchSize(100)
                .limit(100)
                .skip(10)
                .cursorType(CursorType.NonTailable)
                .oplogReplay(false)
                .noCursorTimeout(false)
                .partial(false)
                .collation(null)
                .comment('my comment')
                .hint(new Document('hint', 1))
                .min(new Document('min', 1))
                .max(new Document('max', 1))
                .maxScan(42L)
                .returnKey(false)
                .showRecordId(false)
                .snapshot(false)
        def findIterable = new FindIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference, readConcern, executor,
                new Document('filter', 1), findOptions)

        when: 'default input should be as expected'
        findIterable.into([]) { result, t -> }

        def operation = executor.getReadOperation() as FindOperation<Document>
        def readPreference = executor.getReadPreference()

        then:
        expect operation, isTheSameAs(new FindOperation<Document>(namespace, new DocumentCodec())
                .filter(new BsonDocument('filter', new BsonInt32(1)))
                .sort(new BsonDocument('sort', new BsonInt32(1)))
                .modifiers(new BsonDocument('modifier', new BsonInt32(1)))
                .projection(new BsonDocument('projection', new BsonInt32(1)))
                .maxTime(10000, MILLISECONDS)
                .maxAwaitTime(20000, MILLISECONDS)
                .batchSize(100)
                .limit(100)
                .skip(10)
                .cursorType(CursorType.NonTailable)
                .slaveOk(true)
                .comment('my comment')
                .hint(new BsonDocument('hint', new BsonInt32(1)))
                .min(new BsonDocument('min', new BsonInt32(1)))
                .max(new BsonDocument('max', new BsonInt32(1)))
                .maxScan(42L)
                .returnKey(false)
                .showRecordId(false)
                .snapshot(false)
        )
        readPreference == secondary()

        when: 'overriding initial options'
        findIterable.filter(new Document('filter', 2))
                .sort(new Document('sort', 2))
                .modifiers(new Document('modifier', 2))
                .projection(new Document('projection', 2))
                .maxTime(9, SECONDS)
                .maxAwaitTime(18, SECONDS)
                .batchSize(99)
                .limit(99)
                .skip(9)
                .cursorType(CursorType.Tailable)
                .oplogReplay(true)
                .noCursorTimeout(true)
                .partial(true)
                .collation(collation)
                .comment('alt comment')
                .hint(new Document('hint', 2))
                .min(new Document('min', 2))
                .max(new Document('max', 2))
                .maxScan(88L)
                .returnKey(true)
                .showRecordId(true)
                .snapshot(true)
                .into([]) { result, t -> }

        operation = executor.getReadOperation() as FindOperation<Document>

        then: 'should use the overrides'
        expect operation, isTheSameAs(new FindOperation<Document>(namespace, new DocumentCodec())
                .filter(new BsonDocument('filter', new BsonInt32(2)))
                .sort(new BsonDocument('sort', new BsonInt32(2)))
                .modifiers(new BsonDocument('modifier', new BsonInt32(2)))
                .projection(new BsonDocument('projection', new BsonInt32(2)))
                .maxTime(9000, MILLISECONDS)
                .maxAwaitTime(18000, MILLISECONDS)
                .batchSize(99)
                .limit(99)
                .skip(9)
                .cursorType(CursorType.Tailable)
                .oplogReplay(true)
                .noCursorTimeout(true)
                .partial(true)
                .slaveOk(true)
                .collation(collation)
                .comment('alt comment')
                .hint(new BsonDocument('hint', new BsonInt32(2)))
                .min(new BsonDocument('min', new BsonInt32(2)))
                .max(new BsonDocument('max', new BsonInt32(2)))
                .maxScan(88L)
                .returnKey(true)
                .showRecordId(true)
                .snapshot(true)
        )
    }

    def 'should handle mixed types'() {
        given:
        def cursor = Stub(AsyncBatchCursor) {
            next(_) >> {
                it[0].onResult(null, null)
            }
        }
        def executor = new TestOperationExecutor([cursor]);
        def findOptions = new FindOptions()
        def findIterable = new FindIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference, readConcern, executor,
                new Document('filter', 1), findOptions)

        when:
        findIterable.filter(new Document('filter', 1))
                .sort(new BsonDocument('sort', new BsonInt32(1)))
                .modifiers(new Document('modifier', 1))
                .batchSize(100)
                .into([]) { result, t -> }

        def operation = executor.getReadOperation() as FindOperation<Document>

        then:
        expect operation, isTheSameAs(new FindOperation<Document>(namespace, new DocumentCodec())
                .filter(new BsonDocument('filter', new BsonInt32(1)))
                .sort(new BsonDocument('sort', new BsonInt32(1)))
                .modifiers(new BsonDocument('modifier', new BsonInt32(1)))
                .cursorType(CursorType.NonTailable)
                .slaveOk(true)
                .batchSize(100)
        )
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
        def findOptions = new FindOptions()
        def mongoIterable = new FindIterableImpl(null, new MongoNamespace('db', 'coll'), Document, Document, codecRegistry,
                readPreference, readConcern, executor, new Document(), findOptions)

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
        def mongoIterable = new FindIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference,
                readConcern, Stub(AsyncOperationExecutor), new Document(), new FindOptions())
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
    }

    def 'should get and set batchSize as expected'() {
        when:
        def batchSize = 5
        def mongoIterable = new FindIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference,
                readConcern, Stub(AsyncOperationExecutor), new Document(), new FindOptions())

        then:
        mongoIterable.getBatchSize() == 0

        when:
        mongoIterable.batchSize(batchSize)

        then:
        mongoIterable.getBatchSize() == batchSize
    }
}
