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
import com.mongodb.client.ClientSession
import com.mongodb.client.model.Collation
import com.mongodb.internal.operation.BatchCursor
import com.mongodb.internal.operation.DistinctOperation
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.Document
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.DocumentCodec
import org.bson.codecs.DocumentCodecProvider
import org.bson.codecs.ValueCodecProvider
import org.bson.codecs.configuration.CodecConfigurationException
import spock.lang.Specification

import java.util.function.Consumer

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.ReadPreference.secondary
import static com.mongodb.client.internal.TestHelper.CSOT_MAX_TIME
import static com.mongodb.client.internal.TestHelper.CSOT_NO_TIMEOUT
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static org.bson.codecs.configuration.CodecRegistries.fromProviders
import static spock.util.matcher.HamcrestSupport.expect

class DistinctIterableSpecification extends Specification {

    def namespace = new MongoNamespace('db', 'coll')
    def codecRegistry = fromProviders([new ValueCodecProvider(), new DocumentCodecProvider(), new BsonValueCodecProvider()])
    def readPreference = secondary()
    def readConcern = ReadConcern.MAJORITY
    def collation = Collation.builder().locale('en').build()

    def 'should build the expected DistinctOperation'() {
        given:
        def executor = new TestOperationExecutor([null, null]);
        def distinctIterable = new DistinctIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference, readConcern,
                executor, 'field', new BsonDocument(), true, null)

        when: 'default input should be as expected'
        distinctIterable.iterator()

        def operation = executor.getReadOperation() as DistinctOperation<Document>
        def readPreference = executor.getReadPreference()

        then:
        expect operation, isTheSameAs(new DistinctOperation<Document>(CSOT_NO_TIMEOUT, namespace, 'field', new DocumentCodec())
                .filter(new BsonDocument()).retryReads(true))
        readPreference == secondary()

        when: 'overriding initial options'
        distinctIterable.filter(new Document('field', 1)).maxTime(99, MILLISECONDS).batchSize(99).collation(collation).iterator()

        operation = executor.getReadOperation() as DistinctOperation<Document>

        then: 'should use the overrides'
        expect operation, isTheSameAs(new DistinctOperation<Document>(CSOT_MAX_TIME, namespace, 'field',
                new DocumentCodec())
                .filter(new BsonDocument('field', new BsonInt32(1))).collation(collation).retryReads(true))
    }

    def 'should use ClientSession'() {
        given:
        def batchCursor = Stub(BatchCursor) {
            _ * hasNext() >> { false }
        }
        def executor = new TestOperationExecutor([batchCursor, batchCursor]);
        def distinctIterable = new DistinctIterableImpl(clientSession, namespace, Document, Document, codecRegistry, readPreference,
                readConcern, executor, 'field', new BsonDocument(), true, null)

        when:
        distinctIterable.first()

        then:
        executor.getClientSession() == clientSession

        when:
        distinctIterable.iterator()

        then:
        executor.getClientSession() == clientSession

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should handle exceptions correctly'() {
        given:
        def codecRegistry = fromProviders([new ValueCodecProvider(), new BsonValueCodecProvider()])
        def executor = new TestOperationExecutor([new MongoException('failure')])
        def distinctIterable = new DistinctIterableImpl(null, namespace, Document, BsonDocument, codecRegistry, readPreference,
                readConcern, executor, 'field', new BsonDocument(), true, null)

        when: 'The operation fails with an exception'
        distinctIterable.iterator()

        then: 'the future should handle the exception'
        thrown(MongoException)

        when: 'a codec is missing'
        distinctIterable.filter(new Document('field', 1)).iterator()
        then:
        thrown(CodecConfigurationException)
    }

    def 'should follow the MongoIterable interface as expected'() {
        given:
        def cannedResults = [new Document('_id', 1), new Document('_id', 2), new Document('_id', 3)]
        def cursor = {
            Stub(BatchCursor) {
                def count = 0
                def results;
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
        def executor = new TestOperationExecutor([cursor(), cursor(), cursor(), cursor()]);
        def mongoIterable = new DistinctIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference, ReadConcern.LOCAL,
                executor, 'field', new BsonDocument(), true, null)

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
        def mongoIterable = new DistinctIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference, readConcern,
                Stub(OperationExecutor), 'field', new BsonDocument(), true, null)

        then:
        mongoIterable.getBatchSize() == null

        when:
        mongoIterable.batchSize(batchSize)

        then:
        mongoIterable.getBatchSize() == batchSize
    }
}
