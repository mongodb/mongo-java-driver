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
import com.mongodb.client.ClientSession
import com.mongodb.internal.operation.BatchCursor
import com.mongodb.internal.operation.ListCollectionsOperation
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.Document
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.DocumentCodec
import org.bson.codecs.DocumentCodecProvider
import org.bson.codecs.ValueCodecProvider
import spock.lang.Specification

import java.util.function.Consumer

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.ReadPreference.secondary
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static org.bson.codecs.configuration.CodecRegistries.fromProviders
import static spock.util.matcher.HamcrestSupport.expect

class ListCollectionsIterableSpecification extends Specification {

    def codecRegistry = fromProviders([new ValueCodecProvider(), new DocumentCodecProvider(),
                                       new BsonValueCodecProvider()])
    def readPreference = secondary()

    def 'should build the expected listCollectionOperation'() {
        given:
        def executor = new TestOperationExecutor([null, null, null]);
        def listCollectionIterable = new ListCollectionsIterableImpl<Document>(null, 'db', false, Document, codecRegistry,
                readPreference, executor)
                .filter(new Document('filter', 1))
                .batchSize(100)
                .maxTime(1000, MILLISECONDS)
        def listCollectionNamesIterable = new ListCollectionsIterableImpl<Document>(null, 'db', true, Document, codecRegistry,
                readPreference, executor)

        when: 'default input should be as expected'
        listCollectionIterable.iterator()

        def operation = executor.getReadOperation() as ListCollectionsOperation<Document>
        def readPreference = executor.getReadPreference()

        then:
        expect operation, isTheSameAs(new ListCollectionsOperation<Document>('db', new DocumentCodec())
                .filter(new BsonDocument('filter', new BsonInt32(1))).batchSize(100).maxTime(1000, MILLISECONDS)
                .retryReads(true))
        readPreference == secondary()

        when: 'overriding initial options'
        listCollectionIterable.filter(new Document('filter', 2)).batchSize(99).maxTime(999, MILLISECONDS).iterator()

        operation = executor.getReadOperation() as ListCollectionsOperation<Document>

        then: 'should use the overrides'
        expect operation, isTheSameAs(new ListCollectionsOperation<Document>('db', new DocumentCodec())
                .filter(new BsonDocument('filter', new BsonInt32(2))).batchSize(99).maxTime(999, MILLISECONDS)
                .retryReads(true))

        when: 'requesting collection names only'
        listCollectionNamesIterable.iterator()

        operation = executor.getReadOperation() as ListCollectionsOperation<Document>

        then: 'should create operation with nameOnly'
        expect operation, isTheSameAs(new ListCollectionsOperation<Document>('db', new DocumentCodec()).nameOnly(true)
                .retryReads(true))
    }

    def 'should use ClientSession'() {
        given:
        def batchCursor = Stub(BatchCursor) {
            _ * hasNext() >> { false }
        }
        def executor = new TestOperationExecutor([batchCursor, batchCursor]);
        def listCollectionIterable = new ListCollectionsIterableImpl<Document>(clientSession, 'db', false, Document, codecRegistry,
                readPreference, executor)

        when:
        listCollectionIterable.first()

        then:
        executor.getClientSession() == clientSession

        when:
        listCollectionIterable.iterator()

        then:
        executor.getClientSession() == clientSession

        where:
        clientSession << [null, Stub(ClientSession)]
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
        def mongoIterable = new ListCollectionsIterableImpl<Document>(null, 'db', false, Document, codecRegistry, readPreference,
                executor)

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
        def mongoIterable = new ListCollectionsIterableImpl<Document>(null, 'db', false, Document, codecRegistry, readPreference,
                Stub(OperationExecutor))

        then:
        mongoIterable.getBatchSize() == null

        when:
        mongoIterable.batchSize(batchSize)

        then:
        mongoIterable.getBatchSize() == batchSize
    }
}
