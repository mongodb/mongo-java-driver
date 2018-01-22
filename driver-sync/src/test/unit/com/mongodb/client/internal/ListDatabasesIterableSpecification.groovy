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

package com.mongodb.client.internal

import com.mongodb.Block
import com.mongodb.Function
import com.mongodb.operation.BatchCursor
import com.mongodb.operation.ListDatabasesOperation
import org.bson.BsonDocument
import org.bson.Document
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.DocumentCodec
import org.bson.codecs.DocumentCodecProvider
import org.bson.codecs.ValueCodecProvider
import spock.lang.Specification

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.ReadPreference.secondary
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static org.bson.codecs.configuration.CodecRegistries.fromProviders
import static spock.util.matcher.HamcrestSupport.expect

class ListDatabasesIterableSpecification extends Specification {

    def codecRegistry = fromProviders([new ValueCodecProvider(), new DocumentCodecProvider(),
                                       new BsonValueCodecProvider()])
    def readPreference = secondary()

    def 'should build the expected listCollectionOperation'() {
        given:
        def executor = new TestOperationExecutor([null, null]);
        def listDatabaseIterable = new ListDatabasesIterableImpl<Document>(null, Document, codecRegistry, readPreference, executor)
                .maxTime(1000, MILLISECONDS)

        when: 'default input should be as expected'
        listDatabaseIterable.iterator()

        def operation = executor.getReadOperation() as ListDatabasesOperation<Document>
        def readPreference = executor.getReadPreference()

        then:
        expect operation, isTheSameAs(new ListDatabasesOperation<Document>(new DocumentCodec()).maxTime(1000, MILLISECONDS))
        readPreference == secondary()

        when: 'overriding initial options'
        listDatabaseIterable.maxTime(999, MILLISECONDS).filter(Document.parse('{a: 1}')).nameOnly(true).iterator()

        operation = executor.getReadOperation() as ListDatabasesOperation<Document>

        then: 'should use the overrides'
        expect operation, isTheSameAs(new ListDatabasesOperation<Document>(new DocumentCodec()).maxTime(999, MILLISECONDS)
                .filter(BsonDocument.parse('{a: 1}')).nameOnly(true))
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
        def mongoIterable = new ListDatabasesIterableImpl<Document>(null, Document, codecRegistry, readPreference, executor)

        when:
        def results = mongoIterable.first()

        then:
        results == cannedResults[0]

        when:
        def count = 0
        mongoIterable.forEach(new Block<Document>() {
            @Override
            void apply(Document document) {
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
        def mongoIterable = new ListDatabasesIterableImpl<Document>(null, Document, codecRegistry, readPreference,
                Stub(OperationExecutor))

        then:
        mongoIterable.getBatchSize() == null

        when:
        mongoIterable.batchSize(batchSize)

        then:
        mongoIterable.getBatchSize() == batchSize
    }
}
