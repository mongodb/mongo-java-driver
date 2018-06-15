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

import com.mongodb.Block
import com.mongodb.Function
import com.mongodb.MongoException
import com.mongodb.MongoNamespace
import com.mongodb.ReadConcern
import com.mongodb.WriteConcern
import com.mongodb.client.model.Collation
import com.mongodb.client.model.changestream.ChangeStreamDocument
import com.mongodb.client.model.changestream.ChangeStreamDocumentCodec
import com.mongodb.client.model.changestream.ChangeStreamLevel
import com.mongodb.client.model.changestream.FullDocument
import com.mongodb.operation.BatchCursor
import com.mongodb.operation.ChangeStreamOperation
import com.mongodb.client.ClientSession
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonTimestamp
import org.bson.Document
import org.bson.RawBsonDocument
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
    def readConcern = ReadConcern.MAJORITY
    def writeConcern = WriteConcern.MAJORITY
    def collation = Collation.builder().locale('en').build()
    def startAtOperationTime = new BsonTimestamp(99)

    def 'should build the expected ChangeStreamOperation'() {
        given:
        def executor = new TestOperationExecutor([null, null, null, null, null])
        def pipeline = [new Document('$match', 1)]
        def changeStreamIterable = new ChangeStreamIterableImpl(null, namespace, codecRegistry, readPreference, readConcern,
                executor, pipeline, Document, ChangeStreamLevel.COLLECTION)

        when: 'default input should be as expected'
        changeStreamIterable.iterator()

        def codec = new ChangeStreamDocumentCodec(Document, codecRegistry)
        def operation = executor.getReadOperation() as ChangeStreamOperation<Document>
        def readPreference = executor.getReadPreference()

        then:
        expect operation, isTheSameAs(new ChangeStreamOperation<Document>(namespace, FullDocument.DEFAULT,
                [BsonDocument.parse('{$match: 1}')], codec, ChangeStreamLevel.COLLECTION))
        readPreference == secondary()

        when: 'overriding initial options'
        def resumeToken = RawBsonDocument.parse('{_id: {a: 1}}')
        changeStreamIterable.collation(collation).maxAwaitTime(99, MILLISECONDS)
                .fullDocument(FullDocument.UPDATE_LOOKUP).resumeAfter(resumeToken).startAtOperationTime(startAtOperationTime).iterator()

        operation = executor.getReadOperation() as ChangeStreamOperation<Document>

        then: 'should use the overrides'
        expect operation, isTheSameAs(new ChangeStreamOperation<Document>(namespace, FullDocument.UPDATE_LOOKUP,
                [BsonDocument.parse('{$match: 1}')], codec, ChangeStreamLevel.COLLECTION)
                .collation(collation).maxAwaitTime(99, MILLISECONDS)
                .resumeAfter(resumeToken).startAtOperationTime(startAtOperationTime))
    }

    def 'should use ClientSession'() {
        given:
        def batchCursor = Stub(BatchCursor) {
            _ * hasNext() >> { false }
        }
        def executor = new TestOperationExecutor([batchCursor, batchCursor])
        def changeStreamIterable = new ChangeStreamIterableImpl(clientSession, namespace, codecRegistry, readPreference, readConcern,
                executor, [], Document, ChangeStreamLevel.COLLECTION)

        when:
        changeStreamIterable.first()

        then:
        executor.getClientSession() == clientSession

        when:
        changeStreamIterable.iterator()

        then:
        executor.getClientSession() == clientSession

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should handle exceptions correctly'() {
        given:
        def altRegistry = fromProviders([new ValueCodecProvider(), new BsonValueCodecProvider()])
        def executor = new TestOperationExecutor([new MongoException('failure')])
        def pipeline = [new BsonDocument('$match', new BsonInt32(1))]
        def changeStreamIterable = new ChangeStreamIterableImpl(null, namespace, codecRegistry, readPreference, readConcern,
                executor, pipeline, BsonDocument, ChangeStreamLevel.COLLECTION)

        when: 'The operation fails with an exception'
        changeStreamIterable.iterator()

        then:
        thrown(MongoException)

        when: 'a codec is missing'
        new ChangeStreamIterableImpl(null, namespace, altRegistry, readPreference, readConcern, executor, pipeline, Document,
                ChangeStreamLevel.COLLECTION).iterator()

        then:
        thrown(CodecConfigurationException)

        when: 'pipeline contains null'
        new ChangeStreamIterableImpl(null, namespace, codecRegistry, readPreference, readConcern, executor, [null], Document,
                ChangeStreamLevel.COLLECTION).iterator()

        then:
        thrown(IllegalArgumentException)
    }

    def 'should follow the MongoIterable interface as expected'() {
        given:
        def count = 0
        def cannedResults = ['{_id: 1}', '{_id: 2}', '{_id: 3}'].collect {
            new ChangeStreamDocument(RawBsonDocument.parse(it), null, Document.parse(it), BsonDocument.parse(it), null, null)
        }
        def executor = new TestOperationExecutor([cursor(cannedResults), cursor(cannedResults), cursor(cannedResults),
                                                  cursor(cannedResults)])
        def mongoIterable = new ChangeStreamIterableImpl(null, namespace, codecRegistry, readPreference, readConcern, executor, [],
                Document, ChangeStreamLevel.COLLECTION)

        when:
        def results = mongoIterable.first()

        then:
        results == cannedResults[0]

        when:
        mongoIterable.forEach(new Block<ChangeStreamDocument<Document>>() {
            @Override
            void apply(ChangeStreamDocument<Document> result) {
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
        mongoIterable.map(new Function<ChangeStreamDocument<Document>, Integer>() {
            @Override
            Integer apply(ChangeStreamDocument<Document> document) {
                document.getFullDocument().getInteger('_id')
            }
        }).into(target)

        then:
        target == [1, 2, 3]
    }

    def 'should be able to return the raw results'() {
        given:
        def count = 0
        def cannedResults = ['{_id: 1}', '{_id: 2}', '{_id: 3}'].collect { RawBsonDocument.parse(it) }
        def executor = new TestOperationExecutor([cursor(cannedResults), cursor(cannedResults), cursor(cannedResults),
                                                  cursor(cannedResults)])
        def mongoIterable = new ChangeStreamIterableImpl(null, namespace, codecRegistry, readPreference, readConcern, executor, [],
                Document, ChangeStreamLevel.COLLECTION).withDocumentClass(RawBsonDocument)

        when:
        def results = mongoIterable.first()

        then:
        results == cannedResults[0]

        when:
        mongoIterable.forEach(new Block<RawBsonDocument>() {
            @Override
            void apply(final RawBsonDocument rawBsonDocument) {
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
        mongoIterable.map(new Function<BsonDocument, Integer>() {
            @Override
            Integer apply(BsonDocument document) {
                document.getInt32('_id').intValue()
            }
        }).into(target)

        then:
        target == [1, 2, 3]
    }


    def 'should get and set batchSize as expected'() {
        when:
        def batchSize = 5
        def mongoIterable = new ChangeStreamIterableImpl(null, namespace, codecRegistry, readPreference, readConcern,
                Stub(OperationExecutor), [BsonDocument.parse('{$match: 1}')], BsonDocument, ChangeStreamLevel.COLLECTION)

        then:
        mongoIterable.getBatchSize() == null

        when:
        mongoIterable.batchSize(batchSize)

        then:
        mongoIterable.getBatchSize() == batchSize
    }

    def cursor(List<?> cannedResults) {
        Stub(BatchCursor) {
            def counter = 0
            def results
            def getResult = {
                counter++
                results = counter == 1 ? cannedResults : null
                results
            }
            next() >> {
                getResult()
            }
            hasNext() >> {
                counter == 0
            }
        }
    }

}
