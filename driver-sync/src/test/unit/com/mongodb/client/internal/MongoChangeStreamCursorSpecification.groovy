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

import com.mongodb.ServerAddress
import com.mongodb.ServerCursor
import com.mongodb.internal.operation.AggregateResponseBatchCursor
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.RawBsonDocument
import org.bson.codecs.Decoder
import org.bson.codecs.RawBsonDocumentCodec
import spock.lang.Specification

class MongoChangeStreamCursorSpecification extends Specification {
    def 'should get server cursor and address'() {
        given:
        def batchCursor = Stub(AggregateResponseBatchCursor)
        def decoder = Mock(Decoder)
        def resumeToken = Mock(BsonDocument)
        def address = new ServerAddress('host', 27018)
        def serverCursor = new ServerCursor(5, address)
        batchCursor.getServerAddress() >> address
        batchCursor.getServerCursor() >> serverCursor
        def cursor = new MongoChangeStreamCursorImpl(batchCursor, decoder, resumeToken)

        expect:
        cursor.serverAddress.is(address)
        cursor.serverCursor.is(serverCursor)
    }

    def 'should throw on remove'() {
        given:
        def batchCursor = Stub(AggregateResponseBatchCursor)
        def decoder = Mock(Decoder)
        def resumeToken = Mock(BsonDocument)
        def cursor = new MongoChangeStreamCursorImpl(batchCursor, decoder, resumeToken)

        when:
        cursor.remove()

        then:
        thrown(UnsupportedOperationException)
    }

    def 'should close batch cursor'() {
        given:
        def batchCursor = Mock(AggregateResponseBatchCursor)
        def decoder = Mock(Decoder)
        def resumeToken = Mock(BsonDocument)
        def cursor = new MongoChangeStreamCursorImpl(batchCursor, decoder, resumeToken)

        when:
        cursor.close()

        then:
        1 * batchCursor.close()
    }

    def 'next should throw if there is no next'() {
        given:
        def batchCursor = Stub(AggregateResponseBatchCursor)
        def codec = new RawBsonDocumentCodec();
        def resumeToken = Mock(BsonDocument)

        batchCursor.hasNext() >> false

        def cursor = new MongoChangeStreamCursorImpl(batchCursor, codec, resumeToken)

        when:
        cursor.next()

        then:
        thrown(NoSuchElementException)
    }


    def 'should get next from batch cursor'() {
        given:
        def firstBatch = [RawBsonDocument.parse('{ _id: { _data: 1 }, x: 1 }'),
                          RawBsonDocument.parse('{ _id: { _data: 2 }, x: 1 }')]
        def secondBatch = [RawBsonDocument.parse('{ _id: { _data: 3 }, x: 2 }')]

        def batchCursor = Stub(AggregateResponseBatchCursor)
        def codec = new RawBsonDocumentCodec();
        def resumeToken = Mock(BsonDocument)

        batchCursor.hasNext() >>> [true, true, true, true, false]
        batchCursor.next() >>> [firstBatch, secondBatch]

        def cursor = new MongoChangeStreamCursorImpl(batchCursor, codec, resumeToken)

        expect:
        cursor.hasNext()
        cursor.next() == firstBatch[0]
        cursor.hasNext()
        cursor.next() == firstBatch[1]
        cursor.hasNext()
        cursor.next() == secondBatch[0]
        !cursor.hasNext()
    }

    def 'should try next from batch cursor'() {
        given:
        def firstBatch = [RawBsonDocument.parse('{ _id: { _data: 1 }, x: 1 }'),
                          RawBsonDocument.parse('{ _id: { _data: 2 }, x: 1 }')]
        def secondBatch = [RawBsonDocument.parse('{ _id: { _data: 3 }, x: 2 }')]

        def batchCursor = Stub(AggregateResponseBatchCursor)
        def codec = new RawBsonDocumentCodec();
        def resumeToken = Mock(BsonDocument)

        batchCursor.tryNext() >>> [firstBatch, null, secondBatch, null]

        def cursor = new MongoChangeStreamCursorImpl(batchCursor, codec, resumeToken)

        expect:
        cursor.tryNext() == firstBatch[0]
        cursor.tryNext() == firstBatch[1]
        cursor.tryNext() == null
        cursor.tryNext() == secondBatch[0]
        cursor.tryNext() == null
    }

    def 'should get cached resume token after next'() {
        given:
        def firstBatch = [RawBsonDocument.parse('{ _id: { _data: 1 }, x: 1 }'),
                          RawBsonDocument.parse('{ _id: { _data: 2 }, x: 1 }')]
        def secondBatch = [RawBsonDocument.parse('{ _id: { _data: 3 }, x: 2 }')]

        def batchCursor = Stub(AggregateResponseBatchCursor)
        def codec = new RawBsonDocumentCodec();
        def resumeToken = new BsonDocument('_data', new BsonInt32(1))

        batchCursor.hasNext() >>> [true, true, true, false]
        batchCursor.next() >>> [firstBatch, secondBatch]
        batchCursor.getPostBatchResumeToken() >>> [new BsonDocument('_data', new BsonInt32(2)),
                                                   new BsonDocument('_data', new BsonInt32(2)),
                                                   new BsonDocument('_data', new BsonInt32(3)),
                                                   new BsonDocument('_data', new BsonInt32(3))]

        def cursor = new MongoChangeStreamCursorImpl(batchCursor, codec, resumeToken)

        expect:
        cursor.getResumeToken() == resumeToken
        cursor.next()
        cursor.getResumeToken() == new BsonDocument('_data', new BsonInt32(1))
        cursor.next()
        cursor.getResumeToken() == new BsonDocument('_data', new BsonInt32(2))
        cursor.next()
        cursor.getResumeToken() == new BsonDocument('_data', new BsonInt32(3))
    }

    def 'should get cached resume token after tryNext'() {
        given:
        def firstBatch = [RawBsonDocument.parse('{ _id: { _data: 1 }, x: 1 }'),
                          RawBsonDocument.parse('{ _id: { _data: 2 }, x: 1 }')]
        def secondBatch = [RawBsonDocument.parse('{ _id: { _data: 3 }, x: 2 }')]

        def batchCursor = Stub(AggregateResponseBatchCursor)
        def codec = new RawBsonDocumentCodec();
        def resumeToken = new BsonDocument('_data', new BsonInt32(1))

        batchCursor.hasNext() >>> [true, true, true, false]
        batchCursor.tryNext() >>> [firstBatch, null, secondBatch, null]
        batchCursor.getPostBatchResumeToken() >>> [new BsonDocument('_data', new BsonInt32(2)),
                                                   new BsonDocument('_data', new BsonInt32(2)),
                                                   new BsonDocument('_data', new BsonInt32(2)),
                                                   new BsonDocument('_data', new BsonInt32(2)),
                                                   new BsonDocument('_data', new BsonInt32(3)),
                                                   new BsonDocument('_data', new BsonInt32(3)),
                                                   new BsonDocument('_data', new BsonInt32(3))]

        def cursor = new MongoChangeStreamCursorImpl(batchCursor, codec, resumeToken)

        expect:
        cursor.getResumeToken() == resumeToken
        cursor.tryNext()
        cursor.getResumeToken() == new BsonDocument('_data', new BsonInt32(1))
        cursor.tryNext()
        cursor.getResumeToken() == new BsonDocument('_data', new BsonInt32(2))
        !cursor.tryNext()
        cursor.getResumeToken() == new BsonDocument('_data', new BsonInt32(2))
        cursor.tryNext()
        cursor.getResumeToken() == new BsonDocument('_data', new BsonInt32(3))
        !cursor.tryNext()
        cursor.getResumeToken() == new BsonDocument('_data', new BsonInt32(3))
    }
}
