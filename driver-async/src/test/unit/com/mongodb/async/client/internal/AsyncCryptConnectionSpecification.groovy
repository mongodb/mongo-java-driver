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

package com.mongodb.async.client.internal

import com.mongodb.ReadPreference
import com.mongodb.ServerAddress
import com.mongodb.async.SingleResultCallback
import com.mongodb.connection.AsyncConnection
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ConnectionId
import com.mongodb.connection.ServerId
import com.mongodb.connection.SplittablePayload
import com.mongodb.internal.connection.NoOpSessionContext
import com.mongodb.internal.validator.NoOpFieldNameValidator
import org.bson.BsonArray
import org.bson.BsonBinary
import org.bson.BsonBinaryReader
import org.bson.BsonBinaryWriter
import org.bson.BsonDocument
import org.bson.BsonDocumentWrapper
import org.bson.BsonInt32
import org.bson.BsonString
import org.bson.Document
import org.bson.RawBsonDocument
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.DecoderContext
import org.bson.codecs.DocumentCodec
import org.bson.codecs.EncoderContext
import org.bson.codecs.RawBsonDocumentCodec
import org.bson.io.BasicOutputBuffer
import spock.lang.Specification

import static com.mongodb.connection.ServerType.STANDALONE
import static com.mongodb.connection.SplittablePayload.Type.INSERT

class AsyncCryptConnectionSpecification extends Specification {

    def 'should encrypt and decrypt a command'() {
        given:
        def callback = Mock(SingleResultCallback)
        def wrappedConnection = Mock(AsyncConnection)
        def crypt = Mock(Crypt)
        def cryptConnection = new AsyncCryptConnection(wrappedConnection, crypt)
        def codec = new DocumentCodec()
        def encryptedCommand = toRaw(new BsonDocument('find', new BsonString('test'))
                .append('ssid', new BsonBinary(6 as byte, new byte[10])))

        def encryptedResponse = toRaw(new BsonDocument('ok', new BsonInt32(1))
                .append('cursor',
                        new BsonDocument('firstBatch',
                                new BsonArray([new BsonDocument('_id', new BsonInt32(1))
                                                       .append('ssid', new BsonBinary(6 as byte, new byte[10]))]))))

        def decryptedResponse = toRaw(new BsonDocument('ok', new BsonInt32(1))
                .append('cursor', new BsonDocument('firstBatch',
                        new BsonArray([new BsonDocument('_id', new BsonInt32(1))
                                               .append('ssid', new BsonString('555-55-5555'))]))))

        when:
        cryptConnection.commandAsync('db',
                new BsonDocumentWrapper(new Document('find', 'test')
                        .append('filter', new Document('ssid', '555-55-5555')), codec),
                new NoOpFieldNameValidator(), ReadPreference.primary(), codec,
                NoOpSessionContext.INSTANCE, callback)

        then:
        _ * wrappedConnection.getDescription() >> {
            new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())), 8, STANDALONE,
                    1000, 1024 * 16_000, 1024 * 48_000, [])
        }
        1 * crypt.encrypt('db', toRaw(new BsonDocument('find', new BsonString('test'))
                .append('filter', new BsonDocument('ssid', new BsonString('555-55-5555')))), _ as SingleResultCallback) >> {
            it.last().onResult(encryptedCommand, null)
        }
        1 * wrappedConnection.commandAsync('db', encryptedCommand, _ as NoOpFieldNameValidator, ReadPreference.primary(),
                _ as RawBsonDocumentCodec, NoOpSessionContext.INSTANCE, true, null, null, _ as SingleResultCallback) >> {
            it.last().onResult(encryptedResponse, null)
        }
        1 * crypt.decrypt(encryptedResponse, _ as SingleResultCallback) >> {
            it.last().onResult(decryptedResponse, null)
        }
        1 * callback.onResult(rawToDocument(decryptedResponse), null)
    }

    def 'should split at 2 MiB'() {
        given:
        def callback = Mock(SingleResultCallback)
        def wrappedConnection = Mock(AsyncConnection)
        def crypt = Mock(Crypt)
        def cryptConnection = new AsyncCryptConnection(wrappedConnection, crypt)
        def codec = new DocumentCodec()
        def bytes = new byte[2097152 - 85]
        def payload = new SplittablePayload(INSERT, [
                new BsonDocumentWrapper(new Document('_id', 1).append('ssid', '555-55-5555').append('b', bytes), codec),
                new BsonDocumentWrapper(new Document('_id', 2).append('ssid', '666-66-6666').append('b', bytes), codec)
        ])
        def encryptedCommand = toRaw(new BsonDocument('insert', new BsonString('test')).append('documents', new BsonArray(
                [
                        new BsonDocument('_id', new BsonInt32(1))
                                .append('ssid', new BsonBinary(6 as byte, new byte[10]))
                                .append('b', new BsonBinary(bytes))
                ])))

        def encryptedResponse = toRaw(new BsonDocument('ok', new BsonInt32(1)))
        def decryptedResponse = encryptedResponse

        when:
        cryptConnection.commandAsync('db',
                new BsonDocumentWrapper(new Document('insert', 'test'), codec),
                new NoOpFieldNameValidator(), ReadPreference.primary(), new BsonDocumentCodec(),
                NoOpSessionContext.INSTANCE, true,
                payload,
                new NoOpFieldNameValidator(), callback)

        then:
        _ * wrappedConnection.getDescription() >> {
            new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())), 8, STANDALONE,
                    1000, 1024 * 16_000, 1024 * 48_000, [])
        }
        1 * crypt.encrypt('db',
                toRaw(new BsonDocument('insert', new BsonString('test')).append('documents',
                        new BsonArray([
                                new BsonDocument('_id', new BsonInt32(1))
                                        .append('ssid', new BsonString('555-55-5555'))
                                        .append('b', new BsonBinary(bytes))
                        ]))), _ as SingleResultCallback) >> {
            it.last().onResult(encryptedCommand, null)
        }
        1 * wrappedConnection.commandAsync('db', encryptedCommand, _ as NoOpFieldNameValidator, ReadPreference.primary(),
                _ as RawBsonDocumentCodec, NoOpSessionContext.INSTANCE, true, null, null, _ as SingleResultCallback) >> {
            it.last().onResult(encryptedResponse, null)
        }
        1 * crypt.decrypt(encryptedResponse, _ as SingleResultCallback) >> {
            it.last().onResult(decryptedResponse, null)
        }
        1 * callback.onResult(rawToBsonDocument(decryptedResponse), null)
        payload.getPosition() == 1
    }

    def 'should split at maxBatchCount'() {
        given:
        def callback = Mock(SingleResultCallback)
        def wrappedConnection = Mock(AsyncConnection)
        def crypt = Mock(Crypt)
        def cryptConnection = new AsyncCryptConnection(wrappedConnection, crypt)
        def codec = new DocumentCodec()
        def maxBatchCount = 2
        def payload = new SplittablePayload(INSERT, [
                new BsonDocumentWrapper(new Document('_id', 1), codec),
                new BsonDocumentWrapper(new Document('_id', 2), codec),
                new BsonDocumentWrapper(new Document('_id', 3), codec)
        ])
        def encryptedCommand = toRaw(new BsonDocument('insert', new BsonString('test')).append('documents', new BsonArray(
                [
                        new BsonDocument('_id', new BsonInt32(1)),
                        new BsonDocument('_id', new BsonInt32(2)),

                ])))

        def encryptedResponse = toRaw(new BsonDocument('ok', new BsonInt32(1)))
        def decryptedResponse = encryptedResponse

        when:
        cryptConnection.commandAsync('db',
                new BsonDocumentWrapper(new Document('insert', 'test'), codec),
                new NoOpFieldNameValidator(), ReadPreference.primary(), new BsonDocumentCodec(),
                NoOpSessionContext.INSTANCE, true,
                payload, new NoOpFieldNameValidator(), callback)

        then:
        _ * wrappedConnection.getDescription() >> {
            new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())), 8, STANDALONE,
                    maxBatchCount, 1024 * 16_000, 1024 * 48_000, [])
        }
        1 * crypt.encrypt('db',
                toRaw(new BsonDocument('insert', new BsonString('test')).append('documents',
                        new BsonArray([
                                new BsonDocument('_id', new BsonInt32(1)),
                                new BsonDocument('_id', new BsonInt32(2))
                        ]))), _ as SingleResultCallback) >> {
            it.last().onResult(encryptedCommand, null)
        }
        1 * wrappedConnection.commandAsync('db', encryptedCommand, _ as NoOpFieldNameValidator, ReadPreference.primary(),
                _ as RawBsonDocumentCodec, NoOpSessionContext.INSTANCE, true, null, null, _ as SingleResultCallback) >> {
            it.last().onResult(encryptedResponse, null)
        }
        1 * crypt.decrypt(encryptedResponse, _ as SingleResultCallback) >> {
            it.last().onResult(decryptedResponse, null)
        }
        1 * callback.onResult(rawToBsonDocument(decryptedResponse), null)
        payload.getPosition() == 2
    }

    RawBsonDocument toRaw(BsonDocument document) {
        def buffer = new BasicOutputBuffer()
        def writer = new BsonBinaryWriter(buffer)
        new BsonDocumentCodec().encode(writer, document, EncoderContext.builder().build())
        new RawBsonDocument(buffer.getInternalBuffer(), 0, buffer.getSize())
    }

    Document rawToDocument(RawBsonDocument document) {
        new DocumentCodec().decode(new BsonBinaryReader(document.getByteBuffer().asNIO()), DecoderContext.builder().build())
    }

    BsonDocument rawToBsonDocument(RawBsonDocument document) {
        new BsonDocumentCodec().decode(new BsonBinaryReader(document.getByteBuffer().asNIO()), DecoderContext.builder().build())
    }
}
