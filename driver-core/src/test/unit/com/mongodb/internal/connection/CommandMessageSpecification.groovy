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

package com.mongodb.internal.connection

import com.mongodb.MongoClientException
import com.mongodb.MongoNamespace
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.connection.ClusterConnectionMode
import com.mongodb.connection.ServerType
import com.mongodb.internal.IgnorableRequestContext
import com.mongodb.internal.TimeoutContext
import com.mongodb.internal.bulk.InsertRequest
import com.mongodb.internal.bulk.WriteRequestWithIndex
import com.mongodb.internal.session.SessionContext
import com.mongodb.internal.validator.NoOpFieldNameValidator
import org.bson.BsonArray
import org.bson.BsonBinary
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonMaximumSizeExceededException
import org.bson.BsonString
import org.bson.BsonTimestamp
import org.bson.ByteBuf
import org.bson.ByteBufNIO
import org.bson.codecs.BsonDocumentCodec
import org.bson.io.BasicOutputBuffer
import spock.lang.Specification

import java.nio.ByteBuffer

import static com.mongodb.internal.connection.SplittablePayload.Type.INSERT
import static com.mongodb.internal.operation.ServerVersionHelper.FOUR_DOT_ZERO_WIRE_VERSION
import static com.mongodb.internal.operation.ServerVersionHelper.LATEST_WIRE_VERSION

class CommandMessageSpecification extends Specification {

    def namespace = new MongoNamespace('db.test')
    def command = new BsonDocument('find', new BsonString(namespace.collectionName))
    def fieldNameValidator = NoOpFieldNameValidator.INSTANCE

    def 'should encode command message with OP_MSG when server version is >= 3.6'() {
        given:
        def message = new CommandMessage(namespace, command, fieldNameValidator, readPreference,
                MessageSettings.builder()
                        .maxWireVersion(LATEST_WIRE_VERSION)
                        .serverType(serverType as ServerType)
                        .sessionSupported(true)
                        .build(),
                responseExpected, null, null, clusterConnectionMode, null)
        def output = new BasicOutputBuffer()

        when:
        message.encode(output, operationContext)

        then:
        def byteBuf = new ByteBufNIO(ByteBuffer.wrap(output.toByteArray()))
        def messageHeader = new MessageHeader(byteBuf, 512)
        def replyHeader = new ReplyHeader(byteBuf, messageHeader)
        messageHeader.opCode == OpCode.OP_MSG.value
        replyHeader.requestId < RequestMessage.currentGlobalId
        replyHeader.responseTo == 0
        replyHeader.hasMoreToCome() != responseExpected

        def expectedCommandDocument = command.clone()
                .append('$db', new BsonString(namespace.databaseName))

        if (operationContext.getSessionContext().clusterTime != null) {
            expectedCommandDocument.append('$clusterTime', operationContext.getSessionContext().clusterTime)
        }
        if (operationContext.getSessionContext().hasSession() && responseExpected) {
            expectedCommandDocument.append('lsid', operationContext.getSessionContext().sessionId)
        }

        if (readPreference != ReadPreference.primary()) {
            expectedCommandDocument.append('$readPreference', readPreference.toDocument())
        } else if (clusterConnectionMode == ClusterConnectionMode.SINGLE && serverType != ServerType.SHARD_ROUTER) {
            expectedCommandDocument.append('$readPreference', ReadPreference.primaryPreferred().toDocument())
        }
        getCommandDocument(byteBuf, replyHeader) == expectedCommandDocument

        where:
        [readPreference, serverType, clusterConnectionMode, operationContext, responseExpected, isCryptd] << [
                [ReadPreference.primary(), ReadPreference.secondary()],
                [ServerType.REPLICA_SET_PRIMARY, ServerType.SHARD_ROUTER],
                [ClusterConnectionMode.SINGLE, ClusterConnectionMode.MULTIPLE],
                [
                        new OperationContext(
                                IgnorableRequestContext.INSTANCE,
                                Stub(SessionContext) {
                                    hasSession() >> false
                                    getClusterTime() >> null
                                    getSessionId() >> new BsonDocument('id', new BsonBinary([1, 2, 3] as byte[]))
                                    getReadConcern() >> ReadConcern.DEFAULT
                                }, Stub(TimeoutContext), null),
                        new OperationContext(
                                IgnorableRequestContext.INSTANCE,
                                Stub(SessionContext) {
                                    hasSession() >> false
                                    getClusterTime() >> new BsonDocument('clusterTime', new BsonTimestamp(42, 1))
                                    getReadConcern() >> ReadConcern.DEFAULT
                                }, Stub(TimeoutContext), null),
                        new OperationContext(
                                IgnorableRequestContext.INSTANCE,
                                Stub(SessionContext) {
                                    hasSession() >> true
                                    getClusterTime() >> null
                                    getSessionId() >> new BsonDocument('id', new BsonBinary([1, 2, 3] as byte[]))
                                    getReadConcern() >> ReadConcern.DEFAULT
                                }, Stub(TimeoutContext), null),
                        new OperationContext(
                                IgnorableRequestContext.INSTANCE,
                                Stub(SessionContext) {
                                    hasSession() >> true
                                    getClusterTime() >> new BsonDocument('clusterTime', new BsonTimestamp(42, 1))
                                    getSessionId() >> new BsonDocument('id', new BsonBinary([1, 2, 3] as byte[]))
                                    getReadConcern() >> ReadConcern.DEFAULT
                                }, Stub(TimeoutContext), null)
                ],
                [true, false],
                [true, false]
        ].combinations()
    }

    String getString(final ByteBuf byteBuf) {
        def byteArrayOutputStream = new ByteArrayOutputStream()
        def cur = byteBuf.get()
        while (cur != 0) {
            byteArrayOutputStream.write(cur)
            cur = byteBuf.get()
        }
        new String(byteArrayOutputStream.toByteArray(), 'UTF-8')
    }

    def 'should get command document'() {
        given:
        def message = new CommandMessage(namespace, originalCommandDocument, fieldNameValidator, ReadPreference.primary(),
                MessageSettings.builder().maxWireVersion(maxWireVersion).build(), true, payload, NoOpFieldNameValidator.INSTANCE,
                ClusterConnectionMode.MULTIPLE, null)
        def output = new ByteBufferBsonOutput(new SimpleBufferProvider())
        message.encode(output, new OperationContext(IgnorableRequestContext.INSTANCE, NoOpSessionContext.INSTANCE,
                Stub(TimeoutContext), null))

        when:
        def commandDocument = message.getCommandDocument(output)

        def expectedCommandDocument = new BsonDocument('insert', new BsonString('coll')).append('documents',
                new BsonArray([new BsonDocument('_id', new BsonInt32(1)), new BsonDocument('_id', new BsonInt32(2))]))
        expectedCommandDocument.append('$db', new BsonString(namespace.getDatabaseName()))
        then:
        commandDocument == expectedCommandDocument


        where:
        [maxWireVersion, originalCommandDocument, payload] << [
                [
                        LATEST_WIRE_VERSION,
                        new BsonDocument('insert', new BsonString('coll')),
                        new SplittablePayload(INSERT, [new BsonDocument('_id', new BsonInt32(1)),
                                                       new BsonDocument('_id', new BsonInt32(2))]
                                .withIndex().collect { doc, i -> new WriteRequestWithIndex(new InsertRequest(doc), i) }, true),
                ],
                [
                        LATEST_WIRE_VERSION,
                        new BsonDocument('insert', new BsonString('coll')).append('documents',
                                new BsonArray([new BsonDocument('_id', new BsonInt32(1)), new BsonDocument('_id', new BsonInt32(2))])),
                        null
                ]
        ]
    }

    def 'should respect the max message size'() {
        given:
        def maxMessageSize = 1024
        def messageSettings = MessageSettings.builder().maxMessageSize(maxMessageSize).maxWireVersion(LATEST_WIRE_VERSION).build()
        def insertCommand = new BsonDocument('insert', new BsonString(namespace.collectionName))
        def payload = new SplittablePayload(INSERT, [new BsonDocument('_id', new BsonInt32(1)).append('a', new BsonBinary(new byte[913])),
                                                     new BsonDocument('_id', new BsonInt32(2)).append('b', new BsonBinary(new byte[441])),
                                                     new BsonDocument('_id', new BsonInt32(3)).append('c', new BsonBinary(new byte[450])),
                                                     new BsonDocument('_id', new BsonInt32(4)).append('b', new BsonBinary(new byte[441])),
                                                     new BsonDocument('_id', new BsonInt32(5)).append('c', new BsonBinary(new byte[451]))]
                .withIndex().collect { doc, i -> new WriteRequestWithIndex(new InsertRequest(doc), i) }, true)
        def message = new CommandMessage(namespace, insertCommand, fieldNameValidator, ReadPreference.primary(), messageSettings,
                false, payload, fieldNameValidator, ClusterConnectionMode.MULTIPLE, null)
        def output = new BasicOutputBuffer()
        def sessionContext = Stub(SessionContext) {
            getReadConcern() >> ReadConcern.DEFAULT
        }

        when:
        message.encode(output, new OperationContext(IgnorableRequestContext.INSTANCE, sessionContext,
                Stub(TimeoutContext), null))
        def byteBuf = new ByteBufNIO(ByteBuffer.wrap(output.toByteArray()))
        def messageHeader = new MessageHeader(byteBuf, maxMessageSize)

        then:
        messageHeader.opCode == OpCode.OP_MSG.value
        messageHeader.requestId < RequestMessage.currentGlobalId
        messageHeader.responseTo == 0
        messageHeader.messageLength == 1024
        byteBuf.getInt() == 0
        payload.getPosition() == 1
        payload.hasAnotherSplit()

        when:
        payload = payload.getNextSplit()
        message = new CommandMessage(namespace, insertCommand, fieldNameValidator, ReadPreference.primary(), messageSettings,
                false, payload, fieldNameValidator, ClusterConnectionMode.MULTIPLE, null)
        output.truncateToPosition(0)
        message.encode(output, new OperationContext(IgnorableRequestContext.INSTANCE, sessionContext, Stub(TimeoutContext), null))
        byteBuf = new ByteBufNIO(ByteBuffer.wrap(output.toByteArray()))
        messageHeader = new MessageHeader(byteBuf, maxMessageSize)

        then:
        messageHeader.opCode == OpCode.OP_MSG.value
        messageHeader.requestId < RequestMessage.currentGlobalId
        messageHeader.responseTo == 0
        messageHeader.messageLength == 1024
        byteBuf.getInt() == 0
        payload.getPosition() == 2
        payload.hasAnotherSplit()

        when:
        payload = payload.getNextSplit()
        message = new CommandMessage(namespace, insertCommand, fieldNameValidator, ReadPreference.primary(), messageSettings,
                false, payload, fieldNameValidator, ClusterConnectionMode.MULTIPLE, null)
        output.truncateToPosition(0)
        message.encode(output, new OperationContext(IgnorableRequestContext.INSTANCE, sessionContext, Stub(TimeoutContext), null))
        byteBuf = new ByteBufNIO(ByteBuffer.wrap(output.toByteArray()))
        messageHeader = new MessageHeader(byteBuf, maxMessageSize)

        then:
        messageHeader.opCode == OpCode.OP_MSG.value
        messageHeader.requestId < RequestMessage.currentGlobalId
        messageHeader.responseTo == 0
        messageHeader.messageLength == 552
        byteBuf.getInt() == 0
        payload.getPosition() == 1
        payload.hasAnotherSplit()

        when:
        payload = payload.getNextSplit()
        message = new CommandMessage(namespace, insertCommand, fieldNameValidator, ReadPreference.primary(), messageSettings,
                false, payload, fieldNameValidator, ClusterConnectionMode.MULTIPLE, null)
        output.truncateToPosition(0)
        message.encode(output, new OperationContext(IgnorableRequestContext.INSTANCE,
                sessionContext,
                Stub(TimeoutContext),
                null))
        byteBuf = new ByteBufNIO(ByteBuffer.wrap(output.toByteArray()))
        messageHeader = new MessageHeader(byteBuf, maxMessageSize)

        then:
        messageHeader.opCode == OpCode.OP_MSG.value
        messageHeader.requestId < RequestMessage.currentGlobalId
        messageHeader.responseTo == 0
        messageHeader.messageLength == 562
        byteBuf.getInt() == 1 << 1
        payload.getPosition() == 1
        !payload.hasAnotherSplit()
    }

    def 'should respect the max batch count'() {
        given:
        def messageSettings = MessageSettings.builder().maxBatchCount(2).maxWireVersion(LATEST_WIRE_VERSION).build()
        def payload = new SplittablePayload(INSERT, [new BsonDocument('a', new BsonBinary(new byte[900])),
                                                     new BsonDocument('b', new BsonBinary(new byte[450])),
                                                     new BsonDocument('c', new BsonBinary(new byte[450]))]
                .withIndex().collect { doc, i -> new WriteRequestWithIndex(new InsertRequest(doc), i) }, true)
        def message = new CommandMessage(namespace, command, fieldNameValidator, ReadPreference.primary(), messageSettings,
                false, payload, fieldNameValidator, ClusterConnectionMode.MULTIPLE, null)
        def output = new BasicOutputBuffer()
        def sessionContext = Stub(SessionContext) {
            getReadConcern() >> ReadConcern.DEFAULT
        }

        when:
        message.encode(output, new OperationContext(IgnorableRequestContext.INSTANCE, sessionContext,
                Stub(TimeoutContext),
                null))
        def byteBuf = new ByteBufNIO(ByteBuffer.wrap(output.toByteArray()))
        def messageHeader = new MessageHeader(byteBuf, 2048)

        then:
        messageHeader.opCode == OpCode.OP_MSG.value
        messageHeader.requestId < RequestMessage.currentGlobalId
        messageHeader.responseTo == 0
        messageHeader.messageLength == 1497
        byteBuf.getInt() == 0
        payload.getPosition() == 2
        payload.hasAnotherSplit()

        when:
        payload = payload.getNextSplit()
        message = new CommandMessage(namespace, command, fieldNameValidator, ReadPreference.primary(), messageSettings,
                false, payload, fieldNameValidator, ClusterConnectionMode.MULTIPLE, null)
        output.truncateToPosition(0)
        message.encode(output, new OperationContext(IgnorableRequestContext.INSTANCE, sessionContext,
                Stub(TimeoutContext), null))
        byteBuf = new ByteBufNIO(ByteBuffer.wrap(output.toByteArray()))
        messageHeader = new MessageHeader(byteBuf, 1024)

        then:
        messageHeader.opCode == OpCode.OP_MSG.value
        messageHeader.requestId < RequestMessage.currentGlobalId
        messageHeader.responseTo == 0
        byteBuf.getInt() == 1 << 1
        payload.getPosition() == 1
        !payload.hasAnotherSplit()
    }

    def 'should throw if payload document bigger than max document size'() {
        given:
        def messageSettings = MessageSettings.builder().maxDocumentSize(900)
                .maxWireVersion(LATEST_WIRE_VERSION).build()
        def payload = new SplittablePayload(INSERT, [new BsonDocument('a', new BsonBinary(new byte[900]))]
                .withIndex().collect { doc, i -> new WriteRequestWithIndex(new InsertRequest(doc), i) }, true)
        def message = new CommandMessage(namespace, command, fieldNameValidator, ReadPreference.primary(), messageSettings,
                false, payload, fieldNameValidator, ClusterConnectionMode.MULTIPLE, null)
        def output = new BasicOutputBuffer()
        def sessionContext = Stub(SessionContext) {
            getReadConcern() >> ReadConcern.DEFAULT
        }

        when:
        message.encode(output, new OperationContext(IgnorableRequestContext.INSTANCE, sessionContext,
                Stub(TimeoutContext), null))

        then:
        thrown(BsonMaximumSizeExceededException)
    }

    def 'should throw if wire version and sharded cluster does not support transactions'() {
        given:
        def messageSettings = MessageSettings.builder().serverType(ServerType.SHARD_ROUTER)
                .maxWireVersion(FOUR_DOT_ZERO_WIRE_VERSION).build()
        def payload = new SplittablePayload(INSERT, [new BsonDocument('a', new BsonInt32(1))], true)
        def message = new CommandMessage(namespace, command, fieldNameValidator, ReadPreference.primary(), messageSettings,
                false, payload, fieldNameValidator, ClusterConnectionMode.MULTIPLE, null)
        def output = new BasicOutputBuffer()
        def sessionContext = Stub(SessionContext) {
            getReadConcern() >> ReadConcern.DEFAULT
            hasActiveTransaction() >> true
        }

        when:
        message.encode(output, new OperationContext(IgnorableRequestContext.INSTANCE, sessionContext,
                Stub(TimeoutContext), null))

        then:
        thrown(MongoClientException)
    }

    private static BsonDocument getCommandDocument(ByteBufNIO byteBuf, ReplyHeader replyHeader) {
        new ReplyMessage<BsonDocument>(new ResponseBuffers(replyHeader, byteBuf), new BsonDocumentCodec(), 0).document
    }
}
