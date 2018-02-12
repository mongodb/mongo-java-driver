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

package com.mongodb.connection

import com.mongodb.MongoNamespace
import com.mongodb.ReadPreference
import com.mongodb.internal.connection.NoOpSessionContext
import com.mongodb.internal.validator.NoOpFieldNameValidator
import com.mongodb.session.SessionContext
import org.bson.BsonArray
import org.bson.BsonBinary
import org.bson.BsonBinaryReader
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonMaximumSizeExceededException
import org.bson.BsonSerializationException
import org.bson.BsonString
import org.bson.BsonTimestamp
import org.bson.ByteBuf
import org.bson.ByteBufNIO
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.DecoderContext
import org.bson.io.BasicOutputBuffer
import org.bson.io.BsonInput
import org.bson.io.ByteBufferBsonInput
import spock.lang.Specification

import java.nio.ByteBuffer

import static com.mongodb.connection.SplittablePayload.Type.INSERT

class CommandMessageSpecification extends Specification {

    def namespace = new MongoNamespace('db.test')
    def command = new BsonDocument('find', new BsonString(namespace.collectionName))
    def fieldNameValidator = new NoOpFieldNameValidator()

    def 'should encode command message with OP_MSG when server version is >= 3.6'() {
        given:
        def message = new CommandMessage(namespace, command, fieldNameValidator, readPreference,
                MessageSettings.builder()
                        .serverVersion(new ServerVersion(3, 6))
                        .serverType(serverType)
                        .build(),
                responseExpected, null, null, clusterConnectionMode)
        def output = new BasicOutputBuffer()

        when:
        message.encode(output, sessionContext)

        then:
        def byteBuf = new ByteBufNIO(ByteBuffer.wrap(output.toByteArray()))
        def messageHeader = new MessageHeader(byteBuf, 512)
        def replyHeader = new ReplyHeader(byteBuf, messageHeader)
        messageHeader.opCode == OpCode.OP_MSG.value
        replyHeader.requestId < RequestMessage.currentGlobalId
        replyHeader.responseTo == 0
        replyHeader.opMsgFlagBits == (responseExpected ? 0 : 2)

        def expectedCommandDocument = command.clone()
                .append('$db', new BsonString(namespace.databaseName))

        if (sessionContext.clusterTime != null) {
            expectedCommandDocument.append('$clusterTime', sessionContext.clusterTime)
        }
        if (sessionContext.hasSession() && responseExpected) {
            expectedCommandDocument.append('lsid', sessionContext.sessionId)
        }

        if (readPreference != ReadPreference.primary()) {
            expectedCommandDocument.append('$readPreference', readPreference.toDocument())
        } else if (clusterConnectionMode == ClusterConnectionMode.SINGLE && serverType != ServerType.SHARD_ROUTER) {
            expectedCommandDocument.append('$readPreference', ReadPreference.primaryPreferred().toDocument())
        }
        getCommandDocument(byteBuf, replyHeader) == expectedCommandDocument

        where:
        [readPreference, serverType, clusterConnectionMode, sessionContext, responseExpected] << [
                [ReadPreference.primary(), ReadPreference.secondary()],
                [ServerType.REPLICA_SET_PRIMARY, ServerType.SHARD_ROUTER],
                [ClusterConnectionMode.SINGLE, ClusterConnectionMode.MULTIPLE],
                [
                        Stub(SessionContext) {
                            hasSession() >> false
                            getClusterTime() >> null
                            getSessionId() >> new BsonDocument('id', new BsonBinary([1, 2, 3] as byte[]))
                        },
                        Stub(SessionContext) {
                            hasSession() >> false
                            getClusterTime() >> new BsonDocument('clusterTime', new BsonTimestamp(42, 1))
                        },
                        Stub(SessionContext) {
                            hasSession() >> true
                            getClusterTime() >> null
                            getSessionId() >> new BsonDocument('id', new BsonBinary([1, 2, 3] as byte[]))
                        },
                        Stub(SessionContext) {
                            hasSession() >> true
                            getClusterTime() >> new BsonDocument('clusterTime', new BsonTimestamp(42, 1))
                            getSessionId() >> new BsonDocument('id', new BsonBinary([1, 2, 3] as byte[]))
                        }
                ],
                [true, false]
        ].combinations()
    }

    def 'should encode command message with OP_QUERY when server version is < 3.6'() {
        given:
        def message = new CommandMessage(namespace, command, fieldNameValidator, readPreference,
                MessageSettings.builder()
                        .serverVersion(new ServerVersion(3, 4))
                        .serverType(serverType)
                        .build(),
                responseExpected, null, null, clusterConnectionMode)
        def output = new BasicOutputBuffer()
        def expectedFlagBits = 0
        if (readPreference.isSlaveOk()) {
            expectedFlagBits = 4
        } else if (clusterConnectionMode == ClusterConnectionMode.SINGLE && serverType != ServerType.SHARD_ROUTER) {
            expectedFlagBits = 4
        }
        BsonDocument expectedCommand
        if (readPreference.isSlaveOk()) {
            expectedCommand = new BsonDocument('$query', command).append('$readPreference', readPreference.toDocument())
        } else {
            expectedCommand = command
        }

        when:
        message.encode(output, NoOpSessionContext.INSTANCE)

        then:
        def byteBuf = new ByteBufNIO(ByteBuffer.wrap(output.toByteArray()))
        def messageHeader = new MessageHeader(byteBuf, 512)
        messageHeader.opCode == OpCode.OP_QUERY.value
        messageHeader.requestId < RequestMessage.currentGlobalId
        messageHeader.responseTo == 0
        def flagBits = byteBuf.getInt()
        flagBits == expectedFlagBits
        def collectionName = getString(byteBuf)
        collectionName == 'db.test'
        def numberToSkip = byteBuf.getInt()
        numberToSkip == 0
        def numberToReturn = byteBuf.getInt()
        numberToReturn == -1

        getCommandDocument(byteBuf) == expectedCommand

        where:
        [readPreference, serverType, clusterConnectionMode, responseExpected] << [
                [ReadPreference.primary(), ReadPreference.secondary()],
                [ServerType.REPLICA_SET_PRIMARY, ServerType.SHARD_ROUTER],
                [ClusterConnectionMode.SINGLE, ClusterConnectionMode.MULTIPLE],
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
                MessageSettings.builder().serverVersion(serverVersion).build(), true, payload, new NoOpFieldNameValidator(),
                ClusterConnectionMode.MULTIPLE)
        def output = new ByteBufferBsonOutput(new SimpleBufferProvider())
        message.encode(output, NoOpSessionContext.INSTANCE)

        when:
        def commandDocument = message.getCommandDocument(output)

        def expectedCommandDocument = new BsonDocument('insert', new BsonString('coll')).append('documents',
                new BsonArray([new BsonDocument('_id', new BsonInt32(1)), new BsonDocument('_id', new BsonInt32(2))]))
        if (serverVersion == new ServerVersion(3, 6)) {
            expectedCommandDocument.append('$db', new BsonString(namespace.getDatabaseName()))
        }
        then:
        commandDocument == expectedCommandDocument


        where:
        [serverVersion, originalCommandDocument, payload] << [
                [
                        new ServerVersion(3, 4),
                        new BsonDocument('insert', new BsonString('coll')),
                        new SplittablePayload(INSERT, [new BsonDocument('_id', new BsonInt32(1)),
                                                       new BsonDocument('_id', new BsonInt32(2))]),
                ],
                [
                        new ServerVersion(3, 4),
                        new BsonDocument('insert', new BsonString('coll')).append('documents',
                                new BsonArray([new BsonDocument('_id', new BsonInt32(1)), new BsonDocument('_id', new BsonInt32(2))])),
                        null
                ],
                [
                        new ServerVersion(3, 6),
                        new BsonDocument('insert', new BsonString('coll')),
                        new SplittablePayload(INSERT, [new BsonDocument('_id', new BsonInt32(1)),
                                                       new BsonDocument('_id', new BsonInt32(2))]),
                ],
                [
                        new ServerVersion(3, 6),
                        new BsonDocument('insert', new BsonString('coll')).append('documents',
                                new BsonArray([new BsonDocument('_id', new BsonInt32(1)), new BsonDocument('_id', new BsonInt32(2))])),
                        null
                ]
        ]
    }

    def 'should respect the max message size'() {
        given:
        def maxMessageSize = 1024
        def messageSettings = MessageSettings.builder().maxMessageSize(maxMessageSize).serverVersion(new ServerVersion(3, 6)).build()
        def insertCommand = new BsonDocument('insert', new BsonString(namespace.collectionName))
        def payload = new SplittablePayload(INSERT, [new BsonDocument('a', new BsonBinary(new byte[922])),
                                                     new BsonDocument('b', new BsonBinary(new byte[450])),
                                                     new BsonDocument('c', new BsonBinary(new byte[459])),
                                                     new BsonDocument('b', new BsonBinary(new byte[450])),
                                                     new BsonDocument('c', new BsonBinary(new byte[460]))])
        def message = new CommandMessage(namespace, insertCommand, fieldNameValidator, ReadPreference.primary(), messageSettings,
                false, payload, fieldNameValidator, ClusterConnectionMode.MULTIPLE)
        def output = new BasicOutputBuffer()
        def sessionContext = Stub(SessionContext)

        when:
        message.encode(output, sessionContext)
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
                false, payload, fieldNameValidator, ClusterConnectionMode.MULTIPLE)
        output.truncateToPosition(0)
        message.encode(output, sessionContext)
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
                false, payload, fieldNameValidator, ClusterConnectionMode.MULTIPLE)
        output.truncateToPosition(0)
        message.encode(output, sessionContext)
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
                false, payload, fieldNameValidator, ClusterConnectionMode.MULTIPLE)
        output.truncateToPosition(0)
        message.encode(output, sessionContext)
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
        def messageSettings = MessageSettings.builder().maxBatchCount(2).serverVersion(new ServerVersion(3, 6)).build()
        def payload = new SplittablePayload(INSERT, [new BsonDocument('a', new BsonBinary(new byte[900])),
                                                     new BsonDocument('b', new BsonBinary(new byte[450])),
                                                     new BsonDocument('c', new BsonBinary(new byte[450]))])
        def message = new CommandMessage(namespace, command, fieldNameValidator, ReadPreference.primary(), messageSettings,
                false, payload, fieldNameValidator, ClusterConnectionMode.MULTIPLE)
        def output = new BasicOutputBuffer()
        def sessionContext = Stub(SessionContext)

        when:
        message.encode(output, sessionContext)
        def byteBuf = new ByteBufNIO(ByteBuffer.wrap(output.toByteArray()))
        def messageHeader = new MessageHeader(byteBuf, 2048)

        then:
        messageHeader.opCode == OpCode.OP_MSG.value
        messageHeader.requestId < RequestMessage.currentGlobalId
        messageHeader.responseTo == 0
        messageHeader.messageLength == 1463
        byteBuf.getInt() == 0
        payload.getPosition() == 2
        payload.hasAnotherSplit()

        when:
        payload = payload.getNextSplit()
        message = new CommandMessage(namespace, command, fieldNameValidator, ReadPreference.primary(), messageSettings,
                false, payload, fieldNameValidator, ClusterConnectionMode.MULTIPLE)
        output.truncateToPosition(0)
        message.encode(output, sessionContext)
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
        def messageSettings = MessageSettings.builder().maxDocumentSize(900).serverVersion(new ServerVersion(3, 6)).build()
        def payload = new SplittablePayload(INSERT, [new BsonDocument('a', new BsonBinary(new byte[900]))])
        def message = new CommandMessage(namespace, command, fieldNameValidator, ReadPreference.primary(), messageSettings,
                false, payload, fieldNameValidator, ClusterConnectionMode.MULTIPLE)
        def output = new BasicOutputBuffer()
        def sessionContext = Stub(SessionContext)

        when:
        message.encode(output, sessionContext)

        then:
        thrown(BsonMaximumSizeExceededException)
    }

    private static BsonDocument getCommandDocument(ByteBufNIO byteBuf, ReplyHeader replyHeader) {
        new ReplyMessage<BsonDocument>(new ResponseBuffers(replyHeader, byteBuf), new BsonDocumentCodec(), 0).documents.get(0)
    }

    private static BsonDocument getCommandDocument(ByteBufNIO byteBuf) {
        BsonInput bsonInput = new ByteBufferBsonInput(byteBuf);
        BsonBinaryReader reader = new BsonBinaryReader(bsonInput);
        new BsonDocumentCodec().decode(reader, DecoderContext.builder().build())
    }
}
