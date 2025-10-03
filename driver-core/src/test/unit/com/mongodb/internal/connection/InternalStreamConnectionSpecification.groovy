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

import com.mongodb.MongoCommandException
import com.mongodb.MongoInternalException
import com.mongodb.MongoInterruptedException
import com.mongodb.MongoOperationTimeoutException
import com.mongodb.MongoSocketClosedException
import com.mongodb.MongoSocketException
import com.mongodb.MongoSocketReadException
import com.mongodb.MongoSocketReadTimeoutException
import com.mongodb.MongoSocketWriteException
import com.mongodb.ReadConcern
import com.mongodb.ServerAddress
import com.mongodb.async.FutureResultCallback
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ConnectionId
import com.mongodb.connection.ServerConnectionState
import com.mongodb.connection.ServerDescription
import com.mongodb.connection.ServerId
import com.mongodb.connection.ServerType
import com.mongodb.event.CommandFailedEvent
import com.mongodb.event.CommandStartedEvent
import com.mongodb.event.CommandSucceededEvent
import com.mongodb.internal.ExceptionUtils.MongoCommandExceptionUtils
import com.mongodb.internal.TimeoutContext
import com.mongodb.internal.session.SessionContext
import com.mongodb.internal.validator.NoOpFieldNameValidator
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonReader
import org.bson.BsonString
import org.bson.ByteBufNIO
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.DecoderContext
import org.bson.codecs.configuration.CodecConfigurationException
import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.channels.ClosedByInterruptException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS_WITH_INFINITE_TIMEOUT
import static com.mongodb.ReadPreference.primary
import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE
import static com.mongodb.connection.ClusterConnectionMode.SINGLE
import static com.mongodb.connection.ConnectionDescription.getDefaultMaxMessageSize
import static com.mongodb.connection.ConnectionDescription.getDefaultMaxWriteBatchSize
import static com.mongodb.connection.ServerDescription.getDefaultMaxDocumentSize
import static com.mongodb.internal.connection.MessageHelper.LEGACY_HELLO
import static com.mongodb.internal.connection.MessageHelper.LEGACY_HELLO_LOWER
import static com.mongodb.internal.operation.ServerVersionHelper.LATEST_WIRE_VERSION
import static java.util.concurrent.TimeUnit.NANOSECONDS
import static java.util.concurrent.TimeUnit.SECONDS

@SuppressWarnings(['UnusedVariable'])
class InternalStreamConnectionSpecification extends Specification {
    private static final ServerId SERVER_ID = new ServerId(new ClusterId(), new ServerAddress())

    def database = 'admin'
    def fieldNameValidator = NoOpFieldNameValidator.INSTANCE
    def helper = new StreamHelper()
    def serverAddress = new ServerAddress()
    def connectionId = new ConnectionId(SERVER_ID, 1, 1)
    def commandListener = new TestCommandListener()
    def messageSettings = MessageSettings.builder().maxWireVersion(LATEST_WIRE_VERSION).build()

    def connectionDescription = new ConnectionDescription(connectionId, 3,
            ServerType.STANDALONE, getDefaultMaxWriteBatchSize(), getDefaultMaxDocumentSize(), getDefaultMaxMessageSize(), [])
    def serverDescription = ServerDescription.builder()
            .ok(true)
            .state(ServerConnectionState.CONNECTED)
            .type(ServerType.STANDALONE)
            .address(serverAddress)
            .build()
    def internalConnectionInitializationDescription =
            new InternalConnectionInitializationDescription(connectionDescription, serverDescription)
    def stream = Mock(Stream) {
        openAsync(_, _) >> { it.last().completed(null) }
    }
    def streamFactory = Mock(StreamFactory) {
        create(_) >> { stream }
    }
    def initializer = Mock(InternalConnectionInitializer) {
        startHandshake(_, _) >> { internalConnectionInitializationDescription }
        finishHandshake(_, _, _) >> { internalConnectionInitializationDescription }
        startHandshakeAsync(_, _, _) >> { it[2].onResult(internalConnectionInitializationDescription, null) }
        finishHandshakeAsync(_, _, _, _) >> { it[3].onResult(internalConnectionInitializationDescription, null) }
    }

    def getConnection() {
        new InternalStreamConnection(SINGLE, SERVER_ID, new TestConnectionGenerationSupplier(), streamFactory, [], commandListener,
                initializer)
    }

    def getOpenedConnection() {
        def connection = getConnection()
        connection.open(OPERATION_CONTEXT)
        connection
    }

    def 'should change the description when opened'() {
        when:
        def connection = getConnection()

        then:
        connection.getDescription().getServerType() == ServerType.UNKNOWN
        connection.getDescription().getConnectionId().getServerValue() == null
        connection.getInitialServerDescription() == ServerDescription.builder()
                .address(serverAddress)
                .type(ServerType.UNKNOWN)
                .state(ServerConnectionState.CONNECTING)
                .lastUpdateTimeNanos(connection.getInitialServerDescription().getLastUpdateTime(NANOSECONDS))
                .build()
        when:
        connection.open(OPERATION_CONTEXT)

        then:
        connection.opened()
        connection.getDescription().getServerType() == ServerType.STANDALONE
        connection.getDescription().getConnectionId().getServerValue() == 1
        connection.getDescription() == connectionDescription
        connection.getInitialServerDescription() == serverDescription
    }


    def 'should change the description when opened asynchronously'() {
        when:
        def connection = getConnection()
        def futureResultCallback = new FutureResultCallback<Void>()

        then:
        connection.getDescription().getServerType() == ServerType.UNKNOWN
        connection.getDescription().getConnectionId().getServerValue() == null
        connection.getInitialServerDescription() == ServerDescription.builder()
                .address(serverAddress)
                .type(ServerType.UNKNOWN)
                .state(ServerConnectionState.CONNECTING)
                .lastUpdateTimeNanos(connection.getInitialServerDescription().getLastUpdateTime(NANOSECONDS))
                .build()

        when:
        connection.openAsync(OPERATION_CONTEXT, futureResultCallback)
        futureResultCallback.get()

        then:
        connection.opened()
        connection.getDescription() == connectionDescription
        connection.getInitialServerDescription() == serverDescription
    }

    def 'should close the stream when initialization throws an exception'() {
        given:
        def failedInitializer = Mock(InternalConnectionInitializer) {
            startHandshake(_, _) >> { throw new MongoInternalException('Something went wrong') }
        }
        def connection = new InternalStreamConnection(SINGLE, SERVER_ID, new TestConnectionGenerationSupplier(), streamFactory, [], null,
                failedInitializer)

        when:
        connection.open(OPERATION_CONTEXT)

        then:
        thrown MongoInternalException
        connection.isClosed()
    }


    def 'should close the stream when initialization throws an exception asynchronously'() {
        given:
        def failedInitializer = Mock(InternalConnectionInitializer) {
            startHandshakeAsync(_, _, _) >> { it[2].onResult(null, new MongoInternalException('Something went wrong')) }
        }
        def connection = new InternalStreamConnection(SINGLE, SERVER_ID, new TestConnectionGenerationSupplier(), streamFactory, [], null,
                failedInitializer)

        when:
        def futureResultCallback = new FutureResultCallback<Void>()
        connection.openAsync(OPERATION_CONTEXT, futureResultCallback)
        futureResultCallback.get()

        then:
        thrown MongoInternalException
        connection.isClosed()
    }

    def 'should close the stream when writing a message throws an exception'() {
        given:
        stream.write(_, _) >> { throw new IOException('Something went wrong') }

        def connection = getOpenedConnection()
        def (buffers1, messageId1) = helper.hello()
        def (buffers2, messageId2) = helper.hello()

        when:
        connection.sendMessage(buffers1, messageId1, OPERATION_CONTEXT)

        then:
        connection.isClosed()
        thrown MongoSocketWriteException

        when:
        connection.sendMessage(buffers2, messageId2, OPERATION_CONTEXT)

        then:
        thrown MongoSocketClosedException
    }


    def 'should close the stream when writing a message throws an exception asynchronously'() {
        given:
        def (buffers1, messageId1, sndCallbck1, rcvdCallbck1) = helper.helloAsync()
        def (buffers2, messageId2, sndCallbck2, rcvdCallbck2) = helper.helloAsync()
        int seen = 0

        stream.writeAsync(_, _, _) >> { buffers, operationContext, callback ->
            if (seen == 0) {
                seen += 1
                return callback.failed(new IOException('Something went wrong'))
            }
            callback.completed(null)
        }

        def connection = getOpenedConnection()

        when:
        connection.sendMessageAsync(buffers1, messageId1, OPERATION_CONTEXT, sndCallbck1)
        sndCallbck1.get(10, SECONDS)

        then:
        thrown MongoSocketWriteException
        connection.isClosed()

        when:
        connection.sendMessageAsync(buffers2, messageId2, OPERATION_CONTEXT, sndCallbck2)
        sndCallbck2.get(10, SECONDS)

        then:
        thrown MongoSocketClosedException
    }

    def 'should close the stream when reading the message header throws an exception'() {
        given:
        stream.read(16, _) >> { throw new IOException('Something went wrong') }

        def connection = getOpenedConnection()
        def (buffers1, messageId1) = helper.hello()
        def (buffers2, messageId2) = helper.hello()

        when:
        connection.sendMessage(buffers1, messageId1, OPERATION_CONTEXT)
        connection.sendMessage(buffers2, messageId2, OPERATION_CONTEXT)
        connection.receiveMessage(messageId1, OPERATION_CONTEXT)

        then:
        connection.isClosed()
        thrown MongoSocketReadException

        when:
        connection.receiveMessage(messageId2, OPERATION_CONTEXT)

        then:
        thrown MongoSocketClosedException
    }

    def 'should throw MongoInternalException when reply header message length > max message length'() {
        given:
        stream.read(36, _) >> { helper.headerWithMessageSizeGreaterThanMax(1) }

        def connection = getOpenedConnection()

        when:
        connection.receiveMessage(1, OPERATION_CONTEXT)

        then:
        thrown(MongoInternalException)
        connection.isClosed()
    }

    def 'should throw MongoInternalException when reply header message length > max message length asynchronously'() {
        given:
        stream.readAsync(16, _, _) >> { numBytes, operationContext, handler ->
            handler.completed(helper.headerWithMessageSizeGreaterThanMax(1, connectionDescription.maxMessageSize))
        }

        def connection = getOpenedConnection()
        def callback = new FutureResultCallback()

        when:
        connection.receiveMessageAsync(1, OPERATION_CONTEXT, callback)
        callback.get()

        then:
        thrown(MongoInternalException)
        connection.isClosed()
    }

    def 'should throw MongoInterruptedException and leave the interrupt status set when Stream.write throws InterruptedIOException'() {
        given:
        stream.write(_, _) >> { throw new InterruptedIOException() }
        def connection = getOpenedConnection()
        Thread.currentThread().interrupt()

        when:
        connection.sendMessage([new ByteBufNIO(ByteBuffer.allocate(1))], 1, OPERATION_CONTEXT)

        then:
        Thread.interrupted()
        thrown(MongoInterruptedException)
        connection.isClosed()
    }

    def 'should throw MongoInterruptedException and leave the interrupt status unset when Stream.write throws InterruptedIOException'() {
        given:
        stream.write(_, _) >> { throw new InterruptedIOException() }
        def connection = getOpenedConnection()

        when:
        connection.sendMessage([new ByteBufNIO(ByteBuffer.allocate(1))], 1, OPERATION_CONTEXT)

        then:
        !Thread.interrupted()
        thrown(MongoInterruptedException)
        connection.isClosed()
    }

    def 'should throw MongoInterruptedException and leave the interrupt status set when Stream.write throws ClosedByInterruptException'() {
        given:
        stream.write(_, _) >> { throw new ClosedByInterruptException() }
        def connection = getOpenedConnection()
        Thread.currentThread().interrupt()

        when:
        connection.sendMessage([new ByteBufNIO(ByteBuffer.allocate(1))], 1, OPERATION_CONTEXT)

        then:
        Thread.interrupted()
        thrown(MongoInterruptedException)
        connection.isClosed()
    }

    def 'should throw MongoInterruptedException when Stream.write throws SocketException and the thread is interrupted'() {
        given:
        stream.write(_, _) >> { throw new SocketException() }
        def connection = getOpenedConnection()
        Thread.currentThread().interrupt()

        when:
        connection.sendMessage([new ByteBufNIO(ByteBuffer.allocate(1))], 1, OPERATION_CONTEXT)

        then:
        Thread.interrupted()
        thrown(MongoInterruptedException)
        connection.isClosed()
    }

    def 'should throw MongoSocketWriteException when Stream.write throws SocketException and the thread is not interrupted'() {
        given:
        stream.write(_, _) >> { throw new SocketException() }
        def connection = getOpenedConnection()

        when:
        connection.sendMessage([new ByteBufNIO(ByteBuffer.allocate(1))], 1, OPERATION_CONTEXT)

        then:
        thrown(MongoSocketWriteException)
        connection.isClosed()
    }

    def 'should throw MongoInterruptedException and leave the interrupt status set when Stream.read throws InterruptedIOException'() {
        given:
        stream.read(_, _) >> { throw new InterruptedIOException() }
        def connection = getOpenedConnection()
        Thread.currentThread().interrupt()

        when:
        connection.receiveMessage(1, OPERATION_CONTEXT)

        then:
        Thread.interrupted()
        thrown(MongoInterruptedException)
        connection.isClosed()
    }

    def 'should throw MongoInterruptedException and leave the interrupt status unset when Stream.read throws InterruptedIOException'() {
        given:
        stream.read(_, _) >> { throw new InterruptedIOException() }
        def connection = getOpenedConnection()

        when:
        connection.receiveMessage(1, OPERATION_CONTEXT)

        then:
        !Thread.interrupted()
        thrown(MongoInterruptedException)
        connection.isClosed()
    }

    def 'should throw MongoInterruptedException and leave the interrupt status set when Stream.read throws ClosedByInterruptException'() {
        given:
        stream.read(_, _) >> { throw new ClosedByInterruptException() }
        def connection = getOpenedConnection()
        Thread.currentThread().interrupt()

        when:
        connection.receiveMessage(1, OPERATION_CONTEXT)

        then:
        Thread.interrupted()
        thrown(MongoInterruptedException)
        connection.isClosed()
    }

    def 'should throw MongoInterruptedException when Stream.read throws SocketException and the thread is interrupted'() {
        given:
        stream.read(_, _) >> { throw new SocketException() }
        def connection = getOpenedConnection()
        Thread.currentThread().interrupt()

        when:
        connection.receiveMessage(1, OPERATION_CONTEXT)

        then:
        Thread.interrupted()
        thrown(MongoInterruptedException)
        connection.isClosed()
    }

    def 'should throw MongoSocketReadException when Stream.read throws SocketException and the thread is not interrupted'() {
        given:
        stream.read(_, _) >> { throw new SocketException() }
        def connection = getOpenedConnection()

        when:
        connection.receiveMessage(1, OPERATION_CONTEXT)

        then:
        thrown(MongoSocketReadException)
        connection.isClosed()
    }

    def 'Should throw timeout exception with underlying socket exception as a cause when Stream.read throws SocketException'() {
        given:
        stream.read(_, _) >> { throw new SocketTimeoutException() }
        def connection = getOpenedConnection()

        when:
        connection.receiveMessage(1, OPERATION_CONTEXT.withTimeoutContext(
                new TimeoutContext(TIMEOUT_SETTINGS_WITH_INFINITE_TIMEOUT)))

        then:
        def timeoutException = thrown(MongoOperationTimeoutException)
        def mongoSocketReadTimeoutException = timeoutException.getCause()
        mongoSocketReadTimeoutException instanceof MongoSocketReadTimeoutException
        mongoSocketReadTimeoutException.getCause() instanceof SocketTimeoutException

        connection.isClosed()
    }

    def 'Should wrap MongoSocketReadTimeoutException with MongoOperationTimeoutException'() {
        given:
        stream.read(_, _) >> { throw new MongoSocketReadTimeoutException("test", new ServerAddress(), null) }
        def connection = getOpenedConnection()

        when:
        connection.receiveMessage(1, OPERATION_CONTEXT.withTimeoutContext(
                new TimeoutContext(TIMEOUT_SETTINGS_WITH_INFINITE_TIMEOUT)))

        then:
        def timeoutException = thrown(MongoOperationTimeoutException)
        def mongoSocketReadTimeoutException = timeoutException.getCause()
        mongoSocketReadTimeoutException instanceof MongoSocketReadTimeoutException
        mongoSocketReadTimeoutException.getCause() == null

        connection.isClosed()
    }


    def 'Should wrap SocketException with timeout exception when Stream.read throws SocketException async'() {
        given:
        stream.readAsync(_ , _, _) >> { numBytes, operationContext, handler ->
            handler.failed(new SocketTimeoutException())
        }
        def connection = getOpenedConnection()
        def callback = new FutureResultCallback()
        def operationContext = OPERATION_CONTEXT.withTimeoutContext(
                new TimeoutContext(TIMEOUT_SETTINGS_WITH_INFINITE_TIMEOUT))
        when:
        connection.receiveMessageAsync(1, operationContext, callback)
        callback.get()

        then:
        def timeoutException = thrown(MongoOperationTimeoutException)
        def mongoSocketReadTimeoutException = timeoutException.getCause()
        mongoSocketReadTimeoutException instanceof MongoSocketReadTimeoutException
        mongoSocketReadTimeoutException.getCause() instanceof SocketTimeoutException

        connection.isClosed()
    }

    def 'Should wrap MongoSocketReadTimeoutException with MongoOperationTimeoutException async'() {
        given:
        stream.readAsync(_, _, _) >> { numBytes, operationContext, handler ->
            handler.failed(new MongoSocketReadTimeoutException("test", new ServerAddress(), null))
        }

        def connection = getOpenedConnection()
        def callback = new FutureResultCallback()
        def operationContext = OPERATION_CONTEXT.withTimeoutContext(
                new TimeoutContext(TIMEOUT_SETTINGS_WITH_INFINITE_TIMEOUT))
        when:
        connection.receiveMessageAsync(1, operationContext, callback)
        callback.get()

        then:
        def timeoutException = thrown(MongoOperationTimeoutException)
        def mongoSocketReadTimeoutException = timeoutException.getCause()
        mongoSocketReadTimeoutException instanceof MongoSocketReadTimeoutException
        mongoSocketReadTimeoutException.getCause() == null

        connection.isClosed()
    }

    def 'should close the stream when reading the message header throws an exception asynchronously'() {
        given:
        int seen = 0
        def (buffers1, messageId1, sndCallbck1, rcvdCallbck1) = helper.helloAsync()
        def (buffers2, messageId2, sndCallbck2, rcvdCallbck2) = helper.helloAsync()
        def headers = helper.generateHeaders([messageId1, messageId2])

        stream.writeAsync(_, _, _) >> { buffers, operationContext, callback ->
            callback.completed(null)
        }
        stream.readAsync(16, _, _) >> { numBytes, operationContext, handler ->
            if (seen == 0) {
                seen += 1
                return handler.failed(new IOException('Something went wrong'))
            }
            handler.completed(headers.pop())
        }
        stream.readAsync(94, _, _) >> { numBytes, operationContext, handler ->
            handler.completed(helper.defaultBody())
        }
        def connection = getOpenedConnection()

        when:
        connection.sendMessageAsync(buffers1, messageId1, OPERATION_CONTEXT, sndCallbck1)
        connection.sendMessageAsync(buffers2, messageId2, OPERATION_CONTEXT, sndCallbck2)
        connection.receiveMessageAsync(messageId1, OPERATION_CONTEXT, rcvdCallbck1)
        connection.receiveMessageAsync(messageId2, OPERATION_CONTEXT, rcvdCallbck2)
        rcvdCallbck1.get(1, SECONDS)

        then:
        thrown MongoSocketReadException
        connection.isClosed()

        when:
        rcvdCallbck2.get(1, SECONDS)

        then:
        thrown MongoSocketClosedException
    }

    def 'should close the stream when reading the message body throws an exception'() {
        given:
        stream.read(16, _) >> helper.defaultMessageHeader(1)
        stream.read(90, _) >> { throw new IOException('Something went wrong') }

        def connection = getOpenedConnection()

        when:
        connection.receiveMessage(1, OPERATION_CONTEXT)

        then:
        connection.isClosed()
        thrown MongoSocketReadException

        when:
        connection.receiveMessage(1, OPERATION_CONTEXT)

        then:
        thrown MongoSocketClosedException
    }


    def 'should close the stream when reading the message body throws an exception asynchronously'() {
        given:
        def (buffers1, messageId1, sndCallbck1, rcvdCallbck1) = helper.helloAsync()
        def (buffers2, messageId2, sndCallbck2, rcvdCallbck2) = helper.helloAsync()
        def headers = helper.generateHeaders([messageId1, messageId2])

        stream.writeAsync(_, _, _) >> { buffers, operationContext, callback ->
            callback.completed(null)
        }
        stream.readAsync(16, _, _) >> { numBytes, operationContext, handler ->
            handler.completed(headers.remove(0))
        }
        stream.readAsync(_, _, _) >> { numBytes, operationContext, handler ->
            handler.failed(new IOException('Something went wrong'))
        }
        def connection = getOpenedConnection()

        when:
        connection.sendMessageAsync(buffers1, messageId1, OPERATION_CONTEXT, sndCallbck1)
        connection.sendMessageAsync(buffers2, messageId2, OPERATION_CONTEXT, sndCallbck2)
        connection.receiveMessageAsync(messageId1, OPERATION_CONTEXT, rcvdCallbck1)
        rcvdCallbck1.get(1, SECONDS)

        then:
        thrown MongoSocketReadException
        connection.isClosed()

        when:
        connection.receiveMessageAsync(messageId2, OPERATION_CONTEXT, rcvdCallbck2)
        rcvdCallbck2.get(1, SECONDS)

        then:
        thrown MongoSocketClosedException
    }

    def 'should not close the stream on a command exception'() {
        given:
        def connection = getOpenedConnection()
        def pingCommandDocument = new BsonDocument('ping', new BsonInt32(1))
        def commandMessage = new CommandMessage(database, pingCommandDocument, fieldNameValidator, primary(), messageSettings, MULTIPLE,
                null)
        def response = '{ok : 0, errmsg : "failed"}'
        stream.getBuffer(1024) >> { new ByteBufNIO(ByteBuffer.wrap(new byte[1024])) }
        stream.read(16, _) >> helper.messageHeader(commandMessage.getId(), response)
        stream.read(_, _) >> helper.reply(response)

        when:
        connection.sendAndReceive(commandMessage, new BsonDocumentCodec(), OPERATION_CONTEXT)

        then:
        thrown(MongoCommandException)
        !connection.isClosed()
    }

    def 'should not close the stream on an asynchronous command exception'() {
        given:
        def connection = getOpenedConnection()
        def pingCommandDocument = new BsonDocument('ping', new BsonInt32(1))
        def commandMessage = new CommandMessage(database, pingCommandDocument, fieldNameValidator, primary(), messageSettings, MULTIPLE,
                null)
        def callback = new FutureResultCallback()
        def response = '{ok : 0, errmsg : "failed"}'

        stream.getBuffer(1024) >> { new ByteBufNIO(ByteBuffer.wrap(new byte[1024])) }
        stream.writeAsync(_, _, _) >> { buffers, operationContext, handler ->
            handler.completed(null)
        }
        stream.readAsync(16, _, _) >> { numBytes, operationContext, handler ->
            handler.completed(helper.defaultMessageHeader(commandMessage.getId()))
        }
        stream.readAsync(_, _, _) >> { numBytes, operationContext, handler ->
            handler.completed(helper.reply(response))
        }

        when:
        connection.sendAndReceiveAsync(commandMessage, new BsonDocumentCodec(), OPERATION_CONTEXT, callback)
        callback.get()

        then:
        thrown(MongoCommandException)
        !connection.isClosed()
    }

    def 'should notify all asynchronous writers of an exception'() {
        given:
        int numberOfOperations = 3
        ExecutorService streamPool = Executors.newFixedThreadPool(1)

        def messages = (1..numberOfOperations).collect { helper.helloAsync() }

        def streamLatch = new CountDownLatch(1)
        stream.writeAsync(_, _, _) >> { buffers, operationContext, callback ->
            streamPool.submit {
                streamLatch.await()
                callback.failed(new IOException())
            }
        }

        when:
        def connection = getOpenedConnection()
        def callbacks = []
        (1..numberOfOperations).each { n ->
            def (buffers, messageId, sndCallbck, rcvdCallbck) = messages.pop()
            connection.sendMessageAsync(buffers, messageId, OPERATION_CONTEXT, sndCallbck)
            callbacks.add(sndCallbck)
        }
        streamLatch.countDown()

        then:
        expectException(callbacks.pop())
        expectException(callbacks.pop())
        expectException(callbacks.pop())

        cleanup:
        streamPool.shutdown()
    }

    def 'should send events for successful command'() {
        given:
        def connection = getOpenedConnection()
        def pingCommandDocument = new BsonDocument('ping', new BsonInt32(1))
        def commandMessage = new CommandMessage(database, pingCommandDocument, fieldNameValidator, primary(), messageSettings, MULTIPLE,
                null)
        stream.getBuffer(1024) >> { new ByteBufNIO(ByteBuffer.wrap(new byte[1024])) }
        stream.read(16, _) >> helper.defaultMessageHeader(commandMessage.getId())
        stream.read(90, _) >> helper.defaultReply()

        when:
        connection.sendAndReceive(commandMessage, new BsonDocumentCodec(), OPERATION_CONTEXT)

        then:
        commandListener.eventsWereDelivered([
                new CommandStartedEvent(null, 1, 1, connection.getDescription(), 'admin', 'ping',
                        pingCommandDocument.append('$db', new BsonString('admin'))),
                new CommandSucceededEvent(null, 1, 1, connection.getDescription(), 'admin', 'ping',
                        new BsonDocument('ok', new BsonInt32(1)), 1000)])
    }

    def 'should send events for successful command with decoding error'() {
        given:
        def connection = getOpenedConnection()
        def pingCommandDocument = new BsonDocument('ping', new BsonInt32(1))
        def commandMessage = new CommandMessage(database, pingCommandDocument, fieldNameValidator, primary(), messageSettings, MULTIPLE,
                null)
        stream.getBuffer(1024) >> { new ByteBufNIO(ByteBuffer.wrap(new byte[1024])) }
        stream.read(16, _) >> helper.defaultMessageHeader(commandMessage.getId())
        stream.read(90, _) >> helper.defaultReply()

        when:
        connection.sendAndReceive(commandMessage, {
            BsonReader reader, DecoderContext decoderContext -> throw new CodecConfigurationException('')
        }, OPERATION_CONTEXT)

        then:
        thrown(CodecConfigurationException)
        commandListener.eventsWereDelivered([
                new CommandStartedEvent(null, 1, 1, connection.getDescription(), 'admin', 'ping',
                        pingCommandDocument.append('$db', new BsonString('admin'))),
                new CommandSucceededEvent(null, 1, 1, connection.getDescription(), 'admin', 'ping',
                        new BsonDocument('ok', new BsonInt32(1)), 1000)])
    }

    def 'should extract cluster and operation time into session context'() {
        given:
        def connection = getOpenedConnection()
        def pingCommandDocument = new BsonDocument('ping', new BsonInt32(1))
        def commandMessage = new CommandMessage(database, pingCommandDocument, fieldNameValidator, primary(), messageSettings, MULTIPLE,
                null)
        def response = '''{
                            ok : 1,
                            operationTime : { $timestamp : { "t" : 40, "i" : 20 } },
                            $clusterTime :  { clusterTime : { $timestamp : { "t" : 42, "i" : 21 } } }
                          }'''
        stream.getBuffer(1024) >> { new ByteBufNIO(ByteBuffer.wrap(new byte[1024])) }
        stream.read(16, _) >> helper.defaultMessageHeader(commandMessage.getId())
        stream.read(_, _) >> helper.reply(response)
        def sessionContext = Mock(SessionContext) {
            1 * advanceOperationTime(BsonDocument.parse(response).getTimestamp('operationTime'))
            1 * advanceClusterTime(BsonDocument.parse(response).getDocument('$clusterTime'))
            getReadConcern() >> ReadConcern.DEFAULT
        }
        def operationContext = OPERATION_CONTEXT.withSessionContext(sessionContext)

        when:
        connection.sendAndReceive(commandMessage, new BsonDocumentCodec(), operationContext)

        then:
        true
    }

    def 'should extract cluster and operation time into session context asynchronously'() {
        given:
        def connection = getOpenedConnection()
        def pingCommandDocument = new BsonDocument('ping', new BsonInt32(1))
        def commandMessage = new CommandMessage(database, pingCommandDocument, fieldNameValidator, primary(), messageSettings, MULTIPLE,
                null)
        def callback = new FutureResultCallback()
        def response = '''{
                            ok : 1,
                            operationTime : { $timestamp : { "t" : 40, "i" : 20 } },
                            $clusterTime :  { clusterTime : { $timestamp : { "t" : 42, "i" : 21 } } }
                          }'''
        stream.getBuffer(1024) >> { new ByteBufNIO(ByteBuffer.wrap(new byte[1024])) }
        stream.writeAsync(_, _, _) >> { buffers, operationContext, handler ->
            handler.completed(null)
        }
        stream.readAsync(16, _, _) >> { numBytes, operationContext, handler ->
            handler.completed(helper.defaultMessageHeader(commandMessage.getId()))
        }
        stream.readAsync(_, _, _) >> { numBytes, operationContext, handler ->
            handler.completed(helper.reply(response))
        }
        def sessionContext = Mock(SessionContext) {
            1 * advanceOperationTime(BsonDocument.parse(response).getTimestamp('operationTime'))
            1 * advanceClusterTime(BsonDocument.parse(response).getDocument('$clusterTime'))
            getReadConcern() >> ReadConcern.DEFAULT
        }
        def operationContext = OPERATION_CONTEXT.withSessionContext(sessionContext)

        when:
        connection.sendAndReceiveAsync(commandMessage, new BsonDocumentCodec(), operationContext, callback)
        callback.get()

        then:
        true
    }

    def 'should send events for command failure with exception writing message'() {
        given:
        def connection = getOpenedConnection()
        def pingCommandDocument = new BsonDocument('ping', new BsonInt32(1))
        def commandMessage = new CommandMessage(database, pingCommandDocument, fieldNameValidator, primary(), messageSettings, MULTIPLE,
                null)
        stream.getBuffer(1024) >> { new ByteBufNIO(ByteBuffer.wrap(new byte[1024])) }
        stream.write(_, _) >> { throw new MongoSocketWriteException('Failed to write', serverAddress, new IOException()) }

        when:
        connection.sendAndReceive(commandMessage, new BsonDocumentCodec(), OPERATION_CONTEXT)

        then:
        def e = thrown(MongoSocketWriteException)
        commandListener.eventsWereDelivered([
                new CommandStartedEvent(null, 1, 1, connection.getDescription(), 'admin', 'ping',
                        pingCommandDocument.append('$db', new BsonString('admin'))),
                new CommandFailedEvent(null, 1, 1, connection.getDescription(), 'admin', 'ping', 0, e)])
    }

    def 'should send events for command failure with exception reading header'() {
        given:
        def connection = getOpenedConnection()
        def pingCommandDocument = new BsonDocument('ping', new BsonInt32(1))
        def commandMessage = new CommandMessage(database, pingCommandDocument, fieldNameValidator, primary(), messageSettings, MULTIPLE,
                null)
        stream.getBuffer(1024) >> { new ByteBufNIO(ByteBuffer.wrap(new byte[1024])) }
        stream.read(16, _) >> { throw new MongoSocketReadException('Failed to read', serverAddress) }

        when:
        connection.sendAndReceive(commandMessage, new BsonDocumentCodec(), OPERATION_CONTEXT)

        then:
        def e = thrown(MongoSocketReadException)
        commandListener.eventsWereDelivered([
                new CommandStartedEvent(null, 1, 1, connection.getDescription(), 'admin', 'ping',
                        pingCommandDocument.append('$db', new BsonString('admin'))),
                new CommandFailedEvent(null, 1, 1, connection.getDescription(), 'admin', 'ping', 0, e)])
    }

    def 'should send events for command failure with exception reading body'() {
        given:
        def connection = getOpenedConnection()
        def pingCommandDocument = new BsonDocument('ping', new BsonInt32(1))
        def commandMessage = new CommandMessage(database, pingCommandDocument, fieldNameValidator, primary(), messageSettings, MULTIPLE,
                null)
        stream.getBuffer(1024) >> { new ByteBufNIO(ByteBuffer.wrap(new byte[1024])) }
        stream.read(16, _) >> helper.defaultMessageHeader(commandMessage.getId())
        stream.read(90, _) >> { throw new MongoSocketReadException('Failed to read', serverAddress) }

        when:
        connection.sendAndReceive(commandMessage, new BsonDocumentCodec(), OPERATION_CONTEXT)

        then:
        def e = thrown(MongoSocketException)
        commandListener.eventsWereDelivered([
                new CommandStartedEvent(null, 1, 1, connection.getDescription(), 'admin', 'ping',
                        pingCommandDocument.append('$db', new BsonString('admin'))),
                new CommandFailedEvent(null, 1, 1, connection.getDescription(), 'admin', 'ping', 0, e)])
    }

    def 'should send events for command failure with exception from failed command'() {
        given:
        def connection = getOpenedConnection()
        def pingCommandDocument = new BsonDocument('ping', new BsonInt32(1))
        def commandMessage = new CommandMessage(database, pingCommandDocument, fieldNameValidator, primary(), messageSettings, MULTIPLE,
                null)
        def response = '{ok : 0, errmsg : "failed"}'
        stream.getBuffer(1024) >> { new ByteBufNIO(ByteBuffer.wrap(new byte[1024])) }
        stream.read(16, _) >> helper.messageHeader(commandMessage.getId(), response)
        stream.read(_, _) >> helper.reply(response)

        when:
        connection.sendAndReceive(commandMessage, new BsonDocumentCodec(), OPERATION_CONTEXT)

        then:
        def e = thrown(MongoCommandException)
        commandListener.eventsWereDelivered([
                new CommandStartedEvent(null, 1, 1, connection.getDescription(), 'admin', 'ping',
                        pingCommandDocument.append('$db', new BsonString('admin'))),
                new CommandFailedEvent(null, 1, 1, connection.getDescription(), 'admin', 'ping', 0, e)])
    }

    def 'should send events with elided command and response in successful security-sensitive commands'() {
        given:
        def securitySensitiveCommandName = securitySensitiveCommand.keySet().iterator().next()
        def connection = getOpenedConnection()
        def commandMessage = new CommandMessage(database, securitySensitiveCommand, fieldNameValidator, primary(), messageSettings,
                MULTIPLE, null)
        stream.getBuffer(1024) >> { new ByteBufNIO(ByteBuffer.wrap(new byte[1024])) }
        stream.read(16, _) >> helper.defaultMessageHeader(commandMessage.getId())
        stream.read(90, _) >> helper.defaultReply()

        when:
        connection.sendAndReceive(commandMessage, new BsonDocumentCodec(), OPERATION_CONTEXT)

        then:
        commandListener.eventsWereDelivered([
                new CommandStartedEvent(null, 1, 1, connection.getDescription(), 'admin', securitySensitiveCommandName,
                        new BsonDocument()),
                new CommandSucceededEvent(null, 1, 1, connection.getDescription(), 'admin', securitySensitiveCommandName,
                        new BsonDocument(), 1)])

        where:
        securitySensitiveCommand << [
                new BsonDocument('authenticate', new BsonInt32(1)),
                new BsonDocument('saslStart', new BsonInt32(1)),
                new BsonDocument('saslContinue', new BsonInt32(1)),
                new BsonDocument('getnonce', new BsonInt32(1)),
                new BsonDocument('createUser', new BsonInt32(1)),
                new BsonDocument('updateUser', new BsonInt32(1)),
                new BsonDocument('copydbgetnonce', new BsonInt32(1)),
                new BsonDocument('copydbsaslstart', new BsonInt32(1)),
                new BsonDocument('copydb', new BsonInt32(1)),
                new BsonDocument('hello', new BsonInt32(1)).append('speculativeAuthenticate', new BsonDocument()),
                new BsonDocument(LEGACY_HELLO_LOWER, new BsonInt32(1)).append('speculativeAuthenticate', new BsonDocument()),
                new BsonDocument(LEGACY_HELLO, new BsonInt32(1)).append('speculativeAuthenticate', new BsonDocument())
        ]
    }

    def 'should send failed event with redacted exception in failed security-sensitive commands'() {
        given:
        def connection = getOpenedConnection()
        def commandMessage = new CommandMessage(database, securitySensitiveCommand, fieldNameValidator, primary(), messageSettings,
                MULTIPLE, null)
        stream.getBuffer(1024) >> { new ByteBufNIO(ByteBuffer.wrap(new byte[1024])) }
        stream.read(16, _) >> helper.defaultMessageHeader(commandMessage.getId())
        stream.read(_, _) >> helper.reply('{ok : 0, errmsg : "failed"}')

        when:
        connection.sendAndReceive(commandMessage, new BsonDocumentCodec(), OPERATION_CONTEXT)

        then:
        thrown(MongoCommandException)
        CommandFailedEvent failedEvent = commandListener.getEvents().get(1)
        failedEvent.throwable.class == MongoCommandException
        MongoCommandException e = failedEvent.throwable
        MongoCommandExceptionUtils.SecurityInsensitiveResponseField.fieldNames().containsAll(e.getResponse().keySet())

        where:
        securitySensitiveCommand << [
                new BsonDocument('authenticate', new BsonInt32(1)),
                new BsonDocument('saslStart', new BsonInt32(1)),
                new BsonDocument('saslContinue', new BsonInt32(1)),
                new BsonDocument('getnonce', new BsonInt32(1)),
                new BsonDocument('createUser', new BsonInt32(1)),
                new BsonDocument('updateUser', new BsonInt32(1)),
                new BsonDocument('copydbgetnonce', new BsonInt32(1)),
                new BsonDocument('copydbsaslstart', new BsonInt32(1)),
                new BsonDocument('copydb', new BsonInt32(1)),
                new BsonDocument('hello', new BsonInt32(1)).append('speculativeAuthenticate', new BsonDocument()),
                new BsonDocument(LEGACY_HELLO_LOWER, new BsonInt32(1)).append('speculativeAuthenticate', new BsonDocument()),
                new BsonDocument(LEGACY_HELLO, new BsonInt32(1)).append('speculativeAuthenticate', new BsonDocument())
        ]
    }

    def 'should send events for successful asynchronous command'() {
        given:
        def connection = getOpenedConnection()
        def pingCommandDocument = new BsonDocument('ping', new BsonInt32(1))
        def commandMessage = new CommandMessage(database, pingCommandDocument, fieldNameValidator, primary(), messageSettings, MULTIPLE,
                null)
        def callback = new FutureResultCallback()

        stream.getBuffer(1024) >> { new ByteBufNIO(ByteBuffer.wrap(new byte[1024])) }
        stream.writeAsync(_, _, _) >> { buffers, operationContext, handler ->
            handler.completed(null)
        }
        stream.readAsync(16, _, _) >> { numBytes, operationContext, handler ->
            handler.completed(helper.defaultMessageHeader(commandMessage.getId()))
        }
        stream.readAsync(90, _, _) >> { numBytes, operationContext, handler ->
            handler.completed(helper.defaultReply())
        }

        when:
        connection.sendAndReceiveAsync(commandMessage, new BsonDocumentCodec(), OPERATION_CONTEXT, callback)
        callback.get()

        then:
        commandListener.eventsWereDelivered([
                new CommandStartedEvent(null, 1, 1, connection.getDescription(), 'admin', 'ping',
                        pingCommandDocument.append('$db', new BsonString('admin'))),
                new CommandSucceededEvent(null, 1, 1, connection.getDescription(), 'admin', 'ping',
                        new BsonDocument('ok', new BsonInt32(1)), 1000)])
    }

    def 'should send events for successful asynchronous command with decoding error'() {
        given:
        def connection = getOpenedConnection()
        def pingCommandDocument = new BsonDocument('ping', new BsonInt32(1))
        def commandMessage = new CommandMessage(database, pingCommandDocument, fieldNameValidator, primary(), messageSettings, MULTIPLE,
                null)
        def callback = new FutureResultCallback()

        stream.getBuffer(1024) >> { new ByteBufNIO(ByteBuffer.wrap(new byte[1024])) }
        stream.writeAsync(_, _, _) >> { buffers, operationContext, handler ->
            handler.completed(null)
        }
        stream.readAsync(16, _, _) >> { numBytes, operationContext, handler ->
            handler.completed(helper.defaultMessageHeader(commandMessage.getId()))
        }
        stream.readAsync(90, _, _) >> { numBytes, operationContext, handler ->
            handler.completed(helper.defaultReply())
        }

        when:
        connection.sendAndReceiveAsync(commandMessage, {
            BsonReader reader, DecoderContext decoderContext -> throw new CodecConfigurationException('')
        }, OPERATION_CONTEXT, callback)
        callback.get()

        then:
        thrown(CodecConfigurationException)
        commandListener.eventsWereDelivered([
                new CommandStartedEvent(null, 1, 1, connection.getDescription(), 'admin', 'ping',
                        pingCommandDocument.append('$db', new BsonString('admin'))),
                new CommandSucceededEvent(null, 1, 1, connection.getDescription(), 'admin', 'ping',
                        new BsonDocument('ok', new BsonInt32(1)), 1000)])
    }


    def 'should send events for asynchronous command failure with exception writing message'() {
        given:
        def connection = getOpenedConnection()
        def pingCommandDocument = new BsonDocument('ping', new BsonInt32(1))
        def commandMessage = new CommandMessage(database, pingCommandDocument, fieldNameValidator, primary(), messageSettings, MULTIPLE,
                null)
        def callback = new FutureResultCallback()

        stream.getBuffer(1024) >> { new ByteBufNIO(ByteBuffer.wrap(new byte[1024])) }
        stream.writeAsync(_, _, _) >> { buffers, operationContext, handler ->
            handler.failed(new MongoSocketWriteException('failed', serverAddress, new IOException()))
        }

        when:
        connection.sendAndReceiveAsync(commandMessage, new BsonDocumentCodec(), OPERATION_CONTEXT, callback)
        callback.get()

        then:
        def e = thrown(MongoSocketWriteException)
        commandListener.eventsWereDelivered([
                new CommandStartedEvent(null, 1, 1, connection.getDescription(), 'admin', 'ping',
                        pingCommandDocument.append('$db', new BsonString('admin'))),
                new CommandFailedEvent(null, 1, 1, connection.getDescription(), 'admin', 'ping', 0, e)])
    }

    def 'should send events for asynchronous command failure with exception reading header'() {
        given:
        def connection = getOpenedConnection()
        def pingCommandDocument = new BsonDocument('ping', new BsonInt32(1))
        def commandMessage = new CommandMessage(database, pingCommandDocument, fieldNameValidator, primary(), messageSettings, MULTIPLE,
                null)
        def callback = new FutureResultCallback()

        stream.getBuffer(1024) >> { new ByteBufNIO(ByteBuffer.wrap(new byte[1024])) }
        stream.writeAsync(_, _, _) >> { buffers, operationContext, handler ->
            handler.completed(null)
        }
        stream.readAsync(16, _, _) >> { numBytes, operationContext, handler ->
            handler.failed(new MongoSocketReadException('Failed to read', serverAddress))
        }

        when:
        connection.sendAndReceiveAsync(commandMessage, new BsonDocumentCodec(), OPERATION_CONTEXT, callback)
        callback.get()

        then:
        def e = thrown(MongoSocketReadException)
        commandListener.eventsWereDelivered([
                new CommandStartedEvent(null, 1, 1, connection.getDescription(), 'admin', 'ping',
                        pingCommandDocument.append('$db', new BsonString('admin'))),
                new CommandFailedEvent(null, 1, 1, connection.getDescription(), 'admin', 'ping', 0, e)])
    }

    def 'should send events for asynchronous command failure with exception reading body'() {
        given:
        def connection = getOpenedConnection()
        def pingCommandDocument = new BsonDocument('ping', new BsonInt32(1))
        def commandMessage = new CommandMessage(database, pingCommandDocument, fieldNameValidator, primary(), messageSettings, MULTIPLE,
                null)
        def callback = new FutureResultCallback()

        stream.getBuffer(1024) >> { new ByteBufNIO(ByteBuffer.wrap(new byte[1024])) }
        stream.writeAsync(_, _, _) >> { buffers, operationContext, handler ->
            handler.completed(null)
        }
        stream.readAsync(16, _, _) >> { numBytes, operationContext, handler ->
            handler.completed(helper.defaultMessageHeader(commandMessage.getId()))
        }
        stream.readAsync(90, _, _) >> { numBytes, operationContext, handler ->
            handler.failed(new MongoSocketReadException('Failed to read', serverAddress))
        }

        when:
        connection.sendAndReceiveAsync(commandMessage, new BsonDocumentCodec(), OPERATION_CONTEXT, callback)
        callback.get()

        then:
        def e = thrown(MongoSocketReadException)
        commandListener.eventsWereDelivered([
                new CommandStartedEvent(null, 1, 1, connection.getDescription(), 'admin', 'ping',
                        pingCommandDocument.append('$db', new BsonString('admin'))),
                new CommandFailedEvent(null, 1, 1, connection.getDescription(), 'admin', 'ping', 0, e)])
    }

    def 'should send events for asynchronous command failure with exception from failed command'() {
        given:
        def connection = getOpenedConnection()
        def pingCommandDocument = new BsonDocument('ping', new BsonInt32(1))
        def commandMessage = new CommandMessage(database, pingCommandDocument, fieldNameValidator, primary(), messageSettings, MULTIPLE,
                null)
        def callback = new FutureResultCallback()
        def response = '{ok : 0, errmsg : "failed"}'

        stream.getBuffer(1024) >> { new ByteBufNIO(ByteBuffer.wrap(new byte[1024])) }
        stream.writeAsync(_, _, _) >> { buffers, operationContext, handler ->
            handler.completed(null)
        }
        stream.readAsync(16, _, _) >> { numBytes, operationContext, handler ->
            handler.completed(helper.defaultMessageHeader(commandMessage.getId()))
        }
        stream.readAsync(_, _, _) >> { numBytes, operationContext, handler ->
            handler.completed(helper.reply(response))
        }

        when:
        connection.sendAndReceiveAsync(commandMessage, new BsonDocumentCodec(), OPERATION_CONTEXT, callback)
        callback.get()

        then:
        def e = thrown(MongoCommandException)
        commandListener.eventsWereDelivered([
                new CommandStartedEvent(null, 1, 1, connection.getDescription(), 'admin', 'ping',
                        pingCommandDocument.append('$db', new BsonString('admin'))),
                new CommandFailedEvent(null, 1, 1, connection.getDescription(), 'admin', 'ping', 0, e)])
    }

    def 'should send events with elided command and response in successful security-sensitive asynchronous commands'() {
        given:
        def securitySensitiveCommandName = securitySensitiveCommand.keySet().iterator().next()
        def connection = getOpenedConnection()
        def commandMessage = new CommandMessage(database, securitySensitiveCommand, fieldNameValidator, primary(), messageSettings,
                MULTIPLE, null)
        def callback = new FutureResultCallback()

        stream.getBuffer(1024) >> { new ByteBufNIO(ByteBuffer.wrap(new byte[1024])) }
        stream.writeAsync(_, _, _) >> { buffers, operationContext, handler ->
            handler.completed(null)
        }
        stream.readAsync(16, _, _) >> { numBytes, operationContext, handler ->
            handler.completed(helper.defaultMessageHeader(commandMessage.getId()))
        }
        stream.readAsync(90, _, _) >> { numBytes, operationContext, handler ->
            handler.completed(helper.defaultReply())
        }

        when:
        connection.sendAndReceiveAsync(commandMessage, new BsonDocumentCodec(), OPERATION_CONTEXT, callback)
        callback.get()

        then:
        commandListener.eventsWereDelivered([
                new CommandStartedEvent(null, 1, 1, connection.getDescription(), 'admin', securitySensitiveCommandName,
                        new BsonDocument()),
                new CommandSucceededEvent(null, 1, 1, connection.getDescription(), 'admin', securitySensitiveCommandName,
                        new BsonDocument(), 1)])

        where:
        securitySensitiveCommand << [
                new BsonDocument('authenticate', new BsonInt32(1)),
                new BsonDocument('saslStart', new BsonInt32(1)),
                new BsonDocument('saslContinue', new BsonInt32(1)),
                new BsonDocument('getnonce', new BsonInt32(1)),
                new BsonDocument('createUser', new BsonInt32(1)),
                new BsonDocument('updateUser', new BsonInt32(1)),
                new BsonDocument('copydbgetnonce', new BsonInt32(1)),
                new BsonDocument('copydbsaslstart', new BsonInt32(1)),
                new BsonDocument('copydb', new BsonInt32(1)),
                new BsonDocument('hello', new BsonInt32(1)).append('speculativeAuthenticate', new BsonDocument()),
                new BsonDocument(LEGACY_HELLO_LOWER, new BsonInt32(1)).append('speculativeAuthenticate', new BsonDocument()),
                new BsonDocument(LEGACY_HELLO, new BsonInt32(1)).append('speculativeAuthenticate', new BsonDocument())
        ]
    }

    private static boolean expectException(rcvdCallbck) {
        try {
            rcvdCallbck.get()
            false
        } catch (MongoSocketWriteException e) {
            true
        }
    }
}
