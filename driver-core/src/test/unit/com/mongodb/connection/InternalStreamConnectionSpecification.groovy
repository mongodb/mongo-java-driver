/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import category.Async
import category.SlowUnit
import com.mongodb.MongoInternalException
import com.mongodb.MongoNamespace
import com.mongodb.MongoSocketClosedException
import com.mongodb.MongoSocketReadException
import com.mongodb.MongoSocketWriteException
import com.mongodb.ServerAddress
import com.mongodb.async.FutureResultCallback
import com.mongodb.async.SingleResultCallback
import com.mongodb.event.ConnectionListener
import com.mongodb.event.ConnectionMessageReceivedEvent
import com.mongodb.event.ConnectionMessagesSentEvent
import org.bson.BsonBinaryWriter
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.ByteBuf
import org.bson.ByteBufNIO
import org.bson.Document
import org.bson.codecs.DocumentCodec
import org.bson.codecs.EncoderContext
import org.bson.io.BasicOutputBuffer
import org.bson.io.OutputBuffer
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf
import spock.lang.Specification
import spock.util.concurrent.AsyncConditions

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.security.SecureRandom
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

import static MongoNamespace.COMMAND_COLLECTION_NAME
import static com.mongodb.CustomMatchers.compare
import static com.mongodb.connection.ConnectionDescription.getDefaultMaxMessageSize
import static com.mongodb.connection.ConnectionDescription.getDefaultMaxWriteBatchSize
import static com.mongodb.connection.ServerDescription.getDefaultMaxDocumentSize
import static java.util.concurrent.TimeUnit.SECONDS

@SuppressWarnings(['UnusedVariable'])
class InternalStreamConnectionSpecification extends Specification {
    private static final ServerId SERVER_ID = new ServerId(new ClusterId(), new ServerAddress())

    def helper = new StreamHelper()
    def serverAddress = new ServerAddress()
    def connectionId = new ConnectionId(SERVER_ID, 1, 1)

    def connectionDescription = new ConnectionDescription(connectionId, new ServerVersion(), ServerType.STANDALONE,
                                                          getDefaultMaxWriteBatchSize(), getDefaultMaxDocumentSize(),
                                                          getDefaultMaxMessageSize())
    def stream = Mock(Stream) {
        openAsync(_) >> { it[0].completed(null) }
    }
    def streamFactory = Mock(StreamFactory) {
        create(_) >> { stream }
    }
    def initializer = Mock(InternalConnectionInitializer) {
        initialize(_) >> { connectionDescription }
        initializeAsync(_, _) >> { it[1].onResult(connectionDescription, null) }
    }
    def listener = Mock(ConnectionListener)

    def getConnection() {
        new InternalStreamConnection(SERVER_ID, streamFactory, initializer, listener)
    }

    def getOpenedConnection() {
        def connection = getConnection();
        connection.open()
        connection
    }

    def 'should fire connection opened event'() {
        when:
        getConnection().open()

        then:
        1 * listener.connectionOpened(_)
    }

    def 'should fire connection closed event'() {
        when:
        getOpenedConnection().close()

        then:
        1 * listener.connectionClosed(_)
    }

    def 'should fire messages sent event'() {
        given:
        def connection = getOpenedConnection()
        def (buffers1, messageId1) = helper.isMaster()
        def messageSize = helper.remaining(buffers1)
        stream.write(_) >> {
            helper.write(buffers1)
        }
        when:
        connection.sendMessage(buffers1, messageId1)


        then:
        1 * listener.messagesSent {
            compare(new ConnectionMessagesSentEvent(connectionId, messageId1, messageSize), it)
        }
    }

    @Category(Async)
    @IgnoreIf({ javaVersion < 1.7 })
    def 'should fire message sent event asynchronously'() {
        def (buffers1, messageId1) = helper.isMaster()
        def messageSize = helper.remaining(buffers1)
        def connection = getOpenedConnection()
        def latch = new CountDownLatch(1);
        stream.writeAsync(_, _) >> { List<ByteBuf> buffers, AsyncCompletionHandler<Void> callback ->
            helper.write(buffers1)
            callback.completed(null)
        }

        when:
        connection.sendMessageAsync(buffers1, messageId1, new SingleResultCallback<Void>() {
            @Override
            void onResult(final Void result, final Throwable t) {
                latch.countDown();
            }
        })
        latch.await()

        then:
        1 * listener.messagesSent {
            compare(new ConnectionMessagesSentEvent(connectionId, messageId1, messageSize), it)
        }
    }

    def 'should fire message received event'() {
        given:
        def connection = getOpenedConnection()
        def (buffers1, messageId1) = helper.isMaster()
        stream.read(_) >>> helper.read([messageId1])

        when:
        connection.receiveMessage(messageId1)

        then:
        1 * listener.messageReceived {
            compare(new ConnectionMessageReceivedEvent(connectionId, messageId1, 110), it)
        }
    }

    @Category(Async)
    @IgnoreIf({ javaVersion < 1.7 })
    def 'should fire message received event asynchronously'() {
        given:
        def (buffers1, messageId1) = helper.isMaster()
        stream.readAsync(36, _) >> { int numBytes, AsyncCompletionHandler<ByteBuf> handler ->
            handler.completed(helper.header(messageId1))
        }
        stream.readAsync(74, _) >> { int numBytes, AsyncCompletionHandler<ByteBuf> handler ->
            handler.completed(helper.body())
        }
        def connection = getOpenedConnection()
        def latch = new CountDownLatch(1);

        when:
        connection.receiveMessageAsync(messageId1, new SingleResultCallback<ResponseBuffers>() {
            @Override
            void onResult(final ResponseBuffers result, final Throwable t) {
                latch.countDown();
            }
        })
        latch.await()

        then:
        1 * listener.messageReceived {
            compare(new ConnectionMessageReceivedEvent(connectionId, messageId1, 110), it)
        }
    }

    def 'should change the connection description when opened'() {
        when:
        def connection = getConnection()

        then:
        connection.getDescription().getServerType() == ServerType.UNKNOWN
        connection.getDescription().getConnectionId().getServerValue() == null

        when:
        connection.open()

        then:
        connection.opened()
        connection.getDescription().getServerType() == ServerType.STANDALONE
        connection.getDescription().getConnectionId().getServerValue() == 1

    }

    @Category(Async)
    @IgnoreIf({ javaVersion < 1.7 })
    def 'should change the connection description when opened asynchronously'() {
        when:
        def connection = getConnection()
        def futureResultCallback = new FutureResultCallback<Void>()

        then:
        connection.getDescription().getServerType() == ServerType.UNKNOWN
        connection.getDescription().getConnectionId().getServerValue() == null

        when:
        connection.openAsync(futureResultCallback)
        futureResultCallback.get(60, SECONDS)

        then:
        connection.opened()
        connection.getDescription().getServerType() == ServerType.STANDALONE
        connection.getDescription().getConnectionId().getServerValue() == 1

    }

    def 'should handle out of order messages on the stream'() {
        // Connect then: Send(1), Send(2), Send(3), Receive(3), Receive(2), Receive(1)
        given:
        ExecutorService pool = Executors.newFixedThreadPool(3)
        def connection = getOpenedConnection()
        def (buffers1, messageId1) = helper.isMaster()
        def (buffers2, messageId2) = helper.isMaster()
        def (buffers3, messageId3) = helper.isMaster()
        stream.read(_) >>> helper.read([messageId1, messageId2, messageId3], ordered)

        when:
        connection.sendMessage(buffers1, messageId1)
        connection.sendMessage(buffers2, messageId2)
        connection.sendMessage(buffers3, messageId3)

        then:
        def conds = new AsyncConditions()
        [messageId1, messageId2, messageId3].each { messageId ->
            pool.submit({
                            conds.evaluate {
                                assert connection.receiveMessage(messageId).replyHeader.responseTo == messageId
                            }
                        } as Runnable)

        }
        conds.await(10000)

        cleanup:
        pool.shutdown()

        where:
        ordered << [true, false]
    }

    @Category(Async)
    @IgnoreIf({ javaVersion < 1.7 })
    def 'should handle out of order messages on the stream asynchronously'() {
        // Connect then: SendAsync(1), SendAsync(2), SendAsync(3), ReceiveAsync(3), ReceiveAsync(2), ReceiveAsync(1)
        given:
        def (buffers1, messageId1, sndCallbck1, rcvdCallbck1) = helper.isMasterAsync()
        def (buffers2, messageId2, sndCallbck2, rcvdCallbck2) = helper.isMasterAsync()
        def (buffers3, messageId3, sndCallbck3, rcvdCallbck3) = helper.isMasterAsync()
        def headers = helper.generateHeaders([messageId1, messageId2, messageId3], ordered)

        stream.writeAsync(_, _) >> { List<ByteBuf> buffers, AsyncCompletionHandler<Void> callback ->
            callback.completed(null)
        }

        stream.readAsync(36, _) >> { int numBytes, AsyncCompletionHandler<ByteBuf> handler ->
            handler.completed(headers.pop())
        }
        stream.readAsync(74, _) >> { int numBytes, AsyncCompletionHandler<ByteBuf> handler ->
            handler.completed(helper.body())
        }

        def connection = getOpenedConnection()

        when:
        connection.sendMessageAsync(buffers1, messageId1, sndCallbck1)
        connection.sendMessageAsync(buffers2, messageId2, sndCallbck2)
        connection.sendMessageAsync(buffers3, messageId3, sndCallbck3)
        connection.receiveMessageAsync(messageId3, rcvdCallbck3)
        connection.receiveMessageAsync(messageId2, rcvdCallbck2)
        connection.receiveMessageAsync(messageId1, rcvdCallbck1)

        then:
        rcvdCallbck1.get().replyHeader.responseTo == messageId1
        rcvdCallbck2.get().replyHeader.responseTo == messageId2
        rcvdCallbck3.get().replyHeader.responseTo == messageId3

        where:
        ordered << [true, false]
    }

    def 'should close the stream when initialization throws an exception'() {
        given:
        def failedInitializer = Mock(InternalConnectionInitializer) {
            initialize(_) >> { throw new MongoInternalException('Something went wrong') }
        }
        def connection = new InternalStreamConnection(SERVER_ID, streamFactory, failedInitializer, listener)

        when:
        connection.open()

        then:
        thrown MongoInternalException
        connection.isClosed()
    }

    @Category(Async)
    @IgnoreIf({ javaVersion < 1.7 })
    def 'should close the stream when initialization throws an exception asynchronously'() {
        given:
        def failedInitializer = Mock(InternalConnectionInitializer) {
            initializeAsync(_, _) >> { it[1].onResult(null, new MongoInternalException('Something went wrong')); }
        }
        def connection = new InternalStreamConnection(SERVER_ID, streamFactory, failedInitializer, listener)

        when:
        def futureResultCallback = new FutureResultCallback<Void>()
        connection.openAsync(futureResultCallback)
        futureResultCallback.get(60, SECONDS)

        then:
        thrown MongoInternalException
        connection.isClosed()
    }

    def 'should close the stream when writing a message throws an exception'() {
        given:
        stream.write(_) >> { throw new IOException('Something went wrong') }

        def connection = getOpenedConnection()
        def (buffers1, messageId1) = helper.isMaster()
        def (buffers2, messageId2) = helper.isMaster()

        when:
        connection.sendMessage(buffers1, messageId1)

        then:
        connection.isClosed()
        thrown MongoSocketWriteException

        when:
        connection.sendMessage(buffers2, messageId2)

        then:
        thrown MongoSocketClosedException
    }

    @Category(Async)
    @IgnoreIf({ javaVersion < 1.7 })
    def 'should close the stream when writing a message throws an exception asynchronously'() {
        given:
        def (buffers1, messageId1, sndCallbck1, rcvdCallbck1) = helper.isMasterAsync()
        def (buffers2, messageId2, sndCallbck2, rcvdCallbck2) = helper.isMasterAsync()
        def headers = helper.generateHeaders([messageId1, messageId2])
        int seen = 0

        stream.writeAsync(_, _) >> { List<ByteBuf> buffers, AsyncCompletionHandler<Void> callback ->
            if (seen == 0) {
                seen += 1
                return callback.failed(new IOException('Something went wrong'))
            }
            callback.completed(null)
        }

        def connection = getOpenedConnection()

        when:
        connection.sendMessageAsync(buffers1, messageId1, sndCallbck1)
        sndCallbck1.get(10, SECONDS)

        then:
        thrown MongoSocketWriteException
        connection.isClosed()

        when:
        connection.sendMessageAsync(buffers2, messageId2, sndCallbck2)
        sndCallbck2.get(10, SECONDS)

        then:
        thrown MongoSocketClosedException
    }

    def 'should close the stream when reading the message header throws an exception'() {
        given:
        stream.read(36) >> { throw new IOException('Something went wrong') }
        stream.read(74) >> helper.body()

        def connection = getOpenedConnection()
        def (buffers1, messageId1) = helper.isMaster()
        def (buffers2, messageId2) = helper.isMaster()

        when:
        connection.sendMessage(buffers1, messageId1)
        connection.sendMessage(buffers2, messageId2)
        connection.receiveMessage(messageId1)

        then:
        connection.isClosed()
        thrown MongoSocketReadException

        when:
        connection.receiveMessage(messageId2)

        then:
        thrown MongoSocketClosedException
    }

    def 'should throw MongoInternalException when reply header message length > max message length'() {
        given:
        stream.read(36) >> { helper.headerWithMessageSizeGreaterThanMax(1) }

        def connection = getOpenedConnection()

        when:
        connection.receiveMessage(1)

        then:
        thrown(MongoInternalException)
        connection.isClosed()
    }

    def 'should throw MongoInternalException when reply header message length > max message length asynchronously'() {
        given:
        stream.readAsync(36, _) >> { int numBytes, AsyncCompletionHandler<ByteBuf> handler ->
            handler.completed(helper.headerWithMessageSizeGreaterThanMax(1))
        }

        def connection = getOpenedConnection()
        def callback = new FutureResultCallback()

        when:
        connection.receiveMessageAsync(1, callback)
        callback.get()

        then:
        thrown(MongoInternalException)
        connection.isClosed()
    }

    @Category(Async)
    @IgnoreIf({ javaVersion < 1.7 })
    def 'should close the stream when reading the message header throws an exception asynchronously'() {
        given:
        int seen = 0
        def (buffers1, messageId1, sndCallbck1, rcvdCallbck1) = helper.isMasterAsync()
        def (buffers2, messageId2, sndCallbck2, rcvdCallbck2) = helper.isMasterAsync()
        def headers = helper.generateHeaders([messageId1, messageId2])

        stream.writeAsync(_, _) >> { List<ByteBuf> buffers, AsyncCompletionHandler<Void> callback ->
            callback.completed(null)
        }
        stream.readAsync(36, _) >> { int numBytes, AsyncCompletionHandler<ByteBuf> handler ->
            if (seen == 0) {
                seen += 1
                return handler.failed(new IOException('Something went wrong'))
            }
            handler.completed(headers.pop())
        }
        stream.readAsync(74, _) >> { int numBytes, AsyncCompletionHandler<ByteBuf> handler ->
            handler.completed(helper.body())
        }
        def connection = getOpenedConnection()

        when:
        connection.sendMessageAsync(buffers1, messageId1, sndCallbck1)
        connection.sendMessageAsync(buffers2, messageId2, sndCallbck2)
        connection.receiveMessageAsync(messageId1, rcvdCallbck1)
        connection.receiveMessageAsync(messageId2, rcvdCallbck2)
        rcvdCallbck1.get(10, SECONDS)

        then:
        thrown MongoSocketReadException
        connection.isClosed()

        when:
        rcvdCallbck2.get(10, SECONDS)

        then:
        thrown MongoSocketClosedException
    }

    def 'should close the stream when reading the message body throws an exception'() {
        given:
        def (buffers1, messageId1) = helper.isMaster()
        def (buffers2, messageId2) = helper.isMaster()
        def headers = helper.generateHeaders([messageId1, messageId2])

        stream.read(36) >> { headers.pop() }
        stream.read(74) >> { throw new IOException('Something went wrong') }

        def connection = getOpenedConnection()

        when:
        connection.sendMessage(buffers1, messageId1)
        connection.sendMessage(buffers2, messageId2)
        connection.receiveMessage(messageId1)

        then:
        connection.isClosed()
        thrown MongoSocketReadException

        when:
        connection.receiveMessage(messageId2)

        then:
        thrown MongoSocketClosedException
    }

    @Category(Async)
    @IgnoreIf({ javaVersion < 1.7 })
    def 'should close the stream when reading the message body throws an exception asynchronously'() {
        given:
        def (buffers1, messageId1, sndCallbck1, rcvdCallbck1) = helper.isMasterAsync()
        def (buffers2, messageId2, sndCallbck2, rcvdCallbck2) = helper.isMasterAsync()
        def headers = helper.generateHeaders([messageId1, messageId2])

        stream.writeAsync(_, _) >> { List<ByteBuf> buffers, AsyncCompletionHandler<Void> callback ->
            callback.completed(null)
        }
        stream.readAsync(36, _) >> { int numBytes, AsyncCompletionHandler<ByteBuf> handler ->
            handler.completed(headers.remove(0))
        }
        stream.readAsync(74, _) >> { int numBytes, AsyncCompletionHandler<ByteBuf> handler ->
            handler.failed(new IOException('Something went wrong'))
        }
        def connection = getOpenedConnection()

        when:
        connection.sendMessageAsync(buffers1, messageId1, sndCallbck1)
        connection.sendMessageAsync(buffers2, messageId2, sndCallbck2)
        connection.receiveMessageAsync(messageId1, rcvdCallbck1)
        rcvdCallbck1.get(10, SECONDS)

        then:
        thrown MongoSocketReadException
        connection.isClosed()

        when:
        connection.receiveMessageAsync(messageId2, rcvdCallbck2)
        rcvdCallbck2.get(10, SECONDS)

        then:
        thrown MongoSocketClosedException
    }

    def 'should notify all asynchronous writers of an exception'() {
        given:
        int numberOfOperations = 3
        ExecutorService streamPool = Executors.newFixedThreadPool(1)

        def messages = (1..numberOfOperations).collect { helper.isMasterAsync() }
        def headers = messages.collect { buffers, messageId, sndCallbck, rcvdCallbck -> helper.header(messageId) }

        def streamLatch = new CountDownLatch(1)
        stream.writeAsync(_, _) >> { List<ByteBuf> buffers, AsyncCompletionHandler<Void> callback ->
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
            connection.sendMessageAsync(buffers, messageId, sndCallbck)
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

    private static boolean expectException(rcvdCallbck) {
        try {
            rcvdCallbck.get()
            false
        } catch (MongoSocketWriteException e) {
            true
        }
    }

    @IgnoreIf({ System.getProperty('ignoreSlowUnitTests') == 'true' })
    @Category(SlowUnit)
    def 'should have threadsafe connection pipelining'() {
        given:
        int threads = 10
        int numberOfOperations = 10000
        ExecutorService pool = Executors.newFixedThreadPool(threads)
        def messages = (1..numberOfOperations).collect { helper.isMaster() }
        def headers = helper.generateHeaders(messages.collect { buffer, messageId -> messageId })
        stream.read(36) >> { headers.pop() }
        stream.read(74) >> { helper.body() }

        when:
        def connection = getOpenedConnection()

        then:
        def conds = new AsyncConditions()
        def latch = new CountDownLatch(numberOfOperations)
        (1..numberOfOperations).each { n ->
            def (buffers, messageId) = messages.pop()
            pool.submit({ connection.sendMessage(buffers, messageId) } as Runnable)
            pool.submit({
                            conds.evaluate {
                                assert connection.receiveMessage(messageId).replyHeader.responseTo == messageId
                            }
                            latch.countDown()
                        } as Runnable)

        }
        latch.await(10, SECONDS)
        conds.await(10)

        cleanup:
        pool.shutdown()
    }

    @Category([Async, SlowUnit])
    @IgnoreIf({ System.getProperty('ignoreSlowUnitTests') == 'true' || javaVersion < 1.7 })
    def 'should have threadsafe connection pipelining asynchronously'() {
        given:
        int threads = 10
        int numberOfOperations = 10000
        ExecutorService pool = Executors.newFixedThreadPool(threads)
        ExecutorService streamPool = Executors.newFixedThreadPool(4)

        def messages = (1..numberOfOperations).collect { helper.isMasterAsync() }
        def headers = messages.collect { buffers, messageId, sndCallbck, rcvdCallbck -> helper.header(messageId) }

        stream.writeAsync(_, _) >> { List<ByteBuf> buffers, AsyncCompletionHandler<Void> callback ->
            streamPool.submit {
                callback.completed(null)
            }
        }
        stream.readAsync(36, _) >> { int numBytes, AsyncCompletionHandler<ByteBuf> handler ->
            streamPool.submit {
                handler.completed(headers.pop())
            }
        }
        stream.readAsync(74, _) >> { int numBytes, AsyncCompletionHandler<ByteBuf> handler ->
            streamPool.submit {
                handler.completed(helper.body())
            }
        }

        when:
        def connection = getOpenedConnection()

        then:
        def latch = new CountDownLatch(numberOfOperations)
        def conds = new AsyncConditions()
        (1..numberOfOperations).each { n ->
            def (buffers, messageId, sndCallbck, rcvdCallbck) = messages.pop()

            pool.submit {
                connection.sendMessageAsync(buffers, messageId, sndCallbck)
            }
            pool.submit {
                            connection.receiveMessageAsync(messageId, rcvdCallbck)
                            conds.evaluate {
                                assert rcvdCallbck.get().replyHeader.responseTo == messageId
                            }
                            latch.countDown()
                        }
        }

        latch.await(10, SECONDS)
        conds.await(10)

        cleanup:
        pool.shutdown()
        streamPool.shutdown()
    }

    class StreamHelper {
        int nextMessageId = 900000 // Generates a message then adds one to the id

        def remaining(List<ByteBuf> buffers) {
            int remaining = 0
            buffers.each {
                remaining += it.remaining()
            }
            remaining
        }

        def write(List<ByteBuf> buffers) {
            buffers.each {
                it.get(new byte[it.remaining()])
            }
        }


        def read(List<Integer> messageIds) {
            read(messageIds, true)
        }

        def read(List<Integer> messageIds, boolean ordered) {
            List<ByteBuf> headers = messageIds.collect { header(it) }
            List<ByteBuf> bodies = messageIds.collect { body() }
            if (!ordered) {
                Collections.shuffle(headers, new SecureRandom())
            }
            [headers, bodies].transpose().flatten()
        }

        def header(messageId) {
            ByteBuffer headerByteBuffer = ByteBuffer.allocate(36).with {
                order(ByteOrder.LITTLE_ENDIAN);
                putInt(110);           // messageLength
                putInt(4);             // requestId
                putInt(messageId);     // responseTo
                putInt(1);             // opCode
                putInt(0);             // responseFlags
                putLong(0);            // cursorId
                putInt(0);             // starting from
                putInt(1);             // number returned
            }
            headerByteBuffer.flip()
            new ByteBufNIO(headerByteBuffer)
        }

        def headerWithMessageSizeGreaterThanMax(messageId) {
            ByteBuffer headerByteBuffer = ByteBuffer.allocate(36).with {
                order(ByteOrder.LITTLE_ENDIAN);
                putInt(connectionDescription.maxMessageSize + 1);   // messageLength
                putInt(4);             // requestId
                putInt(messageId);     // responseTo
                putInt(1);             // opCode
                putInt(0);             // responseFlags
                putLong(0);            // cursorId
                putInt(0);             // starting from
                putInt(1);             // number returned
            }
            headerByteBuffer.flip()
            new ByteBufNIO(headerByteBuffer)
        }

        def body() {
            def okResponse = ['connectionId': 1, 'n': 0, 'syncMillis': 0, 'writtenTo': null, 'err': null, 'ok': 1] as Document
            OutputBuffer outputBuffer = new BasicOutputBuffer()
            BsonBinaryWriter binaryResponse = new BsonBinaryWriter(outputBuffer)
            new DocumentCodec().encode(binaryResponse, okResponse, EncoderContext.builder().build())
            new ByteBufNIO(ByteBuffer.allocate(outputBuffer.size()).put(outputBuffer.toByteArray()))
        }

        def generateHeaders(List<Integer> messageIds) {
            generateHeaders(messageIds, true)
        }

        def generateHeaders(List<Integer> messageIds, boolean ordered) {
            List<ByteBuf> headers = messageIds.collect { header(it) }
            if (!ordered) {
                Collections.shuffle(headers, new SecureRandom())
            }
            headers
        }

        def isMaster() {
            def command = new CommandMessage(new MongoNamespace('admin', COMMAND_COLLECTION_NAME).getFullName(),
                                             new BsonDocument('ismaster', new BsonInt32(1)),
                                             false, MessageSettings.builder().build());
            OutputBuffer outputBuffer = new BasicOutputBuffer();
            command.encode(outputBuffer);
            nextMessageId++
            [outputBuffer.byteBuffers, nextMessageId]
        }

        def isMasterAsync() {
            isMaster() + [new FutureResultCallback<Void>(), new FutureResultCallback<ResponseBuffers>()]
        }
    }
}
