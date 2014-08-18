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
import category.Slow
import com.mongodb.MongoException
import com.mongodb.MongoInternalException
import com.mongodb.MongoNamespace
import com.mongodb.MongoSocketClosedException
import com.mongodb.MongoSocketReadException
import com.mongodb.MongoSocketWriteException
import com.mongodb.codecs.DocumentCodec
import com.mongodb.event.ConnectionListener
import com.mongodb.operation.QueryFlag
import com.mongodb.operation.SingleResultFuture
import com.mongodb.protocol.message.CommandMessage
import com.mongodb.protocol.message.MessageSettings
import org.bson.BsonBinaryWriter
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.ByteBuf
import org.bson.ByteBufNIO
import org.bson.codecs.EncoderContext
import org.bson.io.BasicOutputBuffer
import org.bson.io.OutputBuffer
import org.junit.experimental.categories.Category
import org.mongodb.Document
import spock.lang.Specification
import spock.util.concurrent.AsyncConditions

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.security.SecureRandom
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

import static MongoNamespace.COMMAND_COLLECTION_NAME

@SuppressWarnings(['UnusedVariable'])
class InternalStreamConnectionSpecification extends Specification {
    private static final String CLUSTER_ID = '1'
    def helper = new StreamHelper()
    def stream = Stub(Stream)
    def listener = Mock(ConnectionListener)
    def connectionInitializer = Mock(ConnectionInitializer)

    def 'should fire connection opened event'() {
        given:
        def connection = new InternalStreamConnection(CLUSTER_ID, stream, connectionInitializer, listener)
        stream.read(_) >>> helper.read(1)
        def (buffers1, messageId1) = helper.isMaster()

        when:
        connection.sendMessage(buffers1, messageId1)

        then:
        1 * listener.connectionOpened(_)
    }

    def 'should fire connection closed event'() {
        given:
        def connection = new InternalStreamConnection(CLUSTER_ID, stream, connectionInitializer, listener)

        when:
        connection.close()

        then:
        1 * listener.connectionClosed(_)
    }

    def 'should fire messages sent event'() {
        given:
        def connection = new InternalStreamConnection(CLUSTER_ID, stream, connectionInitializer, listener)
        stream.read(_) >>> helper.read(1)
        def (buffers1, messageId1) = helper.isMaster()

        when:
        connection.sendMessage(buffers1, messageId1)

        then:
        1 * listener.messagesSent(_)
    }

    def 'should fire message sent event asynchronously'() {
        stream.writeAsync(_, _) >> { List<ByteBuf> buffers, AsyncCompletionHandler<Void> callback ->
            callback.completed(null)
        }

        def connection = new InternalStreamConnection(CLUSTER_ID, stream, connectionInitializer, listener)
        connectionInitializer.initialize(_) >> { SingleResultCallback<Void> callback ->
            callback.onResult(null, null);
        }
        def (buffers1, messageId1) = helper.isMaster()
        def latch = new CountDownLatch(1);

        when:
        connection.sendMessageAsync(buffers1, messageId1, new SingleResultCallback<Void>() {
            @Override
            void onResult(final Void result, final MongoException e) {
                latch.countDown();
            }
        })
        latch.await()

        then:
        1 * listener.messagesSent(_)
    }

    def 'should fire message received event'() {
        given:
        stream.read(_) >>> helper.read(1)
        def connection = new InternalStreamConnection(CLUSTER_ID, stream, connectionInitializer, listener)
        def (buffers1, messageId1) = helper.isMaster()

        when:
        connection.receiveMessage(messageId1)

        then:
        1 * listener.messageReceived(_)
    }

    @Category(Async)
    def 'should fire message received event asynchronously'() {
        given:
        stream.readAsync(36, _) >> { int numBytes, AsyncCompletionHandler<ByteBuf> handler ->
            handler.completed(helper.header())
        }
        stream.readAsync(74, _) >> { int numBytes, AsyncCompletionHandler<ByteBuf> handler ->
            handler.completed(helper.body())
        }
        def connection = new InternalStreamConnection(CLUSTER_ID, stream, connectionInitializer, listener)

        connectionInitializer.initialize(_) >> { SingleResultCallback<Void> callback ->
            callback.onResult(null, null);
        }
        def (buffers1, messageId1) = helper.isMaster()
        def latch = new CountDownLatch(1);

        when:
        connection.receiveMessageAsync(messageId1, new SingleResultCallback<ResponseBuffers>() {
            @Override
            void onResult(final ResponseBuffers result, final MongoException e) {
                latch.countDown();
            }
        })
        latch.await()

        then:
        1 * listener.messageReceived(_)
    }

    def 'should handle out of order messages on the stream'() {
        // Connect then: Send(1), Send(2), Send(3), Receive(3), Receive(2), Receive(1)
        given:
        stream.read(_) >>> helper.read(3, ordered)

        def connection = new InternalStreamConnection(CLUSTER_ID, stream, connectionInitializer, listener)
        def (buffers1, messageId1) = helper.isMaster()
        def (buffers2, messageId2) = helper.isMaster()
        def (buffers3, messageId3) = helper.isMaster()

        when:
        connection.sendMessage(buffers1, messageId1)
        connection.sendMessage(buffers2, messageId2)
        connection.sendMessage(buffers3, messageId3)

        then:
        connection.receiveMessage(messageId3).replyHeader.responseTo == messageId3
        connection.receiveMessage(messageId2).replyHeader.responseTo == messageId2
        connection.receiveMessage(messageId1).replyHeader.responseTo == messageId1

        where:
        ordered << [true, false]
    }

    def 'should handle out of order messages on the stream asynchronously'() {
        // Connect then: SendAsync(1), SendAsync(2), SendAsync(3), ReceiveAsync(3), ReceiveAsync(2), ReceiveAsync(1)
        given:
        stream.writeAsync(_, _) >> { List<ByteBuf> buffers, AsyncCompletionHandler<Void> callback ->
            callback.completed(null)
        }

        def headers = helper.generateHeaders(3, ordered)
        stream.readAsync(36, _) >> { int numBytes, AsyncCompletionHandler<ByteBuf> handler ->
            handler.completed(headers.pop())
        }
        stream.readAsync(74, _) >> { int numBytes, AsyncCompletionHandler<ByteBuf> handler ->
            handler.completed(helper.body())
        }

        def connection = new InternalStreamConnection(CLUSTER_ID, stream, connectionInitializer, listener)
        connectionInitializer.initialize(_) >> { SingleResultCallback<Void> callback ->
            callback.onResult(null, null);
        }
        def sndLatch = new CountDownLatch(3)
        def rcvdLatch = new CountDownLatch(3)
        def (buffers1, messageId1, sndCallbck1, rcvdCallbck1, fSndResult1, fRespBuffers1) = helper.isMasterAsync(sndLatch, rcvdLatch)
        def (buffers2, messageId2, sndCallbck2, rcvdCallbck2, fSndResult2, fRespBuffers2) = helper.isMasterAsync(sndLatch, rcvdLatch)
        def (buffers3, messageId3, sndCallbck3, rcvdCallbck3, fSndResult3, fRespBuffers3) = helper.isMasterAsync(sndLatch, rcvdLatch)

        when:
        connection.sendMessageAsync(buffers1, messageId1, sndCallbck1)
        connection.sendMessageAsync(buffers2, messageId2, sndCallbck2)
        connection.sendMessageAsync(buffers3, messageId3, sndCallbck3)

        then:
        sndLatch.await()

        when:
        connection.receiveMessageAsync(messageId3, rcvdCallbck3)
        connection.receiveMessageAsync(messageId2, rcvdCallbck2)
        connection.receiveMessageAsync(messageId1, rcvdCallbck1)
        rcvdLatch.await()

        then:
        fRespBuffers1.get().replyHeader.responseTo == messageId1
        fRespBuffers2.get().replyHeader.responseTo == messageId2
        fRespBuffers3.get().replyHeader.responseTo == messageId3

        where:
        ordered << [true, false]
    }

    def 'should handle out of order messages on the stream mixed synchronicity'() {
        // Connect then: Send(1), SendAsync(2), Send(3), ReceiveAsync(3), Receive(2), ReceiveAsync(1)
        given:
        stream.read(36) >> { helper.header() }
        stream.read(74) >> { helper.body() }
        stream.writeAsync(_, _) >> { List<ByteBuf> buffers, AsyncCompletionHandler<Void> callback ->
            callback.completed(null)
        }
        stream.readAsync(36, _) >> { int numBytes, AsyncCompletionHandler<ByteBuf> handler ->
            handler.completed(helper.header())
        }
        stream.readAsync(74, _) >> { int numBytes, AsyncCompletionHandler<ByteBuf> handler ->
            handler.completed(helper.body())
        }

        def connection = new InternalStreamConnection(CLUSTER_ID, stream, connectionInitializer, listener)
        def sndLatch = new CountDownLatch(1)
        def rcvdLatch = new CountDownLatch(2)
        def (buffers1, messageId1, sndCallbck1, rcvdCallbck1, fSndResult1, fRespBuffers1) = helper.isMasterAsync(sndLatch, rcvdLatch)
        def (buffers2, messageId2, sndCallbck2, rcvdCallbck2, fSndResult2, fRespBuffers2) = helper.isMasterAsync(sndLatch, rcvdLatch)
        def (buffers3, messageId3, sndCallbck3, rcvdCallbck3, fSndResult3, fRespBuffers3) = helper.isMasterAsync(sndLatch, rcvdLatch)

        when:
        connection.sendMessage(buffers1, messageId1)
        connection.sendMessageAsync(buffers2, messageId2, sndCallbck2)
        connection.sendMessage(buffers3, messageId3)

        then:
        sndLatch.await()

        when:
        connection.receiveMessageAsync(messageId3, rcvdCallbck3)
        ResponseBuffers responseBuffers2 = connection.receiveMessage(messageId2)
        connection.receiveMessageAsync(messageId1, rcvdCallbck1)
        rcvdLatch.await()

        then:
        fRespBuffers1.get().replyHeader.responseTo == messageId1
        responseBuffers2.replyHeader.responseTo == messageId2
        fRespBuffers3.get().replyHeader.responseTo == messageId3
    }

    def 'failed initialization should close the connection and fail'() {
        given:
        connectionInitializer.initialize() >> { throw new MongoInternalException('Something went wrong') }

        def connection = new InternalStreamConnection(CLUSTER_ID, stream, connectionInitializer, listener)
        def (buffers1, messageId1) = helper.isMaster()
        def (buffers2, messageId2) = helper.isMaster()

        when:
        connection.sendMessage(buffers1, messageId1)

        then:
        connection.isClosed()
        thrown MongoInternalException

        when:
        connection.sendMessage(buffers2, messageId2)

        then:
        thrown MongoSocketClosedException
    }

    def 'failed initialization should close the connection and fail asynchronously'() {
        given:
        stream.writeAsync(_, _) >>  { List<ByteBuf> buffers, AsyncCompletionHandler<Void> callback ->
            callback.completed(null)
        }
        stream.readAsync(36, _) >> { int numBytes, AsyncCompletionHandler<ByteBuf> handler ->
            handler.completed(helper.header())
        }
        stream.readAsync(74, _) >> { int numBytes, AsyncCompletionHandler<ByteBuf> handler ->
            handler.completed(helper.body())
        }

        def connection = new InternalStreamConnection(CLUSTER_ID, stream, connectionInitializer, listener)
        connectionInitializer.initialize(_) >> { SingleResultCallback<Void> callback ->
            callback.onResult(null, new MongoInternalException('Something went wrong'));
        }
        def sndLatch = new CountDownLatch(2)
        def rcvdLatch = new CountDownLatch(2)
        def (buffers1, messageId1, sndCallbck1, rcvdCallbck1, fSndResult1, fRespBuffers1) = helper.isMasterAsync(sndLatch, rcvdLatch)
        def (buffers2, messageId2, sndCallbck2, rcvdCallbck2, fSndResult2, fRespBuffers2) = helper.isMasterAsync(sndLatch, rcvdLatch)

        when:
        connection.sendMessageAsync(buffers1, messageId1, sndCallbck1)
        connection.sendMessageAsync(buffers2, messageId2, sndCallbck2)
        sndLatch.await()

        then:
        connection.isClosed()

        when:
        fSndResult1.get()

        then:
        thrown MongoInternalException

        when:
        fSndResult2.get()

        then:
        thrown MongoSocketClosedException
    }

    def 'failed writes should close the connection and fail'() {
        given:
        stream.write(_) >> { throw new IOException('Something went wrong') }

        def connection = new InternalStreamConnection(CLUSTER_ID, stream, connectionInitializer, listener)
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

    def 'failed writes should close the connection and fail asynchronously'() {
        given:
        int seen = 0
        stream.writeAsync(_, _) >>  { List<ByteBuf> buffers, AsyncCompletionHandler<Void> callback ->
            if (seen == 0) {
                seen += 1
                return callback.failed(new IOException('Something went wrong'))
            }
            callback.completed(null)
        }
        stream.readAsync(36, _) >> { int numBytes, AsyncCompletionHandler<ByteBuf> handler ->
            handler.completed(helper.header())
        }
        stream.readAsync(74, _) >> { int numBytes, AsyncCompletionHandler<ByteBuf> handler ->
            handler.completed(helper.body())
        }

        def connection = new InternalStreamConnection(CLUSTER_ID, stream, connectionInitializer, listener)
        connectionInitializer.initialize(_) >> { SingleResultCallback<Void> callback ->
            callback.onResult(null, null);
        }
        def sndLatch = new CountDownLatch(2)
        def rcvdLatch = new CountDownLatch(2)
        def (buffers1, messageId1, sndCallbck1, rcvdCallbck1, fSndResult1, fRespBuffers1) = helper.isMasterAsync(sndLatch, rcvdLatch)
        def (buffers2, messageId2, sndCallbck2, rcvdCallbck2, fSndResult2, fRespBuffers2) = helper.isMasterAsync(sndLatch, rcvdLatch)

        when:
        connection.sendMessageAsync(buffers1, messageId1, sndCallbck1)
        connection.sendMessageAsync(buffers2, messageId2, sndCallbck2)
        sndLatch.await()

        then:
        connection.isClosed()

        when:
        fSndResult1.get()

        then:
        thrown MongoSocketWriteException

        when:
        fSndResult2.get()

        then:
        thrown MongoSocketClosedException
    }

    def 'failed reads (header) should close the stream and fail'() {
        given:
        stream.read(36) >> { throw new IOException('Something went wrong') }
        stream.read(74) >> helper.body()

        def connection = new InternalStreamConnection(CLUSTER_ID, stream, connectionInitializer, listener)
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

    def 'failed reads (header) should close the stream and fail asynchronously'() {
        given:
        int seen = 0
        stream.writeAsync(_, _) >>  { List<ByteBuf> buffers, AsyncCompletionHandler<Void> callback ->
            callback.completed(null)
        }

        stream.readAsync(36, _) >> { int numBytes, AsyncCompletionHandler<ByteBuf> handler ->
            if (seen == 0) {
                seen += 1
                return handler.failed(new IOException('Something went wrong'))
            }
            handler.completed(helper.header())
        }
        stream.readAsync(74, _) >> { int numBytes, AsyncCompletionHandler<ByteBuf> handler ->
            handler.completed(helper.body())
        }

        def connection = new InternalStreamConnection(CLUSTER_ID, stream, connectionInitializer, listener)
        connectionInitializer.initialize(_) >> { SingleResultCallback<Void> callback ->
            callback.onResult(null, null);
        }

        def sndLatch = new CountDownLatch(2)
        def rcvdLatch = new CountDownLatch(2)
        def (buffers1, messageId1, sndCallbck1, rcvdCallbck1, fSndResult1, fRespBuffers1) = helper.isMasterAsync(sndLatch, rcvdLatch)
        def (buffers2, messageId2, sndCallbck2, rcvdCallbck2, fSndResult2, fRespBuffers2) = helper.isMasterAsync(sndLatch, rcvdLatch)

        when:
        connection.sendMessageAsync(buffers1, messageId1, sndCallbck1)
        connection.sendMessageAsync(buffers2, messageId2, sndCallbck2)

        then:
        sndLatch.await()

        when:
        connection.receiveMessageAsync(messageId1, rcvdCallbck1)
        connection.receiveMessageAsync(messageId2, rcvdCallbck2)
        rcvdLatch.await()

        then:
        connection.isClosed()

        when:
        fRespBuffers1.get()

        then:
        thrown MongoSocketReadException

        when:
        fRespBuffers2.get()

        then:
        thrown MongoSocketClosedException
    }

    def 'failed reads (body) should close the stream and fail'() {
        given:
        stream.read(36) >> { helper.header() }
        stream.read(74) >> { throw new IOException('Something went wrong') }

        def connection = new InternalStreamConnection(CLUSTER_ID, stream, connectionInitializer, listener)
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

    def 'failed reads (body) should close the stream and fail asynchronously'() {
        given:
        int seen = 0
        stream.writeAsync(_, _) >>  { List<ByteBuf> buffers, AsyncCompletionHandler<Void> callback ->
            callback.completed(null)
        }

        stream.readAsync(36, _) >> { int numBytes, AsyncCompletionHandler<ByteBuf> handler ->
            handler.completed(helper.header())
        }
        stream.readAsync(74, _) >> { int numBytes, AsyncCompletionHandler<ByteBuf> handler ->
            if (seen == 0) {
                seen += 1
                return handler.failed(new IOException('Something went wrong'))
            }
            handler.completed(helper.body())
        }
        def connection = new InternalStreamConnection(CLUSTER_ID, stream, connectionInitializer, listener)
        connectionInitializer.initialize(_) >> { SingleResultCallback<Void> callback ->
            callback.onResult(null, null);
        }
        def sndLatch = new CountDownLatch(2)
        def rcvdLatch = new CountDownLatch(2)
        def (buffers1, messageId1, sndCallbck1, rcvdCallbck1, fSndResult1, fRespBuffers1) = helper.isMasterAsync(sndLatch, rcvdLatch)
        def (buffers2, messageId2, sndCallbck2, rcvdCallbck2, fSndResult2, fRespBuffers2) = helper.isMasterAsync(sndLatch, rcvdLatch)

        when:
        connection.sendMessageAsync(buffers1, messageId1, sndCallbck1)
        connection.sendMessageAsync(buffers2, messageId2, sndCallbck2)

        then:
        sndLatch.await()

        when:
        connection.receiveMessageAsync(messageId1, rcvdCallbck1)
        connection.receiveMessageAsync(messageId2, rcvdCallbck2)
        rcvdLatch.await()

        then:
        connection.isClosed()

        when:
        fRespBuffers1.get()

        then:
        thrown MongoSocketReadException

        when:
        fRespBuffers2.get()

        then:
        thrown MongoSocketClosedException
    }


    @Category(Slow)
    def 'the connection pipelining should be thread safe'() {
        given:
        int threads = 10
        ExecutorService pool = Executors.newFixedThreadPool(threads)
        stream.read(36) >> { helper.header() }
        stream.read(74) >> { helper.body() }

        when:
        def connection = new InternalStreamConnection(CLUSTER_ID, stream, connectionInitializer, listener)

        then:
        (1..100000).each { n ->
            def conds = new AsyncConditions()
            def (buffers, messageId) = helper.isMaster()

            pool.submit( { connection.sendMessage(buffers, messageId) } as Runnable )
            pool.submit( { conds.evaluate {
                                assert connection.receiveMessage(messageId).replyHeader.responseTo == messageId
                         } } as Runnable)

            conds.await(10)
        }

        cleanup:
        pool.shutdown()
    }

    @Category(Slow)
    def 'the connection pipelining should be thread safe asynchronously'() {
        given:
        int threads = 10
        ExecutorService pool = Executors.newFixedThreadPool(threads)
        stream.writeAsync(_, _) >> { List<ByteBuf> buffers, AsyncCompletionHandler<Void> callback ->
            callback.completed(null)
        }
        stream.readAsync(36, _) >> { int numBytes, AsyncCompletionHandler<ByteBuf> handler ->
            handler.completed(helper.header())
        }
        stream.readAsync(74, _) >> { int numBytes, AsyncCompletionHandler<ByteBuf> handler ->
            handler.completed(helper.body())
        }

        when:
        def connection = new InternalStreamConnection(CLUSTER_ID, stream, connectionInitializer, listener)
        connectionInitializer.initialize(_) >> { SingleResultCallback<Void> callback ->
            callback.onResult(null, null);
        }

        then:
        (1..100000).each { n ->
            def (buffers, messageId, sndCallbck, rcvdCallbck, fSndResult, fRespBuffers) = helper.isMasterAsync(new CountDownLatch(1),
                                                                                                               new CountDownLatch(1))

            pool.submit( { connection.sendMessageAsync(buffers, messageId, sndCallbck) } as Runnable )
            pool.submit( { connection.receiveMessageAsync(messageId, rcvdCallbck) } as Runnable )

            assert fRespBuffers.get().replyHeader.responseTo == messageId
        }

        cleanup:
        pool.shutdown()
    }

    class StreamHelper {
        int nextMessageId = isMaster().get(1) + 1 // Generates a message then adds one to the id

        def read(int count) {
            read(count, false)
        }

        def read(int count, boolean ordered) {
            List<ByteBuf> headers = (1..count).collect { header() }
            List<ByteBuf> bodies = (1..count).collect { body() }
            if (!ordered) {
                Collections.shuffle(headers, new SecureRandom())
            }
            [headers, bodies].transpose().flatten()
        }

        def header() {
            ByteBuffer headerByteBuffer = ByteBuffer.allocate(36).with {
                order(ByteOrder.LITTLE_ENDIAN);
                putInt(110);           // messageLength
                putInt(4);             // requestId
                putInt(nextMessageId); // responseTo
                putInt(1);             // opCode
                putInt(0);             // responseFlags
                putLong(0);            // cursorId
                putInt(0);             // starting from
                putInt(1);             // number returned
            }
            headerByteBuffer.flip()
            nextMessageId++
            new ByteBufNIO(headerByteBuffer)
        }

        def body() {
            def okResponse = ['connectionId': 1, 'n': 0, 'syncMillis': 0, 'writtenTo': null, 'err': null, 'ok': 1] as Document
            def binaryResponse = new BsonBinaryWriter(new BasicOutputBuffer(), false)
            new DocumentCodec().encode(binaryResponse, okResponse, EncoderContext.builder().build())
            binaryResponse.buffer.byteBuffers.get(0)
        }

        def generateHeaders(int amount, boolean ordered) {
            List<ByteBuf> headers = (1..amount).collect { header() }.reverse()
            if (!ordered) {
                Collections.shuffle(headers, new SecureRandom())
            }
            headers
        }

        def isMaster() {
            def command = new CommandMessage(new MongoNamespace('admin', COMMAND_COLLECTION_NAME).getFullName(),
                                             new BsonDocument('ismaster', new BsonInt32(1)),
                                             EnumSet.noneOf(QueryFlag),
                                             MessageSettings.builder().build());
            OutputBuffer buffer = new BasicOutputBuffer();
            command.encode(buffer);
            [buffer.byteBuffers, command.getId()]
        }

        def isMasterAsync(CountDownLatch sndLatch, CountDownLatch rcvdLatch) {
            SingleResultFuture<Void> futureSendResult = new SingleResultFuture<Void>()
            SingleResultFuture<ResponseBuffers> futureResponseBuffers = new SingleResultFuture<ResponseBuffers>()
            isMaster() + [new SingleResultCallback<Void>() {
                @Override
                void onResult(final Void result, final MongoException e) {
                    sndLatch.countDown()
                    futureSendResult.init(result, e)
                }
            }, new SingleResultCallback<ResponseBuffers>() {
                @Override
                void onResult(final ResponseBuffers result, final MongoException e) {
                    rcvdLatch.countDown()
                    futureResponseBuffers.init(result, e)
                }
            }, futureSendResult, futureResponseBuffers]

        }
    }
}
