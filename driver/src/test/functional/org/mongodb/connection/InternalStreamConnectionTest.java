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

package org.mongodb.connection;

import category.Async;
import org.bson.io.OutputBuffer;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mongodb.Document;
import org.mongodb.MongoCredential;
import org.mongodb.MongoException;
import org.mongodb.MongoNamespace;
import org.mongodb.ServerCursor;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.event.ConnectionEvent;
import org.mongodb.event.ConnectionListener;
import org.mongodb.event.ConnectionMessageReceivedEvent;
import org.mongodb.event.ConnectionMessagesSentEvent;
import org.mongodb.protocol.KillCursor;
import org.mongodb.protocol.message.CommandMessage;
import org.mongodb.protocol.message.KillCursorsMessage;
import org.mongodb.protocol.message.MessageSettings;
import org.mongodb.protocol.message.RequestMessage;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.mongodb.Fixture.getBufferProvider;
import static org.mongodb.Fixture.getPrimary;
import static org.mongodb.Fixture.getSSLSettings;
import static org.mongodb.MongoNamespace.COMMAND_COLLECTION_NAME;

// This is a Java test so that we can use categories.
@Category(Async.class)
public class InternalStreamConnectionTest {
    private static final String CLUSTER_ID = "1";
    private Stream stream;

    @After
    public void tearDown() {
        stream.close();
    }

    @Test
    public void shouldFireMessagesSentEventAsync() throws InterruptedException {
        // given
        TestConnectionListener listener = new TestConnectionListener();
        stream = new AsynchronousSocketChannelStreamFactory(SocketSettings.builder().build(), getSSLSettings()).create(getPrimary());
        InternalStreamConnection connection = new InternalStreamConnection(CLUSTER_ID, stream, Collections.<MongoCredential>emptyList(),
                                                                           getBufferProvider(), listener);
        OutputBuffer buffer = new PooledByteBufferOutputBuffer(getBufferProvider());
        RequestMessage message = new KillCursorsMessage(new KillCursor(new ServerCursor(1, getPrimary())),
                                                        MessageSettings.builder().build());
        message.encode(buffer);

        // when
        listener.reset();
        final CountDownLatch latch = new CountDownLatch(1);
        connection.sendMessageAsync(buffer.getByteBuffers(), message.getId(), new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final MongoException e) {
                latch.countDown();
            }
        });
        latch.await();

        // then
        assertEquals(1, listener.messagesSentCount());
    }

    @Test
    public void shouldFireMessageReceiveEventAsync() throws InterruptedException {
        // given
        TestConnectionListener listener = new TestConnectionListener();
        stream = new AsynchronousSocketChannelStreamFactory(SocketSettings.builder().build(), getSSLSettings()).create(getPrimary());
        InternalStreamConnection connection = new InternalStreamConnection(CLUSTER_ID, stream, Collections.<MongoCredential>emptyList(),
                                                                           getBufferProvider(), listener);
        OutputBuffer buffer = new PooledByteBufferOutputBuffer(getBufferProvider());
        RequestMessage message = new CommandMessage(new MongoNamespace("admin", COMMAND_COLLECTION_NAME).getFullName(),
                                                    new Document("ismaster", 1), new DocumentCodec(), MessageSettings.builder().build());
        message.encode(buffer);

        // when
        listener.reset();
        connection.sendMessage(buffer.getByteBuffers(), message.getId());
        final CountDownLatch latch = new CountDownLatch(1);
        connection.receiveMessageAsync(new SingleResultCallback<ResponseBuffers>() {
            @Override
            public void onResult(final ResponseBuffers result, final MongoException e) {
                latch.countDown();
            }
        });
        latch.await();

        // then
        assertEquals(1, listener.messageReceivedCount());
    }

    private static final class TestConnectionListener implements ConnectionListener {
        private int connectionOpenedCount;
        private int connectionClosedCount;
        private int messagesSentCount;
        private int messageReceivedCount;

        @Override
        public void connectionOpened(final ConnectionEvent event) {
            connectionOpenedCount++;
        }

        @Override
        public void connectionClosed(final ConnectionEvent event) {
            connectionClosedCount++;
        }

        @Override
        public void messagesSent(final ConnectionMessagesSentEvent event) {
            messagesSentCount++;
        }

        @Override
        public void messageReceived(final ConnectionMessageReceivedEvent event) {
            messageReceivedCount++;
        }

        public void reset() {
            connectionOpenedCount = 0;
            connectionClosedCount = 0;
            messagesSentCount = 0;
            messageReceivedCount = 0;
        }

        public int connectionOpenedCount() {
            return connectionOpenedCount;
        }

        public int connectionClosedCount() {
            return connectionClosedCount;
        }

        public int messagesSentCount() {
            return messagesSentCount;
        }

        public int messageReceivedCount() {
            return messageReceivedCount;
        }
    }
}
