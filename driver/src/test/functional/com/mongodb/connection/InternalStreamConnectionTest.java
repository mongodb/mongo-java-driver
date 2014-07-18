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

package com.mongodb.connection;

import category.Async;
import com.mongodb.MongoException;
import com.mongodb.event.ConnectionEvent;
import com.mongodb.event.ConnectionListener;
import com.mongodb.event.ConnectionMessageReceivedEvent;
import com.mongodb.event.ConnectionMessagesSentEvent;
import com.mongodb.protocol.KillCursor;
import com.mongodb.protocol.message.CommandMessage;
import com.mongodb.protocol.message.KillCursorsMessage;
import com.mongodb.protocol.message.MessageSettings;
import com.mongodb.protocol.message.RequestMessage;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.io.OutputBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mongodb.MongoCredential;
import org.mongodb.MongoNamespace;
import org.mongodb.ServerCursor;
import org.mongodb.operation.QueryFlag;

import java.util.Collections;
import java.util.EnumSet;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.mongodb.Fixture.getAsyncStreamFactory;
import static org.mongodb.Fixture.getPrimary;
import static org.mongodb.MongoNamespace.COMMAND_COLLECTION_NAME;

// This is a Java test so that we can use categories.
@Category(Async.class)
public class InternalStreamConnectionTest {
    private static final String CLUSTER_ID = "1";
    private StreamFactory factory = getAsyncStreamFactory();
    private Stream stream;

    @Before
    public void setUp() throws InterruptedException {
        stream = factory.create(getPrimary());
    }

    @After
    public void tearDown() {
        stream.close();
    }

    @Test
    public void shouldFireMessagesSentEventAsync() throws InterruptedException {
        // given
        TestConnectionListener listener = new TestConnectionListener();
        InternalStreamConnection connection = new InternalStreamConnection(CLUSTER_ID, stream, Collections.<MongoCredential>emptyList(),
                                                                           listener);
        OutputBuffer buffer = new ByteBufferOutputBuffer(connection);
        RequestMessage message = new KillCursorsMessage(new KillCursor(new ServerCursor(1, getPrimary())));
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
        InternalStreamConnection connection = new InternalStreamConnection(CLUSTER_ID, stream, Collections.<MongoCredential>emptyList(),
                                                                           listener);
        OutputBuffer buffer = new ByteBufferOutputBuffer(connection);
        RequestMessage message = new CommandMessage(new MongoNamespace("admin", COMMAND_COLLECTION_NAME).getFullName(),
                                                    new BsonDocument("ismaster", new BsonInt32(1)),
                                                    EnumSet.noneOf(QueryFlag.class),
                                                    MessageSettings.builder().build());
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
        private int messagesSentCount;
        private int messageReceivedCount;

        @Override
        public void connectionOpened(final ConnectionEvent event) {
        }

        @Override
        public void connectionClosed(final ConnectionEvent event) {
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
            messagesSentCount = 0;
            messageReceivedCount = 0;
        }

        public int messagesSentCount() {
            return messagesSentCount;
        }

        public int messageReceivedCount() {
            return messageReceivedCount;
        }
    }
}
