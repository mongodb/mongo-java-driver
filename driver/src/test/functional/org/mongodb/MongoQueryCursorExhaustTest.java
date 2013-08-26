/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb;

import org.bson.ByteBuf;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.connection.Channel;
import org.mongodb.connection.ChannelReceiveArgs;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.Server;
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.ServerDescription;
import org.mongodb.operation.Find;
import org.mongodb.operation.QueryFlag;
import org.mongodb.operation.ReadPreferenceServerSelector;
import org.mongodb.operation.ServerChannelProvider;
import org.mongodb.session.ServerChannelProviderOptions;
import org.mongodb.session.Session;

import java.util.EnumSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mongodb.Fixture.getBufferProvider;
import static org.mongodb.Fixture.getCluster;
import static org.mongodb.Fixture.getSession;
import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;

public class MongoQueryCursorExhaustTest extends DatabaseTestCase {

    private final byte[] bytes = new byte[10000];

    @Before
    public void setUp() throws Exception {
        super.setUp();

        for (int i = 0; i < 1000; i++) {
            collection.insert(new Document("_id", i).append("bytes", bytes));
        }
    }

    @Test
    public void testExhaustReadAllDocuments() {
        MongoQueryCursor<Document> cursor = new MongoQueryCursor<Document>(collection.getNamespace(),
                new Find().addFlags(EnumSet.of(QueryFlag.Exhaust)),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getBufferProvider(), getSession(), false);

        int count = 0;
        while (cursor.hasNext()) {
            cursor.next();
            count++;
        }
        assertEquals(1000, count);
    }

    @Test
    public void testExhaustCloseBeforeReadingAllDocuments() {
        Server server = getCluster().getServer(new ReadPreferenceServerSelector(ReadPreference.primary()));
        Channel channel = server.getChannel();
        try {
            SingleChannelSession singleChannelSession = new SingleChannelSession(server.getDescription(), channel);

            MongoQueryCursor<Document> cursor = new MongoQueryCursor<Document>(collection.getNamespace(),
                    new Find().addFlags(EnumSet.of(QueryFlag.Exhaust)),
                    collection.getOptions().getDocumentCodec(), collection.getCodec(), getBufferProvider(), singleChannelSession, false);

            cursor.next();
            cursor.close();

            cursor = new MongoQueryCursor<Document>(collection.getNamespace(),
                    new Find().limit(1).select(new Document("_id", 1)).order(new Document("_id", -1)),
                    collection.getOptions().getDocumentCodec(), collection.getCodec(), getBufferProvider(), singleChannelSession, false);
            assertEquals(new Document("_id", 999), cursor.next());

            singleChannelSession.channel.close();
        } finally {
            channel.close();
        }
    }

    private static class SingleChannelSession implements Session {
        private ServerDescription description;
        private Channel channel;
        private boolean isClosed;

        public SingleChannelSession(final ServerDescription description, final Channel channel) {
            this.description = description;
            this.channel = channel;
        }

        @Override
        public void close() {
            isClosed = true;
        }

        @Override
        public boolean isClosed() {
            return isClosed;
        }

        @Override
        public ServerChannelProvider createServerChannelProvider(final ServerChannelProviderOptions options) {
            return new ServerChannelProvider() {
                @Override
                public ServerDescription getServerDescription() {
                    return description;
                }

                @Override
                public Channel getChannel() {
                    return new DelayedCloseChannel(channel);
                }
            };
        }
    }

    private static class DelayedCloseChannel implements Channel {
        private Channel wrapped;
        private boolean isClosed;


        public DelayedCloseChannel(final Channel wrapped) {
            this.wrapped = notNull("wrapped", wrapped);
        }

        @Override
        public ServerAddress getServerAddress() {
            isTrue("open", !isClosed());
            return wrapped.getServerAddress();
        }

        @Override
        public void sendMessage(final List<ByteBuf> byteBuffers) {
            isTrue("open", !isClosed());
            wrapped.sendMessage(byteBuffers);
        }

        @Override
        public ResponseBuffers receiveMessage(final ChannelReceiveArgs channelReceiveArgs) {
            isTrue("open", !isClosed());
            return wrapped.receiveMessage(channelReceiveArgs);
        }

        @Override
        public String getId() {
            return wrapped.getId();
        }

        @Override
        public void close() {
            isClosed = true;
        }

        @Override
        public boolean isClosed() {
            return isClosed;
        }
    }
}
