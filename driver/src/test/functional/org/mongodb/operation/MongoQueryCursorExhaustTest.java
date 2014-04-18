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

package org.mongodb.operation;

import org.bson.ByteBuf;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.DatabaseTestCase;
import org.mongodb.Document;
import org.mongodb.ReadPreference;
import org.mongodb.binding.ConnectionSource;
import org.mongodb.binding.ReadBinding;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.ServerDescription;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.protocol.QueryProtocol;
import org.mongodb.protocol.QueryResult;

import java.util.EnumSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;
import static org.mongodb.Fixture.getBinding;
import static org.mongodb.Fixture.isSharded;
import static org.mongodb.ReadPreference.primary;
import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;

public class MongoQueryCursorExhaustTest extends DatabaseTestCase {

    private final byte[] bytes = new byte[10000];
    private EnumSet<QueryFlag> exhaustFlag = EnumSet.of(QueryFlag.Exhaust);
    private QueryResult<Document> firstBatch;
    private Connection exhaustConnection;

    @Before
    public void setUp() {
        super.setUp();

        for (int i = 0; i < 1000; i++) {
            collection.insert(new Document("_id", i).append("bytes", bytes));
        }

        exhaustConnection = getBinding().getReadConnectionSource().getConnection();
        firstBatch = new QueryProtocol<Document>(collection.getNamespace(), exhaustFlag, 0, 0, new Document(), null,
                                                 new DocumentCodec(), new DocumentCodec())
                     .execute(exhaustConnection);

    }


    @Test
    public void testExhaustReadAllDocuments() {
        assumeFalse(isSharded());

        MongoQueryCursor<Document> cursor = new MongoQueryCursor<Document>(collection.getNamespace(), firstBatch, 0, 0,
                                                                           new DocumentCodec(), exhaustConnection);

        int count = 0;
        while (cursor.hasNext()) {
            cursor.next();
            count++;
        }
        cursor.close();
        assertEquals(1000, count);
    }

    @Test
    public void testExhaustCloseBeforeReadingAllDocuments() {
        assumeFalse(isSharded());
        try {
            SingleConnectionBinding singleConnectionBinding = new SingleConnectionBinding(exhaustConnection);
            ConnectionSource source = singleConnectionBinding.getReadConnectionSource();
            MongoQueryCursor<Document> cursor = new MongoQueryCursor<Document>(collection.getNamespace(), firstBatch, 0, 0,
                                                                               new DocumentCodec(),
                                                                               source.getConnection());

            cursor.next();
            cursor.close();

            new QueryProtocol<Document>(collection.getNamespace(), EnumSet.noneOf(QueryFlag.class), 0, 0, new Document(), null,
                                        new DocumentCodec(), new DocumentCodec())
            .execute(source.getConnection());

            singleConnectionBinding.connection.close();
        } finally {
            exhaustConnection.close();
        }
    }

    private static class SingleConnectionBinding implements ReadBinding {
        private final Connection connection;

        public SingleConnectionBinding(final Connection connection) {
            this.connection = connection;
        }

        @Override
        public int getCount() {
            return 1;
        }

        @Override
        public ReadPreference getReadPreference() {
            return primary();
        }

        @Override
        public ConnectionSource getReadConnectionSource() {
            return new ConnectionSource() {
                @Override
                public Connection getConnection() {
                    return new DelayedCloseConnection(connection);
                }

                @Override
                public ConnectionSource retain() {
                    return this;
                }

                @Override
                public int getCount() {
                    return 1;
                }

                @Override
                public void release() {
                }
            };
        }

        @Override
        public ReadBinding retain() {
            return this;
        }

        @Override
        public void release() {
        }
    }

    private static class DelayedCloseConnection implements Connection {
        private final Connection wrapped;
        private boolean isClosed;


        public DelayedCloseConnection(final Connection wrapped) {
            this.wrapped = notNull("wrapped", wrapped);
        }

        @Override
        public ServerAddress getServerAddress() {
            isTrue("open", !isClosed());
            return wrapped.getServerAddress();
        }

        @Override
        public ByteBuf getBuffer(final int capacity) {
            isTrue("open", !isClosed());
            return wrapped.getBuffer(capacity);
        }

        @Override
        public ServerDescription getServerDescription() {
            isTrue("open", !isClosed());
            return wrapped.getServerDescription();
        }

        @Override
        public void sendMessage(final List<ByteBuf> byteBuffers, final int lastRequestId) {
            isTrue("open", !isClosed());
            wrapped.sendMessage(byteBuffers, lastRequestId);
        }

        @Override
        public ResponseBuffers receiveMessage(final int responseTo) {
            isTrue("open", !isClosed());
            return wrapped.receiveMessage(responseTo);
        }

        @Override
        public void sendMessageAsync(final List<ByteBuf> byteBuffers, final int lastRequestId, final SingleResultCallback<Void> callback) {
            isTrue("open", !isClosed());
            wrapped.sendMessageAsync(byteBuffers, lastRequestId, callback);
        }

        @Override
        public void receiveMessageAsync(final int responseTo,
                                        final SingleResultCallback<ResponseBuffers> callback) {
            isTrue("open", !isClosed());
            wrapped.receiveMessageAsync(responseTo, callback);
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
