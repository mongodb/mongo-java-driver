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

import org.junit.Before;
import org.junit.Test;
import org.mongodb.connection.BaseConnection;
import org.mongodb.connection.ChannelAwareOutputBuffer;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ServerAddress;
import org.mongodb.session.ServerSelectingSession;
import org.mongodb.connection.ServerSelector;
import org.mongodb.session.Session;
import org.mongodb.session.SessionBindingType;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.QueryOption;

import java.util.EnumSet;

import static org.junit.Assert.assertEquals;
import static org.mongodb.Fixture.getBufferPool;
import static org.mongodb.Fixture.getSession;
import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;

public class MongoQueryCursorExhaustTest extends DatabaseTestCase {

    @Before
    public void setUp() throws Exception {
        super.setUp();

        for (int i = 0; i < 1000; i++) {
            collection.insert(new Document("_id", i));
        }
    }

    @Test
    public void testExhaustReadAllDocuments() {
        MongoQueryCursor<Document> cursor = new MongoQueryCursor<Document>(collection.getNamespace(),
                new MongoFind().addOptions(EnumSet.of(QueryOption.Exhaust)),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), getSession(), getBufferPool());

        int count = 0;
        while (cursor.hasNext()) {
            cursor.next();
            count++;
        }
        assertEquals(1000, count);
    }

    @Test
    public void testExhaustCloseBeforeReadingAllDocuments() {
        SingleConnectionServerSelectingSession singleConnectionSession = new SingleConnectionServerSelectingSession(getSession()
                .getConnection());

        MongoQueryCursor<Document> cursor = new MongoQueryCursor<Document>(collection.getNamespace(),
                new MongoFind().addOptions(EnumSet.of(QueryOption.Exhaust)),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), singleConnectionSession, getBufferPool());

        cursor.next();
        cursor.close();

        cursor = new MongoQueryCursor<Document>(collection.getNamespace(),
                new MongoFind().limit(1).order(new Document("_id", -1)),
                collection.getOptions().getDocumentCodec(), collection.getCodec(), singleConnectionSession, getBufferPool());
        assertEquals(new Document("_id", 999), cursor.next());

        singleConnectionSession.connection.close();
    }

    private static class SingleConnectionServerSelectingSession implements ServerSelectingSession {
        private Connection connection;
        private boolean isClosed;

        public SingleConnectionServerSelectingSession(final Connection connection) {
            this.connection = connection;
        }

        @Override
        public Connection getConnection(final ServerSelector serverSelector) {
            return getConnection();
        }

        @Override
        public Session getBoundSession(final ServerSelector serverSelector, final SessionBindingType sessionBindingType) {
            return new SingleConnectionServerSelectingSession(new DelayedCloseConnection(connection));
        }

        @Override
        public Connection getConnection() {
            return new DelayedCloseConnection(connection);
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

    private static class DelayedCloseConnection implements Connection {
        private Connection wrapped;
        private boolean isClosed;


        public DelayedCloseConnection(final Connection wrapped) {
            this.wrapped = notNull("wrapped", wrapped);
        }

        @Override
        public ServerAddress getServerAddress() {
            isTrue("open", !isClosed());
            return getWrapped().getServerAddress();
        }

        @Override
        public void sendMessage(final ChannelAwareOutputBuffer buffer) {
            isTrue("open", !isClosed());
            wrapped.sendMessage(buffer);
        }

        @Override
        public ResponseBuffers receiveMessage() {
            isTrue("open", !isClosed());
            return wrapped.receiveMessage();
        }

        protected BaseConnection getWrapped() {
            return wrapped;
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
