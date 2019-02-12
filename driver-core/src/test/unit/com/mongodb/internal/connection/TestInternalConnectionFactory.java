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

package com.mongodb.internal.connection;

import com.mongodb.async.SingleResultCallback;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.ServerType;
import com.mongodb.session.SessionContext;
import org.bson.ByteBuf;
import org.bson.codecs.Decoder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.connection.ServerDescription.getDefaultMaxDocumentSize;

class TestInternalConnectionFactory implements InternalConnectionFactory {
    private final AtomicInteger incrementingId = new AtomicInteger();
    private final List<TestInternalConnection> createdConnections = new ArrayList<TestInternalConnection>();

    @Override
    public InternalConnection create(final ServerId serverId) {
        TestInternalConnection connection = new TestInternalConnection(serverId);
        createdConnections.add(connection);
        return connection;
    }

    List<TestInternalConnection> getCreatedConnections() {
        return createdConnections;
    }

    int getNumCreatedConnections() {
        return createdConnections.size();
    }

    class TestInternalConnection implements InternalConnection {
        private final ConnectionId connectionId;
        private boolean closed;
        private boolean opened;

        TestInternalConnection(final ServerId serverId) {
            this.connectionId = new ConnectionId(serverId, incrementingId.incrementAndGet(), null);
        }

        public void open() {
            opened = true;
        }

        @Override
        public void openAsync(final SingleResultCallback<Void> callback) {
            opened = true;
            callback.onResult(null, null);
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public boolean opened() {
            return opened;
        }

        @Override
        public boolean isClosed() {
            return closed;
        }

        @Override
        public ByteBuf getBuffer(final int size) {
            return null;
        }

        @Override
        public void sendMessage(final List<ByteBuf> byteBuffers, final int lastRequestId) {
        }

        @Override
        public <T> T sendAndReceive(final CommandMessage message, final Decoder<T> decoder, final SessionContext sessionContext) {
            return null;
        }

        @Override
        public <T> void sendAndReceiveAsync(final CommandMessage message, final Decoder<T> decoder,
                                            final SessionContext sessionContext, final SingleResultCallback<T> callback) {
            callback.onResult(null, null);
        }

        @Override
        public ResponseBuffers receiveMessage(final int responseTo) {
            return null;
        }

        @Override
        public void sendMessageAsync(final List<ByteBuf> byteBuffers, final int lastRequestId, final SingleResultCallback<Void> callback) {
            callback.onResult(null, null);
        }

        @Override
        public void receiveMessageAsync(final int responseTo, final SingleResultCallback<ResponseBuffers> callback) {
            callback.onResult(null, null);
        }

        @Override
        public ConnectionDescription getDescription() {
            return new ConnectionDescription(connectionId, 7, ServerType.UNKNOWN, 1000,
                    getDefaultMaxDocumentSize(), 100000, Collections.<String>emptyList());

        }
    }
}
