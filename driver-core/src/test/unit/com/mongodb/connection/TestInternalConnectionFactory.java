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

import com.mongodb.ServerAddress;
import org.bson.ByteBuf;

import java.util.ArrayList;
import java.util.List;

class TestInternalConnectionFactory implements InternalConnectionFactory {
    private final List<TestInternalConnection> createdConnections = new ArrayList<TestInternalConnection>();

    @Override
    public InternalConnection create(final ServerAddress serverAddress) {
        TestInternalConnection connection = new TestInternalConnection(serverAddress);
        createdConnections.add(connection);
        return connection;
    }

    List<TestInternalConnection> getCreatedConnections() {
        return createdConnections;
    }

    int getNumCreatedConnections() {
        return createdConnections.size();
    }

    public static class TestInternalConnection implements InternalConnection {
        private final ServerAddress serverAddress;
        private boolean closed;

        public TestInternalConnection(final ServerAddress serverAddress) {
            this.serverAddress = serverAddress;
        }

        @Override
        public ByteBuf getBuffer(final int size) {
            return null;
        }

        @Override
        public void sendMessage(final List<ByteBuf> byteBuffers, final int lastRequestId) {
        }

        @Override
        public ResponseBuffers receiveMessage(final int responseTo) {
            return null;
        }

        @Override
        public void sendMessageAsync(final List<ByteBuf> byteBuffers, final int lastRequestId, final SingleResultCallback<Void> callback) {
        }

        @Override
        public void receiveMessageAsync(final int responseTo, final SingleResultCallback<ResponseBuffers> callback) {
        }

        @Override
        public String getId() {
            return Integer.toString(hashCode());
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public boolean isClosed() {
            return closed;
        }

        @Override
        public ServerAddress getServerAddress() {
            return serverAddress;
        }
    }
}
