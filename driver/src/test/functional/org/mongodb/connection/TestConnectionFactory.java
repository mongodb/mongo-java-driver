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

package org.mongodb.connection;

import org.bson.ByteBuf;

import java.util.ArrayList;
import java.util.List;

class TestConnectionFactory implements ConnectionFactory {
    private List<TestConnection> createdConnections = new ArrayList<TestConnection>();

    @Override
    public Connection create(final ServerAddress serverAddress) {
        TestConnection connection = new TestConnection(serverAddress);
        createdConnections.add(connection);
        return connection;
    }

    List<TestConnection> getCreatedConnections() {
        return createdConnections;
    }

    int getNumCreatedConnections() {
        return createdConnections.size();
    }

    public static class TestConnection implements Connection {
        private final ServerAddress serverAddress;
        private boolean closed;

        public TestConnection(final ServerAddress serverAddress) {
            this.serverAddress = serverAddress;
        }

        @Override
        public void sendMessage(final List<ByteBuf> byteBuffers) {
        }

        @Override
        public ResponseBuffers receiveMessage() {
            return null;
        }

        @Override
        public void sendMessageAsync(final List<ByteBuf> byteBuffers, final SingleResultCallback<Void> callback) {
        }

        @Override
        public void receiveMessageAsync(final SingleResultCallback<ResponseBuffers> callback) {
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
