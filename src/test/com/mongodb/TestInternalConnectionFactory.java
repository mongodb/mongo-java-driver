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

package com.mongodb;

import java.util.ArrayList;
import java.util.List;

class TestInternalConnectionFactory implements ConnectionFactory {
    private final List<TestInternalConnection> createdConnections = new ArrayList<TestInternalConnection>();

    @Override
    public Connection create(final ServerAddress serverAddress, final PooledConnectionProvider provider, final int generation) {
        TestInternalConnection connection = new TestInternalConnection(generation);
        createdConnections.add(connection);
        return connection;
    }

    List<TestInternalConnection> getCreatedConnections() {
        return createdConnections;
    }

    int getNumCreatedConnections() {
        return createdConnections.size();
    }

    public static class TestInternalConnection implements Connection {
        private boolean closed;
        private long openedAt = System.currentTimeMillis();
        private final int generation;

        public TestInternalConnection(final int generation) {
            this.generation = generation;
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public int getGeneration() {
            return generation;
        }

        @Override
        public long getOpenedAt() {
            return openedAt;
        }

        @Override
        public long getLastUsedAt() {
            return openedAt;
        }

        @Override
        public boolean isClosed() {
            return closed;
        }
    }
}