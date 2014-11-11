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

import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadWriteBinding;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.Connection;
import com.mongodb.connection.Server;
import com.mongodb.connection.ServerDescription;
import com.mongodb.selector.PrimaryServerSelector;
import com.mongodb.selector.ReadPreferenceServerSelector;

import static com.mongodb.assertions.Assertions.isTrue;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A binding that ensures that all reads use the same connection, and all writes use the same connection.
 */
public class SingleConnectionBinding implements ReadWriteBinding {
    private final ReadPreference readPreference;
    private final Connection readConnection;
    private final Connection writeConnection;
    private final Server readServer;
    private final Server writeServer;

    private int count = 1;

    /**
     * Create a new session with the given cluster.
     *
     * @param cluster     a non-null Cluster which will be used to select a server to bind to
     * @param maxWaitTime the maximum time to wait for a connection to become available.
     * @param timeUnit    a non-null TimeUnit for the maxWaitTime
     */
    public SingleConnectionBinding(final Cluster cluster, final ReadPreference readPreference) {
        this.readPreference = readPreference;
        readServer = cluster.selectServer(new ReadPreferenceServerSelector(readPreference), 1, SECONDS);
        readConnection = readServer.getConnection();
        writeServer = cluster.selectServer(new PrimaryServerSelector(), 1, SECONDS);
        writeConnection = writeServer.getConnection();
    }

    @Override
    public int getCount() {
        return count;
    }

    @Override
    public SingleConnectionBinding retain() {
        count++;
        return this;
    }

    @Override
    public void release() {
        count--;
        if (count == 0) {
            readConnection.release();
            writeConnection.release();
        }
    }

    @Override
    public ReadPreference getReadPreference() {
        isTrue("open", getCount() > 0);
        return readPreference;
    }

    @Override
    public ConnectionSource getReadConnectionSource() {
        isTrue("open", getCount() > 0);
        return new SingleConnectionSource(readServer, readConnection);
    }

    @Override
    public ConnectionSource getWriteConnectionSource() {
        isTrue("open", getCount() > 0);
        return new SingleConnectionSource(writeServer, writeConnection);
    }

    private final class SingleConnectionSource implements ConnectionSource {
        private final Connection connection;
        private final Server server;
        private int count = 1;

        public SingleConnectionSource(final Server server, final Connection connection) {
            this.server = server;
            this.connection = connection;
            SingleConnectionBinding.this.retain();
        }

        @Override
        public ServerDescription getServerDescription() {
            return server.getDescription();
        }

        @Override
        public Connection getConnection() {
            isTrue("open", getCount() > 0);
            return connection.retain();
        }

        @Override
        public int getCount() {
            return count;
        }

        @Override
        public SingleConnectionSource retain() {
            count++;
            return this;
        }

        @Override
        public void release() {
            count--;
            if (getCount() == 0) {
                SingleConnectionBinding.this.release();
            }
        }
    }
}
