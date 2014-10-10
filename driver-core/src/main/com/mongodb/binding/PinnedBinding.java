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

package com.mongodb.binding;

import com.mongodb.ReadPreference;
import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.Connection;
import com.mongodb.connection.Server;
import com.mongodb.connection.ServerDescription;
import com.mongodb.selector.PrimaryServerSelector;
import com.mongodb.selector.ReadPreferenceServerSelector;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A binding that ensures that reads to the primary use the same connection as writes, while reads to any other server go to the same server
 * so long as the read preference has not been changed.
 *
 * @since 3.0
 */
@NotThreadSafe
public class PinnedBinding extends AbstractReferenceCounted implements ReadWriteBinding {
    private ReadPreference readPreference = ReadPreference.primary();
    private ReadPreference lastRequestedReadPreference;
    private Server serverForReads;
    private Server serverForWrites;
    private Connection connectionForReads;
    private Connection connectionForWrites;
    private final Cluster cluster;
    private final long maxWaitTimeMS;

    /**
     * Create a new session with the given cluster.
     *
     * @param cluster     a non-null Cluster which will be used to select a server to bind to
     * @param maxWaitTime the maximum time to wait for a connection to become available.
     * @param timeUnit    a non-null TimeUnit for the maxWaitTime
     */
    public PinnedBinding(final Cluster cluster, final long maxWaitTime, final TimeUnit timeUnit) {
        this.cluster = notNull("cluster", cluster);
        notNull("timeUnit", timeUnit);
        maxWaitTimeMS = MILLISECONDS.convert(maxWaitTime, timeUnit);
    }

    /**
     * Sets the read preference for determining which servers are eligible to read from. Changing this may change the server which is 
     * used for reading from.
     *
     * @param readPreference a non-null ReadPreference for read operations
     */
    public void setReadPreference(final ReadPreference readPreference) {
        this.readPreference = readPreference;
    }

    @Override
    public PinnedBinding retain() {
        super.retain();
        return this;
    }

    @Override
    public void release() {
        super.release();
        if (getCount() == 0) {
            if (connectionForReads != null) {
                connectionForReads.release();
            }
            if (connectionForWrites != null) {
                connectionForWrites.release();
            }
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
        if (connectionForReads == null || !readPreference.equals(lastRequestedReadPreference)) {
            lastRequestedReadPreference = readPreference;
            if (connectionForReads != null) {
                connectionForReads.release();
                connectionForReads = null;
            }
            serverForReads = cluster.selectServer(new ReadPreferenceServerSelector(readPreference), maxWaitTimeMS, MILLISECONDS);
            connectionForReads = serverForReads.getConnection();
        }
        if (serverForWrites != null && serverForReads.getDescription().isPrimary()) {
            return new PinnedConnectionSource(serverForWrites, connectionForWrites);
        } else {
            return new PinnedConnectionSource(serverForReads, connectionForReads);
        }
    }

    @Override
    public ConnectionSource getWriteConnectionSource() {
        isTrue("open", getCount() > 0);
        if (connectionForWrites == null) {
            serverForWrites = cluster.selectServer(new PrimaryServerSelector(), maxWaitTimeMS, MILLISECONDS);
            connectionForWrites = serverForWrites.getConnection();
        }
        return new PinnedConnectionSource(serverForWrites, connectionForWrites);
    }

    private static final class PinnedConnectionSource extends AbstractReferenceCounted implements ConnectionSource {
        private final Connection connection;
        private final Server server;

        public PinnedConnectionSource(final Server server, final Connection connection) {
            this.server = server;
            this.connection = connection.retain();
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
        public PinnedConnectionSource retain() {
            super.retain();
            return this;
        }

        @Override
        public void release() {
            super.release();
            if (getCount() == 0) {
                connection.release();
            }
        }
    }
}
