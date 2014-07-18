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
import com.mongodb.selector.PrimaryServerSelector;
import com.mongodb.selector.ReadPreferenceServerSelector;
import org.mongodb.connection.Cluster;
import org.mongodb.connection.Connection;
import org.mongodb.connection.Server;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A binding that ensures that reads to the primary use the same connection as writes, while reads to any other server go to the same
 * server so long as the read preference has not been changed.
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
    private long maxWaitTimeMS;

    /**
     * Create a new session with the given cluster.
     *
     * @param cluster the cluster, which may not be null
     */
    public PinnedBinding(final Cluster cluster, final long maxWaitTime, final TimeUnit timeUnit) {
        this.cluster = notNull("cluster", cluster);
        maxWaitTimeMS = MILLISECONDS.convert(maxWaitTime, timeUnit);
    }

    public void setReadPreference(final ReadPreference readPreference) {
        this.readPreference = readPreference;
    }

    @Override
    public PinnedBinding retain() {
        super.retain();
        return this;
    }

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
        Connection connectionToUse;
        if (serverForWrites != null && serverForReads.getDescription().getAddress().equals(serverForWrites.getDescription().getAddress())) {
            connectionToUse = connectionForWrites;
        } else {
            connectionToUse = connectionForReads;
        }
        return new MyConnectionSource(connectionToUse);
    }

    @Override
    public ConnectionSource getWriteConnectionSource() {
        isTrue("open", getCount() > 0);
        if (connectionForWrites == null) {
            serverForWrites = cluster.selectServer(new PrimaryServerSelector(), maxWaitTimeMS, MILLISECONDS);
            connectionForWrites = serverForWrites.getConnection();
        }
        return new MyConnectionSource(connectionForWrites);
    }

    private static final class MyConnectionSource extends AbstractReferenceCounted implements ConnectionSource {
        private final Connection connection;

        public MyConnectionSource(final Connection connection) {
            this.connection = connection.retain();
        }

        @Override
        public Connection getConnection() {
            isTrue("open", getCount() > 0);
            return connection.retain();
        }

        @Override
        public MyConnectionSource retain() {
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
