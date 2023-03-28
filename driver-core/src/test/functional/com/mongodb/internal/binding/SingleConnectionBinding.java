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

package com.mongodb.internal.binding;

import com.mongodb.ReadPreference;
import com.mongodb.RequestContext;
import com.mongodb.ServerApi;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.IgnorableRequestContext;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.NoOpSessionContext;
import com.mongodb.internal.connection.ServerTuple;
import com.mongodb.internal.selector.ReadPreferenceServerSelector;
import com.mongodb.internal.selector.WritableServerSelector;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.lang.Nullable;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * A binding that ensures that all reads use the same connection, and all writes use the same connection.
 *
 * <p>If the readPreference is {#link ReadPreference.primary()} then all reads and writes will use the same connection.</p>
 */
public class SingleConnectionBinding implements ReadWriteBinding {
    private final ReadPreference readPreference;
    private final Connection readConnection;
    private final Connection writeConnection;
    private final ServerDescription readServerDescription;
    private final ServerDescription writeServerDescription;
    private int count = 1;
    @Nullable
    private final ServerApi serverApi;
    private final OperationContext operationContext;

    /**
     * Create a new binding with the given cluster.
     *
     * @param cluster     a non-null Cluster which will be used to select a server to bind to
     * @param readPreference the readPreference for reads, if not primary a separate connection will be used for reads
     */
    public SingleConnectionBinding(final Cluster cluster, final ReadPreference readPreference, @Nullable final ServerApi serverApi) {
        this.serverApi = serverApi;
        operationContext = new OperationContext();
        notNull("cluster", cluster);
        this.readPreference = notNull("readPreference", readPreference);
        ServerTuple writeServerTuple = cluster.selectServer(new WritableServerSelector(), operationContext);
        writeServerDescription = writeServerTuple.getServerDescription();
        writeConnection = writeServerTuple.getServer().getConnection(operationContext);
        ServerTuple readServerTuple = cluster.selectServer(new ReadPreferenceServerSelector(readPreference), operationContext);
        readServerDescription = readServerTuple.getServerDescription();
        readConnection = readServerTuple.getServer().getConnection(operationContext);
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
    public int release() {
        count--;
        if (count == 0) {
            writeConnection.release();
            readConnection.release();
        }
        return count;
    }

    @Override
    public ReadPreference getReadPreference() {
        isTrue("open", getCount() > 0);
        return readPreference;
    }

    @Override
    public ConnectionSource getReadConnectionSource() {
        isTrue("open", getCount() > 0);
        if (readPreference == primary()) {
            return getWriteConnectionSource();
        } else {
            return new SingleConnectionSource(readServerDescription, readConnection);
        }
    }

    @Override
    public ConnectionSource getReadConnectionSource(final int minWireVersion, final ReadPreference fallbackReadPreference) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SessionContext getSessionContext() {
        return NoOpSessionContext.INSTANCE;
    }

    @Override
    @Nullable
    public ServerApi getServerApi() {
        return serverApi;
    }

    @Override
    public RequestContext getRequestContext() {
        return IgnorableRequestContext.INSTANCE;
    }

    @Override
    public OperationContext getOperationContext() {
        return operationContext;
    }

    @Override
    public ConnectionSource getWriteConnectionSource() {
        isTrue("open", getCount() > 0);
        return new SingleConnectionSource(writeServerDescription, writeConnection);
    }

    private final class SingleConnectionSource implements ConnectionSource {
        private final ServerDescription serverDescription;
        private final Connection connection;
        private int count = 1;

        SingleConnectionSource(final ServerDescription serverDescription, final Connection connection) {
            this.serverDescription = serverDescription;
            this.connection = connection;
            SingleConnectionBinding.this.retain();
        }

        @Override
        public ServerDescription getServerDescription() {
            return serverDescription;
        }

        @Override
        public SessionContext getSessionContext() {
            return NoOpSessionContext.INSTANCE;
        }

        @Override
        public OperationContext getOperationContext() {
            return operationContext;
        }

        @Override
        public ServerApi getServerApi() {
            return serverApi;
        }

        @Override
        public RequestContext getRequestContext() {
            return IgnorableRequestContext.INSTANCE;
        }

        @Override
        public ReadPreference getReadPreference() {
            return readPreference;
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
        public int release() {
            count--;
            if (count == 0) {
                SingleConnectionBinding.this.release();
            }
            return count;
        }
    }
}
