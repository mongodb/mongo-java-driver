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

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.connection.Server;
import com.mongodb.internal.selector.ReadPreferenceServerSelector;
import com.mongodb.internal.selector.ReadPreferenceWithFallbackServerSelector;
import com.mongodb.internal.selector.ServerAddressSelector;
import com.mongodb.internal.selector.WritableServerSelector;
import com.mongodb.selector.ServerSelector;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * A simple ReadWriteBinding implementation that supplies write connection sources bound to a possibly different primary each time, and a
 * read connection source bound to a possible different server each time.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class AsyncClusterBinding extends AbstractReferenceCounted implements AsyncClusterAwareReadWriteBinding {
    private final Cluster cluster;
    private final ReadPreference readPreference;
    private final ReadConcern readConcern;
    private final OperationContext operationContext;

    /**
     * Creates an instance.
     *
     * @param cluster        a non-null Cluster which will be used to select a server to bind to
     * @param readPreference a non-null ReadPreference for read operations
     * @param readConcern    a non-null read concern
     * @param operationContext the operation context
     * <p>This class is not part of the public API and may be removed or changed at any time</p>
     */
    public AsyncClusterBinding(final Cluster cluster, final ReadPreference readPreference, final ReadConcern readConcern,
            final OperationContext operationContext) {
        this.cluster = notNull("cluster", cluster);
        this.readPreference = notNull("readPreference", readPreference);
        this.readConcern = notNull("readConcern", readConcern);
        this.operationContext = notNull("operationContext", operationContext);
    }

    @Override
    public AsyncClusterAwareReadWriteBinding retain() {
        super.retain();
        return this;
    }

    @Override
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    @Override
    public OperationContext getOperationContext() {
        return operationContext;
    }

    @Override
    public void getReadConnectionSource(final SingleResultCallback<AsyncConnectionSource> callback) {
        getAsyncClusterBindingConnectionSource(new ReadPreferenceServerSelector(readPreference), callback);
    }

    @Override
    public void getReadConnectionSource(final int minWireVersion, final ReadPreference fallbackReadPreference,
            final SingleResultCallback<AsyncConnectionSource> callback) {
        // Assume 5.0+ for load-balanced mode
        if (cluster.getSettings().getMode() == ClusterConnectionMode.LOAD_BALANCED) {
            getReadConnectionSource(callback);
        } else {
            ReadPreferenceWithFallbackServerSelector readPreferenceWithFallbackServerSelector
                    = new ReadPreferenceWithFallbackServerSelector(readPreference, minWireVersion, fallbackReadPreference);
            cluster.selectServerAsync(readPreferenceWithFallbackServerSelector, operationContext, (result, t) -> {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    callback.onResult(new AsyncClusterBindingConnectionSource(result.getServer(), result.getServerDescription(),
                            readPreferenceWithFallbackServerSelector.getAppliedReadPreference()), null);
                }
            });
        }
    }

    @Override
    public void getWriteConnectionSource(final SingleResultCallback<AsyncConnectionSource> callback) {
        getAsyncClusterBindingConnectionSource(new WritableServerSelector(), callback);
    }

    @Override
    public void getConnectionSource(final ServerAddress serverAddress, final SingleResultCallback<AsyncConnectionSource> callback) {
        getAsyncClusterBindingConnectionSource(new ServerAddressSelector(serverAddress), callback);
    }

    private void getAsyncClusterBindingConnectionSource(final ServerSelector serverSelector,
                                                        final SingleResultCallback<AsyncConnectionSource> callback) {
        cluster.selectServerAsync(serverSelector, operationContext, (result, t) -> {
            if (t != null) {
                callback.onResult(null, t);
            } else {
                callback.onResult(new AsyncClusterBindingConnectionSource(result.getServer(), result.getServerDescription(),
                        readPreference), null);
            }
        });
    }

    private final class AsyncClusterBindingConnectionSource extends AbstractReferenceCounted implements AsyncConnectionSource {
        private final Server server;
        private final ServerDescription serverDescription;
        private final ReadPreference appliedReadPreference;

        private AsyncClusterBindingConnectionSource(final Server server, final ServerDescription serverDescription,
                final ReadPreference appliedReadPreference) {
            this.server = server;
            this.serverDescription = serverDescription;
            this.appliedReadPreference = appliedReadPreference;
            operationContext.getTimeoutContext().minRoundTripTimeMS(NANOSECONDS.toMillis(serverDescription.getMinRoundTripTimeNanos()));
            AsyncClusterBinding.this.retain();
        }

        @Override
        public ServerDescription getServerDescription() {
            return serverDescription;
        }

        @Override
        public OperationContext getOperationContext() {
            return operationContext;
        }

        @Override
        public ReadPreference getReadPreference() {
            return appliedReadPreference;
        }

        @Override
        public void getConnection(final SingleResultCallback<AsyncConnection> callback) {
            server.getConnectionAsync(operationContext, callback);
        }

        public AsyncConnectionSource retain() {
            super.retain();
            AsyncClusterBinding.this.retain();
            return this;
        }

        @Override
        public int release() {
            int count = super.release();
            AsyncClusterBinding.this.release();
            return count;
        }
    }
}
