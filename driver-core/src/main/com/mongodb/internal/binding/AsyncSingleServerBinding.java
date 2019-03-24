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
import com.mongodb.ServerAddress;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.Server;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.binding.AbstractReferenceCounted;
import com.mongodb.internal.connection.NoOpSessionContext;
import com.mongodb.internal.selector.ServerAddressSelector;
import com.mongodb.selector.ServerSelector;
import com.mongodb.session.SessionContext;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A simple binding where all connection sources are bound to the server specified in the constructor.
 *
 * @since 3.11
 */
public class AsyncSingleServerBinding extends AbstractReferenceCounted implements AsyncReadWriteBinding {
    private final Cluster cluster;
    private final ServerAddress serverAddress;
    private final ReadPreference readPreference;

    /**
     * Creates an instance, defaulting to {@link com.mongodb.ReadPreference#primary()} for reads.
     * @param cluster       a non-null  Cluster which will be used to select a server to bind to
     * @param serverAddress a non-null  address of the server to bind to
     */
    public AsyncSingleServerBinding(final Cluster cluster, final ServerAddress serverAddress) {
        this(cluster, serverAddress, ReadPreference.primary());
    }

    /**
     * Creates an instance.
     * @param cluster        a non-null  Cluster which will be used to select a server to bind to
     * @param serverAddress  a non-null  address of the server to bind to
     * @param readPreference a non-null  ReadPreference for read operations
     */
    public AsyncSingleServerBinding(final Cluster cluster, final ServerAddress serverAddress, final ReadPreference readPreference) {
        this.cluster = notNull("cluster", cluster);
        this.serverAddress = notNull("serverAddress", serverAddress);
        this.readPreference = notNull("readPreference", readPreference);
    }

    @Override
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    @Override
    public void getReadConnectionSource(final SingleResultCallback<AsyncConnectionSource> callback) {
        getAsyncSingleServerBindingConnectionSource(new ServerAddressSelector(serverAddress), callback);
    }

    @Override
    public void getWriteConnectionSource(final SingleResultCallback<AsyncConnectionSource> callback) {
        getAsyncSingleServerBindingConnectionSource(new ServerAddressSelector(serverAddress), callback);
    }

    private void getAsyncSingleServerBindingConnectionSource(final ServerSelector serverSelector,
                                                             final SingleResultCallback<AsyncConnectionSource> callback) {
        cluster.selectServerAsync(serverSelector, new SingleResultCallback<Server>() {
            @Override
            public void onResult(final Server result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    callback.onResult(new AsyncSingleServerBindingConnectionSource(result), null);
                }
            }
        });
    }

    @Override
    public SessionContext getSessionContext() {
        return NoOpSessionContext.INSTANCE;
    }

    @Override
    public AsyncSingleServerBinding retain() {
        super.retain();
        return this;
    }

    private final class AsyncSingleServerBindingConnectionSource extends AbstractReferenceCounted implements AsyncConnectionSource {
        private final Server server;

        private AsyncSingleServerBindingConnectionSource(final Server server) {
            AsyncSingleServerBinding.this.retain();
            this.server = server;
        }

        @Override
        public ServerDescription getServerDescription() {
            return server.getDescription();
        }

        @Override
        public SessionContext getSessionContext() {
            return NoOpSessionContext.INSTANCE;
        }

        @Override
        public void getConnection(final SingleResultCallback<AsyncConnection> callback) {
            server.getConnectionAsync(callback);
        }

        public AsyncConnectionSource retain() {
            super.retain();
            return this;
        }

        @Override
        public void release() {
            super.release();
            if (super.getCount() == 0) {
                AsyncSingleServerBinding.this.release();
            }
        }
    }
}
