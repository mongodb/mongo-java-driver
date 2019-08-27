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

import com.mongodb.MongoInternalException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ReadPreference;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.connection.NoOpSessionContext;
import com.mongodb.internal.connection.Server;
import com.mongodb.internal.selector.ReadPreferenceServerSelector;
import com.mongodb.internal.selector.WritableServerSelector;
import com.mongodb.session.SessionContext;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * An asynchronous binding that ensures that all reads use the same connection, and all writes use the same connection.
 *
 * <p>If the readPreference is {#link ReadPreference.primary()} then all reads and writes will use the same connection.</p>
 */
public class AsyncSingleConnectionBinding extends AbstractReferenceCounted implements AsyncReadWriteBinding {
    private final ReadPreference readPreference;
    private AsyncConnection readConnection;
    private AsyncConnection writeConnection;
    private volatile Server readServer;
    private volatile Server writeServer;

    /**
     * Create a new binding with the given cluster.
     *
     * @param cluster     a non-null Cluster which will be used to select a server to bind to
     * @param maxWaitTime the maximum time to wait for a connection to become available.
     * @param timeUnit    a non-null TimeUnit for the maxWaitTime
     */
    public AsyncSingleConnectionBinding(final Cluster cluster, final long maxWaitTime, final TimeUnit timeUnit) {
        this(cluster, primary(), maxWaitTime, timeUnit);
    }

    /**
     * Create a new binding with the given cluster.
     *
     * @param cluster        a non-null Cluster which will be used to select a server to bind to
     * @param readPreference the readPreference for reads, if not primary a separate connection will be used for reads
     * @param maxWaitTime    the maximum time to wait for a connection to become available.
     * @param timeUnit       a non-null TimeUnit for the maxWaitTime
     */
    public AsyncSingleConnectionBinding(final Cluster cluster, final ReadPreference readPreference,
                                        final long maxWaitTime, final TimeUnit timeUnit) {

        notNull("cluster", cluster);
        this.readPreference = notNull("readPreference", readPreference);
        final CountDownLatch latch = new CountDownLatch(2);
        cluster.selectServerAsync(new WritableServerSelector(), new SingleResultCallback<Server>() {
            @Override
            public void onResult(final Server result, final Throwable t) {
                if (t == null) {
                    writeServer = result;
                    latch.countDown();
                }
            }
        });
        cluster.selectServerAsync(new ReadPreferenceServerSelector(readPreference), new SingleResultCallback<Server>() {
            @Override
            public void onResult(final Server result, final Throwable t) {
                if (t == null) {
                    readServer = result;
                    latch.countDown();
                }
            }
        });

        awaitLatch(maxWaitTime, timeUnit, latch);

        if (writeServer == null || readServer == null) {
            throw new MongoInternalException("Failure to select server");
        }

        final CountDownLatch writeServerLatch = new CountDownLatch(1);
        writeServer.getConnectionAsync(new SingleResultCallback<AsyncConnection>() {
            @Override
            public void onResult(final AsyncConnection result, final Throwable t) {
                writeConnection = result;
                writeServerLatch.countDown();
            }
        });

        awaitLatch(maxWaitTime, timeUnit, writeServerLatch);

        if (writeConnection == null) {
            throw new MongoInternalException("Failure to get connection");
        }

        final CountDownLatch readServerLatch = new CountDownLatch(1);

        readServer.getConnectionAsync(new SingleResultCallback<AsyncConnection>() {
            @Override
            public void onResult(final AsyncConnection result, final Throwable t) {
                readConnection = result;
                readServerLatch.countDown();
            }
        });
        awaitLatch(maxWaitTime, timeUnit, readServerLatch);

        if (readConnection == null) {
            throw new MongoInternalException("Failure to get connection");
        }
    }

    private void awaitLatch(final long maxWaitTime, final TimeUnit timeUnit, final CountDownLatch latch) {
        try {
            if (!latch.await(maxWaitTime, timeUnit)) {
                throw new MongoTimeoutException("Failed to get servers");
            }
        } catch (InterruptedException e) {
            throw new MongoInternalException(e.getMessage(), e);
        }
    }

    @Override
    public AsyncReadWriteBinding retain() {
        super.retain();
        return this;
    }

    @Override
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    @Override
    public SessionContext getSessionContext() {
        return NoOpSessionContext.INSTANCE;
    }

    @Override
    public void getReadConnectionSource(final SingleResultCallback<AsyncConnectionSource> callback) {
        isTrue("open", getCount() > 0);
        if (readPreference == primary()) {
            getWriteConnectionSource(callback);
        } else {
            callback.onResult(new SingleAsyncConnectionSource(readServer, readConnection), null);
        }
    }

    @Override
    public void getWriteConnectionSource(final SingleResultCallback<AsyncConnectionSource> callback) {
        isTrue("open", getCount() > 0);
        callback.onResult(new SingleAsyncConnectionSource(writeServer, writeConnection), null);
    }

    @Override
    public void release() {
        super.release();
        if (getCount() == 0) {
            readConnection.release();
            writeConnection.release();
        }
    }

    private final class SingleAsyncConnectionSource extends AbstractReferenceCounted implements AsyncConnectionSource {
        private final Server server;
        private final AsyncConnection connection;

        private SingleAsyncConnectionSource(final Server server, final AsyncConnection connection) {
            this.server = server;
            this.connection = connection;
            AsyncSingleConnectionBinding.this.retain();
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
            isTrue("open", super.getCount() > 0);
            callback.onResult(connection.retain(), null);
        }

        public AsyncConnectionSource retain() {
            super.retain();
            return this;
        }

        @Override
        public void release() {
            super.release();
            if (super.getCount() == 0) {
                AsyncSingleConnectionBinding.this.release();
            }
        }
    }
}
