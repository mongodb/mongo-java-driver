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
import com.mongodb.RequestContext;
import com.mongodb.ServerApi;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.IgnorableRequestContext;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.connection.NoOpSessionContext;
import com.mongodb.internal.connection.Server;
import com.mongodb.internal.selector.ReadPreferenceServerSelector;
import com.mongodb.internal.selector.WritableServerSelector;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.lang.Nullable;

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
    private volatile ServerDescription readServerDescription;
    private volatile ServerDescription writeServerDescription;
    @Nullable
    private final ServerApi serverApi;

    /**
     * Create a new binding with the given cluster.
     *  @param cluster     a non-null Cluster which will be used to select a server to bind to
     * @param maxWaitTime the maximum time to wait for a connection to become available.
     * @param timeUnit    a non-null TimeUnit for the maxWaitTime
     * @param serverApi   the server api, which may be null
     */
    public AsyncSingleConnectionBinding(final Cluster cluster, final long maxWaitTime, final TimeUnit timeUnit,
                                        @Nullable final ServerApi serverApi) {
        this(cluster, primary(), maxWaitTime, timeUnit, serverApi);
    }

    /**
     * Create a new binding with the given cluster.
     *  @param cluster        a non-null Cluster which will be used to select a server to bind to
     * @param readPreference the readPreference for reads, if not primary a separate connection will be used for reads
     * @param maxWaitTime    the maximum time to wait for a connection to become available.
     * @param timeUnit       a non-null TimeUnit for the maxWaitTime
     * @param serverApi      the server api, which may be null
     */
    public AsyncSingleConnectionBinding(final Cluster cluster, final ReadPreference readPreference,
                                        final long maxWaitTime, final TimeUnit timeUnit, @Nullable final ServerApi serverApi) {
        this.serverApi = serverApi;

        notNull("cluster", cluster);
        this.readPreference = notNull("readPreference", readPreference);
        CountDownLatch latch = new CountDownLatch(2);
        cluster.selectServerAsync(new WritableServerSelector(), (result, t) -> {
            if (t == null) {
                writeServer = result.getServer();
                writeServerDescription = result.getServerDescription();
                latch.countDown();
            }
        });
        cluster.selectServerAsync(new ReadPreferenceServerSelector(readPreference), (result, t) -> {
            if (t == null) {
                readServer = result.getServer();
                readServerDescription = result.getServerDescription();
                latch.countDown();
            }
        });

        awaitLatch(maxWaitTime, timeUnit, latch);

        if (writeServer == null || readServer == null) {
            throw new MongoInternalException("Failure to select server");
        }

        CountDownLatch writeServerLatch = new CountDownLatch(1);
        writeServer.getConnectionAsync((result, t) -> {
            writeConnection = result;
            writeServerLatch.countDown();
        });

        awaitLatch(maxWaitTime, timeUnit, writeServerLatch);

        if (writeConnection == null) {
            throw new MongoInternalException("Failure to get connection");
        }

        CountDownLatch readServerLatch = new CountDownLatch(1);

        readServer.getConnectionAsync((result, t) -> {
            readConnection = result;
            readServerLatch.countDown();
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
    @Nullable
    public ServerApi getServerApi() {
        return serverApi;
    }

    @Override
    public RequestContext getRequestContext() {
        return IgnorableRequestContext.INSTANCE;
    }

    @Override
    public void getReadConnectionSource(final SingleResultCallback<AsyncConnectionSource> callback) {
        isTrue("open", getCount() > 0);
        if (readPreference == primary()) {
            getWriteConnectionSource(callback);
        } else {
            callback.onResult(new SingleAsyncConnectionSource(readServerDescription, readConnection), null);
        }
    }

    @Override
    public void getReadConnectionSource(final int minWireVersion, final ReadPreference fallbackReadPreference,
            final SingleResultCallback<AsyncConnectionSource> callback) {
        getReadConnectionSource(callback);
    }

    @Override
    public void getWriteConnectionSource(final SingleResultCallback<AsyncConnectionSource> callback) {
        isTrue("open", getCount() > 0);
        callback.onResult(new SingleAsyncConnectionSource(writeServerDescription, writeConnection), null);
    }

    @Override
    public int release() {
        int count = super.release();
        if (count == 0) {
            readConnection.release();
            writeConnection.release();
        }
        return count;
    }

    private final class SingleAsyncConnectionSource extends AbstractReferenceCounted implements AsyncConnectionSource {
        private final ServerDescription serverDescription;
        private final AsyncConnection connection;

        private SingleAsyncConnectionSource(final ServerDescription serverDescription,
                                            final AsyncConnection connection) {
            this.serverDescription = serverDescription;
            this.connection = connection;
            AsyncSingleConnectionBinding.this.retain();
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
        @Nullable
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
        public void getConnection(final SingleResultCallback<AsyncConnection> callback) {
            isTrue("open", getCount() > 0);
            callback.onResult(connection.retain(), null);
        }

        public AsyncConnectionSource retain() {
            super.retain();
            return this;
        }

        @Override
        public int release() {
            int count = super.release();
            if (count == 0) {
                AsyncSingleConnectionBinding.this.release();
            }
            return count;
        }
    }
}
