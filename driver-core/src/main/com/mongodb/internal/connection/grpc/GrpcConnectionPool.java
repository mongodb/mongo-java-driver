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

package com.mongodb.internal.connection.grpc;

import com.mongodb.MongoConnectionPoolClearedException;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerId;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.connection.ConnectionGenerationSupplier;
import com.mongodb.internal.connection.ConnectionPool;
import com.mongodb.internal.connection.DefaultConnectionPool.ServiceStateManager;
import com.mongodb.internal.connection.InternalConnection;
import com.mongodb.internal.connection.InternalConnectionFactory;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.lang.NonNull;
import com.mongodb.lang.Nullable;
import org.bson.types.ObjectId;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.fail;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A non-pooling {@link ConnectionPool} for {@link GrpcStream}-based {@link InternalConnection}s.
 * <p>
 * This class is not part of the public API and may be removed or changed at any time.</p>
 */
// VAKOTODO events, logging? Here https://docs.google.com/document/d/1efw8AykgYHE9mwuaS3p3H-mHv3wPUDMPasDJxyH6Ts8/edit#heading=h.8r6ec9yofp1
// it is said not to do any of events/logging that CMAP requires.
@ThreadSafe
public final class GrpcConnectionPool implements ConnectionPool {
    private final ServerId serverId;
    private final InternalConnectionFactory internalConnectionFactory;
    private final ConnectionPoolSettings settings;
    private final boolean loadBalanced;
    private final ServiceStateManager serviceStateManager;
    private final AtomicInteger generation;
    private final ConnectionGenerationSupplier generationSupplier;

    public GrpcConnectionPool(
            final ServerId serverId,
            final InternalConnectionFactory internalConnectionFactory,
            final ConnectionPoolSettings settings,
            final boolean loadBalanced) {
        this.serverId = serverId;
        this.internalConnectionFactory = internalConnectionFactory;
        this.settings = settings;
        this.loadBalanced = loadBalanced;
        serviceStateManager = new ServiceStateManager();
        generation = new AtomicInteger();
        generationSupplier = new ConnectionGenerationSupplier() {
            @Override
            public int getGeneration() {
                return GrpcConnectionPool.this.getGeneration();
            }

            @Override
            public int getGeneration(@NonNull final ObjectId serviceId) {
                return serviceStateManager.getGeneration(serviceId);
            }
        };
    }

    @Override
    public InternalConnection get(final OperationContext operationContext) throws MongoConnectionPoolClearedException {
        return get(operationContext, settings.getMaxWaitTime(MILLISECONDS), MILLISECONDS);
    }

    @Override
    public InternalConnection get(final OperationContext operationContext, final long timeout, final TimeUnit timeUnit)
            throws MongoConnectionPoolClearedException {
        InternalConnection connection = internalConnectionFactory.create(serverId, generationSupplier);
        connection.open();
        return connection;
    }

    @Override
    public void getAsync(final OperationContext operationContext, final SingleResultCallback<InternalConnection> callback) {
        fail("VAKOTODO");
    }

    @Override
    public void invalidate(@Nullable final Throwable cause) {
        assertFalse(loadBalanced);
        generation.incrementAndGet();
        // VAKOTODO pause
    }

    @Override
    public void invalidate(final ObjectId serviceId, final int generation) {
        assertTrue(loadBalanced);
        // there is nothing to do here because we do not pool connections
    }

    @Override
    public void ready() {
        // VAKOTODO
    }

    @Override
    public void close() {
        // VAKOTODO
    }

    @Override
    public int getGeneration() {
        return generation.get();
    }
}
