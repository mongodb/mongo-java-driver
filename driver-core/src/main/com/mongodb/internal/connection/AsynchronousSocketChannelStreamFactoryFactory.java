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

package com.mongodb.internal.connection;

import com.mongodb.MongoClientException;
import com.mongodb.connection.AsyncTransportSettings;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.thread.DaemonThreadFactory;
import com.mongodb.internal.thread.MongoThreadPoolExecutor;
import com.mongodb.lang.Nullable;
import com.mongodb.spi.dns.InetAddressResolver;

import java.io.IOException;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;

import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;

/**
 * A {@code StreamFactoryFactory} implementation for AsynchronousSocketChannel-based streams.
 *
 * @see java.nio.channels.AsynchronousSocketChannel
 */
public final class AsynchronousSocketChannelStreamFactoryFactory implements StreamFactoryFactory {
    private final InetAddressResolver inetAddressResolver;
    private final AsynchronousChannelGroup group;
    private final ExecutorService ownedExecutorService;

    @VisibleForTesting(otherwise = PRIVATE)
    AsynchronousSocketChannelStreamFactoryFactory(final InetAddressResolver inetAddressResolver) {
        this(inetAddressResolver, null);
    }

    /**
     * @param applicationSuppliedOwnedExecutorService Owned {@link ExecutorService} from {@link AsyncTransportSettings#getExecutorService()}.
     */
    AsynchronousSocketChannelStreamFactoryFactory(
            final InetAddressResolver inetAddressResolver,
            @Nullable final ExecutorService applicationSuppliedOwnedExecutorService) {
        this.inetAddressResolver = inetAddressResolver;
        try {
            if (applicationSuppliedOwnedExecutorService == null) {
                // We try to create a group similarly to how
                // the system-wide default `AsynchronousChannelGroup` is created.
                // This means:
                // - creating the executor similarly to `Executors.newCachedThreadPool`;
                // - creating the group via `withCachedThreadPool`;
                // - requesting the implementation specific default by passing negative `initialSize`.
                ownedExecutorService = new MongoThreadPoolExecutor(
                        0, Integer.MAX_VALUE, Duration.ofSeconds(60), new SynchronousQueue<>(), new DaemonThreadFactory("IOExecutor"));
                group = AsynchronousChannelGroup.withCachedThreadPool(ownedExecutorService, -1);
            } else {
                ownedExecutorService = applicationSuppliedOwnedExecutorService;
                group = AsynchronousChannelGroup.withThreadPool(ownedExecutorService);
            }
        } catch (IOException e) {
            throw new MongoClientException("Unable to create an asynchronous channel group", e);
        }
    }

    @Override
    public StreamFactory create(final SocketSettings socketSettings, final SslSettings sslSettings) {
        return new AsynchronousSocketChannelStreamFactory(
                inetAddressResolver, socketSettings, sslSettings, () -> AsynchronousSocketChannel.open(group));
    }

    /**
     * @return The {@link ExecutorService} used by the {@link StreamFactory} created via {@link #create(SocketSettings, SslSettings)}.
     * It may be provided by an application via {@link AsyncTransportSettings#getExecutorService()}.
     */
    @Override
    public Executor getExecutor() {
        return ownedExecutorService;
    }

    @Override
    public void close() {
        // termination of the `group` results in the orderly shutdown of the `ownedExecutorService`
        group.shutdown();
    }
}
