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

import com.mongodb.connection.AsyncTransportSettings;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.internal.thread.DaemonThreadFactory;
import com.mongodb.internal.thread.MongoThreadPoolExecutor;
import com.mongodb.lang.Nullable;
import com.mongodb.spi.dns.InetAddressResolver;

import java.nio.channels.AsynchronousChannelGroup;
import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A {@code StreamFactoryFactory} implementation for AsynchronousSocketChannel-based streams.
 *
 * @see java.nio.channels.AsynchronousSocketChannel
 */
public final class AsynchronousSocketChannelStreamFactoryFactory implements StreamFactoryFactory {
    private final InetAddressResolver inetAddressResolver;
    @Nullable
    private final AsynchronousChannelGroup group;
    private final MongoThreadPoolExecutor ownedExecutorBackingClientExecutor;

    public AsynchronousSocketChannelStreamFactoryFactory(final InetAddressResolver inetAddressResolver) {
        this(inetAddressResolver, null);
    }

    AsynchronousSocketChannelStreamFactoryFactory(
            final InetAddressResolver inetAddressResolver,
            @Nullable final AsynchronousChannelGroup group) {
        this.inetAddressResolver = inetAddressResolver;
        this.group = group;
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        ownedExecutorBackingClientExecutor = new MongoThreadPoolExecutor(
                availableProcessors, availableProcessors, Duration.ofMinutes(5),
                new LinkedBlockingQueue<>(), new DaemonThreadFactory("ClientExecutor"));
        ownedExecutorBackingClientExecutor.allowCoreThreadTimeOut(true);
    }

    @Override
    public StreamFactory create(final SocketSettings socketSettings, final SslSettings sslSettings) {
        return new AsynchronousSocketChannelStreamFactory(
                inetAddressResolver, socketSettings, sslSettings, group);
    }

    /**
     * @return VAKOTODO create ticket, leave a TODO
     * A dedicated {@link MongoThreadPoolExecutor}, which is suboptimal. To make things right, this should be
     * The {@link ExecutorService} used by the {@link StreamFactory} created via {@link #create(SocketSettings, SslSettings)}.
     * It may be provided by an application via {@link AsyncTransportSettings#getExecutorService()}.
     */
    @Override
    public Executor getExecutor() {
        return ownedExecutorBackingClientExecutor;
    }

    @Override
    public void close() {
        try {
            if (group != null) {
                group.shutdown();
            }
        } finally {
            ownedExecutorBackingClientExecutor.shutdown();
        }
    }
}
