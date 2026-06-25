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
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.internal.thread.AsyncClientExecutor;
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
    private final AsyncClientExecutor clientExecutor;

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
                new LinkedBlockingQueue<>(), new DaemonThreadFactory("ClientExecutor"), Loggers.getLogger("client"));
        ownedExecutorBackingClientExecutor.allowCoreThreadTimeOut(true);
        clientExecutor = AsyncClientExecutor.backedBy(ownedExecutorBackingClientExecutor);
    }

    @Override
    public StreamFactory create(final SocketSettings socketSettings, final SslSettings sslSettings) {
        return new AsynchronousSocketChannelStreamFactory(
                inetAddressResolver, socketSettings, sslSettings, group);
    }

    /**
     * VAKOTODO create ticket, leave a TODO
     * To make things right, this should be
     * the {@linkplain AsyncClientExecutor} {@linkplain AsyncClientExecutor#backedBy(Executor) backed by} same {@link ExecutorService} that
     * the {@link AsynchronousChannelGroup} in {@link AsynchronousSocketChannelStreamFactory} uses
     * (see {@link #create(SocketSettings, SslSettings)}).
     * That {@link ExecutorService} may be provided by an application via {@link AsyncTransportSettings#getExecutorService()}.
     * But currently it is a separate {@link MongoThreadPoolExecutor}.
     */
    @Override
    public AsyncClientExecutor getClientExecutor() {
        return clientExecutor;
    }

    @Override
    public void close() {
        try {
            clientExecutor.close();
        } finally {
            try {
                if (group != null) {
                    group.shutdown();
                }
            } finally {
                ownedExecutorBackingClientExecutor.shutdown();
            }
        }
    }
}
