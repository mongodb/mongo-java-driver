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

import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.lang.Nullable;
import com.mongodb.spi.dns.InetAddressResolver;

import java.nio.channels.AsynchronousChannelGroup;

/**
 * A {@code StreamFactoryFactory} implementation for AsynchronousSocketChannel-based streams.
 *
 * @see java.nio.channels.AsynchronousSocketChannel
 */
public final class AsynchronousSocketChannelStreamFactoryFactory implements StreamFactoryFactory {
    private final InetAddressResolver inetAddressResolver;
    @Nullable
    private final AsynchronousChannelGroup group;

    public AsynchronousSocketChannelStreamFactoryFactory(final InetAddressResolver inetAddressResolver) {
        this(inetAddressResolver, null);
    }

    AsynchronousSocketChannelStreamFactoryFactory(
            final InetAddressResolver inetAddressResolver,
            @Nullable final AsynchronousChannelGroup group) {
        this.inetAddressResolver = inetAddressResolver;
        this.group = group;
    }

    @Override
    public StreamFactory create(final SocketSettings socketSettings, final SslSettings sslSettings) {
        return new AsynchronousSocketChannelStreamFactory(
                inetAddressResolver, socketSettings, sslSettings, group);
    }

    @Override
    public void close() {
        if (group != null) {
            group.shutdown();
        }
    }
}
