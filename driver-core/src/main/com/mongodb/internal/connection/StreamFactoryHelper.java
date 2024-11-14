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
import com.mongodb.MongoClientSettings;
import com.mongodb.connection.AsyncTransportSettings;
import com.mongodb.connection.NettyTransportSettings;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.TransportSettings;
import com.mongodb.internal.connection.netty.NettyStreamFactoryFactory;
import com.mongodb.spi.dns.InetAddressResolver;

import java.io.IOException;
import java.nio.channels.AsynchronousChannelGroup;
import java.util.concurrent.ExecutorService;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class StreamFactoryHelper {

    public static StreamFactory getSyncStreamFactory(final MongoClientSettings settings,
            final InetAddressResolver inetAddressResolver, final SocketSettings socketSettings) {
        TransportSettings transportSettings = settings.getTransportSettings();
        if (transportSettings == null) {
            return new SocketStreamFactory(inetAddressResolver, socketSettings, settings.getSslSettings());
        } else if (transportSettings instanceof AsyncTransportSettings) {
            throw new MongoClientException("Unsupported transport settings in sync: " + transportSettings.getClass().getName());
        } else if (transportSettings instanceof NettyTransportSettings) {
            return getNettyStreamFactoryFactory(inetAddressResolver, (NettyTransportSettings) transportSettings)
                    .create(socketSettings, settings.getSslSettings());
        } else {
            throw new MongoClientException("Unsupported transport settings: " + transportSettings.getClass().getName());
        }
    }

    public static StreamFactoryFactory getAsyncStreamFactoryFactory(final MongoClientSettings settings,
            final InetAddressResolver inetAddressResolver) {
        TransportSettings transportSettings = settings.getTransportSettings();
        if (transportSettings == null || transportSettings instanceof AsyncTransportSettings) {
            ExecutorService executorService = transportSettings == null
                    ? null
                    : ((AsyncTransportSettings) transportSettings).getExecutorService();
            if (settings.getSslSettings().isEnabled()) {
                return new TlsChannelStreamFactoryFactory(inetAddressResolver, executorService);
            } else {
                AsynchronousChannelGroup group;
                try {
                    group = AsynchronousChannelGroup.withThreadPool(executorService);
                } catch (IOException e) {
                    throw new MongoClientException("Unable to create an asynchronous channel group", e);
                }
                return new AsynchronousSocketChannelStreamFactoryFactory(inetAddressResolver, group);
            }
        } else  if (transportSettings instanceof NettyTransportSettings) {
            return getNettyStreamFactoryFactory(inetAddressResolver, (NettyTransportSettings) transportSettings);
        } else {
            throw new MongoClientException("Unsupported transport settings: " + transportSettings.getClass().getName());
        }
    }

    private static NettyStreamFactoryFactory getNettyStreamFactoryFactory(final InetAddressResolver inetAddressResolver,
            final NettyTransportSettings transportSettings) {
        return NettyStreamFactoryFactory.builder()
                .applySettings(transportSettings)
                .inetAddressResolver(inetAddressResolver)
                .build();
    }

    private StreamFactoryHelper() {
    }
}
