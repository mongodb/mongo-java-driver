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

package com.mongodb.connection;

import com.mongodb.ServerAddress;
import com.mongodb.internal.connection.AsynchronousSocketChannelStream;
import com.mongodb.internal.connection.PowerOfTwoBufferPool;

import java.nio.channels.AsynchronousChannelGroup;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Factory to create a Stream that's an AsynchronousSocketChannelStream. Throws an exception if SSL is enabled.
 *
 * @since 3.0
 */
public class AsynchronousSocketChannelStreamFactory implements StreamFactory {
    private final BufferProvider bufferProvider = new PowerOfTwoBufferPool();
    private final SocketSettings settings;
    private final AsynchronousChannelGroup group;

    /**
     * Create a new factory with the default {@code BufferProvider} and {@code AsynchronousChannelGroup}.
     *
     * @param settings    the settings for the connection to a MongoDB server
     * @param sslSettings the settings for connecting via SSL
     */
    public AsynchronousSocketChannelStreamFactory(final SocketSettings settings, final SslSettings sslSettings) {
        this(settings, sslSettings, null);
    }

    /**
     * Create a new factory.
     *
     * @param settings the socket settings
     * @param sslSettings the SSL settings
     * @param group the {@code AsynchronousChannelGroup} to use or null for the default group
     *
     * @since 3.6
     */
    public AsynchronousSocketChannelStreamFactory(final SocketSettings settings, final SslSettings sslSettings,
                                                  final AsynchronousChannelGroup group) {
        if (sslSettings.isEnabled()) {
            throw new UnsupportedOperationException("No SSL support in java.nio.channels.AsynchronousSocketChannel. For SSL support use "
                    + "com.mongodb.connection.netty.NettyStreamFactoryFactory");
        }

        this.settings = notNull("settings", settings);
        this.group = group;
    }

    @Override
    public Stream create(final ServerAddress serverAddress) {
        return new AsynchronousSocketChannelStream(serverAddress, settings, bufferProvider, group);
    }

}
