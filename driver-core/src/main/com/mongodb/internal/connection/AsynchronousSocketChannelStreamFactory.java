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

import com.mongodb.ServerAddress;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.spi.dns.InetAddressResolver;

import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * Factory to create a Stream that's an AsynchronousSocketChannelStream. Throws an exception if SSL is enabled.
 */
public class AsynchronousSocketChannelStreamFactory implements StreamFactory {
    private final PowerOfTwoBufferPool bufferProvider = PowerOfTwoBufferPool.DEFAULT;
    private final SocketSettings settings;
    private final InetAddressResolver inetAddressResolver;

    /**
     * Create a new factory with the default {@code BufferProvider} and {@code AsynchronousChannelGroup}.
     *
     * @param settings    the settings for the connection to a MongoDB server
     * @param sslSettings the settings for connecting via SSL
     */
    public AsynchronousSocketChannelStreamFactory(final InetAddressResolver inetAddressResolver, final SocketSettings settings,
            final SslSettings sslSettings) {
        assertFalse(sslSettings.isEnabled());
        this.inetAddressResolver = inetAddressResolver;
        this.settings = notNull("settings", settings);
    }

    @Override
    public Stream create(final ServerAddress serverAddress) {
        return new AsynchronousSocketChannelStream(serverAddress, inetAddressResolver, settings, bufferProvider);
    }

}
