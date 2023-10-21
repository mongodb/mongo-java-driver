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
import com.mongodb.ServerAddress;
import com.mongodb.UnixServerAddress;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.spi.dns.InetAddressResolver;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import java.security.NoSuchAlgorithmException;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Optional.ofNullable;

/**
 * Factory for creating instances of {@code SocketStream}.
 */
public class SocketStreamFactory implements StreamFactory {
    private final InetAddressResolver inetAddressResolver;
    private final SocketSettings settings;
    private final SslSettings sslSettings;
    private final BufferProvider bufferProvider = PowerOfTwoBufferPool.DEFAULT;

    /**
     * Creates a new factory with the given settings for connecting to servers and the given SSL settings
     *
     * @param inetAddressResolver resolver
     * @param settings            the SocketSettings for connecting to a MongoDB server
     * @param sslSettings         whether SSL is enabled.
     */
    public SocketStreamFactory(final InetAddressResolver inetAddressResolver, final SocketSettings settings,
            final SslSettings sslSettings) {
        this.inetAddressResolver = inetAddressResolver;
        this.settings = notNull("settings", settings);
        this.sslSettings = notNull("sslSettings", sslSettings);
    }

    @Override
    public Stream create(final ServerAddress serverAddress) {
        Stream stream;
        if (serverAddress instanceof UnixServerAddress) {
            if (sslSettings.isEnabled()) {
                throw new MongoClientException("Socket based connections do not support ssl");
            }
            stream = new UnixSocketChannelStream((UnixServerAddress) serverAddress, settings, sslSettings, bufferProvider);
        } else {
            if (sslSettings.isEnabled()) {
                stream = new SocketStream(serverAddress, inetAddressResolver, settings, sslSettings, getSslContext().getSocketFactory(),
                        bufferProvider);
            } else {
                stream = new SocketStream(serverAddress, inetAddressResolver, settings, sslSettings, SocketFactory.getDefault(),
                        bufferProvider);
            }
        }
        return stream;
    }

    private SSLContext getSslContext() {
        try {
            return ofNullable(sslSettings.getContext()).orElse(SSLContext.getDefault());
        } catch (NoSuchAlgorithmException e) {
            throw new MongoClientException("Unable to create default SSLContext", e);
        }
    }
}
