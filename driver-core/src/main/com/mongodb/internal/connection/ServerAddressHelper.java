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


import com.mongodb.MongoClientSettings;
import com.mongodb.MongoSocketException;
import com.mongodb.ServerAddress;
import com.mongodb.UnixServerAddress;
import com.mongodb.lang.Nullable;
import com.mongodb.spi.dns.InetAddressResolver;
import com.mongodb.spi.dns.InetAddressResolverProvider;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class ServerAddressHelper {
    @Nullable
    private static final InetAddressResolver LOCATED_INET_ADDRESS_RESOLVER = StreamSupport.stream(
            ServiceLoader.load(InetAddressResolverProvider.class).spliterator(), false)
            .findFirst()
            .map(InetAddressResolverProvider::create)
            .orElse(null);

    public static ServerAddress createServerAddress(final String host) {
        return createServerAddress(host, ServerAddress.defaultPort());
    }

    public static ServerAddress createServerAddress(final String host, final int port) {
        if (host != null && host.endsWith(".sock")) {
            return new UnixServerAddress(host);
        } else {
            return new ServerAddress(host, port);
        }
    }

    public static InetAddressResolver getInetAddressResolver(final MongoClientSettings settings) {
        InetAddressResolver explicitInetAddressResolver = settings.getInetAddressResolver();
        if (explicitInetAddressResolver != null) {
            return explicitInetAddressResolver;
        } else if (LOCATED_INET_ADDRESS_RESOLVER != null) {
            return LOCATED_INET_ADDRESS_RESOLVER;
        } else {
            return new DefaultInetAddressResolver();
        }
    }

    public static List<InetSocketAddress> getSocketAddresses(final ServerAddress serverAddress, final InetAddressResolver resolver) {
        try {
            return resolver.lookupByName(serverAddress.getHost())
                    .stream()
                    .map(inetAddress -> new InetSocketAddress(inetAddress, serverAddress.getPort())).collect(Collectors.toList());
        } catch (UnknownHostException e) {
            throw new MongoSocketException(e.getMessage(), serverAddress, e);
        }
    }

    private ServerAddressHelper() {
    }
}
