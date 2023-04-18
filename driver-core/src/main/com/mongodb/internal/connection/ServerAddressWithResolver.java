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

import com.mongodb.MongoSocketException;
import com.mongodb.ServerAddress;
import com.mongodb.lang.Nullable;
import com.mongodb.spi.dns.InetAddressResolver;
import com.mongodb.spi.dns.InetAddressResolverProvider;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

final class ServerAddressWithResolver extends ServerAddress {
    private static final long serialVersionUID = 1;

    @Nullable
    private static final InetAddressResolver DEFAULT_INET_ADDRESS_RESOLVER;

    static {
        DEFAULT_INET_ADDRESS_RESOLVER = StreamSupport.stream(ServiceLoader.load(InetAddressResolverProvider.class).spliterator(), false)
                .findFirst()
                .map(InetAddressResolverProvider::create)
                .orElse(null);
    }

    @Nullable
    private final transient InetAddressResolver resolver;

    ServerAddressWithResolver(final ServerAddress serverAddress, @Nullable final InetAddressResolver inetAddressResolver) {
        super(serverAddress.getHost(), serverAddress.getPort());
        this.resolver = inetAddressResolver == null ? DEFAULT_INET_ADDRESS_RESOLVER : inetAddressResolver;
    }

    @Override
    public InetSocketAddress getSocketAddress() {
        if (resolver == null) {
            return super.getSocketAddress();
        }

        return getSocketAddresses().get(0);
    }

    @Override
    public List<InetSocketAddress> getSocketAddresses() {
        if (resolver == null || isIpLiteral()) {
            return super.getSocketAddresses();
        }
        try {
            return resolver.lookupByName(getHost())
                    .stream()
                    .map(inetAddress -> new InetSocketAddress(inetAddress, getPort())).collect(Collectors.toList());
        } catch (UnknownHostException e) {
            throw new MongoSocketException(e.getMessage(), this, e);
        }
    }

    // If this returns true, it's either an IP literal or a malformed hostname.  But either way, skip lookup via resolver
    private boolean isIpLiteral() {
        return getHost().charAt(0) == '[' || Character.digit(getHost().charAt(0), 16) != -1 || (getHost().charAt(0) == ':');
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        ServerAddressWithResolver that = (ServerAddressWithResolver) o;
        return Objects.equals(resolver, that.resolver);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), resolver);
    }
}
