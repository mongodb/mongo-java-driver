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
package com.mongodb.internal.graalvm;

import com.mongodb.internal.connection.DefaultInetAddressResolver;
import com.mongodb.spi.dns.InetAddressResolver;
import com.mongodb.spi.dns.InetAddressResolverProvider;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import static java.lang.String.format;

public final class CustomInetAddressResolverProvider implements InetAddressResolverProvider {
    private static volatile boolean used = false;

    public CustomInetAddressResolverProvider() {
    }

    @Override
    public InetAddressResolver create() {
        return new CustomInetAddressResolver();
    }

    static void assertUsed() throws AssertionError {
        if (!used) {
            throw new AssertionError(format("%s is not used", CustomInetAddressResolverProvider.class.getSimpleName()));
        }
    }

    private static void markUsed() {
        used = true;
    }

    private static final class CustomInetAddressResolver implements InetAddressResolver {
        private final DefaultInetAddressResolver wrapped;

        CustomInetAddressResolver() {
            wrapped = new DefaultInetAddressResolver();
        }

        @Override
        public List<InetAddress> lookupByName(final String host) throws UnknownHostException {
            markUsed();
            return wrapped.lookupByName(host);
        }
    }
}
