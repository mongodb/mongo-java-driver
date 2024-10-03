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

import com.mongodb.internal.dns.JndiDnsClient;
import com.mongodb.spi.dns.DnsClient;
import com.mongodb.spi.dns.DnsClientProvider;
import com.mongodb.spi.dns.DnsException;

import java.util.List;

import static java.lang.String.format;

public final class CustomDnsClientProvider implements DnsClientProvider {
    private static volatile boolean used = false;

    public CustomDnsClientProvider() {
    }

    @Override
    public DnsClient create() {
        return new CustomDnsClient();
    }

    static void assertUsed() throws AssertionError {
        if (!used) {
            throw new AssertionError(format("%s is not used", CustomDnsClientProvider.class.getSimpleName()));
        }
    }

    private static void markUsed() {
        used = true;
    }

    private static final class CustomDnsClient implements DnsClient {
        private final JndiDnsClient wrapped;

        CustomDnsClient() {
            wrapped = new JndiDnsClient();
        }

        @Override
        public List<String> getResourceRecordData(final String name, final String type) throws DnsException {
            markUsed();
            return wrapped.getResourceRecordData(name, type);
        }
    }
}
