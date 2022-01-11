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

package com.mongodb.internal.dns;

import com.mongodb.MongoConfigurationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.naming.Context;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DefaultDnsResolverTest {
    private static final String TEST_HOST = "test1.test.build.10gen.cc";
    private static final String DEFAULT_PROVIDER_URL_VALUE = System.getProperty(Context.PROVIDER_URL);

    @AfterEach
    public void resetDefaultProviderUrl() {
        if (DEFAULT_PROVIDER_URL_VALUE != null) {
            System.setProperty(Context.PROVIDER_URL, DEFAULT_PROVIDER_URL_VALUE);
        }
    }

    @Test
    public void nonDnsProviderUrlShouldBeIgnored() {
        System.setProperty(Context.PROVIDER_URL, "file:///tmp/provider.txt");
        assertDoesNotThrow(() -> new DefaultDnsResolver().resolveHostFromSrvRecords(TEST_HOST, "mongodb"));
    }

    @Test
    public void dnsProviderUrlShouldNotBeIgnored() {
        System.setProperty(Context.PROVIDER_URL, "dns:///mongodb.unknown.server.com");
        assertThrows(MongoConfigurationException.class, () -> new DefaultDnsResolver().resolveHostFromSrvRecords(TEST_HOST, "mongodb"));
    }
}
