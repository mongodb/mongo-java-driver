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
import com.mongodb.spi.dns.DnsClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.naming.Context;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DefaultDnsResolverTest {
    private static final String TEST_HOST = "test1.test.build.10gen.cc";
    private static final String DEFAULT_PROVIDER_URL_VALUE = System.getProperty(Context.PROVIDER_URL);

    private static DefaultDnsResolver resolverReturning(final String... srvTargets) {
        DnsClient dnsClient = (name, type) -> {
            assertEquals("_mongodb._tcp." + TEST_HOST, name);
            assertEquals("SRV", type);
            List<String> records = new ArrayList<>();
            for (String target : srvTargets) {
                records.add("10 5 27017 " + target);
            }
            return records;
        };
        return new DefaultDnsResolver(dnsClient);
    }

    @AfterEach
    public void resetDefaultProviderUrl() {
        if (DEFAULT_PROVIDER_URL_VALUE != null) {
            System.setProperty(Context.PROVIDER_URL, DEFAULT_PROVIDER_URL_VALUE);
        }
    }

    @Test
    public void nonDnsProviderUrlShouldBeIgnored() {
        System.setProperty(Context.PROVIDER_URL, "file:///tmp/provider.txt");
        assertDoesNotThrow(() -> new DefaultDnsResolver().resolveHostFromSrvRecords(TEST_HOST, "mongodb", null));
    }

    @Test
    public void dnsProviderUrlShouldNotBeIgnored() {
        System.setProperty(Context.PROVIDER_URL, "dns:///mongodb.unknown.server.com");
        assertThrows(MongoConfigurationException.class, () -> new DefaultDnsResolver().resolveHostFromSrvRecords(TEST_HOST, "mongodb", null));
    }

    @Test
    public void shouldAcceptResolvedHostEndingWithSrvAllowedHostsSuffix() {
        DefaultDnsResolver resolver = resolverReturning("localhost.build.10gen.cc.");
        assertEquals(singletonList("localhost.build.10gen.cc:27017"),
                resolver.resolveHostFromSrvRecords(TEST_HOST, "mongodb", ".build.10gen.cc"));
    }


    @Test
    public void shouldMatchSrvAllowedHostsSuffixCaseInsensitively() {
        DefaultDnsResolver resolver = resolverReturning("LOCALHOST.Build.10GEN.cc.");
        assertEquals(singletonList("LOCALHOST.Build.10GEN.cc:27017"),
                resolver.resolveHostFromSrvRecords(TEST_HOST, "mongodb", ".build.10gen.cc"));
    }

    @Test
    public void shouldThrowWhenResolvedHostDoesNotEndWithSrvAllowedHostsSuffix() {
        DefaultDnsResolver resolver = resolverReturning("localhost.build.10gen.cc.");
        MongoConfigurationException e = assertThrows(MongoConfigurationException.class,
                () -> resolver.resolveHostFromSrvRecords(TEST_HOST, "mongodb", ".test.build.10gen.cc"));
        assertTrue(e.getMessage().contains("srvAllowedHostsSuffix"));
    }

    @Test
    public void shouldThrowWhenAnyResolvedHostDoesNotEndWithSrvAllowedHostsSuffix() {
        DefaultDnsResolver resolver = resolverReturning("ok.build.10gen.cc.", "bad.evil.example.com.");
        assertThrows(MongoConfigurationException.class,
                () -> resolver.resolveHostFromSrvRecords(TEST_HOST, "mongodb", ".build.10gen.cc"));
    }

    @Test
    public void shouldThrowWhenResolvedHostEqualsSrvAllowedHostsSuffixWithoutAdditionalLabel() {
        // a leading '.' is prepended, so a bare host equal to the suffix has no label boundary and must be rejected
        DefaultDnsResolver resolver = resolverReturning("build.10gen.cc.");
        assertThrows(MongoConfigurationException.class,
                () -> resolver.resolveHostFromSrvRecords(TEST_HOST, "mongodb", ".build.10gen.cc"));
    }
}
