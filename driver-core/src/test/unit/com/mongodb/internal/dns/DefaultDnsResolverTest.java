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

import com.mongodb.MongoClientException;
import com.mongodb.MongoConfigurationException;
import org.junit.jupiter.api.Test;

import javax.naming.Context;

import static org.junit.jupiter.api.Assertions.fail;

public class DefaultDnsResolverTest {
    private static final String TEST_HOST = "test1.test.build.10gen.cc";

    @Test
    public void nonDnsProviderUrlShouldBeIgnored() {
        String currentValue = System.getProperty(Context.PROVIDER_URL);
        try {
            System.setProperty(Context.PROVIDER_URL, "file:///tmp/provider.txt");
            new DefaultDnsResolver().resolveHostFromSrvRecords(TEST_HOST, "mongodb");
        } catch (MongoClientException e) {
            fail("Resolution should succeed");
        }
        finally {
            if (currentValue != null) {
                System.setProperty(Context.PROVIDER_URL, currentValue);
            }
        }
    }

    @Test
    public void dnsProviderUrlShouldNotBeIgnored() {
        String currentValue = System.getProperty(Context.PROVIDER_URL);
        try {
            System.setProperty(Context.PROVIDER_URL, "dns:///mongodb.unknown.server.com");
            new DefaultDnsResolver().resolveHostFromSrvRecords(TEST_HOST, "mongodb");
            fail("Resolution should fail");
        } catch (MongoConfigurationException e) {
            // expected
        }
        finally {
            if (currentValue != null) {
                System.setProperty(Context.PROVIDER_URL, currentValue);
            }
        }
    }
}
