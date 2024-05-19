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

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

final class DnsSpi {
    private static final Logger LOGGER = LoggerFactory.getLogger(DnsSpi.class);

    public static void main(final String... args) {
        useInetAddressResolverProvider(args);
        useDnsClientProvider();
    }

    private static void useInetAddressResolverProvider(final String... args) {
        try (MongoClient client = args.length == 0 ? MongoClients.create() : MongoClients.create(args[0])) {
            ArrayList<String> databaseNames = client.listDatabaseNames().into(new ArrayList<>());
            LOGGER.info("Database names: {}", databaseNames);
        }
        CustomInetAddressResolverProvider.assertUsed();
    }

    private static void useDnsClientProvider() {
        try (MongoClient client = MongoClients.create(MongoClientSettings.builder()
                .applyToClusterSettings(builder -> builder
                        .srvHost("a.b.c")
                        // `MongoClient` uses `CustomDnsClientProvider` asynchronously,
                        // and by waiting for server selection that cannot succeed due to `a.b.c` not resolving to an IP address,
                        // we give `MongoClient` enough time to use `CustomDnsClientProvider`.
                        // This is a tolerable race condition for a test.
                        .serverSelectionTimeout(2, TimeUnit.SECONDS))
                .build())) {
            ArrayList<String> databaseNames = client.listDatabaseNames().into(new ArrayList<>());
            LOGGER.info("Database names: {}", databaseNames);
        } catch (RuntimeException e) {
            try {
                CustomDnsClientProvider.assertUsed();
            } catch (AssertionError err) {
                err.addSuppressed(e);
                throw err;
            }
            // an exception is expected because `a.b.c` does not resolve to an IP address
        }
    }

    private DnsSpi() {
    }
}
