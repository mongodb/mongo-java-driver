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
            LOGGER.info("Database names: {}", client.listDatabaseNames().into(new ArrayList<>()));
        }
        CustomInetAddressResolverProvider.assertUsed();
    }

    private static void useDnsClientProvider() {
        try (MongoClient client = MongoClients.create(MongoClientSettings.builder()
                .applyToClusterSettings(builder -> builder
                        .srvHost("a.b.c")
                        .serverSelectionTimeout(1, TimeUnit.MILLISECONDS))
                .build())) {
            LOGGER.info("Database names: {}", client.listDatabaseNames().into(new ArrayList<>()));
        } catch (RuntimeException e) {
            try {
                CustomDnsClientProvider.assertUsed();
            } catch (AssertionError err) {
                err.addSuppressed(e);
                throw err;
            }
            // an exception is expected provided that the assertion succeeds
        }
    }

    private DnsSpi() {
    }
}
