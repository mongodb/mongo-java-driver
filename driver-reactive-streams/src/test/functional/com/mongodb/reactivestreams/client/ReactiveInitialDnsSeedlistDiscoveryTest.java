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

package com.mongodb.reactivestreams.client;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.InitialDnsSeedlistDiscoveryTest;
import com.mongodb.client.MongoClient;
import com.mongodb.lang.NonNull;
import com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient;
import org.bson.BsonDocument;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.file.Path;
import java.util.List;

// See https://github.com/mongodb/specifications/tree/master/source/initial-dns-seedlist-discovery/tests
@RunWith(Parameterized.class)
public class ReactiveInitialDnsSeedlistDiscoveryTest extends InitialDnsSeedlistDiscoveryTest {

    public ReactiveInitialDnsSeedlistDiscoveryTest(final String filename, final Path parentDirectory, final String uri,
            final List<String> seeds, final Integer numSeeds, final List<String> hosts, final Integer numHosts,
            final boolean isError, final BsonDocument options) {
        super(filename, parentDirectory, uri, seeds, numSeeds, hosts, numHosts, isError, options);
    }

    @Override
    public MongoClient createMongoClient(@NonNull final MongoClientSettings settings) {
        return new SyncMongoClient(MongoClients.create(settings));
    }
}
