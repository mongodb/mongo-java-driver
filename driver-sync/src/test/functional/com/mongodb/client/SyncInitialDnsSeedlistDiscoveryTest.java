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

package com.mongodb.client;

import com.mongodb.MongoClientSettings;
import org.bson.BsonDocument;

import java.util.List;

public class SyncInitialDnsSeedlistDiscoveryTest extends InitialDnsSeedlistDiscoveryTest {
    public SyncInitialDnsSeedlistDiscoveryTest(final String filename, final String parentDirectory, final String uri,
            final List<String> seeds, final Integer numSeeds, final List<String> hosts, final Integer numHosts,
            final BsonDocument options, final BsonDocument parsedOptions,
            final boolean isError, final boolean executePingCommand) {
        super(filename, parentDirectory, uri, seeds, numSeeds, hosts, numHosts, options, parsedOptions, isError, executePingCommand);
    }

    @Override
    public MongoClient createMongoClient(final MongoClientSettings settings) {
        return MongoClients.create(settings);
    }
}
