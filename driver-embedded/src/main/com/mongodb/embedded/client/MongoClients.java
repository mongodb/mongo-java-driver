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

package com.mongodb.embedded.client;

import com.mongodb.client.MongoClient;
import com.mongodb.client.internal.MongoClientImpl;
import com.mongodb.connection.Cluster;

/**
 * A factory for {@link MongoClient} instances.
 *
 * @see MongoClient
 * @since 3.8
 */
public final class MongoClients {

    /**
     * Initializes the mongod library for use.
     *
     * <p>The library must be called at most once per process before calling {@link #create(MongoClientSettings)}.</p>
     * @param mongoEmbeddedSettings the settings for the embedded driver.
     */
    public static void init(final MongoEmbeddedSettings mongoEmbeddedSettings) {
    }

    /**
     * Creates a new client.
     *
     * @param mongoClientSettings the mongoClientSettings
     * @return the client
     */
    public static MongoClient create(final MongoClientSettings mongoClientSettings) {
        Cluster cluster = new EmbeddedCluster(mongoClientSettings);
        return new MongoClientImpl(cluster, mongoClientSettings.getWrappedMongoClientSettings(), null);
    }

    /**
     * Closes down the mongod library
     */
    public static void close() {
    }

    private MongoClients() {
    }
}
