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
import com.mongodb.embedded.capi.MongoEmbeddedCAPI;
import com.mongodb.embedded.capi.MongoEmbeddedLibrary;

/**
 * A factory for {@link MongoClient} instances.
 *
 * @see MongoClient
 * @since 3.8
 */
public final class MongoClients {

    private static MongoEmbeddedLibrary mongoEmbeddedLibrary;

    /**
     * Initializes the mongod library for use.
     *
     * <p>The library must be called at most once per process before calling {@link #create(MongoClientSettings)}.</p>
     * @param mongoEmbeddedSettings the settings for the embedded driver.
     */
    public static synchronized void init(final MongoEmbeddedSettings mongoEmbeddedSettings) {
        if (mongoEmbeddedLibrary != null) {
            throw new MongoClientEmbeddedException("The mongo embedded library has already been initialized");
        }
        try {
            mongoEmbeddedLibrary = MongoEmbeddedCAPI.create(mongoEmbeddedSettings.getYamlConfig(),
                    mongoEmbeddedSettings.getLogLevel().toCapiLogLevel(), mongoEmbeddedSettings.getLibraryPath());
        } catch (Exception e) {
            throw new MongoClientEmbeddedException("The mongo embedded library could not be initialized", e);
        }

    }

    /**
     * Creates a new client.
     *
     * @param mongoClientSettings the mongoClientSettings
     * @return the client
     */
    public static synchronized MongoClient create(final MongoClientSettings mongoClientSettings) {
        if (mongoEmbeddedLibrary == null) {
            throw new MongoClientEmbeddedException("The mongo embedded library must be initialized first.");
        }
        try {
            Cluster cluster = new EmbeddedCluster(mongoEmbeddedLibrary, mongoClientSettings);
            return new MongoClientImpl(cluster, mongoClientSettings.getWrappedMongoClientSettings(), null);
        } catch (Exception e) {
            throw new MongoClientEmbeddedException("Could not create a new embedded cluster."
                    + " Ensure existing MongoClients are fully closed before trying to create a new one.", e);
        }
    }

    /**
     * Closes down the mongod library
     */
    public static synchronized void close() {
        if (mongoEmbeddedLibrary != null) {
            try {
                mongoEmbeddedLibrary.close();
            } catch (Exception e) {
                throw new MongoClientEmbeddedException("Could not close the mongo embedded library."
                        + " Ensure that any MongoClient instances have been closed first.", e);
            }
            mongoEmbeddedLibrary = null;

        }
    }

    private MongoClients() {
    }
}
