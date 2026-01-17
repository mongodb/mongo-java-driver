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

package com.mongodb.client.internal;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoDriverInformation;
import com.mongodb.client.MongoClient;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.connection.InternalMongoClientSettings;
import com.mongodb.internal.connection.StreamFactoryFactory;
import com.mongodb.lang.Nullable;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.connection.ServerAddressHelper.getInetAddressResolver;
import static com.mongodb.internal.connection.StreamFactoryHelper.getSyncStreamFactoryFactory;

/**
 * Internal factory for MongoClient instances.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class InternalMongoClients {

    private InternalMongoClients() {
    }

    /**
     * Creates a new client with the default connection string "mongodb://localhost" and the given internal settings.
     *
     * @param internalSettings the internal settings
     * @return the client
     */
    public static MongoClient create(final InternalMongoClientSettings internalSettings) {
        return create(new ConnectionString("mongodb://localhost"), internalSettings);
    }

    /**
     * Creates a new client with the given connection string and internal settings.
     *
     * @param connectionString the connection string
     * @param internalSettings the internal settings
     * @return the client
     */
    public static MongoClient create(final String connectionString,
                                     final InternalMongoClientSettings internalSettings) {
        return create(new ConnectionString(connectionString), internalSettings);
    }

    /**
     * Creates a new client with the given connection string and internal settings.
     *
     * @param connectionString the connection string
     * @param internalSettings the internal settings
     * @return the client
     */
    public static MongoClient create(final ConnectionString connectionString,
                                     final InternalMongoClientSettings internalSettings) {
        return create(connectionString, null, internalSettings);
    }

    /**
     * Creates a new client with the given connection string, driver information and internal settings.
     *
     * @param connectionString       the connection string
     * @param mongoDriverInformation any driver information to associate with the MongoClient
     * @param internalSettings       the internal settings
     * @return the client
     */
    public static MongoClient create(final ConnectionString connectionString,
                                     @Nullable final MongoDriverInformation mongoDriverInformation,
                                     final InternalMongoClientSettings internalSettings) {
        return create(MongoClientSettings.builder().applyConnectionString(connectionString).build(),
                mongoDriverInformation, internalSettings);
    }

    /**
     * Creates a new client with the given client settings and internal settings.
     *
     * @param settings         the public settings
     * @param internalSettings the internal settings
     * @return the client
     */
    public static MongoClient create(final MongoClientSettings settings,
                                     final InternalMongoClientSettings internalSettings) {
        return create(settings, null, internalSettings);
    }

    /**
     * Creates a new client with the given client settings, driver information and internal settings.
     *
     * @param settings               the public settings
     * @param mongoDriverInformation any driver information to associate with the MongoClient
     * @param internalSettings       the internal settings
     * @return the client
     */
    public static MongoClient create(final MongoClientSettings settings,
                                     @Nullable final MongoDriverInformation mongoDriverInformation,
                                     final InternalMongoClientSettings internalSettings) {
        notNull("settings", settings);
        notNull("internalSettings", internalSettings);

        MongoDriverInformation.Builder builder = mongoDriverInformation == null ? MongoDriverInformation.builder()
                : MongoDriverInformation.builder(mongoDriverInformation);

        MongoDriverInformation driverInfo = builder.driverName("sync").build();

        StreamFactoryFactory syncStreamFactoryFactory = getSyncStreamFactoryFactory(
                settings.getTransportSettings(),
                getInetAddressResolver(settings));

        Cluster cluster = Clusters.createCluster(
                settings,
                driverInfo,
                syncStreamFactoryFactory,
                internalSettings);

        return new MongoClientImpl(cluster, settings, driverInfo, syncStreamFactoryFactory);
    }
}

