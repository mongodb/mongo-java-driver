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

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoDriverInformation;
import com.mongodb.client.internal.Clusters;
import com.mongodb.client.internal.MongoClientImpl;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.connection.StreamFactoryFactory;
import com.mongodb.lang.Nullable;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.connection.ServerAddressHelper.getInetAddressResolver;
import static com.mongodb.internal.connection.StreamFactoryHelper.getSyncStreamFactoryFactory;


/**
 * A factory for {@link MongoClient} instances.  Use of this class is now the recommended way to connect to MongoDB via the Java driver.
 *
 * @see MongoClient
 * @since 3.7
 */
public final class MongoClients {

    /**
     * Creates a new client with the default connection string "mongodb://localhost".
     *
     * @return the client
     */
    public static MongoClient create() {
        return create(new ConnectionString("mongodb://localhost"));
    }

    /**
     * Create a new client with the given client settings.
     *
     * @param settings the settings
     * @return the client
     */
    public static MongoClient create(final MongoClientSettings settings) {
        return create(settings, null);
    }

    /**
     * Create a new client with the given connection string as if by a call to {@link #create(ConnectionString)}.
     *
     * @param connectionString the connection
     * @return the client
     * @see #create(ConnectionString)
     */
    public static MongoClient create(final String connectionString) {
        return create(new ConnectionString(connectionString));
    }

    /**
     * Create a new client with the given connection string.
     * <p>
     * For each of the settings classed configurable via {@link MongoClientSettings}, the connection string is applied by calling the
     * {@code applyConnectionString} method on an instance of setting's builder class, building the setting, and adding it to an instance of
     * {@link com.mongodb.MongoClientSettings.Builder}.
     * </p>
     *
     * @param connectionString the settings
     * @return the client
     *
     * @see com.mongodb.MongoClientSettings.Builder#applyConnectionString(ConnectionString)
     */
    public static MongoClient create(final ConnectionString connectionString) {
        return create(connectionString, null);
    }

    /**
     * Create a new client with the given connection string.
     *
     * <p>Note: Intended for driver and library authors to associate extra driver metadata with the connections.</p>
     *
     * @param connectionString       the settings
     * @param mongoDriverInformation any driver information to associate with the MongoClient
     * @return the client
     * @see MongoClients#create(ConnectionString)
     */
    public static MongoClient create(final ConnectionString connectionString,
                                     @Nullable final MongoDriverInformation mongoDriverInformation) {
        return create(MongoClientSettings.builder().applyConnectionString(connectionString).build(), mongoDriverInformation);
    }

    /**
     * Creates a new client with the given client settings.
     *
     * <p>Note: Intended for driver and library authors to associate extra driver metadata with the connections.</p>
     *
     * @param settings               the settings
     * @param mongoDriverInformation any driver information to associate with the MongoClient
     * @return the client
     */
    public static MongoClient create(final MongoClientSettings settings, @Nullable final MongoDriverInformation mongoDriverInformation) {
        notNull("settings", settings);

        MongoDriverInformation.Builder builder = mongoDriverInformation == null ? MongoDriverInformation.builder()
                : MongoDriverInformation.builder(mongoDriverInformation);

        MongoDriverInformation driverInfo = builder.driverName("sync").build();

        StreamFactoryFactory syncStreamFactoryFactory = getSyncStreamFactoryFactory(
                settings.getTransportSettings(),
                getInetAddressResolver(settings));

        Cluster cluster = Clusters.createCluster(
                settings,
                driverInfo,
                syncStreamFactoryFactory);

        return new MongoClientImpl(cluster, settings, driverInfo, syncStreamFactoryFactory);
    }

    private MongoClients() {
    }
}
