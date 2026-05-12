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
import com.mongodb.client.internal.StreamProcessingClientImpl;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.connection.StreamFactoryFactory;
import com.mongodb.lang.Nullable;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.connection.ServerAddressHelper.getInetAddressResolver;
import static com.mongodb.internal.connection.StreamFactoryHelper.getSyncStreamFactoryFactory;

/**
 * A factory for {@link StreamProcessingClient} instances.
 *
 * <p>The connection string must target an Atlas Stream Processing workspace — a host matching the pattern
 * {@code atlas-stream-*.*a.query.mongodb*.net}. TLS, {@code loadBalanced=true}, and
 * {@code authSource=admin} are applied automatically when the workspace host is detected.</p>
 *
 * @see StreamProcessingClient
 * @since 5.5
 */
public final class StreamProcessingClients {

    /**
     * Creates a new client with the given connection string.
     *
     * @param connectionString the connection string targeting a stream processing workspace
     * @return the client
     * @throws IllegalArgumentException if the connection string does not target a stream processing workspace
     */
    public static StreamProcessingClient create(final String connectionString) {
        return create(new ConnectionString(connectionString));
    }

    /**
     * Creates a new client with the given connection string.
     *
     * @param connectionString the connection string targeting a stream processing workspace
     * @return the client
     * @throws IllegalArgumentException if the connection string does not target a stream processing workspace
     */
    public static StreamProcessingClient create(final ConnectionString connectionString) {
        return create(connectionString, null);
    }

    /**
     * Creates a new client with the given connection string.
     *
     * <p>Note: Intended for driver and library authors to associate extra driver metadata with the connections.</p>
     *
     * @param connectionString       the connection string targeting a stream processing workspace
     * @param mongoDriverInformation any driver information to associate with the client
     * @return the client
     * @throws IllegalArgumentException if the connection string does not target a stream processing workspace
     */
    public static StreamProcessingClient create(final ConnectionString connectionString,
                                                @Nullable final MongoDriverInformation mongoDriverInformation) {
        notNull("connectionString", connectionString);
        if (!connectionString.isAtlasStreamProcessingWorkspace()) {
            throw new IllegalArgumentException(
                    "The connection string does not target an Atlas Stream Processing workspace. "
                    + "Workspace hosts must match the pattern: atlas-stream-*.*a.query.mongodb*.net");
        }
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .build();
        return create(settings, mongoDriverInformation);
    }

    /**
     * Creates a new client with the given settings.
     *
     * @param settings the settings
     * @return the client
     */
    public static StreamProcessingClient create(final MongoClientSettings settings) {
        return create(settings, null);
    }

    /**
     * Creates a new client with the given settings.
     *
     * <p>Note: Intended for driver and library authors to associate extra driver metadata with the connections.</p>
     *
     * @param settings               the settings
     * @param mongoDriverInformation any driver information to associate with the client
     * @return the client
     */
    public static StreamProcessingClient create(final MongoClientSettings settings,
                                                @Nullable final MongoDriverInformation mongoDriverInformation) {
        notNull("settings", settings);

        MongoDriverInformation.Builder builder = mongoDriverInformation == null
                ? MongoDriverInformation.builder()
                : MongoDriverInformation.builder(mongoDriverInformation);
        MongoDriverInformation driverInfo = builder.driverName("stream-processing").build();

        StreamFactoryFactory streamFactoryFactory = getSyncStreamFactoryFactory(
                settings.getTransportSettings(),
                getInetAddressResolver(settings));

        Cluster cluster = Clusters.createCluster(settings, driverInfo, streamFactoryFactory);
        return new StreamProcessingClientImpl(cluster, settings, driverInfo, streamFactoryFactory);
    }

    private StreamProcessingClients() {
    }
}
