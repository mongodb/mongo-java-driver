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

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoDriverInformation;
import com.mongodb.client.MongoClient;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.InternalMongoClientSettings;
import com.mongodb.internal.connection.StreamFactoryFactory;
import com.mongodb.internal.thread.AsyncClientExecutor;
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
     * Creates a new client with the given client settings, driver information and the default internal settings.
     *
     * @param settings               the public settings
     * @param mongoDriverInformation any driver information to associate with the MongoClient
     * @return the client
     */
    public static MongoClient create(final MongoClientSettings settings,
                                     @Nullable final MongoDriverInformation mongoDriverInformation) {
        return create(settings, mongoDriverInformation, InternalMongoClientSettings.DEFAULT);
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

        AsyncClientExecutor clientExecutor = AsyncClientExecutor.NO_OP;
        Cluster cluster = Clusters.createCluster(
                settings,
                driverInfo,
                syncStreamFactoryFactory,
                clientExecutor,
                internalSettings);

        return new MongoClientImpl(cluster, driverInfo, settings, syncStreamFactoryFactory, clientExecutor);
    }
}

