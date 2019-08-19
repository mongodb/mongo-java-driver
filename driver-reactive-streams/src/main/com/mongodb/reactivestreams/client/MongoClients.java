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

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoDriverInformation;
import com.mongodb.reactivestreams.client.internal.MongoClientImpl;
import com.mongodb.reactivestreams.client.internal.build.MongoDriverVersion;
import org.bson.codecs.configuration.CodecRegistry;


/**
 * A factory for MongoClient instances.
 *
 */
@SuppressWarnings("deprecation")
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
     * @deprecated use {@link MongoClients#create(com.mongodb.MongoClientSettings)} instead
     */
    @Deprecated
    public static MongoClient create(final com.mongodb.async.client.MongoClientSettings settings) {
        return create(settings, null);
    }

    /**
     * Create a new client with the given connection string.
     *
     * @param connectionString the connection
     * @return the client
     */
    public static MongoClient create(final String connectionString) {
        return create(new ConnectionString(connectionString));
    }

    /**
     * Create a new client with the given connection string.
     *
     * @param connectionString the settings
     * @return the client
     */
    public static MongoClient create(final ConnectionString connectionString) {
        return create(connectionString, null);
    }

    /**
     * Create a new client with the given connection string.
     *
     * <p>Note: Intended for driver and library authors to associate extra driver metadata with the connections.</p>
     *
     * @param connectionString the settings
     * @param mongoDriverInformation any driver information to associate with the MongoClient
     * @return the client
     * @since 1.3
     */
    public static MongoClient create(final ConnectionString connectionString, final MongoDriverInformation mongoDriverInformation) {
        return create(com.mongodb.async.client.MongoClients.create(connectionString, getMongoDriverInformation(mongoDriverInformation)));
    }

    /**
     * Creates a new client with the given client settings.
     *
     * <p>Note: Intended for driver and library authors to associate extra driver metadata with the connections.</p>
     *
     * @param settings the settings
     * @param mongoDriverInformation any driver information to associate with the MongoClient
     * @return the client
     * @deprecated use {@link MongoClients#create(com.mongodb.MongoClientSettings)} instead
     */
    @Deprecated
    public static MongoClient create(final com.mongodb.async.client.MongoClientSettings settings,
                                     final MongoDriverInformation mongoDriverInformation) {
        return create(com.mongodb.async.client.MongoClients.create(settings, getMongoDriverInformation(mongoDriverInformation)));
    }

    /**
     * Create a new client with the given client settings.
     *
     * @param settings the settings
     * @return the client
     * @since 1.8
     */
    public static MongoClient create(final MongoClientSettings settings) {
        return create(settings, null);
    }

    /**
     * Creates a new client with the given client settings.
     *
     * <p>Note: Intended for driver and library authors to associate extra driver metadata with the connections.</p>
     *
     * @param settings the settings
     * @param mongoDriverInformation any driver information to associate with the MongoClient
     * @return the client
     * @since 1.8
     */
    public static MongoClient create(final MongoClientSettings settings, final MongoDriverInformation mongoDriverInformation) {
        return create(com.mongodb.async.client.MongoClients.create(settings, getMongoDriverInformation(mongoDriverInformation)));
    }

    /**
     * Creates a new client with the given async MongoClient.
     *
     * <p>Note: This shares the {@code MongoClient} between two APIs. Calling close from either API will close the client.</p>
     *
     * @param asyncMongoClient the async MongoClient
     * @return the client
     * @since 1.4
     * @deprecated Deprecated because {@link com.mongodb.async.client.MongoClient} is deprecated.
     */
    @Deprecated
    public static MongoClient create(final com.mongodb.async.client.MongoClient asyncMongoClient) {
        return new MongoClientImpl(asyncMongoClient);
    }

    /**
     * Gets the default codec registry.
     *
     * @return the default codec registry
     * @see com.mongodb.MongoClientSettings#getCodecRegistry()
     * @since 1.4
     */
    public static CodecRegistry getDefaultCodecRegistry() {
        return com.mongodb.async.client.MongoClients.getDefaultCodecRegistry();
    }

    private static MongoDriverInformation getMongoDriverInformation(final MongoDriverInformation mongoDriverInformation) {
        if (mongoDriverInformation == null) {
            return DEFAULT_DRIVER_INFORMATION;
        } else {
            return MongoDriverInformation.builder(mongoDriverInformation)
                    .driverName(MongoDriverVersion.NAME)
                    .driverVersion(MongoDriverVersion.VERSION).build();
        }
    }

    private static final MongoDriverInformation DEFAULT_DRIVER_INFORMATION = MongoDriverInformation.builder()
            .driverName(MongoDriverVersion.NAME).driverVersion(MongoDriverVersion.VERSION).build();

    private MongoClients() {
    }
}
