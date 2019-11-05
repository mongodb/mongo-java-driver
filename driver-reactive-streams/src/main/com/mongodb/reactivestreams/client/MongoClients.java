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
import org.bson.codecs.configuration.CodecRegistry;


/**
 * A factory for MongoClient instances.
 *
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
        return create(com.mongodb.internal.async.client.MongoClients.create(connectionString,
                wrapMongoDriverInformation(mongoDriverInformation)));
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
        return create(com.mongodb.internal.async.client.MongoClients.create(settings, wrapMongoDriverInformation(mongoDriverInformation)));
    }

    /**
     * Gets the default codec registry.
     *
     * @return the default codec registry
     * @see com.mongodb.MongoClientSettings#getCodecRegistry()
     * @since 1.4
     */
    public static CodecRegistry getDefaultCodecRegistry() {
        return com.mongodb.internal.async.client.MongoClients.getDefaultCodecRegistry();
    }

    private static MongoClient create(final com.mongodb.internal.async.client.MongoClient asyncMongoClient) {
        return new MongoClientImpl(asyncMongoClient);
    }

    private static MongoDriverInformation wrapMongoDriverInformation(final MongoDriverInformation mongoDriverInformation) {
        return (mongoDriverInformation == null ? MongoDriverInformation.builder() : MongoDriverInformation.builder(mongoDriverInformation))
                .driverName("reactive-streams").build();
    }

    private MongoClients() {
    }
}
