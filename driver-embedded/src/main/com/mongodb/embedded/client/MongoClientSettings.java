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


import com.mongodb.ConnectionString;
import com.mongodb.MongoClientException;
import com.mongodb.annotations.Immutable;
import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.event.CommandListener;
import org.bson.codecs.configuration.CodecRegistry;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;

/**
 * Various settings to control the behavior of an Embedded {@code MongoClient}.
 *
 * @since 3.8
 * @mongodb.server.release 4.0
 */
@Immutable
public final class MongoClientSettings {
    private static final CodecRegistry DEFAULT_CODEC_REGISTRY = com.mongodb.MongoClientSettings.getDefaultCodecRegistry();

    private final com.mongodb.MongoClientSettings wrappedMongoClientSettings;
    private final String dbPath;

    /**
     * Gets the default codec registry.  It includes the following providers:
     *
     * <ul>
     * <li>{@link org.bson.codecs.ValueCodecProvider}</li>
     * <li>{@link org.bson.codecs.BsonValueCodecProvider}</li>
     * <li>{@link com.mongodb.DBRefCodecProvider}</li>
     * <li>{@link com.mongodb.DBObjectCodecProvider}</li>
     * <li>{@link org.bson.codecs.DocumentCodecProvider}</li>
     * <li>{@link org.bson.codecs.IterableCodecProvider}</li>
     * <li>{@link org.bson.codecs.MapCodecProvider}</li>
     * <li>{@link com.mongodb.client.model.geojson.codecs.GeoJsonCodecProvider}</li>
     * <li>{@link com.mongodb.client.gridfs.codecs.GridFSFileCodecProvider}</li>
     * </ul>
     *
     * @return the default codec registry
     */
    public static CodecRegistry getDefaultCodecRegistry() {
        return DEFAULT_CODEC_REGISTRY;
    }

    /**
     * Convenience method to create a Builder.
     *
     * @return a builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Convenience method to create a from an existing {@code MongoClientSettings}.
     *
     * @param settings create a builder from existing settings
     * @return a builder
     */
    public static Builder builder(final MongoClientSettings settings) {
        return new Builder(settings);
    }

    /**
     * A builder for {@code MongoClientSettings} so that {@code MongoClientSettings} can be immutable, and to support easier construction
     * through chaining.
     */
    @NotThreadSafe
    public static final class Builder {
        private com.mongodb.MongoClientSettings.Builder wrappedBuilder;
        private String dbPath;

        private Builder() {
            wrappedBuilder = com.mongodb.MongoClientSettings.builder();
        }

        private Builder(final MongoClientSettings settings) {
            notNull("settings", settings);
            wrappedBuilder = com.mongodb.MongoClientSettings.builder(settings.wrappedMongoClientSettings);
            dbPath = settings.dbPath;
        }

        /**
         * Takes the settings from the given {@code ConnectionString} and applies them to the builder
         *
         * @param connectionString the connection string containing details of how to connect to MongoDB
         * @return this
         */
        public Builder applyConnectionString(final ConnectionString connectionString) {
            wrappedBuilder.applyConnectionString(connectionString);
            if (connectionString.getHosts().size() == 1) {
                try {
                    dbPath = URLDecoder.decode(connectionString.getHosts().get(0), "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    throw new MongoClientException(format("Connection String contains an invalid host: %s",
                            connectionString.getHosts().get(0)));
                }
            } else {
                throw new MongoClientEmbeddedException(format("Connection String contains invalid hosts: %s", connectionString.getHosts()));
            }
            return this;
        }

        /**
         * Sets the codec registry
         *
         * @param codecRegistry the codec registry
         * @return this
         * @see MongoClientSettings#getCodecRegistry()
         */
        public Builder codecRegistry(final CodecRegistry codecRegistry) {
            wrappedBuilder.codecRegistry(codecRegistry);
            return this;
        }

        /**
         * Adds the given command listener.
         *
         * @param commandListener the command listener
         * @return this
         */
        public Builder addCommandListener(final CommandListener commandListener) {
            wrappedBuilder.addCommandListener(commandListener);
            return this;
        }

        /**
         * Sets the the command listeners
         *
         * @param commandListeners the list of command listeners
         * @return this
         */
        public Builder commandListenerList(final List<CommandListener> commandListeners) {
            wrappedBuilder.commandListenerList(commandListeners);
            return this;
        }

        /**
         * Sets the logical name of the application using this MongoClient.  The application name may be used by the client to identify
         * the application to the server, for use in server logs, slow query logs, and profile collection.
         *
         * @param applicationName the logical name of the application using this MongoClient.  It may be null.
         *                        The UTF-8 encoding may not exceed 128 bytes.
         * @return this
         * @see #getApplicationName()
         */
        public Builder applicationName(final String applicationName) {
           wrappedBuilder.applicationName(applicationName);
            return this;
        }

        /**
         * Sets the dbPath for the mongod.
         *
         * @param dbPath the path for the database
         * @return this
         */
        public Builder dbPath(final String dbPath) {
            this.dbPath = notNull("dbPath", dbPath);
            return this;
        }

        /**
         * Build an instance of {@code MongoClientSettings}.
         *
         * @return the settings from this builder
         */
        public MongoClientSettings build() {
            return new MongoClientSettings(this);
        }
    }

    /**
     * Gets the logical name of the application using this MongoClient.  The application name may be used by the client to identify
     * the application to the server, for use in server logs, slow query logs, and profile collection.
     *
     * <p>Default is null.</p>
     *
     * @return the application name, which may be null
     */
    public String getApplicationName() {
        return wrappedMongoClientSettings.getApplicationName();
    }

    /**
     * The codec registry to use, or null if not set.
     *
     * @return the codec registry
     */
    public CodecRegistry getCodecRegistry() {
        return wrappedMongoClientSettings.getCodecRegistry();
    }

    /**
     * Gets the list of added {@code CommandListener}.
     *
     * <p>The default is an empty list.</p>
     *
     * @return the unmodifiable list of command listeners
     */
    public List<CommandListener> getCommandListeners() {
        return wrappedMongoClientSettings.getCommandListeners();
    }

    /**
     * Gets the dbPath for the embedded mongod.
     *
     * @return the dbPath
     */
    public String getDbPath() {
        return dbPath;
    }

    /**
     * Returns the wrapped MongoClientSettings instance
     *
     * @return the wrapped MongoClient Settings instance.
     */
    public com.mongodb.MongoClientSettings getWrappedMongoClientSettings() {
        return wrappedMongoClientSettings;
    }

    private MongoClientSettings(final Builder builder) {
        isTrue("dbPath is set", builder.dbPath != null && !builder.dbPath.isEmpty());
        dbPath = builder.dbPath;
        wrappedMongoClientSettings = builder.wrappedBuilder.build();
    }
}
