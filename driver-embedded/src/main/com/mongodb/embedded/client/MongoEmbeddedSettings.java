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


import com.mongodb.annotations.Immutable;
import com.mongodb.annotations.NotThreadSafe;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Various settings to configure the underlying mongod library
 *
 * @since 3.8
 * @deprecated the embedded driver will be removed in the next major release
 */
@Deprecated
@Immutable
public final class MongoEmbeddedSettings {
    private final String libraryPath;
    private final String yamlConfig;
    private final MongoEmbeddedLogLevel logLevel;

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
    public static Builder builder(final MongoEmbeddedSettings settings) {
        return new Builder(settings);
    }

    /**
     * A builder for {@code MongoEmbeddedSettings} so that {@code MongoEmbeddedSettings} can be immutable, and to support easier
     * construction through chaining.
     */
    @NotThreadSafe
    public static final class Builder {
        private String libraryPath;
        private String yamlConfig;
        private MongoEmbeddedLogLevel logLevel = MongoEmbeddedLogLevel.LOGGER;

        private Builder() {
        }

        private Builder(final MongoEmbeddedSettings settings) {
            notNull("settings", settings);
            libraryPath = settings.libraryPath;
            yamlConfig = settings.yamlConfig;
            logLevel = settings.logLevel;
        }

        /**
         * Sets the library path for mongod.
         *
         * @param libraryPath the library path for mongod
         * @return this
         */
        public Builder libraryPath(final String libraryPath) {
            this.libraryPath = libraryPath;
            return this;
        }

        /**
         * Sets the yaml configuration for mongod.
         *
         * @param yamlConfig the yaml configuration for mongod
         * @return this
         * @mongodb.driver.manual reference/configuration-options/
         */
        public Builder yamlConfig(final String yamlConfig) {
            this.yamlConfig = yamlConfig;
            return this;
        }

        /**
         * Sets the logging level for the mongod.
         *
         * @param logLevel the library path for mongod
         * @return this
         */
        public Builder logLevel(final MongoEmbeddedLogLevel logLevel) {
            this.logLevel = logLevel;
            return this;
        }

        /**
         * Build an instance of {@code MongoClientSettings}.
         *
         * @return the settings from this builder
         */
        public MongoEmbeddedSettings build() {
            return new MongoEmbeddedSettings(this);
        }
    }

    /**
     * Gets the library path for the embedded mongod
     *
     * @return the library path if set or null
     */
    public String getLibraryPath() {
        return libraryPath;
    }

    /**
     * Returns the yaml configuration for mongod.
     *
     * @return the yaml configuration for mongod
     * @mongodb.driver.manual reference/configuration-options/
     */
    public String getYamlConfig() {
        return yamlConfig;
    }

    /**
     * Gets the logging level for the embedded mongod
     *
     * @return the logging level
     */
    public MongoEmbeddedLogLevel getLogLevel() {
        return logLevel;
    }

    private MongoEmbeddedSettings(final Builder builder) {
        this.libraryPath = builder.libraryPath;
        this.yamlConfig = builder.yamlConfig;
        this.logLevel = builder.logLevel;
    }
}
