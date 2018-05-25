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
 */
@Immutable
public final class MongoEmbeddedSettings {
    private final String libraryPath;

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

        private Builder() {
        }

        private Builder(final MongoEmbeddedSettings settings) {
            notNull("settings", settings);
            libraryPath = settings.libraryPath;
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

    private MongoEmbeddedSettings(final Builder builder) {
        this.libraryPath = builder.libraryPath;
    }
}
