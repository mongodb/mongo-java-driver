/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.client.options;

import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.annotations.Immutable;
import org.bson.codecs.configuration.CodecRegistry;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Various settings to control the behavior of Operations
 *
 * @since 3.0
 */
@Immutable
public class OperationOptions {
    private final WriteConcern writeConcern;
    private final ReadPreference readPreference;
    private final CodecRegistry codecRegistry;

    /**
     * Create a new OperationOptions builder.
     *
     * @return a new OperationOptions builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Merges MongoClientOptions to the current OperationOptions
     *
     * <p>If any options haven't been set then the default MongoClientOptions will be used.</p>
     *
     * @param defaultOptions the MongoClientOptions to default to
     * @return a new MongoCollectionOptions with the merged in default options
     */
    public OperationOptions withDefaults(final OperationOptions defaultOptions) {
        Builder builder = new Builder();
        builder.writeConcern(getWriteConcern() != null ? getWriteConcern() : defaultOptions.getWriteConcern());
        builder.readPreference(getReadPreference() != null ? getReadPreference() : defaultOptions.getReadPreference());
        builder.codecRegistry(getCodecRegistry() != null ? getCodecRegistry() : defaultOptions.getCodecRegistry());
        return builder.build();
    }

    /**
     * Gets the write concern to use.
     *
     * @return {@code WriteConcern} to be used for write operations
     */
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    /**
     * Gets the read preference to use.
     *
     * @return {@code ReadPreference} to be used for read operations.
     */
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    /**
     * Gets the codec registry to use.
     *
     * @return {@code CodecRegistry} the codec registry
     */
    public CodecRegistry getCodecRegistry() {
        return codecRegistry;
    }

    /**
     * A builder for OperationOptions.
     *
     * <p>Note: as OperationOptions are immutable, the builder helpers support easier construction through chaining.</p>
     */
    public static class Builder {
        private WriteConcern writeConcern;
        private ReadPreference readPreference;
        private CodecRegistry codecRegistry;

        /**
         * Gets the write concern to use.
         *
         * @return {@code WriteConcern} to be used for write operations
         */
        public WriteConcern getWriteConcern() {
            return writeConcern;
        }

        /**
         * Gets the read preference to use.
         *
         * @return {@code ReadPreference} to be used for read operations.
         */
        public ReadPreference getReadPreference() {
            return readPreference;
        }

        /**
         * Gets the codec registry to use.
         *
         * @return {@code CodecRegistry} the codec registry
         */
        public CodecRegistry getCodecRegistry() {
            return codecRegistry;
        }

        /**
         * Sets the write concern to use.
         *
         * @param writeConcern the {@code WriteConcern} to be used for write operations
         * @return this
         */
        public Builder writeConcern(final WriteConcern writeConcern) {
            this.writeConcern = notNull("writeConcern", writeConcern);
            return this;
        }

        /**
         * Sets the read preference to use.
         *
         * @param readPreference the {@code ReadPreference} to be used for read operations.
         * @return this
         */
        public Builder readPreference(final ReadPreference readPreference) {
            this.readPreference = notNull("readPreference", readPreference);
            return this;
        }

        /**
         * Sets the codec registry to use.
         *
         * @param codecRegistry the {@code CodecRegistry} the codec registry to be used with the Operation
         * @return this
         */
        public Builder codecRegistry(final CodecRegistry codecRegistry) {
            this.codecRegistry = notNull("codecRegistry", codecRegistry);
            return this;
        }

        /**
         * Builds an instance of OperationOptions.
         *
         * @return the options from this builder
         */
        public OperationOptions build() {
            return new OperationOptions(writeConcern, readPreference, codecRegistry);
        }

        Builder() {
        }
    }

    OperationOptions(final WriteConcern writeConcern,
                         final ReadPreference readPreference,
                         final CodecRegistry codecRegistry) {

        this.writeConcern = writeConcern;
        this.readPreference = readPreference;
        this.codecRegistry = codecRegistry;
    }
}
