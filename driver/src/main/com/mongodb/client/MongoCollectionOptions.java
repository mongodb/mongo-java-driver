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

package com.mongodb.client;

import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.annotations.Immutable;
import org.bson.codecs.configuration.CodecRegistry;

/**
 * Various settings to control the behavior of a {@code MongoCollection}.
 *
 * @since 3.0
 */
@Immutable
public final class MongoCollectionOptions extends MongoDatabaseOptions {

    /**
     * Create a new MongoCollectionOptions builder.
     *
     * @return a new MongoCollectionOptions builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Merges MongoDatabaseOptions to the current MongoCollectionOptions
     *
     * <p>If any options haven't been set then the default MongoDatabaseOptions will be used.</p>
     *
     * @param defaultOptions the MongoDatabaseOptions to default to
     * @return a new MongoCollectionOptions with the merged in default options
     */
    public MongoCollectionOptions withDefaults(final MongoDatabaseOptions defaultOptions) {
        Builder builder = new Builder();
        builder.writeConcern(getWriteConcern() != null ? getWriteConcern() : defaultOptions.getWriteConcern());
        builder.readPreference(getReadPreference() != null ? getReadPreference() : defaultOptions.getReadPreference());
        builder.codecRegistry(getCodecRegistry() != null ? getCodecRegistry() : defaultOptions.getCodecRegistry());
        return builder.build();
    }

    /**
     * A builder for MongoCollectionOptions.
     *
     * <p>Note: as MongoCollectionOptions are immutable, the builder helpers support easier construction through chaining.</p>
     */
    public static final class Builder extends MongoDatabaseOptions.Builder {

        /**
         * Build an instance of MongoCollectionOptions.
         *
         * @return the options from this builder
         */
        public MongoCollectionOptions build() {
            return new MongoCollectionOptions(getWriteConcern(), getReadPreference(), getCodecRegistry());
        }

        @Override
        public Builder writeConcern(final WriteConcern writeConcern) {
            super.writeConcern(writeConcern);
            return this;
        }

        @Override
        public Builder readPreference(final ReadPreference readPreference) {
            super.readPreference(readPreference);
            return this;
        }

        @Override
        public Builder codecRegistry(final CodecRegistry codecRegistry) {
            super.codecRegistry(codecRegistry);
            return this;
        }

        private Builder() {
        }
    }

    private MongoCollectionOptions(final WriteConcern writeConcern, final ReadPreference readPreference,
                                   final CodecRegistry codecRegistry) {
        super(writeConcern, readPreference, codecRegistry);
    }
}
