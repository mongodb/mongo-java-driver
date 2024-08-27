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

package com.mongodb.crypt.capi;

import org.bson.BsonDocument;

/**
 * The rewrap many data key options
 *
 * <p>
 *     The masterKey document MUST have the fields corresponding to the given provider as specified in masterKey.
 * </p>
 *
 * @since 1.5
 */
public class MongoRewrapManyDataKeyOptions {

    private final String provider;
    private final BsonDocument masterKey;

    /**
     * Options builder
     */
    public static class Builder {
        private String provider;
        private BsonDocument masterKey;

        /**
         * The provider
         *
         * @param provider the provider
         * @return this
         */
        public Builder provider(final String provider) {
            this.provider = provider;
            return this;
        }

        /**
         * Add the master key.
         *
         * @param masterKey the master key
         * @return this
         */
        public Builder masterKey(final BsonDocument masterKey) {
            this.masterKey = masterKey;
            return this;
        }

        /**
         * Build the options.
         *
         * @return the options
         */
        public MongoRewrapManyDataKeyOptions build() {
            return new MongoRewrapManyDataKeyOptions(this);
        }
    }

    /**
     * Create a builder for the options.
     *
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * @return the provider name
     */
    public String getProvider() {
        return provider;
    }

    /**
     * Gets the master key for the data key.
     *
     * @return the master key
     */
    public BsonDocument getMasterKey() {
        return masterKey;
    }

    private MongoRewrapManyDataKeyOptions(final Builder builder) {
        provider = builder.provider;
        masterKey = builder.masterKey;
    }
}

