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
 *
 */

package com.mongodb.crypt.capi;

import org.bson.BsonDocument;

import java.util.List;

/**
 * The options for creation of a data key
 */
public class MongoDataKeyOptions {
    private final List<String> keyAltNames;
    private final BsonDocument masterKey;
    private final byte[] keyMaterial;

    /**
     * Options builder
     */
    public static class Builder {
        private List<String> keyAltNames;
        private BsonDocument masterKey;
        private byte[] keyMaterial;

        /**
         * Add alternate key names
         * @param keyAltNames the alternate key names
         * @return this
         */
        public Builder keyAltNames(final List<String> keyAltNames) {
            this.keyAltNames = keyAltNames;
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
         * Add the key material
         *
         * @param keyMaterial the optional custom key material for the data key
         * @return this
         * @since 1.5
         */
        public Builder keyMaterial(final byte[] keyMaterial) {
            this.keyMaterial = keyMaterial;
            return this;
        }

        /**
         * Build the options.
         *
         * @return the options
         */
        public MongoDataKeyOptions build() {
            return new MongoDataKeyOptions(this);
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
     * Gets the alternate key names for the data key.
     *
     * @return the alternate key names
     */
    public List<String> getKeyAltNames() {
        return keyAltNames;
    }

    /**
     * Gets the master key for the data key.
     *
     * @return the master key
     */
    public BsonDocument getMasterKey() {
        return masterKey;
    }

    /**
     * Gets the custom key material if set.
     *
     * @return the custom key material for the data key or null
     * @since 1.5
     */
    public byte[] getKeyMaterial() {
        return keyMaterial;
    }

    private MongoDataKeyOptions(final Builder builder) {
        keyAltNames = builder.keyAltNames;
        masterKey = builder.masterKey;
        keyMaterial = builder.keyMaterial;
    }
}
