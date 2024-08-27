/*
 * Copyright 2019-present MongoDB, Inc.
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

import java.nio.ByteBuffer;

import static org.bson.assertions.Assertions.notNull;

/**
 * The options for configuring a local KMS provider.
 */
public class MongoLocalKmsProviderOptions {

    private final ByteBuffer localMasterKey;

    /**
     * Construct a builder for the options
     *
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Gets the local master key
     *
     * @return the local master key
     */
    public ByteBuffer getLocalMasterKey() {
        return localMasterKey;
    }

    /**
     * The builder for the options
     */
    public static class Builder {
        private ByteBuffer localMasterKey;

        private Builder() {
        }

        /**
         * Sets the local master key.
         *
         * @param localMasterKey the local master key
         * @return this
         */
        public Builder localMasterKey(final ByteBuffer localMasterKey) {
            this.localMasterKey = localMasterKey;
            return this;
        }

        /**
         * Build the options.
         *
         * @return the options
         */
        public MongoLocalKmsProviderOptions build() {
            return new MongoLocalKmsProviderOptions(this);
        }
    }

    private MongoLocalKmsProviderOptions(final Builder builder) {
        this.localMasterKey = notNull("Local KMS provider localMasterKey", builder.localMasterKey);

    }
}
