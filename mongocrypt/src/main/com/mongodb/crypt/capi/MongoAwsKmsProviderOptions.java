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

import static org.bson.assertions.Assertions.notNull;

/**
 * The options for configuring the AWS KMS provider.
 */
public class MongoAwsKmsProviderOptions {

    private final String accessKeyId;
    private final String secretAccessKey;

    /**
     * Construct a builder for the options
     *
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Gets the access key id
     *
     * @return the access key id, which may not be null
     */
    public String getAccessKeyId() {
        return accessKeyId;
    }

    /**
     * Gets the secret access key
     *
     * @return the secret access key, which may not be null
     */
    public String getSecretAccessKey() {
        return secretAccessKey;
    }


    /**
     * The builder for the options
     */
    public static class Builder {
        private String accessKeyId;
        private String secretAccessKey;

        private Builder() {
        }

        /**
         * Sets the access key id.
         *
         * @param accessKeyId the access key id
         * @return this
         */
        public Builder accessKeyId(final String accessKeyId) {
            this.accessKeyId = accessKeyId;
            return this;
        }

        /**
         * Sets the secret access key.
         *
         * @param secretAccessKey the secret access key
         * @return this
         */
        public Builder secretAccessKey(final String secretAccessKey) {
            this.secretAccessKey = secretAccessKey;
            return this;
        }

        /**
         * Build the options.
         *
         * @return the options
         */
        public MongoAwsKmsProviderOptions build() {
            return new MongoAwsKmsProviderOptions(this);
        }
    }

    private MongoAwsKmsProviderOptions(final Builder builder) {
        this.accessKeyId = notNull("AWS KMS provider accessKeyId", builder.accessKeyId);
        this.secretAccessKey = notNull("AWS KMS provider secretAccessKey", builder.secretAccessKey);
    }
}
