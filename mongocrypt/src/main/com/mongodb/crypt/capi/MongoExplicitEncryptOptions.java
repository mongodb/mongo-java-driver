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

import org.bson.BsonBinary;
import org.bson.BsonDocument;

import java.util.Objects;

/**
 * Options for explicit encryption.
 */
public class MongoExplicitEncryptOptions {
    private final BsonBinary keyId;
    private final String keyAltName;
    private final String algorithm;
    private final Long contentionFactor;
    private final String queryType;
    private final BsonDocument rangeOptions;

    /**
     * The builder for the options
     */
    public static class Builder {
        private BsonBinary keyId;
        private String keyAltName;
        private String algorithm;
        private Long contentionFactor;
        private String queryType;
        private BsonDocument rangeOptions;

        private Builder() {
        }

        /**
         * Add the key identifier.
         *
         * @param keyId the key idenfifier
         * @return this
         */
        public Builder keyId(final BsonBinary keyId) {
            this.keyId = keyId;
            return this;
        }

        /**
         * Add the key alternative name.
         *
         * @param keyAltName the key alternative name
         * @return this
         */
        public Builder keyAltName(final String keyAltName) {
            this.keyAltName = keyAltName;
            return this;
        }

        /**
         * Add the encryption algorithm.
         *
         * <p>To insert or query with an "Indexed" encrypted payload, use a MongoClient configured with {@code AutoEncryptionSettings}.
         * {@code AutoEncryptionSettings.bypassQueryAnalysis} may be true.
         * {@code AutoEncryptionSettings.bypassAutoEncryption must be false}.</p>
         *
         * @param algorithm the encryption algorithm
         * @return this
         */
        public Builder algorithm(final String algorithm) {
            this.algorithm = algorithm;
            return this;
        }

        /**
         * The contention factor.
         *
         * <p>It is an error to set contentionFactor when algorithm is not "Indexed".
         * @param contentionFactor the contention factor
         * @return this
         * @since 1.5
         */
        public Builder contentionFactor(final Long contentionFactor) {
            this.contentionFactor = contentionFactor;
            return this;
        }

        /**
         * The QueryType.
         *
         * <p>It is an error to set queryType when algorithm is not "Indexed".</p>
         *
         * @param queryType the query type
         * @return this
         * @since 1.5
         */
        public Builder queryType(final String queryType) {
            this.queryType = queryType;
            return this;
        }

        /**
         * The Range Options.
         *
         * <p>It is an error to set rangeOptions when the algorithm is not "range".</p>
         *
         * @param rangeOptions the range options
         * @return this
         * @since 1.7
         */
        public Builder rangeOptions(final BsonDocument rangeOptions) {
            this.rangeOptions = rangeOptions;
            return this;
        }

        /**
         * Build the options.
         *
         * @return the options
         */
        public MongoExplicitEncryptOptions build() {
            return new MongoExplicitEncryptOptions(this);
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
     * Gets the key identifier
     * @return the key identifier
     */
    public BsonBinary getKeyId() {
        return keyId;
    }

    /**
     * Gets the key alternative name
     * @return the key alternative name
     */
    public String getKeyAltName() {
        return keyAltName;
    }

    /**
     * Gets the encryption algorithm
     * @return the encryption algorithm
     */
    public String getAlgorithm() {
        return algorithm;
    }

    /**
     * Gets the contention factor
     * @return the contention factor
     * @since 1.5
     */
    public Long getContentionFactor() {
        return contentionFactor;
    }

    /**
     * Gets the query type
     * @return the query type
     * @since 1.5
     */
    public String getQueryType() {
        return queryType;
    }

    /**
     * Gets the range options
     * @return the range options
     * @since 1.7
     */
    public BsonDocument getRangeOptions() {
        return rangeOptions;
    }

    private MongoExplicitEncryptOptions(Builder builder) {
        this.keyId = builder.keyId;
        this.keyAltName = builder.keyAltName;
        this.algorithm = builder.algorithm;
        this.contentionFactor = builder.contentionFactor;
        this.queryType = builder.queryType;
        this.rangeOptions = builder.rangeOptions;
        if (!(Objects.equals(algorithm, "Indexed") || Objects.equals(algorithm, "Range"))) {
            if (contentionFactor != null) {
                throw new IllegalStateException(
                        "Invalid configuration, contentionFactor can only be set if algorithm is 'Indexed' or 'Range'");
            } else if (queryType != null) {
                throw new IllegalStateException(
                        "Invalid configuration, queryType can only be set if algorithm is 'Indexed' or 'Range'");
            }
        }
    }

    @Override
    public String toString() {
        return "MongoExplicitEncryptOptions{" +
                "keyId=" + keyId +
                ", keyAltName='" + keyAltName + '\'' +
                ", algorithm='" + algorithm + '\'' +
                ", contentionFactor=" + contentionFactor +
                ", queryType='" + queryType + '\'' +
                ", rangeOptions=" + rangeOptions +
                '}';
    }
}
