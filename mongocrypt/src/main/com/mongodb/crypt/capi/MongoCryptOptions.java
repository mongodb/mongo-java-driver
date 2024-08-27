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

import org.bson.BsonDocument;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static org.bson.assertions.Assertions.isTrue;

/**
 * The options for configuring MongoCrypt.
 */
public class MongoCryptOptions {

    private final MongoAwsKmsProviderOptions awsKmsProviderOptions;
    private final MongoLocalKmsProviderOptions localKmsProviderOptions;
    private final BsonDocument kmsProviderOptions;
    private final Map<String, BsonDocument> localSchemaMap;
    private final boolean needsKmsCredentialsStateEnabled;
    private final Map<String, BsonDocument> encryptedFieldsMap;
    private final BsonDocument extraOptions;
    private final boolean bypassQueryAnalysis;
    private final List<String> searchPaths;


    /**
     * Construct a builder for the options
     *
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Gets the AWS KMS provider options.
     *
     * @return the AWS KMS provider options, which may be null
     */
    public MongoAwsKmsProviderOptions getAwsKmsProviderOptions() {
        return awsKmsProviderOptions;
    }

    /**
     * Gets the local KMS provider options.
     *
     * @return the local KMS provider options, which may be null
     */
    public MongoLocalKmsProviderOptions getLocalKmsProviderOptions() {
        return localKmsProviderOptions;
    }

    /**
     * Returns the KMS provider options.
     *
     * @return the KMS provider options, which may be null
     * @since 1.1
     */
    public BsonDocument getKmsProviderOptions() {
        return kmsProviderOptions;
    }

    /**
     * Gets the local schema map.
     *
     * @return the local schema map
     */
    public Map<String, BsonDocument> getLocalSchemaMap() {
        return localSchemaMap;
    }

    /**
     * Gets whether the MONGOCRYPT_CTX_NEED_KMS_CREDENTIALS is enabled.  Defaults to false
     *
     * @return  whether the MONGOCRYPT_CTX_NEED_KMS_CREDENTIALS is enabled
     * @since 1.4
     */
    public boolean isNeedsKmsCredentialsStateEnabled() {
        return needsKmsCredentialsStateEnabled;
    }

    /**
     * Gets the encrypted fields map.
     *
     * @since 1.5
     * @return the encrypted fields map
     */
    public Map<String, BsonDocument> getEncryptedFieldsMap() {
        return encryptedFieldsMap;
    }

    /**
     * Gets whether automatic analysis of outgoing commands should be disabled.
     *
     * @since  1.5
     * @return true if bypassing query analysis
     */
    public boolean isBypassQueryAnalysis() {
        return bypassQueryAnalysis;
    }

    /**
     * The extraOptions that relate to the mongocryptd process or shared library.
     * @return the extra options
     * @since 1.5
     */
    public BsonDocument getExtraOptions() {
        return extraOptions;
    }

    /**
     * Gets the search paths
     * @return this
     * @since 1.5
     */
    public List<String> getSearchPaths() {
        return searchPaths;
    }

    /**
     * The builder for the options
     */
    public static class Builder {
        private MongoAwsKmsProviderOptions awsKmsProviderOptions;
        private MongoLocalKmsProviderOptions localKmsProviderOptions;
        private BsonDocument kmsProviderOptions = null;
        private Map<String, BsonDocument> localSchemaMap = null;
        private boolean needsKmsCredentialsStateEnabled;
        private Map<String, BsonDocument> encryptedFieldsMap  = null;
        private boolean bypassQueryAnalysis;
        private BsonDocument extraOptions = new BsonDocument();
        private List<String> searchPaths = emptyList();

        private Builder() {
        }

        /**
         * Sets the AWS KMS provider options.
         *
         * @param awsKmsProviderOptions the AWS KMS provider options
         * @return this
         */
        public Builder awsKmsProviderOptions(final MongoAwsKmsProviderOptions awsKmsProviderOptions) {
            this.awsKmsProviderOptions = awsKmsProviderOptions;
            return this;
        }

        /**
         * Sets the local KMS provider options.
         *
         * @param localKmsProviderOptions the local KMS provider options
         * @return this
         */
        public Builder localKmsProviderOptions(final MongoLocalKmsProviderOptions localKmsProviderOptions) {
            this.localKmsProviderOptions = localKmsProviderOptions;
            return this;
        }

        /**
         * Sets the KMS provider options.
         *
         * @param kmsProviderOptions the KMS provider options document
         * @return this
         * @since 1.1
         */
        public Builder kmsProviderOptions(final BsonDocument kmsProviderOptions) {
            this.kmsProviderOptions = kmsProviderOptions;
            return this;
        }

        /**
         * Sets the local schema map.
         *
         * @param localSchemaMap local schema map
         * @return this
         */
        public Builder localSchemaMap(final Map<String, BsonDocument> localSchemaMap) {
            this.localSchemaMap = localSchemaMap;
            return this;
        }

        /**
         * Sets whether the MONGOCRYPT_CTX_NEED_KMS_CREDENTIALS is enabled.  Defaults to false
         *
         * @param needsKmsCredentialsStateEnabled whether the MONGOCRYPT_CTX_NEED_KMS_CREDENTIALS is enabled
         * @return this
         * @since 1.4
         */
        public Builder needsKmsCredentialsStateEnabled(final boolean needsKmsCredentialsStateEnabled) {
            this.needsKmsCredentialsStateEnabled = needsKmsCredentialsStateEnabled;
            return this;
        }

        /**
         * Sets the encrypted fields map.
         *
         * @param encryptedFieldsMap the encrypted fields map
         * @since 1.5
         * @return this
         */
        public Builder encryptedFieldsMap(final Map<String, BsonDocument> encryptedFieldsMap) {
            this.encryptedFieldsMap = encryptedFieldsMap;
            return this;
        }

        /**
         * Sets whether automatic analysis of outgoing commands should be disabled.
         *
         * <p>Set bypassQueryAnalysis to true to use explicit encryption on indexed fields
         * without the MongoDB Enterprise Advanced licensed crypt shared library.</p>
         *
         * @param bypassQueryAnalysis whether the analysis of outgoing commands should be disabled.
         * @since  1.5
         * @return  this
         */
        public Builder bypassQueryAnalysis(final boolean bypassQueryAnalysis) {
            this.bypassQueryAnalysis = bypassQueryAnalysis;
            return this;
        }

        /**
         * The extraOptions that relate to the mongocryptd process or shared library.
         * @param extraOptions the extraOptions
         * @return this
         * @since 1.5
         */
        public Builder extraOptions(final BsonDocument extraOptions) {
            this.extraOptions = extraOptions;
            return this;
        }

        /**
         * Sets search paths
         * @param searchPaths sets search path
         * @return this
         * @since 1.5
         */
        public Builder searchPaths(final List<String> searchPaths) {
            this.searchPaths = searchPaths;
            return this;
        }

        /**
         * Build the options.
         *
         * @return the options
         */
        public MongoCryptOptions build() {
            return new MongoCryptOptions(this);
        }
    }

    private MongoCryptOptions(final Builder builder) {
        isTrue("at least one KMS provider is configured",
                builder.awsKmsProviderOptions != null || builder.localKmsProviderOptions != null
                        || builder.kmsProviderOptions != null );
        this.awsKmsProviderOptions = builder.awsKmsProviderOptions;
        this.localKmsProviderOptions = builder.localKmsProviderOptions;
        this.kmsProviderOptions = builder.kmsProviderOptions;
        this.localSchemaMap = builder.localSchemaMap;
        this.needsKmsCredentialsStateEnabled = builder.needsKmsCredentialsStateEnabled;
        this.encryptedFieldsMap = builder.encryptedFieldsMap;
        this.bypassQueryAnalysis = builder.bypassQueryAnalysis;
        this.extraOptions = builder.extraOptions;
        this.searchPaths = builder.searchPaths;
    }
}
