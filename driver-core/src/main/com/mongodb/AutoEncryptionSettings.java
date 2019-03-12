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

package com.mongodb;

import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;

import java.util.Collections;
import java.util.Map;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * The auto-encryption options.
 *
 * @since 3.11
 */
public final class AutoEncryptionSettings {
    private final MongoClientSettings keyVaultMongoClientSettings;
    private final String keyVaultNamespace;
    private final Map<String, Map<String, Object>> kmsProviders;
    private final Map<String, BsonDocument> namespaceToLocalSchemaDocumentMap;
    private final Map<String, Object> extraOptions;
    private final boolean bypassAutoEncryption;

    /**
     * A builder for {@code AutoEncryptionSettings} so that {@code AutoEncryptionSettings} can be immutable, and to support easier
     * construction through chaining.
     */
    @NotThreadSafe
    public static final class Builder {
        private MongoClientSettings keyVaultMongoClientSettings;
        private String keyVaultNamespace;
        private Map<String, Map<String, Object>> kmsProviders;
        private Map<String, BsonDocument> namespaceToLocalSchemaDocumentMap = Collections.emptyMap();
        private Map<String, Object> extraOptions = Collections.emptyMap();
        private boolean bypassAutoEncryption;

        /**
         * Sets the key vault settings.
         *
         * @param keyVaultMongoClientSettings the key vault mongo client settings, which may be null.
         * @return this
         */
        public Builder keyVaultMongoClientSettings(final MongoClientSettings keyVaultMongoClientSettings) {
            this.keyVaultMongoClientSettings = keyVaultMongoClientSettings;
            return this;
        }

        /**
         * Sets the key vault namespace
         *
         * @param keyVaultNamespace the key vault namespace, which may not be null
         * @return this
         */
        public Builder keyVaultNamespace(final String keyVaultNamespace) {
            this.keyVaultNamespace = notNull("keyVaultNamespace", keyVaultNamespace);
            return this;
        }

        /**
         * Sets the KMS providers map.
         *
         * @param kmsProviders the KMS providers map, which may not be null
         * @return this
         */
        public Builder kmsProviders(final Map<String, Map<String, Object>> kmsProviders) {
            this.kmsProviders = notNull("kmsProviders", kmsProviders);
            return this;
        }

        /**
         * Sets the map from namespace to local schema document
         *
         * @param namespaceToLocalSchemaDocumentMap the map from namespace to local schema document
         * @return this
         */
        public Builder namespaceToLocalSchemaDocumentMap(final Map<String, BsonDocument> namespaceToLocalSchemaDocumentMap) {
            this.namespaceToLocalSchemaDocumentMap = notNull("namespaceToLocalSchemaDocumentMap", namespaceToLocalSchemaDocumentMap);
            return this;
        }

        /**
         * Sets the extra options.
         *
         * @param extraOptions the extra options, which may not be null
         * @return this
         */
        public Builder extraOptions(final Map<String, Object> extraOptions) {
            this.extraOptions = notNull("extraOptions", extraOptions);
            return this;
        }

        /**
         * Sets whether auto-encryption should be bypassed.
         *
         * @param bypassAutoEncryption whether auto-encryption should be bypassed
         * @return this
         */
        public Builder bypassAutoEncryption(final boolean bypassAutoEncryption) {
            this.bypassAutoEncryption = bypassAutoEncryption;
            return this;
        }

        /**
         * Build an instance of {@code AutoEncryptionSettings}.
         *
         * @return the settings from this builder
         */
        public AutoEncryptionSettings build() {
            return new AutoEncryptionSettings(this);
        }

        private Builder() {
        }
    }

    /**
     * Convenience method to create a Builder.
     *
     * @return a builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Gets the key vault settings.
     *
     * @return the key vault settings, which may be null to indicate that the same {@code MongoClient} should be used to access the key
     * vault collection as is used for the rest of the application.
     */
    @Nullable
    public MongoClientSettings getKeyVaultMongoClientSettings() {
        return keyVaultMongoClientSettings;
    }

    /**
     * Gets the key vault namespace.
     *
     * @return the key vault namespace, which may not be null
     */
    public String getKeyVaultNamespace() {
        return keyVaultNamespace;
    }

    /**
     * Gets the map of KMS provider properties
     *
     * @return map of KMS provider properties
     */
    public Map<String, Map<String, Object>> getKmsProviders() {
        return kmsProviders;
    }

    /**
     * Gets the map of namespace to local JSON schema
     *
     * @return map of namespace to local JSON schema
     */
    public Map<String, BsonDocument> getNamespaceToLocalSchemaDocumentMap() {
        return namespaceToLocalSchemaDocumentMap;
    }

    /**
     * Gets the extra options that control the behavior of auto-encryption components.
     *
     * @return the extra options
     */
    public Map<String, Object> getExtraOptions() {
        return extraOptions;
    }

    /**
     * Gets whether auto-encryption should be bypassed.  Even when this option is true, auto-decryption is still enabled.
     *
     * @return true if auto-encryption should be bypassed
     */
    public boolean isBypassAutoEncryption() {
        return bypassAutoEncryption;
    }

    private AutoEncryptionSettings(final Builder builder) {
        this.keyVaultMongoClientSettings = builder.keyVaultMongoClientSettings;
        this.keyVaultNamespace = notNull("keyVaultNamespace", builder.keyVaultNamespace);
        this.kmsProviders = notNull("kmsProviders", builder.kmsProviders);
        this.namespaceToLocalSchemaDocumentMap = notNull("namespaceToLocalSchemaDocumentMap", builder.namespaceToLocalSchemaDocumentMap);
        this.extraOptions = notNull("extraOptions", builder.extraOptions);
        this.bypassAutoEncryption = builder.bypassAutoEncryption;
    }
}
