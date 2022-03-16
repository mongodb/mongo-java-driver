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

import javax.net.ssl.SSLContext;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Collections.unmodifiableMap;

/**
 * The client-side settings for data key creation and explicit encryption.
 *
 * <p>
 * Explicit encryption/decryption is a community feature, enabled with the new {@code com.mongodb.client.vault.ClientEncryption} type,
 * for which this is the settings.
 * </p>
 *
 * @since 3.11
 */
public final class ClientEncryptionSettings {
    private final MongoClientSettings keyVaultMongoClientSettings;
    private final String keyVaultNamespace;
    private final Map<String, Map<String, Object>> kmsProviders;
    private final Map<String, Supplier<Map<String, Object>>> kmsProviderSupplierMap;
    private final Map<String, SSLContext> kmsProviderSslContextMap;
    /**
     * A builder for {@code ClientEncryptionSettings} so that {@code ClientEncryptionSettings} can be immutable, and to support easier
     * construction through chaining.
     */
    @NotThreadSafe
    public static final class Builder {
        private MongoClientSettings keyVaultMongoClientSettings;
        private String keyVaultNamespace;
        private Map<String, Map<String, Object>> kmsProviders;
        private Map<String, Supplier<Map<String, Object>>> kmsProviderSupplierMap = new HashMap<>();
        private Map<String, SSLContext> kmsProviderSslContextMap = new HashMap<>();

        /**
         * Sets the key vault settings.
         *
         * @param keyVaultMongoClientSettings the key vault mongo client settings, which may be null.
         * @return this
         * @see #getKeyVaultMongoClientSettings()
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
         * @see #getKeyVaultNamespace()
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
         * @see #getKmsProviders()
         */
        public Builder kmsProviders(final Map<String, Map<String, Object>> kmsProviders) {
            this.kmsProviders = notNull("kmsProviders", kmsProviders);
            return this;
        }

        /**
         * Set the KMS provider to Supplier map
         *
         * @param kmsProviderSupplierMap the KMS provider to Supplier map, which may not be null
         * @return this
         * @see #getKmsProviderSupplierMap() ()
         * @since 4.6
         */
        public Builder kmsProviderSupplierMap(final Map<String, Supplier<Map<String, Object>>> kmsProviderSupplierMap) {
            this.kmsProviderSupplierMap = notNull("kmsProviderSupplierMap", kmsProviderSupplierMap);
            return this;
        }

        /**
         * Sets the KMS provider to SSLContext map
         *
         * @param kmsProviderSslContextMap the KMS provider to SSLContext map, which may not be null
         * @return this
         * @see #getKmsProviderSslContextMap()
         * @since 4.4
         */
        public Builder kmsProviderSslContextMap(final Map<String, SSLContext> kmsProviderSslContextMap) {
            this.kmsProviderSslContextMap = notNull("kmsProviderSslContextMap", kmsProviderSslContextMap);
            return this;
        }

        /**
         * Build an instance of {@code ClientEncryptionSettings}.
         *
         * @return the settings from this builder
         */
        public ClientEncryptionSettings build() {
            return new ClientEncryptionSettings(this);
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
     * <p>
     * The key vault collection is assumed to reside on the same MongoDB cluster as indicated by the connecting URI. But the optional
     * keyVaultMongoClientSettings can be used to route data key queries to a separate MongoDB cluster, or the same cluster but with a
     * different credential.
     * </p>
     * @return the key vault settings, which may be null to indicate that the same {@code MongoClient} should be used to access the key
     * vault collection as is used for the rest of the application.
     */
    public MongoClientSettings getKeyVaultMongoClientSettings() {
        return keyVaultMongoClientSettings;
    }

    /**
     * Gets the key vault namespace.
     * <p>
     * The key vault namespace refers to a collection that contains all data keys used for encryption and decryption (aka the key vault
     * collection). Data keys are stored as documents in a special MongoDB collection. Data keys are protected with encryption by a KMS
     * provider (AWS, Azure, GCP KMS or a local master key).
     * </p>
     *
     * @return the key vault namespace, which may not be null
     */

    public String getKeyVaultNamespace() {
        return keyVaultNamespace;
    }

    /**
     * Gets the map of KMS provider properties.
     *
     * <p>
     * Multiple KMS providers may be specified. The following KMS providers are supported: "aws", "azure", "gcp" and "local". The
     * kmsProviders map values differ by provider:
     * </p>
     * <p>
     * For "aws", the properties are:
     * </p>
     * <ul>
     *     <li>accessKeyId: a String, the AWS access key identifier</li>
     *     <li>secretAccessKey: a String, the AWS secret access key</li>
     *     <li>sessionToken: an optional String, the AWS session token</li>
     * </ul>
     * <p>
     * For "azure", the properties are:
     * </p>
     * <ul>
     *     <li>tenantId: a String, the tenantId that identifies the organization for the account.</li>
     *     <li>clientId: a String, the clientId to authenticate a registered application.</li>
     *     <li>clientSecret: a String, the client secret to authenticate a registered application.</li>
     *     <li>identityPlatformEndpoint: optional String, a host with optional port. e.g. "example.com" or "example.com:443".
     *     Generally used for private Azure instances.</li>
     * </ul>
     * <p>
     * For "gcp", the properties are:
     * </p>
     * <ul>
     *     <li>email: a String, the service account email to authenticate.</li>
     *     <li>privateKey: a String or byte[], the encoded PKCS#8 encrypted key</li>
     *     <li>endpoint: optional String, a host with optional port. e.g. "example.com" or "example.com:443".</li>
     * </ul>
     * <p>
     * For "kmip", the properties are:
     * </p>
     * <ul>
     *     <li>endpoint: a String, the endpoint as a host with required port. e.g. "example.com:443".</li>
     * </ul>
     * <p>
     * For "local", the properties are:
     * </p>
     * <ul>
     *     <li>key: byte[] of length 96, the local key</li>
     * </ul>
     * <p>
     * It is also permitted for the value of a kms provider to be an empty map, in which case the driver will first
     * </p>
     * <ul>
     *  <li>use the {@link Supplier} configured in {@link #getKmsProviderSupplierMap()} to obtain a non-empty map</li>
     *  <li>attempt to obtain credentials from the environment</li>
     * </ul>     *
     * @return map of KMS provider properties
     */
    public Map<String, Map<String, Object>> getKmsProviders() {
        return unmodifiableMap(kmsProviders);
    }

    /**
     * Gets the KMS provider to Supplier map.
     *
     * <p>
     * If the {@link #getKmsProviders()} map contains an empty map as its value, the driver will use a {@link Supplier} configured for
     * the same provider in this map to obtain a non-empty map that contains the credential for the provider.
     * </p>
     *
     * @return the KMS provider to Supplier map
     * @see #getKmsProviders()
     * @since 4.6
     */
    public Map<String, Supplier<Map<String, Object>>> getKmsProviderSupplierMap() {
        return unmodifiableMap(kmsProviderSupplierMap);
    }

    /**
     * Gets the KMS provider to SSLContext map.
     *
     * <p>
     * If a KMS provider is mapped to a non-null {@link SSLContext}, the context will be used to establish a TLS connection to the KMS.
     * Otherwise, the default context will be used.
     * </p>
     *
     * @return the KMS provider to SSLContext map
     * @since 4.4
     */
    public Map<String, SSLContext> getKmsProviderSslContextMap() {
        return unmodifiableMap(kmsProviderSslContextMap);
    }

    private ClientEncryptionSettings(final Builder builder) {
        this.keyVaultMongoClientSettings = builder.keyVaultMongoClientSettings;
        this.keyVaultNamespace = notNull("keyVaultNamespace", builder.keyVaultNamespace);
        this.kmsProviders = notNull("kmsProviders", builder.kmsProviders);
        this.kmsProviderSupplierMap = notNull("kmsProviderSupplierMap", builder.kmsProviderSupplierMap);
        this.kmsProviderSslContextMap = notNull("kmsProviderSslContextMap", builder.kmsProviderSslContextMap);
    }

}
