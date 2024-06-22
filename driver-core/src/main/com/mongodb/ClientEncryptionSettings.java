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
    private final Map<String, Supplier<Map<String, Object>>> kmsProviderPropertySuppliers;
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
        private Map<String, Supplier<Map<String, Object>>> kmsProviderPropertySuppliers = new HashMap<>();
        private Map<String, SSLContext> kmsProviderSslContextMap = new HashMap<>();

        /**
         * Sets the {@link MongoClientSettings} that will be used to access the key vault.
         *
         * @param keyVaultMongoClientSettings the key vault mongo client settings, which may not be null.
         * @return this
         * @see #getKeyVaultMongoClientSettings()
         */
        public Builder keyVaultMongoClientSettings(final MongoClientSettings keyVaultMongoClientSettings) {
            this.keyVaultMongoClientSettings = notNull("keyVaultMongoClientSettings", keyVaultMongoClientSettings);
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
         * @see #kmsProviderPropertySuppliers(Map)
         * @see #getKmsProviders()
         */
        public Builder kmsProviders(final Map<String, Map<String, Object>> kmsProviders) {
            this.kmsProviders = notNull("kmsProviders", kmsProviders);
            return this;
        }

        /**
         * This method is similar to {@link #kmsProviders(Map)}, but instead of setting properties for KMS providers,
         * it sets {@link Supplier}s of properties.
         *
         * @param kmsProviderPropertySuppliers A {@link Map} where keys identify KMS providers,
         * and values specify {@link Supplier}s of properties for the KMS providers.
         * Must not be null. Each {@link Supplier} must return non-empty properties.
         * @return this
         * @see #getKmsProviderPropertySuppliers()
         * @since 4.6
         */
        public Builder kmsProviderPropertySuppliers(final Map<String, Supplier<Map<String, Object>>> kmsProviderPropertySuppliers) {
            this.kmsProviderPropertySuppliers = notNull("kmsProviderPropertySuppliers", kmsProviderPropertySuppliers);
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
     * Gets the {@link MongoClientSettings} that will be used to access the key vault.
     *
     * @return the key vault settings, which may be not be null
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
     * <p> Multiple KMS providers can be specified within this map. Each KMS provider is identified by a unique key.
     * Keys are formatted as either {@code "KMS provider type"} or {@code "KMS provider type:KMS provider name"} (e.g., "aws" or "aws:myname").
     * <p>
     * Supported KMS provider types include "aws", "azure", "gcp", and "local". The provider name is optional and allows
     * for the configuration of multiple providers of the same type under different names (e.g., "aws:name1" and
     * "aws:name2" could represent different AWS accounts).
     * <p>
     * The kmsProviders map values differ by provider type. The following properties are supported for each provider type:
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
     *  <li>use the {@link Supplier} configured in {@link #getKmsProviderPropertySuppliers()} to obtain a non-empty map</li>
     *  <li>attempt to obtain the properties from the environment</li>
     * </ul>
     * However, KMS providers containing a name (e.g., "aws:myname") does not support dynamically obtaining KMS properties from the {@link Supplier}
     * or environment.
     * @return map of KMS provider properties
     * @see #getKmsProviderPropertySuppliers()
     */
    public Map<String, Map<String, Object>> getKmsProviders() {
        return unmodifiableMap(kmsProviders);
    }

    /**
     * This method is similar to {@link #getKmsProviders()}, but instead of getting properties for KMS providers,
     * it gets {@link Supplier}s of properties.
     * <p>If {@link #getKmsProviders()} returns empty properties for a KMS provider,
     * the driver will use a {@link Supplier} of properties configured for the KMS provider to obtain non-empty properties.</p>
     *
     * @return A {@link Map} where keys identify KMS providers, and values specify {@link Supplier}s of properties for the KMS providers.
     * @since 4.6
     */
    public Map<String, Supplier<Map<String, Object>>> getKmsProviderPropertySuppliers() {
        return unmodifiableMap(kmsProviderPropertySuppliers);
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
        this.keyVaultMongoClientSettings = notNull("keyVaultMongoClientSettings", builder.keyVaultMongoClientSettings);
        this.keyVaultNamespace = notNull("keyVaultNamespace", builder.keyVaultNamespace);
        this.kmsProviders = notNull("kmsProviders", builder.kmsProviders);
        this.kmsProviderPropertySuppliers = notNull("kmsProviderPropertySuppliers", builder.kmsProviderPropertySuppliers);
        this.kmsProviderSslContextMap = notNull("kmsProviderSslContextMap", builder.kmsProviderSslContextMap);
    }

}
