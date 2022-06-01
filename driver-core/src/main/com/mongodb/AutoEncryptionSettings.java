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

import com.mongodb.annotations.Beta;
import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;

import javax.net.ssl.SSLContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableMap;

/**
 * The client-side automatic encryption settings. Client side encryption enables an application to specify what fields in a collection
 * must be encrypted, and the driver automatically encrypts commands sent to MongoDB and decrypts responses.
 * <p>
 * Automatic encryption is an enterprise only feature that only applies to operations on a collection. Automatic encryption is not
 * supported for operations on a database or view and will result in error. To bypass automatic encryption,
 * set bypassAutoEncryption=true in {@code AutoEncryptionSettings}.
 * </p>
 * <p>
 * Explicit encryption/decryption and automatic decryption is a community feature, enabled with the new
 * {@code com.mongodb.client.vault.ClientEncryption} type.
 * </p>
 * <p>
 * A MongoClient configured with bypassAutoEncryption=true will still automatically decrypt.
 * </p>
 * <p>
 * If automatic encryption fails on an operation, use a MongoClient configured with bypassAutoEncryption=true and use
 * ClientEncryption#encrypt to manually encrypt values.
 * </p>
 * <p>
 * Enabling client side encryption reduces the maximum document and message size (using a maxBsonObjectSize of 2MiB and
 * maxMessageSizeBytes of 6MB) and may have a negative performance impact.
 * </p>
 * <p>
 * Automatic encryption requires the authenticated user to have the listCollections privilege action.
 * </p>
 * <p>
 * Supplying an {@code encryptedFieldsMap} provides more security than relying on an encryptedFields obtained from the server.
 * It protects against a malicious server advertising false encryptedFields.
 * </p>
 *
 * @since 3.11
 */
public final class AutoEncryptionSettings {
    private final MongoClientSettings keyVaultMongoClientSettings;
    private final String keyVaultNamespace;
    private final Map<String, Map<String, Object>> kmsProviders;
    private final Map<String, SSLContext> kmsProviderSslContextMap;
    private final Map<String, Supplier<Map<String, Object>>> kmsProviderPropertySuppliers;
    private final Map<String, BsonDocument> schemaMap;
    private final Map<String, Object> extraOptions;
    private final boolean bypassAutoEncryption;
    private final Map<String, BsonDocument> encryptedFieldsMap;
    private final boolean bypassQueryAnalysis;
    private final List<String> searchPaths;

    /**
     * A builder for {@code AutoEncryptionSettings} so that {@code AutoEncryptionSettings} can be immutable, and to support easier
     * construction through chaining.
     */
    @NotThreadSafe
    public static final class Builder {
        private MongoClientSettings keyVaultMongoClientSettings;
        private String keyVaultNamespace;
        private Map<String, Map<String, Object>> kmsProviders;
        private Map<String, SSLContext> kmsProviderSslContextMap = new HashMap<>();
        private Map<String, Supplier<Map<String, Object>>> kmsProviderPropertySuppliers = new HashMap<>();
        private Map<String, BsonDocument> schemaMap = Collections.emptyMap();
        private Map<String, Object> extraOptions = Collections.emptyMap();
        private boolean bypassAutoEncryption;
        private Map<String, BsonDocument> encryptedFieldsMap = Collections.emptyMap();
        private boolean bypassQueryAnalysis;
        private List<String> searchPaths = emptyList();

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
         * @see #kmsProviderPropertySuppliers(Map)
         * @see #getKmsProviders()
         */
        public Builder kmsProviders(final Map<String, Map<String, Object>> kmsProviders) {
            this.kmsProviders = notNull("kmsProviders", kmsProviders);
            return this;
        }

        /**
         * This method is similar to {@link #kmsProviders(Map)}, but instead of configuring properties for KMS providers,
         * it configures {@link Supplier}s of properties.
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
         * Sets the map from namespace to local schema document
         *
         * @param schemaMap the map from namespace to local schema document
         * @return this
         * @see #getSchemaMap()
         */
        public Builder schemaMap(final Map<String, BsonDocument> schemaMap) {
            this.schemaMap = notNull("schemaMap", schemaMap);
            return this;
        }

        /**
         * Sets the extra options.
         *
         * <p>
         *      <strong>Note:</strong> When setting {@code cflePath}, the override path must be given as a path to the csfle
         *      dynamic library file itself, and not simply the directory that contains it.
         * </p>
         *
         * @param extraOptions the extra options, which may not be null
         * @return this
         * @see #getExtraOptions()
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
         * @see #isBypassAutoEncryption()
         */
        public Builder bypassAutoEncryption(final boolean bypassAutoEncryption) {
            this.bypassAutoEncryption = bypassAutoEncryption;
            return this;
        }

        /**
         * Maps a collection namespace to an encryptedFields.
         *
         * <p><strong>Note:</strong> only applies to queryable encryption.
         * Automatic encryption in queryable encryption is configured with the encryptedFields.</p>
         * <p>If a collection is present in both the {@code encryptedFieldsMap} and {@link #schemaMap}, the driver will error.</p>
         * <p>If a collection is present on the {@code encryptedFieldsMap}, the behavior of {@code collection.createCollection()} and
         * {@code collection.drop()} is altered.</p>
         *
         * <p>If a collection is not present on the {@code encryptedFieldsMap} a server-side collection {@code encryptedFieldsMap} may be
         * used by the driver.
         *
         * @param encryptedFieldsMap the mapping of the collection namespace to the encryptedFields
         * @return this
         * @since 4.7
         */
        public Builder encryptedFieldsMap(final Map<String, BsonDocument> encryptedFieldsMap) {
            this.encryptedFieldsMap = notNull("encryptedFieldsMap", encryptedFieldsMap);
            return this;
        }

        /**
         * Enable or disable automatic analysis of outgoing commands.
         *
         * <p>Set bypassQueryAnalysis to true to use explicit encryption on indexed fields
         * without the MongoDB Enterprise Advanced licensed csfle shared library.</p>
         *
         * @param bypassQueryAnalysis whether query analysis should be bypassed
         * @return this
         * @since 4.7
         */
        public Builder bypassQueryAnalysis(final boolean bypassQueryAnalysis) {
            this.bypassQueryAnalysis = bypassQueryAnalysis;
            return this;
        }

        /**
         * Internal API may be removed at any time.
         */
        @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
        public Builder searchPaths(final List<String> searchPaths) {
            this.searchPaths = notNull("searchPaths", searchPaths);
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
     * <p>
     * The key vault collection is assumed to reside on the same MongoDB cluster as the encrypted collections. But the optional
     * keyVaultMongoClientSettings can be used to route data key queries to a separate MongoDB cluster, or the same cluster but using a
     * different credential.
     * </p>
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
     *
     * <p>
     * It is also permitted for the value of a kms provider to be an empty map, in which case the driver will first
     * </p>
     * <ul>
     *  <li>use the {@link Supplier} configured in {@link #getKmsProviderPropertySuppliers()} to obtain a non-empty map</li>
     *  <li>attempt to obtain the properties from the environment</li>
     * </ul>
     *
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

    /**
     * Gets the map of namespace to local JSON schema.
     * <p>
     * Automatic encryption is configured with an "encrypt" field in a collection's JSONSchema. By default, a collection's JSONSchema is
     * periodically polled with the listCollections command. But a JSONSchema may be specified locally with the schemaMap option.
     * </p>
     * <p>
     * The key into the map is the full namespace of the collection, which is {@code &lt;database name>.&lt;collection name>}.  For
     * example, if the database name is {@code "test"} and the collection name is {@code "users"}, then the namesspace is
     * {@code "test.users"}.
     * </p>
     * <p>
     * Supplying a schemaMap provides more security than relying on JSON Schemas obtained from the server. It protects against a
     * malicious server advertising a false JSON Schema, which could trick the client into sending unencrypted data that should be
     * encrypted.
     * </p>
     * <p>
     * Schemas supplied in the schemaMap only apply to configuring automatic encryption for client side encryption. Other validation
     * rules in the JSON schema will not be enforced by the driver and will result in an error.
     * </p>
     *
     * @return map of namespace to local JSON schema
     */
    public Map<String, BsonDocument> getSchemaMap() {
        return schemaMap;
    }

    /**
     * Gets the extra options that control the behavior of auto-encryption components.
     * <p>
     * The extraOptions currently only relate to the mongocryptd process.  The following options keys are supported:
     * </p>
     * <ul>
     * <li>mongocryptdURI: a String which defaults to "mongodb://%2Fvar%2Fmongocryptd.sock" if domain sockets are available or
     * "mongodb://localhost:27020" otherwise.</li>
     * <li>mongocryptdBypassSpawn: a boolean which defaults to false. If true, the driver will not attempt to automatically spawn a
     * mongocryptd process</li>
     * <li>mongocryptdSpawnPath: specifies the full path to the mongocryptd executable. By default the driver spawns mongocryptd from
     * the system path.</li>
     * <li>mongocryptdSpawnArgs: Used to control the behavior of mongocryptd when the driver spawns it. By default, the driver spawns
     * mongocryptd with the single command line argument {@code "--idleShutdownTimeoutSecs=60"}</li>
     * <li>csflePath: Optional, override the path used to load the csfle library. Note: All MongoClient objects in the same process should
     * use the same setting for csflePath, as it is an error to load more that one csfle dynamic library simultaneously in a single
     * operating system process.</li>
     * <li>csfleRequired: boolean, if 'true', refuse to continue encryption without a csfle library.</li>
     * </ul>
     *
     * @return the extra options map
     */
    public Map<String, Object> getExtraOptions() {
        return extraOptions;
    }

    /**
     * Gets whether auto-encryption should be bypassed.  Even when this option is true, auto-decryption is still enabled.
     * <p>
     * This option is useful for cases where the driver throws an exception because it is unable to prove that the command does not
     * contain any fields that should be automatically encrypted, but the application is able to determine that it does not.  For these
     * cases, the application can construct a {@code MongoClient} with {@code AutoEncryptionSettings} with {@code bypassAutoEncryption}
     * enabled.
     * </p>
     *
     * @return true if auto-encryption should be bypassed
     */
    public boolean isBypassAutoEncryption() {
        return bypassAutoEncryption;
    }

    /**
     * Gets the mapping of a collection namespace to encryptedFields.
     *
     * <p><strong>Note:</strong> only applies to Queryable Encryption.
     * Automatic encryption in Queryable Encryption is configured with the encryptedFields.</p>
     * <p>If a collection is present in both the {@code encryptedFieldsMap} and {@link #schemaMap}, the driver will error.</p>
     * <p>If a collection is present on the {@code encryptedFieldsMap}, the behavior of {@code collection.createCollection()} and
     * {@code collection.drop()} is altered.</p>
     *
     * <p>If a collection is not present on the {@code encryptedFieldsMap} a server-side collection {@code encryptedFieldsMap} may be
     * used by the driver.</p>
     *
     * @return the mapping of the collection namespaces to encryptedFields
     * @since 4.7
     */
    @Nullable
    public Map<String, BsonDocument> getEncryptedFieldsMap() {
        return encryptedFieldsMap;
    }

    /**
     * Gets whether automatic analysis of outgoing commands is set.
     *
     * <p>Set bypassQueryAnalysis to true to use explicit encryption on indexed fields
     * without the MongoDB Enterprise Advanced licensed csfle shared library.</p>
     *
     * @return true if query analysis should be bypassed
     * @since 4.7
     */
    public boolean isBypassQueryAnalysis() {
        return bypassQueryAnalysis;
    }


    /**
     * Internal API may be removed at any time.
     */
    @Beta(Beta.Reason.CLIENT)
    @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
    public List<String> getSearchPaths() {
        return searchPaths;
    }

    private AutoEncryptionSettings(final Builder builder) {
        this.keyVaultMongoClientSettings = builder.keyVaultMongoClientSettings;
        this.keyVaultNamespace = notNull("keyVaultNamespace", builder.keyVaultNamespace);
        this.kmsProviders = notNull("kmsProviders", builder.kmsProviders);
        this.kmsProviderSslContextMap = notNull("kmsProviderSslContextMap", builder.kmsProviderSslContextMap);
        this.kmsProviderPropertySuppliers = notNull("kmsProviderPropertySuppliers", builder.kmsProviderPropertySuppliers);
        this.schemaMap = notNull("schemaMap", builder.schemaMap);
        this.extraOptions = notNull("extraOptions", builder.extraOptions);
        this.bypassAutoEncryption = builder.bypassAutoEncryption;
        this.encryptedFieldsMap = builder.encryptedFieldsMap;
        this.bypassQueryAnalysis = builder.bypassQueryAnalysis;
        this.searchPaths = builder.searchPaths;
    }

    @Override
    public String toString() {
        return "AutoEncryptionSettings{<hidden>}";
    }
}
