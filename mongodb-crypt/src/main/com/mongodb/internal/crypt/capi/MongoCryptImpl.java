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

package com.mongodb.internal.crypt.capi;

import com.mongodb.crypt.capi.MongoCryptException;
import com.mongodb.internal.crypt.capi.CAPI.cstring;
import com.mongodb.internal.crypt.capi.CAPI.mongocrypt_binary_t;
import com.mongodb.internal.crypt.capi.CAPI.mongocrypt_ctx_t;
import com.mongodb.internal.crypt.capi.CAPI.mongocrypt_log_fn_t;
import com.mongodb.internal.crypt.capi.CAPI.mongocrypt_status_t;
import com.mongodb.internal.crypt.capi.CAPI.mongocrypt_t;
import com.sun.jna.Pointer;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonString;

import javax.crypto.Cipher;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.mongodb.internal.crypt.capi.CAPI.MONGOCRYPT_CTX_ERROR;
import static com.mongodb.internal.crypt.capi.CAPI.MONGOCRYPT_LOG_LEVEL_ERROR;
import static com.mongodb.internal.crypt.capi.CAPI.MONGOCRYPT_LOG_LEVEL_FATAL;
import static com.mongodb.internal.crypt.capi.CAPI.MONGOCRYPT_LOG_LEVEL_INFO;
import static com.mongodb.internal.crypt.capi.CAPI.MONGOCRYPT_LOG_LEVEL_TRACE;
import static com.mongodb.internal.crypt.capi.CAPI.MONGOCRYPT_LOG_LEVEL_WARNING;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_crypt_shared_lib_version_string;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_ctx_datakey_init;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_ctx_encrypt_init;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_ctx_new;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_ctx_setopt_algorithm;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_ctx_setopt_algorithm_range;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_ctx_setopt_algorithm_text;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_ctx_setopt_contention_factor;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_ctx_setopt_key_alt_name;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_ctx_setopt_key_encryption_key;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_ctx_setopt_key_id;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_ctx_setopt_key_material;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_ctx_setopt_query_type;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_ctx_state;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_destroy;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_init;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_is_crypto_available;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_new;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_setopt_aes_256_ctr;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_setopt_append_crypt_shared_lib_search_path;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_setopt_bypass_query_analysis;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_setopt_crypto_hook_sign_rsaes_pkcs1_v1_5;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_setopt_crypto_hooks;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_setopt_enable_multiple_collinfo;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_setopt_encrypted_field_config_map;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_setopt_key_expiration;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_setopt_kms_provider_aws;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_setopt_kms_provider_local;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_setopt_kms_providers;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_setopt_log_handler;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_setopt_schema_map;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_setopt_set_crypt_shared_lib_path_override;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_setopt_use_need_kms_credentials_state;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_status;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_status_code;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_status_destroy;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_status_message;
import static com.mongodb.internal.crypt.capi.CAPI.mongocrypt_status_new;
import static com.mongodb.internal.crypt.capi.CAPIHelper.toBinary;
import static org.bson.assertions.Assertions.isTrue;
import static org.bson.assertions.Assertions.notNull;

/**
 * MongoCryptImpl is the main implementation of the {@link MongoCrypt} interface.
 * <p>
 * This class is responsible for configuring and managing the native libmongocrypt context,
 * handling encryption and decryption operations, and bridging Java cryptographic hooks
 * when required. It wraps the native resource and provides context creation methods for
 * various cryptographic operations.
 * <p>
 * Key responsibilities:
 * <ul>
 *   <li>Configures libmongocrypt with KMS providers, schema maps, encrypted fields, and other options.</li>
 *   <li>Registers Java cryptographic hooks if native crypto is not available.</li>
 *   <li>Provides context creation for encryption, decryption, key management, and explicit operations.</li>
 *   <li>Manages native resource lifecycle and error handling.</li>
 * </ul>
 */
class MongoCryptImpl implements MongoCrypt {
    private static final Logger LOGGER = Loggers.getLogger();
    private final mongocrypt_t wrapped;

    // Keep a strong reference to all the callbacks so that they don't get garbage collected
    @SuppressWarnings("FieldCanBeLocal")
    private final LogCallback logCallback;

    @SuppressWarnings("FieldCanBeLocal")
    private final CipherCallback aesCBC256EncryptCallback;
    @SuppressWarnings("FieldCanBeLocal")
    private final CipherCallback aesCBC256DecryptCallback;
    @SuppressWarnings("FieldCanBeLocal")
    private final CipherCallback aesCTR256EncryptCallback;
    @SuppressWarnings("FieldCanBeLocal")
    private final CipherCallback aesCTR256DecryptCallback;
    @SuppressWarnings("FieldCanBeLocal")
    private final MacCallback hmacSha512Callback;
    @SuppressWarnings("FieldCanBeLocal")
    private final MacCallback hmacSha256Callback;
    @SuppressWarnings("FieldCanBeLocal")
    private final MessageDigestCallback sha256Callback;
    @SuppressWarnings("FieldCanBeLocal")
    private final SecureRandomCallback secureRandomCallback;
    @SuppressWarnings("FieldCanBeLocal")
    private final SigningRSAESPKCSCallback signingRSAESPKCSCallback;

    private final AtomicBoolean closed;

    /**
     * Constructs a MongoCryptImpl instance and configures the native libmongocrypt context.
     * <p>
     * Registers log handlers, cryptographic hooks, and sets up KMS providers and other options.
     * Throws MongoCryptException if initialization fails.
     */
    MongoCryptImpl(final MongoCryptOptions options) {
        closed = new AtomicBoolean();
        wrapped = mongocrypt_new();
        if (wrapped == null) {
            throw new MongoCryptException("Unable to create new mongocrypt object");
        }

        logCallback = new LogCallback();

        mongocrypt_setopt_enable_multiple_collinfo(wrapped);

        configure(() -> mongocrypt_setopt_log_handler(wrapped, logCallback, null));

        if (mongocrypt_is_crypto_available()) {
            LOGGER.debug("libmongocrypt is compiled with cryptography support, so not registering Java callbacks");
            aesCBC256EncryptCallback = null;
            aesCBC256DecryptCallback = null;
            aesCTR256EncryptCallback = null;
            aesCTR256DecryptCallback = null;
            hmacSha512Callback = null;
            hmacSha256Callback = null;
            sha256Callback = null;
            secureRandomCallback = null;
            signingRSAESPKCSCallback = null;
        } else {
            LOGGER.debug("libmongocrypt is compiled without cryptography support, so registering Java callbacks");
            // We specify NoPadding here because the underlying C library is responsible for padding prior
            // to executing the callback
            aesCBC256EncryptCallback = new CipherCallback("AES", "AES/CBC/NoPadding", Cipher.ENCRYPT_MODE);
            aesCBC256DecryptCallback = new CipherCallback("AES", "AES/CBC/NoPadding", Cipher.DECRYPT_MODE);
            aesCTR256EncryptCallback = new CipherCallback("AES", "AES/CTR/NoPadding", Cipher.ENCRYPT_MODE);
            aesCTR256DecryptCallback = new CipherCallback("AES", "AES/CTR/NoPadding", Cipher.DECRYPT_MODE);

            hmacSha512Callback = new MacCallback("HmacSHA512");
            hmacSha256Callback = new MacCallback("HmacSHA256");
            sha256Callback = new MessageDigestCallback("SHA-256");
            secureRandomCallback = new SecureRandomCallback(new SecureRandom());

            configure(() -> mongocrypt_setopt_crypto_hooks(wrapped, aesCBC256EncryptCallback, aesCBC256DecryptCallback,
                    secureRandomCallback, hmacSha512Callback, hmacSha256Callback,
                    sha256Callback, null));

            signingRSAESPKCSCallback = new SigningRSAESPKCSCallback();
            configure(() -> mongocrypt_setopt_crypto_hook_sign_rsaes_pkcs1_v1_5(wrapped, signingRSAESPKCSCallback, null));
            configure(() -> mongocrypt_setopt_aes_256_ctr(wrapped, aesCTR256EncryptCallback, aesCTR256DecryptCallback, null));
        }

        if (options.getLocalKmsProviderOptions() != null) {
            withBinaryHolder(options.getLocalKmsProviderOptions().getLocalMasterKey(),
                    binary -> configure(() -> mongocrypt_setopt_kms_provider_local(wrapped, binary)));
        }

        if (options.getAwsKmsProviderOptions() != null) {
            configure(() -> mongocrypt_setopt_kms_provider_aws(wrapped,
                                                                new cstring(options.getAwsKmsProviderOptions().getAccessKeyId()), -1,
                                                                new cstring(options.getAwsKmsProviderOptions().getSecretAccessKey()), -1));
        }

        if (options.isNeedsKmsCredentialsStateEnabled()) {
            mongocrypt_setopt_use_need_kms_credentials_state(wrapped);
        }

        if (options.getKmsProviderOptions() != null) {
            withBinaryHolder(options.getKmsProviderOptions(),
                binary -> configure(() -> mongocrypt_setopt_kms_providers(wrapped, binary)));
        }

        if (options.getLocalSchemaMap() != null) {
            BsonDocument localSchemaMapDocument = new BsonDocument();
            localSchemaMapDocument.putAll(options.getLocalSchemaMap());

            withBinaryHolder(localSchemaMapDocument, binary -> configure(() -> mongocrypt_setopt_schema_map(wrapped, binary)));
        }

        if (options.isBypassQueryAnalysis()) {
            mongocrypt_setopt_bypass_query_analysis(wrapped);
        }

        Long keyExpirationMS = options.getKeyExpirationMS();
        if (keyExpirationMS != null) {
            configure(() -> mongocrypt_setopt_key_expiration(wrapped, keyExpirationMS));
        }

        if (options.getEncryptedFieldsMap() != null) {
            BsonDocument localEncryptedFieldsMap = new BsonDocument();
            localEncryptedFieldsMap.putAll(options.getEncryptedFieldsMap());

            withBinaryHolder(localEncryptedFieldsMap,
                    binary -> configure(() -> mongocrypt_setopt_encrypted_field_config_map(wrapped, binary)));
        }

        options.getSearchPaths().forEach(p -> mongocrypt_setopt_append_crypt_shared_lib_search_path(wrapped, new cstring(p)));
        if (options.getExtraOptions().containsKey("cryptSharedLibPath")) {
            mongocrypt_setopt_set_crypt_shared_lib_path_override(wrapped, new cstring(options.getExtraOptions().getString("cryptSharedLibPath").getValue()));
        }

        configure(() -> mongocrypt_init(wrapped));
    }

    /**
     * Creates an encryption context for the given database and command document.
     */
    @Override
    public MongoCryptContext createEncryptionContext(final String database, final BsonDocument commandDocument) {
        isTrue("open", !closed.get());
        notNull("database", database);
        notNull("commandDocument", commandDocument);
        return createMongoCryptContext(commandDocument, createNewMongoCryptContext(),
                (context, binary) -> mongocrypt_ctx_encrypt_init(context, new cstring(database), -1, binary));
    }

    /**
     * Creates a decryption context for the given document.
     */
    @Override
    public MongoCryptContext createDecryptionContext(final BsonDocument document) {
        isTrue("open", !closed.get());
        return createMongoCryptContext(document, createNewMongoCryptContext(), CAPI::mongocrypt_ctx_decrypt_init);
    }

    /**
     * Creates a data key context for the specified KMS provider and options.
     */
    @Override
    public MongoCryptContext createDataKeyContext(final String kmsProvider, final MongoDataKeyOptions options) {
        isTrue("open", !closed.get());
        mongocrypt_ctx_t context = createNewMongoCryptContext();

        BsonDocument keyDocument = new BsonDocument("provider", new BsonString(kmsProvider));
        BsonDocument masterKey = options.getMasterKey();
        if (masterKey != null) {
            masterKey.forEach(keyDocument::append);
        }
        withBinaryHolder(keyDocument,
                binary -> configureContext(context, () -> mongocrypt_ctx_setopt_key_encryption_key(context, binary)));

        if (options.getKeyAltNames() != null) {
            for (String cur : options.getKeyAltNames()) {
                withBinaryHolder(new BsonDocument("keyAltName", new BsonString(cur)),
                        binary -> configureContext(context, () -> mongocrypt_ctx_setopt_key_alt_name(context, binary)));
            }
        }

        if (options.getKeyMaterial() != null) {
            withBinaryHolder(new BsonDocument("keyMaterial", new BsonBinary(options.getKeyMaterial())),
                    binary -> configureContext(context, () -> mongocrypt_ctx_setopt_key_material(context, binary)));
        }

        configureContext(context, () -> mongocrypt_ctx_datakey_init(context));
        return new MongoCryptContextImpl(context);
    }

    /**
     * Creates an explicit encryption context
     */
    @Override
    public MongoCryptContext createExplicitEncryptionContext(final BsonDocument document, final MongoExplicitEncryptOptions options) {
        isTrue("open", !closed.get());
        return createMongoCryptContext(document, configureExplicitEncryption(options), CAPI::mongocrypt_ctx_explicit_encrypt_init);
    }

    /**
     * Creates an explicit encrypt *expression* context
     */
    @Override
    public MongoCryptContext createEncryptExpressionContext(final BsonDocument document, final MongoExplicitEncryptOptions options) {
        isTrue("open", !closed.get());
        return createMongoCryptContext(document, configureExplicitEncryption(options), CAPI::mongocrypt_ctx_explicit_encrypt_expression_init);
    }

    /**
     * Creates an explicit decryption context
     */
    @Override
    public MongoCryptContext createExplicitDecryptionContext(final BsonDocument document) {
        isTrue("open", !closed.get());
        return createMongoCryptContext(document, createNewMongoCryptContext(), CAPI::mongocrypt_ctx_explicit_decrypt_init);
    }

    /**
     * Creates a rewrap many data keys context
     */
    @Override
    public MongoCryptContext createRewrapManyDatakeyContext(final BsonDocument filter, final MongoRewrapManyDataKeyOptions options) {
        isTrue("open", !closed.get());
        mongocrypt_ctx_t context = createNewMongoCryptContext();

        if (options != null && options.getProvider() != null) {
            BsonDocument keyDocument = new BsonDocument("provider", new BsonString(options.getProvider()));
            BsonDocument masterKey = options.getMasterKey();
            if (masterKey != null) {
                masterKey.forEach(keyDocument::append);
            }
            withBinaryHolder(keyDocument,
                binary -> configureContext(context, () -> mongocrypt_ctx_setopt_key_encryption_key(context, binary)));
        }

        return createMongoCryptContext(filter, context, CAPI::mongocrypt_ctx_rewrap_many_datakey_init);
    }

    /**
     * Returns the version string of the loaded crypt shared library.
     */
    @Override
    public String getCryptSharedLibVersionString() {
        cstring versionString = mongocrypt_crypt_shared_lib_version_string(wrapped, null);
        return versionString == null ? null : versionString.toString();
    }

    /**
     * Closes the native libmongocrypt resource.
     * <p>
     * This should be called when the instance is no longer needed to release native resources.
     */
    @Override
    public void close() {
        if (!closed.getAndSet(true)) {
            mongocrypt_destroy(wrapped);
        }
    }

    /**
     * Helper to create a MongoCryptContext from a BSON document and a native context.
     * <p>
     * Applies the given configuration function and checks for errors.
     */
    private MongoCryptContext createMongoCryptContext(final BsonDocument document, final mongocrypt_ctx_t context,
            final BiFunction<mongocrypt_ctx_t, mongocrypt_binary_t, Boolean> configureFunction) {
        withBinaryHolder(document,
            binary -> {
                if (!configureFunction.apply(context, binary)) {
                    MongoCryptContextImpl.throwExceptionFromStatus(context);
                }
            });
        if (mongocrypt_ctx_state(context) == MONGOCRYPT_CTX_ERROR) {
            MongoCryptContextImpl.throwExceptionFromStatus(context);
        }
        return new MongoCryptContextImpl(context);
    }

    /**
     * Helper to create a new native mongocrypt_ctx_t context.
     * <p>
     * Throws if context creation fails.
     */
    private mongocrypt_ctx_t createNewMongoCryptContext() {
        mongocrypt_ctx_t context = mongocrypt_ctx_new(wrapped);
        if (context == null) {
            throwExceptionFromStatus();
        }
        return context;
    }

    /**
     * Configures explicit encryption options on a new native context.
     * <p>
     * Applies key ID, key alt name, algorithm, query type, contention factor, and other options.
     */
    private mongocrypt_ctx_t configureExplicitEncryption(final MongoExplicitEncryptOptions options) {
        mongocrypt_ctx_t context = createNewMongoCryptContext();
        if (options.getKeyId() != null) {
            withBinaryHolder(ByteBuffer.wrap(options.getKeyId().getData()),
                    binary -> configureContext(context, () -> mongocrypt_ctx_setopt_key_id(context, binary)));
        }

        if (options.getKeyAltName() != null) {
            withBinaryHolder(new BsonDocument("keyAltName", new BsonString(options.getKeyAltName())),
                    binary -> configureContext(context, () -> mongocrypt_ctx_setopt_key_alt_name(context, binary)));
        }

        if (options.getAlgorithm() != null) {
            configureContext(context, () -> mongocrypt_ctx_setopt_algorithm(context, new cstring(options.getAlgorithm()), -1));
        }
        if (options.getQueryType() != null) {
            configureContext(context, () -> mongocrypt_ctx_setopt_query_type(context, new cstring(options.getQueryType()), -1));
        }
        if (options.getContentionFactor() != null) {
            configureContext(context, () -> mongocrypt_ctx_setopt_contention_factor(context, options.getContentionFactor()));
        }
        if (options.getRangeOptions() != null) {
            withBinaryHolder(options.getRangeOptions(),
                    binary -> configureContext(context, () -> mongocrypt_ctx_setopt_algorithm_range(context, binary)));
        }
        if (options.getTextOptions() != null) {
            withBinaryHolder(options.getTextOptions(),
                    binary -> configureContext(context, () -> mongocrypt_ctx_setopt_algorithm_text(context, binary)));
        }
        return context;
    }

    /**
     * Configures the main mongocrypt instance with the given supplier that indicates if configuration was successful or not.
     * <p>
     * Throws an exception derived from the mongocrypt status if the configuration fails.
     */
    private void configure(final Supplier<Boolean> successSupplier) {
        if (!successSupplier.get()) {
            throwExceptionFromStatus();
        }
    }

    /**
     * Configures a mongocrypt_ctx_t context instance the given supplier that indicates if configuration was successful or not.
     * <p>
     * Throws an exception derived from the contexts mongocrypt status if the configuration fails.
     */
    private void configureContext(final mongocrypt_ctx_t context, final Supplier<Boolean> successSupplier) {
        if (!successSupplier.get()) {
            MongoCryptContextImpl.throwExceptionFromStatus(context);
        }
    }

    /**
     * Throws a MongoCryptException based on the current status of the native context.
     */
    private void throwExceptionFromStatus() {
        mongocrypt_status_t status = mongocrypt_status_new();
        mongocrypt_status(wrapped, status);
        MongoCryptException e = new MongoCryptException(mongocrypt_status_message(status, null).toString(),
                mongocrypt_status_code(status));
        mongocrypt_status_destroy(status);
        throw e;
    }

    /**
     * Utility method to handle BinaryHolder resource management for ByteBuffer values.
     */
    private static void withBinaryHolder(final ByteBuffer value, final Consumer<mongocrypt_binary_t> consumer) {
        try (BinaryHolder binaryHolder = toBinary(value)) {
            consumer.accept(binaryHolder.getBinary());
        }
    }

    /**
     * Utility method to handle BinaryHolder resource management for BsonDocument values.
     */
    private static void withBinaryHolder(final BsonDocument value, final Consumer<mongocrypt_binary_t> consumer) {
        try (BinaryHolder binaryHolder = toBinary(value)) {
            consumer.accept(binaryHolder.getBinary());
        }
    }

    /**
     * LogCallback bridges native log events to the Java logger.
     * <p>
     * Handles different log levels and forwards messages to the appropriate logger method.
     */
    static class LogCallback implements mongocrypt_log_fn_t {
        @Override
        public void log(final int level, final cstring message, final int messageLength, final Pointer ctx) {
            if (level == MONGOCRYPT_LOG_LEVEL_FATAL) {
                LOGGER.error(message.toString());
            }
            if (level == MONGOCRYPT_LOG_LEVEL_ERROR) {
                LOGGER.error(message.toString());
            }
            if (level == MONGOCRYPT_LOG_LEVEL_WARNING) {
                LOGGER.warn(message.toString());
            }
            if (level == MONGOCRYPT_LOG_LEVEL_INFO) {
                LOGGER.info(message.toString());
            }
            if (level == MONGOCRYPT_LOG_LEVEL_TRACE) {
                LOGGER.trace(message.toString());
            }
        }
    }
}
