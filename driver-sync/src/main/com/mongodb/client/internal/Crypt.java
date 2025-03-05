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

package com.mongodb.client.internal;

import com.mongodb.MongoClientException;
import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.model.vault.RewrapManyDataKeyOptions;
import com.mongodb.crypt.capi.MongoCryptException;
import com.mongodb.internal.capi.MongoCryptHelper;
import com.mongodb.internal.crypt.capi.MongoCrypt;
import com.mongodb.internal.crypt.capi.MongoCryptContext;
import com.mongodb.internal.crypt.capi.MongoDataKeyOptions;
import com.mongodb.internal.crypt.capi.MongoKeyDecryptor;
import com.mongodb.internal.crypt.capi.MongoRewrapManyDataKeyOptions;
import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.Nullable;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.client.vault.EncryptOptionsHelper.asMongoExplicitEncryptOptions;
import static com.mongodb.internal.crypt.capi.MongoCryptContext.State;
import static com.mongodb.internal.thread.InterruptionUtil.translateInterruptedException;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class Crypt implements Closeable {

    private static final RawBsonDocument EMPTY_RAW_BSON_DOCUMENT = RawBsonDocument.parse("{}");
    private final MongoCrypt mongoCrypt;
    private final Map<String, Map<String, Object>> kmsProviders;
    private final Map<String, Supplier<Map<String, Object>>> kmsProviderPropertySuppliers;
    private final CollectionInfoRetriever collectionInfoRetriever;
    private final CommandMarker commandMarker;
    private final KeyRetriever keyRetriever;
    private final KeyManagementService keyManagementService;
    private final boolean bypassAutoEncryption;
    @Nullable
    private final MongoClient collectionInfoRetrieverClient;
    @Nullable
    private final MongoClient keyVaultClient;


    /**
     * Create an instance to use for explicit encryption and decryption, and data key creation.
     *
     * @param mongoCrypt                    the mongoCrypt wrapper
     * @param keyRetriever                  the key retriever
     * @param keyManagementService          the key management service
     * @param kmsProviders                  the KMS provider credentials
     * @param kmsProviderPropertySuppliers  the KMS provider property providers
     */
    Crypt(final MongoCrypt mongoCrypt,
            final KeyRetriever keyRetriever,
            final KeyManagementService keyManagementService,
            final Map<String, Map<String, Object>> kmsProviders,
            final Map<String, Supplier<Map<String, Object>>> kmsProviderPropertySuppliers) {
        this(mongoCrypt, keyRetriever, keyManagementService, kmsProviders, kmsProviderPropertySuppliers,
                false, null, null, null, null);
    }

    /**
     * Create an instance to use for auto-encryption and auto-decryption.
     *
     * @param mongoCrypt                    the mongoCrypt wrapper
     * @param keyRetriever                  the key retriever
     * @param keyManagementService          the key management service
     * @param kmsProviders                  the KMS provider credentials
     * @param kmsProviderPropertySuppliers  the KMS provider property providers
     * @param bypassAutoEncryption          the bypass auto encryption flag
     * @param collectionInfoRetriever       the collection info retriever
     * @param commandMarker                 the command marker
     * @param collectionInfoRetrieverClient the collection info retriever mongo client
     * @param keyVaultClient                the key vault mongo client
     */
    Crypt(final MongoCrypt mongoCrypt,
            final KeyRetriever keyRetriever,
            final KeyManagementService keyManagementService,
            final Map<String, Map<String, Object>> kmsProviders,
            final Map<String, Supplier<Map<String, Object>>> kmsProviderPropertySuppliers,
            final boolean bypassAutoEncryption,
            @Nullable final CollectionInfoRetriever collectionInfoRetriever,
            @Nullable final CommandMarker commandMarker,
            @Nullable final MongoClient collectionInfoRetrieverClient,
            @Nullable final MongoClient keyVaultClient) {
        this.mongoCrypt = mongoCrypt;
        this.keyRetriever = keyRetriever;
        this.keyManagementService = keyManagementService;
        this.kmsProviders = kmsProviders;
        this.kmsProviderPropertySuppliers = kmsProviderPropertySuppliers;
        this.bypassAutoEncryption = bypassAutoEncryption;
        this.collectionInfoRetriever = collectionInfoRetriever;
        this.commandMarker = commandMarker;
        this.collectionInfoRetrieverClient = collectionInfoRetrieverClient;
        this.keyVaultClient = keyVaultClient;
    }

    /**
     * Encrypt the given command
     *
     * @param databaseName the namespace
     * @param command   the unencrypted command
     * @return the encrypted command
     */
    RawBsonDocument encrypt(final String databaseName, final RawBsonDocument command, @Nullable final Timeout timeoutOperation) {
        notNull("databaseName", databaseName);
        notNull("command", command);

        if (bypassAutoEncryption) {
            return command;
        }

       try (MongoCryptContext encryptionContext = mongoCrypt.createEncryptionContext(databaseName, command)) {
           return executeStateMachine(encryptionContext, databaseName, timeoutOperation);
        } catch (MongoCryptException e) {
            throw wrapInMongoException(e);
        }
    }

    /**
     * Decrypt the given command response
     *
     * @param commandResponse the encrypted command response
     * @return the decrypted command response
     */
    RawBsonDocument decrypt(final RawBsonDocument commandResponse,  @Nullable final Timeout timeoutOperation) {
        notNull("commandResponse", commandResponse);
        try (MongoCryptContext decryptionContext = mongoCrypt.createDecryptionContext(commandResponse)) {
            return executeStateMachine(decryptionContext, null, timeoutOperation);
        } catch (MongoCryptException e) {
            throw wrapInMongoException(e);
        }
    }

    /**
     * Create a data key.
     *
     * @param kmsProvider the KMS provider to create the data key for
     * @param options     the data key options
     * @return the document representing the data key to be added to the key vault
     */
    BsonDocument createDataKey(final String kmsProvider, final DataKeyOptions options, @Nullable final Timeout operationTimeout) {
        notNull("kmsProvider", kmsProvider);
        notNull("options", options);

        try (MongoCryptContext dataKeyCreationContext = mongoCrypt.createDataKeyContext(kmsProvider,
                MongoDataKeyOptions.builder()
                        .keyAltNames(options.getKeyAltNames())
                        .masterKey(options.getMasterKey())
                        .keyMaterial(options.getKeyMaterial())
                        .build())) {
            return executeStateMachine(dataKeyCreationContext, null, operationTimeout);
        } catch (MongoCryptException e) {
            throw wrapInMongoException(e);
        }
    }

    /**
     * Encrypt the given value with the given options
     *
     * @param value the value to encrypt
     * @param options the options
     * @return the encrypted value
     */
    BsonBinary encryptExplicitly(final BsonValue value, final EncryptOptions options, @Nullable final Timeout timeoutOperation) {
        notNull("value", value);
        notNull("options", options);

        try (MongoCryptContext encryptionContext = mongoCrypt.createExplicitEncryptionContext(
                new BsonDocument("v", value), asMongoExplicitEncryptOptions(options))) {
            return executeStateMachine(encryptionContext, null, timeoutOperation).getBinary("v");
        } catch (MongoCryptException e) {
            throw wrapInMongoException(e);
        }
    }

    /**
     * Encrypts a Match Expression or Aggregate Expression to query a range index.
     *
     * @param expression the Match Expression or Aggregate Expression
     * @param options    the options
     * @return the encrypted expression
     */
    BsonDocument encryptExpression(final BsonDocument expression, final EncryptOptions options, @Nullable final Timeout timeoutOperation) {
        notNull("expression", expression);
        notNull("options", options);

        try (MongoCryptContext encryptionContext = mongoCrypt.createEncryptExpressionContext(
                new BsonDocument("v", expression), asMongoExplicitEncryptOptions(options))) {
            return executeStateMachine(encryptionContext, null, timeoutOperation).getDocument("v");
        } catch (MongoCryptException e) {
            throw wrapInMongoException(e);
        }
    }

    /**
     * Decrypt the given encrypted value.
     *
     * @param value the encrypted value
     * @return the decrypted value
     */
    BsonValue decryptExplicitly(final BsonBinary value, @Nullable final Timeout timeoutOperation) {
        notNull("value", value);
        try (MongoCryptContext decryptionContext = mongoCrypt.createExplicitDecryptionContext(new BsonDocument("v", value))) {
            return assertNotNull(executeStateMachine(decryptionContext, null, timeoutOperation).get("v"));
        } catch (MongoCryptException e) {
            throw wrapInMongoException(e);
        }
    }

    /**
     * Rewrap data key
     * @param filter the filter
     * @param options the rewrap many data key options
     * @return the decrypted value
     * @since 4.7
     */
    BsonDocument rewrapManyDataKey(final BsonDocument filter, final RewrapManyDataKeyOptions options, @Nullable final Timeout operationTimeout) {
        notNull("filter", filter);
        try {
            try (MongoCryptContext rewrapManyDatakeyContext = mongoCrypt.createRewrapManyDatakeyContext(filter,
                    MongoRewrapManyDataKeyOptions
                            .builder()
                            .provider(options.getProvider())
                            .masterKey(options.getMasterKey())
                            .build())) {
                return executeStateMachine(rewrapManyDatakeyContext, null, operationTimeout);
            }
        } catch (MongoCryptException e) {
            throw wrapInMongoException(e);
        }
    }

    @Override
    @SuppressWarnings("try")
    public void close() {
        //noinspection EmptyTryBlock
        try (MongoCrypt ignored = this.mongoCrypt;
             CommandMarker ignored1 = this.commandMarker;
             MongoClient ignored2 = this.collectionInfoRetrieverClient;
             MongoClient ignored3 = this.keyVaultClient
        ) {
            // just using try-with-resources to ensure they all get closed, even in the case of exceptions
        }
    }

    private RawBsonDocument executeStateMachine(final MongoCryptContext cryptContext, @Nullable final String databaseName, @Nullable final Timeout operationTimeout) {
        while (true) {
            State state = cryptContext.getState();
            switch (state) {
                case NEED_MONGO_COLLINFO:
                    collInfo(cryptContext, notNull("databaseName", databaseName), operationTimeout);
                    break;
                case NEED_MONGO_MARKINGS:
                    mark(cryptContext, notNull("databaseName", databaseName), operationTimeout);
                    break;
                case NEED_KMS_CREDENTIALS:
                    fetchCredentials(cryptContext);
                    break;
                case NEED_MONGO_KEYS:
                    fetchKeys(cryptContext, operationTimeout);
                    break;
                case NEED_KMS:
                    decryptKeys(cryptContext, operationTimeout);
                    break;
                case READY:
                    return cryptContext.finish();
                case DONE:
                    return EMPTY_RAW_BSON_DOCUMENT;
                default:
                    throw new MongoInternalException("Unsupported encryptor state + " + state);
            }
        }
    }

    private void fetchCredentials(final MongoCryptContext cryptContext) {
        cryptContext.provideKmsProviderCredentials(MongoCryptHelper.fetchCredentials(kmsProviders, kmsProviderPropertySuppliers));
    }

    private void collInfo(final MongoCryptContext cryptContext, final String databaseName, @Nullable final Timeout operationTimeout) {
        try {
            List<BsonDocument> results = assertNotNull(collectionInfoRetriever)
                    .filter(databaseName, cryptContext.getMongoOperation(), operationTimeout);
            for (BsonDocument result : results) {
                cryptContext.addMongoOperationResult(result);
            }
            cryptContext.completeMongoOperation();
        } catch (Throwable t) {
            throw MongoException.fromThrowableNonNull(t);
        }
    }

    private void mark(final MongoCryptContext cryptContext, final String databaseName, @Nullable final Timeout operationTimeout) {
        try {
            RawBsonDocument markedCommand = assertNotNull(commandMarker).mark(databaseName, cryptContext.getMongoOperation(), operationTimeout);
            cryptContext.addMongoOperationResult(markedCommand);
            cryptContext.completeMongoOperation();
        } catch (Throwable t) {
            throw wrapInMongoException(t);
        }
    }

    private void fetchKeys(final MongoCryptContext keyBroker, @Nullable final Timeout operationTimeout) {
        try {
            for (BsonDocument bsonDocument : keyRetriever.find(keyBroker.getMongoOperation(), operationTimeout)) {
                keyBroker.addMongoOperationResult(bsonDocument);
            }
            keyBroker.completeMongoOperation();
        } catch (Throwable t) {
            throw MongoException.fromThrowableNonNull(t);
        }
    }

    private void decryptKeys(final MongoCryptContext cryptContext, @Nullable final Timeout operationTimeout) {
        try {
            MongoKeyDecryptor keyDecryptor = cryptContext.nextKeyDecryptor();
            while (keyDecryptor != null) {
                decryptKey(keyDecryptor, operationTimeout);
                keyDecryptor = cryptContext.nextKeyDecryptor();
            }
            cryptContext.completeKeyDecryptors();
        } catch (Throwable t) {
            throw translateInterruptedException(t, "Interrupted while doing IO")
                    .orElseThrow(() -> wrapInMongoException(t));
        }
    }

    private void decryptKey(final MongoKeyDecryptor keyDecryptor, @Nullable final Timeout operationTimeout) throws IOException {
        try (InputStream inputStream = keyManagementService.stream(keyDecryptor.getKmsProvider(), keyDecryptor.getHostName(),
                keyDecryptor.getMessage(), operationTimeout)) {
            int bytesNeeded = keyDecryptor.bytesNeeded();

            while (bytesNeeded > 0) {
                byte[] bytes = new byte[bytesNeeded];
                int bytesRead = inputStream.read(bytes, 0, bytes.length);
                keyDecryptor.feed(ByteBuffer.wrap(bytes, 0, bytesRead));
                bytesNeeded = keyDecryptor.bytesNeeded();
            }
        }
    }

    private MongoException wrapInMongoException(final Throwable t) {
        if (t instanceof MongoException) {
            return (MongoException) t;
        } else {
            return new MongoClientException("Exception in encryption library: " + t.getMessage(), t);
        }
    }
}
