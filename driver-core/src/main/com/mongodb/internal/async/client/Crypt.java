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

package com.mongodb.internal.async.client;

import com.mongodb.MongoClientException;
import com.mongodb.MongoInternalException;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.crypt.capi.MongoCrypt;
import com.mongodb.crypt.capi.MongoCryptContext;
import com.mongodb.crypt.capi.MongoCryptException;
import com.mongodb.crypt.capi.MongoDataKeyOptions;
import com.mongodb.crypt.capi.MongoExplicitEncryptOptions;
import com.mongodb.crypt.capi.MongoKeyDecryptor;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.lang.Nullable;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;

import java.io.Closeable;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.crypt.capi.MongoCryptContext.State;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;

public class Crypt implements Closeable {
    private static final Logger LOGGER = Loggers.getLogger("client");
    private final MongoCrypt mongoCrypt;
    private final CollectionInfoRetriever collectionInfoRetriever;
    private final CommandMarker commandMarker;
    private final KeyRetriever keyRetriever;
    private final KeyManagementService keyManagementService;
    private final boolean bypassAutoEncryption;

    /**
     * Create an instance to use for explicit encryption and decryption, and data key creation.
     *
     * @param mongoCrypt           the mongoCrypt wrapper
     * @param keyRetriever         the key retriever
     * @param keyManagementService the key management service
     */
    Crypt(final MongoCrypt mongoCrypt, final KeyRetriever keyRetriever, final KeyManagementService keyManagementService) {
        this(mongoCrypt, null, null, keyRetriever, keyManagementService, false);
    }

    /**
     * Create an instance to use for auto-encryption and auto-decryption.
     *
     * @param mongoCrypt              the mongoCrypt wrapper
     * @param keyRetriever            the key retriever
     * @param keyManagementService    the key management service
     * @param collectionInfoRetriever the collection info retriever
     * @param commandMarker           the command marker
     */
    Crypt(final MongoCrypt mongoCrypt, @Nullable final CollectionInfoRetriever collectionInfoRetriever,
          @Nullable final CommandMarker commandMarker, final KeyRetriever keyRetriever, final KeyManagementService keyManagementService,
          final boolean bypassAutoEncryption) {
        this.mongoCrypt = mongoCrypt;
        this.collectionInfoRetriever = collectionInfoRetriever;
        this.commandMarker = commandMarker;
        this.keyRetriever = keyRetriever;
        this.keyManagementService = keyManagementService;
        this.bypassAutoEncryption = bypassAutoEncryption;
    }

    /**
     * Encrypt the given command
     *
     * @param databaseName the namespace
     * @param command      the unencrypted command
     * @param callback the callback containing the encrypted command
     */
    public void encrypt(final String databaseName, final RawBsonDocument command,
                        final SingleResultCallback<RawBsonDocument> callback) {
        notNull("databaseName", databaseName);
        notNull("command", command);

        if (bypassAutoEncryption) {
            callback.onResult(command, null);
            return;
        }

        try {
            final MongoCryptContext encryptionContext = mongoCrypt.createEncryptionContext(databaseName, command);
            executeStateMachine(encryptionContext, databaseName, createCallback(new SingleResultCallback<RawBsonDocument>() {
                @Override
                public void onResult(final RawBsonDocument result, final Throwable t) {
                    encryptionContext.close();
                    callback.onResult(result, t);
                }
            }));
        } catch (MongoCryptException e) {
            callback.onResult(null, wrapInClientException(e));
        }
    }

    /**
     * Decrypt the given command response
     *
     * @param commandResponse the encrypted command response
     * @param callback the callback containing the decrypted command response
     */
    public void decrypt(final RawBsonDocument commandResponse, final SingleResultCallback<RawBsonDocument> callback) {
        notNull("commandResponse", commandResponse);
        try {
            final MongoCryptContext decryptionContext = mongoCrypt.createDecryptionContext(commandResponse);
            executeStateMachine(decryptionContext, null, createCallback(new SingleResultCallback<RawBsonDocument>() {
                @Override
                public void onResult(final RawBsonDocument result, final Throwable t) {
                    decryptionContext.close();
                    callback.onResult(result, t);
                }
            }));
        } catch (Throwable t) {
            callback.onResult(null, wrapInClientException(t));
        }
    }

    /**
     * Create a data key.
     *
     * @param kmsProvider the KMS provider to create the data key for
     * @param options     the data key options
     * @param callback the callback containing the document representing the data key to be added to the key vault
     */
    public void createDataKey(final String kmsProvider, final DataKeyOptions options,
                       final SingleResultCallback<RawBsonDocument> callback) {
        notNull("kmsProvider", kmsProvider);
        notNull("options", options);

        try {
            final MongoCryptContext dataKeyCreationContext = mongoCrypt.createDataKeyContext(kmsProvider,
                    MongoDataKeyOptions.builder()
                            .keyAltNames(options.getKeyAltNames())
                            .masterKey(options.getMasterKey())
                            .build());
            executeStateMachine(dataKeyCreationContext, null, createCallback(new SingleResultCallback<RawBsonDocument>() {
                @Override
                public void onResult(final RawBsonDocument result, final Throwable t) {
                    dataKeyCreationContext.close();
                    callback.onResult(result, t);
                }
            }));
        } catch (Throwable t) {
            callback.onResult(null, wrapInClientException(t));
        }
    }

    /**
     * Encrypt the given value with the given options
     *
     * @param value   the value to encrypt
     * @param options the options
     * @param callback the callback containing the encrypted value
     */
    public void encryptExplicitly(final BsonValue value, final EncryptOptions options,
                           final SingleResultCallback<BsonBinary> callback) {
        notNull("value", value);
        notNull("options", options);

        try {
            MongoExplicitEncryptOptions.Builder encryptOptionsBuilder = MongoExplicitEncryptOptions.builder()
                    .algorithm(options.getAlgorithm());

            if (options.getKeyId() != null) {
                encryptOptionsBuilder.keyId(options.getKeyId());
            }

            if (options.getKeyAltName() != null) {
                encryptOptionsBuilder.keyAltName(options.getKeyAltName());
            }

            final MongoCryptContext encryptionContext = mongoCrypt.createExplicitEncryptionContext(
                    new BsonDocument("v", value), encryptOptionsBuilder.build());

            executeStateMachine(encryptionContext, null, createCallback(new SingleResultCallback<RawBsonDocument>() {
                @Override
                public void onResult(final RawBsonDocument result, final Throwable t) {
                    encryptionContext.close();
                    if (t != null) {
                        callback.onResult(null, t);
                    } else {
                        callback.onResult(result.getBinary("v"), null);
                    }
                }
            }));
        } catch (Throwable t) {
            callback.onResult(null, wrapInClientException(t));
        }
    }

    /**
     * Decrypt the given encrypted value.
     *
     * @param value the encrypted value
     * @param callback the callback containing the decrypted value
     */
    public void decryptExplicitly(final BsonBinary value, final SingleResultCallback<BsonValue> callback) {
        notNull("value", value);

        try {
            final MongoCryptContext decryptionContext = mongoCrypt.createExplicitDecryptionContext(new BsonDocument("v", value));
            executeStateMachine(decryptionContext, null, createCallback(new SingleResultCallback<RawBsonDocument>() {
                @Override
                public void onResult(final RawBsonDocument result, final Throwable t) {
                    decryptionContext.close();
                    if (t != null) {
                        callback.onResult(null, t);
                    } else {
                        callback.onResult(result.get("v"), null);
                    }
                }
            }));
        } catch (Throwable t) {
            callback.onResult(null, wrapInClientException(t));
        }
    }

    @Override
    public void close() {
        mongoCrypt.close();
        if (commandMarker != null) {
            commandMarker.close();
        }
        keyRetriever.close();
    }

    private void executeStateMachine(final MongoCryptContext cryptContext, final String databaseName,
                                     final SingleResultCallback<RawBsonDocument> callback) {
        State state = cryptContext.getState();
        switch (state) {
            case NEED_MONGO_COLLINFO:
                collInfo(cryptContext, databaseName, callback);
                break;
            case NEED_MONGO_MARKINGS:
                mark(cryptContext, databaseName, callback);
                break;
            case NEED_MONGO_KEYS:
                fetchKeys(cryptContext, databaseName, callback);
                break;
            case NEED_KMS:
                decryptKeys(cryptContext, databaseName, callback);
                break;
            case READY:
                callback.onResult(cryptContext.finish(), null);
                break;
            default:
                callback.onResult(null, new MongoInternalException("Unsupported encryptor state + " + state));
        }

    }

    private void collInfo(final MongoCryptContext cryptContext, final String databaseName,
                          final SingleResultCallback<RawBsonDocument> callback) {
        collectionInfoRetriever.filter(databaseName, cryptContext.getMongoOperation(), createCallback(
                new SingleResultCallback<BsonDocument>() {
                    @Override
                    public void onResult(final BsonDocument result, final Throwable t) {
                        if (t != null) {
                            callback.onResult(null, t);
                        } else {
                            try {
                                if (result != null) {
                                    cryptContext.addMongoOperationResult(result);
                                }
                                cryptContext.completeMongoOperation();
                                executeStateMachine(cryptContext, databaseName, callback);
                            } catch (Throwable t1) {
                                callback.onResult(null, wrapInClientException(t1));
                            }
                        }
                    }
                }));
    }

    private void mark(final MongoCryptContext cryptContext, final String databaseName,
                      final SingleResultCallback<RawBsonDocument> callback) {
        commandMarker.mark(databaseName, cryptContext.getMongoOperation(), createCallback(new SingleResultCallback<RawBsonDocument>() {
                    @Override
                    public void onResult(final RawBsonDocument result, final Throwable t) {
                        if (t != null) {
                            callback.onResult(null, t);
                        } else {
                            try {
                                cryptContext.addMongoOperationResult(result);
                                cryptContext.completeMongoOperation();
                                executeStateMachine(cryptContext, databaseName, callback);
                            } catch (Throwable t1) {
                                callback.onResult(null, wrapInClientException(t1));
                            }
                        }
                    }
                }));
    }

    private void fetchKeys(final MongoCryptContext cryptContext, final String databaseName,
                           final SingleResultCallback<RawBsonDocument> callback) {
        keyRetriever.find(cryptContext.getMongoOperation(), createCallback(new SingleResultCallback<List<BsonDocument>>() {
                    @Override
                    public void onResult(final List<BsonDocument> results, final Throwable t) {
                        if (t != null) {
                            callback.onResult(null, t);
                        } else {
                            try {
                                for (BsonDocument result : results) {
                                    cryptContext.addMongoOperationResult(result);
                                }
                                cryptContext.completeMongoOperation();
                                executeStateMachine(cryptContext, databaseName, callback);
                            } catch (Throwable t1) {
                                callback.onResult(null, wrapInClientException(t1));
                            }
                        }
                    }
                }));
    }

    private void decryptKeys(final MongoCryptContext cryptContext, final String databaseName,
                             final SingleResultCallback<RawBsonDocument> callback) {
        MongoKeyDecryptor keyDecryptor = cryptContext.nextKeyDecryptor();
        if (keyDecryptor != null) {
            keyManagementService.decryptKey(keyDecryptor, createCallback(new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final Throwable t) {
                    if (t != null) {
                        callback.onResult(null, wrapInClientException(t));
                    } else {
                        decryptKeys(cryptContext, databaseName, callback);
                    }
                }
            }));
        } else {
            try {
                cryptContext.completeKeyDecryptors();
                executeStateMachine(cryptContext, databaseName, callback);
            } catch (Throwable t1) {
                callback.onResult(null, wrapInClientException(t1));
            }
        }
    }

    private MongoClientException wrapInClientException(final Throwable t) {
        return new MongoClientException("Exception in encryption library: " + t.getMessage(), t);
    }

    private <T> SingleResultCallback<T> createCallback(final SingleResultCallback<T> callback) {
        return errorHandlingCallback(callback, LOGGER);
    }
}
