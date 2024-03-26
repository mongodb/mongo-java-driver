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

package com.mongodb.reactivestreams.client.internal.crypt;

import com.mongodb.MongoClientException;
import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.annotations.Beta;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.model.vault.RewrapManyDataKeyOptions;
import com.mongodb.crypt.capi.MongoCrypt;
import com.mongodb.crypt.capi.MongoCryptContext;
import com.mongodb.crypt.capi.MongoCryptException;
import com.mongodb.crypt.capi.MongoDataKeyOptions;
import com.mongodb.crypt.capi.MongoKeyDecryptor;
import com.mongodb.crypt.capi.MongoRewrapManyDataKeyOptions;
import com.mongodb.internal.capi.MongoCryptHelper;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.MongoClient;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.io.Closeable;
import java.util.Map;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.crypt.capi.MongoCryptContext.State;
import static com.mongodb.internal.client.vault.EncryptOptionsHelper.asMongoExplicitEncryptOptions;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class Crypt implements Closeable {
    private static final RawBsonDocument EMPTY_RAW_BSON_DOCUMENT = RawBsonDocument.parse("{}");
    private static final Logger LOGGER = Loggers.getLogger("client");
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
     * @param command      the unencrypted command
     */
    public Mono<RawBsonDocument> encrypt(final String databaseName, final RawBsonDocument command, @Nullable final Timeout operationTimeout) {
        notNull("databaseName", databaseName);
        notNull("command", command);

        if (bypassAutoEncryption) {
            return Mono.fromCallable(() -> command);
        }
        return executeStateMachine(() -> mongoCrypt.createEncryptionContext(databaseName, command), databaseName, operationTimeout);
    }

    /**
     * Decrypt the given command response
     *
     * @param commandResponse the encrypted command response
     */
    public Mono<RawBsonDocument> decrypt(final RawBsonDocument commandResponse, @Nullable final Timeout operationTimeout) {
        notNull("commandResponse", commandResponse);
        return executeStateMachine(() -> mongoCrypt.createDecryptionContext(commandResponse), operationTimeout)
                .onErrorMap(this::wrapInClientException);
    }

    /**
     * Create a data key.
     *
     * @param kmsProvider the KMS provider to create the data key for
     * @param options     the data key options
     */
    public Mono<RawBsonDocument> createDataKey(final String kmsProvider, final DataKeyOptions options, @Nullable final Timeout operationTimeout) {
        notNull("kmsProvider", kmsProvider);
        notNull("options", options);
        return executeStateMachine(() ->
            mongoCrypt.createDataKeyContext(kmsProvider,
                                            MongoDataKeyOptions.builder()
                                                    .keyAltNames(options.getKeyAltNames())
                                                    .masterKey(options.getMasterKey())
                                                    .keyMaterial(options.getKeyMaterial())
                                                    .build()), operationTimeout);
    }

    /**
     * Encrypt the given value with the given options
     *
     * @param value   the value to encrypt
     * @param options the options
     */
    public Mono<BsonBinary> encryptExplicitly(final BsonValue value, final EncryptOptions options, @Nullable final Timeout operationTimeout) {
        return executeStateMachine(() ->
            mongoCrypt.createExplicitEncryptionContext(new BsonDocument("v", value), asMongoExplicitEncryptOptions(options)),
                operationTimeout)
                .map(result -> result.getBinary("v"));
    }

    /**
     * Encrypts a Match Expression or Aggregate Expression to query a range index.
     *
     * @param expression the Match Expression or Aggregate Expression
     * @param options    the options
     * @return the encrypted expression
     * @since 4.9
     * @mongodb.server.release 6.2
     */
    @Beta(Beta.Reason.SERVER)
    public Mono<BsonDocument> encryptExpression(final BsonDocument expression, final EncryptOptions options, @Nullable final Timeout operationTimeout) {
        return executeStateMachine(() ->
                mongoCrypt.createEncryptExpressionContext(new BsonDocument("v", expression), asMongoExplicitEncryptOptions(options)), operationTimeout
        ).map(result -> result.getDocument("v"));
    }

    /**
     * Decrypt the given encrypted value.
     *
     * @param value the encrypted value
     */
    public Mono<BsonValue> decryptExplicitly(final BsonBinary value, @Nullable final Timeout operationTimeout) {
        return executeStateMachine(() -> mongoCrypt.createExplicitDecryptionContext(new BsonDocument("v", value)), operationTimeout)
                .map(result -> result.get("v"));
    }

    /**
     * Rewrap data key
     * @param filter the filter
     * @param options the rewrap many data key options
     * @return the decrypted value
     */
    public Mono<RawBsonDocument> rewrapManyDataKey(final BsonDocument filter, final RewrapManyDataKeyOptions options, @Nullable final Timeout operationTimeout) {
        return executeStateMachine(() ->
                mongoCrypt.createRewrapManyDatakeyContext(filter,
                        MongoRewrapManyDataKeyOptions
                                .builder()
                                .provider(options.getProvider())
                                .masterKey(options.getMasterKey())
                                .build()), operationTimeout
        );
    }


    @Override
    @SuppressWarnings("try")
    public void close() {
        //noinspection EmptyTryBlock
        try (MongoCrypt ignored = this.mongoCrypt;
             CommandMarker ignored1 = this.commandMarker;
             MongoClient ignored2 = this.collectionInfoRetrieverClient;
             MongoClient ignored3 = this.keyVaultClient;
             KeyManagementService ignored4 = this.keyManagementService
        ) {
            // just using try-with-resources to ensure they all get closed, even in the case of exceptions
        }
    }

    private Mono<RawBsonDocument> executeStateMachine(final Supplier<MongoCryptContext> cryptContextSupplier,
                                                      @Nullable final Timeout operationTimeout) {
        return executeStateMachine(cryptContextSupplier, null, operationTimeout);
    }

    private Mono<RawBsonDocument> executeStateMachine(final Supplier<MongoCryptContext> cryptContextSupplier,
                                                      @Nullable final String databaseName, @Nullable final Timeout operationTimeout) {
        try {
            MongoCryptContext cryptContext = cryptContextSupplier.get();
            return Mono.<RawBsonDocument>create(sink -> executeStateMachineWithSink(cryptContext, databaseName, sink, operationTimeout))
                    .onErrorMap(this::wrapInClientException)
                    .doFinally(s -> cryptContext.close());
        } catch (MongoCryptException e) {
            return Mono.error(wrapInClientException(e));
        }
    }

    private void executeStateMachineWithSink(final MongoCryptContext cryptContext, @Nullable final String databaseName,
            final MonoSink<RawBsonDocument> sink, @Nullable final Timeout operationTimeout) {
        State state = cryptContext.getState();
        switch (state) {
            case NEED_MONGO_COLLINFO:
                collInfo(cryptContext, databaseName, sink, operationTimeout);
                break;
            case NEED_MONGO_MARKINGS:
                mark(cryptContext, databaseName, sink, operationTimeout);
                break;
            case NEED_KMS_CREDENTIALS:
                fetchCredentials(cryptContext, databaseName, sink, operationTimeout);
                break;
            case NEED_MONGO_KEYS:
                fetchKeys(cryptContext, databaseName, sink, operationTimeout);
                break;
            case NEED_KMS:
                decryptKeys(cryptContext, databaseName, sink, operationTimeout);
                break;
            case READY:
                sink.success(cryptContext.finish());
                break;
            case DONE:
                sink.success(EMPTY_RAW_BSON_DOCUMENT);
                break;
            default:
                sink.error(new MongoInternalException("Unsupported encryptor state + " + state));
        }
    }

    private void fetchCredentials(final MongoCryptContext cryptContext, @Nullable final String databaseName,
            final MonoSink<RawBsonDocument> sink, @Nullable final Timeout operationTimeout) {
        try {
            cryptContext.provideKmsProviderCredentials(MongoCryptHelper.fetchCredentials(kmsProviders, kmsProviderPropertySuppliers));
            executeStateMachineWithSink(cryptContext, databaseName, sink, operationTimeout);
        } catch (Exception e) {
            sink.error(e);
        }
    }

    private void collInfo(final MongoCryptContext cryptContext,
                          @Nullable final String databaseName,
                          final MonoSink<RawBsonDocument> sink, @Nullable final Timeout operationTimeout) {
        if (collectionInfoRetriever == null) {
            sink.error(new IllegalStateException("Missing collection Info retriever"));
        } else if (databaseName == null) {
            sink.error(new IllegalStateException("Missing database name"));
        } else {
            collectionInfoRetriever.filter(databaseName, cryptContext.getMongoOperation(), operationTimeout)
                    .doOnSuccess(result -> {
                        if (result != null) {
                            cryptContext.addMongoOperationResult(result);
                        }
                        cryptContext.completeMongoOperation();
                        executeStateMachineWithSink(cryptContext, databaseName, sink, operationTimeout);
                    })
                    .doOnError(t -> sink.error(MongoException.fromThrowableNonNull(t)))
                    .subscribe();
        }
    }

    private void mark(final MongoCryptContext cryptContext,
                      @Nullable final String databaseName,
                      final MonoSink<RawBsonDocument> sink,
                      @Nullable final Timeout operationTimeout) {
        if (commandMarker == null) {
            sink.error(wrapInClientException(new MongoInternalException("Missing command marker")));
        } else if (databaseName == null) {
            sink.error(wrapInClientException(new IllegalStateException("Missing database name")));
        } else {
            commandMarker.mark(databaseName, cryptContext.getMongoOperation(), operationTimeout)
                    .doOnSuccess(result -> {
                        cryptContext.addMongoOperationResult(result);
                        cryptContext.completeMongoOperation();
                        executeStateMachineWithSink(cryptContext, databaseName, sink, operationTimeout);
                    })
                    .doOnError(e -> sink.error(wrapInClientException(e)))
                    .subscribe();
        }
    }

    private void fetchKeys(final MongoCryptContext cryptContext,
                           @Nullable final String databaseName,
                           final MonoSink<RawBsonDocument> sink,
                           @Nullable final Timeout operationTimeout) {
        keyRetriever.find(cryptContext.getMongoOperation(), operationTimeout)
                .doOnSuccess(results -> {
                    for (BsonDocument result : results) {
                        cryptContext.addMongoOperationResult(result);
                    }
                    cryptContext.completeMongoOperation();
                    executeStateMachineWithSink(cryptContext, databaseName, sink, operationTimeout);
                })
                .doOnError(t -> sink.error(MongoException.fromThrowableNonNull(t)))
                .subscribe();
    }

    private void decryptKeys(final MongoCryptContext cryptContext,
                             @Nullable final String databaseName,
                             final MonoSink<RawBsonDocument> sink,
                             @Nullable final Timeout operationTimeout) {
        MongoKeyDecryptor keyDecryptor = cryptContext.nextKeyDecryptor();
        if (keyDecryptor != null) {
            keyManagementService.decryptKey(keyDecryptor, operationTimeout)
                    .doOnSuccess(r -> decryptKeys(cryptContext, databaseName, sink, operationTimeout))
                    .doOnError(e -> sink.error(wrapInClientException(e)))
                    .subscribe();
        } else {
            Mono.fromRunnable(cryptContext::completeKeyDecryptors)
                    .doOnSuccess(r -> executeStateMachineWithSink(cryptContext, databaseName, sink, operationTimeout))
                    .doOnError(e -> sink.error(wrapInClientException(e)))
                    .subscribe();
        }
    }

    private Throwable wrapInClientException(final Throwable t) {
        if (t instanceof MongoClientException) {
            return t;
        }
        return new MongoClientException("Exception in encryption library: " + t.getMessage(), t);
    }

}
