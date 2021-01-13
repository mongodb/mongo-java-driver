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
import com.mongodb.reactivestreams.client.MongoClient;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.io.Closeable;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.crypt.capi.MongoCryptContext.State;
import static java.lang.String.format;

public class Crypt implements Closeable {
    private static final Logger LOGGER = Loggers.getLogger("client");
    private final MongoCrypt mongoCrypt;
    private final CollectionInfoRetriever collectionInfoRetriever;
    private final CommandMarker commandMarker;
    private final KeyRetriever keyRetriever;
    private final KeyManagementService keyManagementService;
    private final boolean bypassAutoEncryption;
    @Nullable
    private final MongoClient internalClient;

    /**
     * Create an instance to use for explicit encryption and decryption, and data key creation.
     *
     * @param mongoCrypt           the mongoCrypt wrapper
     * @param keyRetriever         the key retriever
     * @param keyManagementService the key management service
     */
    Crypt(final MongoCrypt mongoCrypt, final KeyRetriever keyRetriever, final KeyManagementService keyManagementService) {
        this(mongoCrypt, null, null, keyRetriever, keyManagementService, false, null);
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
    Crypt(final MongoCrypt mongoCrypt,
          @Nullable final CollectionInfoRetriever collectionInfoRetriever,
          @Nullable final CommandMarker commandMarker,
          final KeyRetriever keyRetriever,
          final KeyManagementService keyManagementService,
          final boolean bypassAutoEncryption,
          @Nullable final MongoClient internalClient) {
        this.mongoCrypt = mongoCrypt;
        this.collectionInfoRetriever = collectionInfoRetriever;
        this.commandMarker = commandMarker;
        this.keyRetriever = keyRetriever;
        this.keyManagementService = keyManagementService;
        this.bypassAutoEncryption = bypassAutoEncryption;
        this.internalClient = internalClient;
    }

    /**
     * Encrypt the given command
     *
     * @param databaseName the namespace
     * @param command      the unencrypted command
     */
    public Mono<RawBsonDocument> encrypt(final String databaseName, final RawBsonDocument command) {
        notNull("databaseName", databaseName);
        notNull("command", command);

        if (bypassAutoEncryption) {
            return Mono.fromCallable(() -> command);
        }
        return executeStateMachine(() -> mongoCrypt.createEncryptionContext(databaseName, command), databaseName);
    }

    /**
     * Decrypt the given command response
     *
     * @param commandResponse the encrypted command response
     */
    public Mono<RawBsonDocument> decrypt(final RawBsonDocument commandResponse) {
        notNull("commandResponse", commandResponse);
        return executeStateMachine(() -> mongoCrypt.createDecryptionContext(commandResponse));
    }

    /**
     * Create a data key.
     *
     * @param kmsProvider the KMS provider to create the data key for
     * @param options     the data key options
     */
    public Mono<RawBsonDocument> createDataKey(final String kmsProvider, final DataKeyOptions options) {
        notNull("kmsProvider", kmsProvider);
        notNull("options", options);
        return executeStateMachine(() ->
            mongoCrypt.createDataKeyContext(kmsProvider,
                                            MongoDataKeyOptions.builder()
                                                    .keyAltNames(options.getKeyAltNames())
                                                    .masterKey(options.getMasterKey())
                                                    .build()));
    }

    /**
     * Encrypt the given value with the given options
     *
     * @param value   the value to encrypt
     * @param options the options
     */
    public Mono<BsonBinary> encryptExplicitly(final BsonValue value, final EncryptOptions options) {
        notNull("value", value);
        notNull("options", options);

        return executeStateMachine(() -> {
            MongoExplicitEncryptOptions.Builder encryptOptionsBuilder = MongoExplicitEncryptOptions.builder()
                    .algorithm(options.getAlgorithm());

            if (options.getKeyId() != null) {
                encryptOptionsBuilder.keyId(options.getKeyId());
            }

            if (options.getKeyAltName() != null) {
                encryptOptionsBuilder.keyAltName(options.getKeyAltName());
            }

            return mongoCrypt.createExplicitEncryptionContext(new BsonDocument("v", value), encryptOptionsBuilder.build());
        }).map(result -> result.getBinary("v"));
    }

    /**
     * Decrypt the given encrypted value.
     *
     * @param value the encrypted value
     */
    public Mono<BsonValue> decryptExplicitly(final BsonBinary value) {
        notNull("value", value);
        return executeStateMachine(() -> mongoCrypt.createExplicitDecryptionContext(new BsonDocument("v", value)))
                .map(result -> result.get("v"));
    }

    @Override
    public void close() {
        //noinspection EmptyTryBlock
        try (MongoCrypt mongoCrypt = this.mongoCrypt;
             CommandMarker commandMarker = this.commandMarker;
             MongoClient internalClient = this.internalClient;
             KeyManagementService keyManagementService = this.keyManagementService
        ) {
            // just using try-with-resources to ensure they all get closed, even in the case of exceptions
        }
    }

    private Mono<RawBsonDocument> executeStateMachine(final Supplier<MongoCryptContext> cryptContextSupplier) {
        return executeStateMachine(cryptContextSupplier, null);
    }

    private Mono<RawBsonDocument> executeStateMachine(final Supplier<MongoCryptContext> cryptContextSupplier,
                                                      @Nullable final String databaseName) {
        try {
            MongoCryptContext cryptContext = cryptContextSupplier.get();
            return Mono.<RawBsonDocument>create(sink -> executeStateMachineWithSink(cryptContext, databaseName, sink))
                    .doFinally(s -> cryptContext.close())
                    .doOnError(t -> LOGGER.error(format("Crypt error: %s", t.getMessage())))
                    .doOnError(MongoCryptException.class, this::wrapInClientException);
        } catch (MongoCryptException e) {
            return Mono.error(wrapInClientException(e));
        }
    }

    private void executeStateMachineWithSink(final MongoCryptContext cryptContext, @Nullable final String databaseName,
            final MonoSink<RawBsonDocument> sink) {
        State state = cryptContext.getState();
        LOGGER.info("executeStateMachine: " + state);
        switch (state) {
            case NEED_MONGO_COLLINFO:
                collInfo(cryptContext, databaseName, sink);
                break;
            case NEED_MONGO_MARKINGS:
                mark(cryptContext, databaseName, sink);
                break;
            case NEED_MONGO_KEYS:
                fetchKeys(cryptContext, databaseName, sink);
                break;
            case NEED_KMS:
                decryptKeys(cryptContext, databaseName, sink);
                break;
            case READY:
                sink.success(cryptContext.finish());
                break;
            default:
                sink.error(new MongoInternalException("Unsupported encryptor state + " + state));
        }
    }

    private void collInfo(final MongoCryptContext cryptContext,
                          @Nullable final String databaseName,
                          final MonoSink<RawBsonDocument> sink) {
        if (collectionInfoRetriever == null) {
            sink.error(new IllegalStateException("Missing collection Info retriever"));
        } else if (databaseName == null) {
            sink.error(new IllegalStateException("Missing database name"));
        } else {
            collectionInfoRetriever.filter(databaseName, cryptContext.getMongoOperation())
                    .doOnSuccess(result -> {
                        if (result != null) {
                            cryptContext.addMongoOperationResult(result);
                        }
                        cryptContext.completeMongoOperation();
                        executeStateMachineWithSink(cryptContext, databaseName, sink);
                    })
                    .doOnError(t -> sink.error(MongoException.fromThrowableNonNull(t)))
                    .subscribe();
        }
    }

    private void mark(final MongoCryptContext cryptContext,
                      @Nullable final String databaseName,
                      final MonoSink<RawBsonDocument> sink) {
        if (commandMarker == null) {
            sink.error(wrapInClientException(new MongoInternalException("Missing command marker")));
        } else if (databaseName == null) {
            sink.error(wrapInClientException(new IllegalStateException("Missing database name")));
        } else {
            commandMarker.mark(databaseName, cryptContext.getMongoOperation())
                    .doOnSuccess(result -> {
                        cryptContext.addMongoOperationResult(result);
                        cryptContext.completeMongoOperation();
                        executeStateMachineWithSink(cryptContext, databaseName, sink);
                    })
                    .doOnError(e -> sink.error(wrapInClientException(e)))
                    .subscribe();
        }
    }

    private void fetchKeys(final MongoCryptContext cryptContext,
                           @Nullable final String databaseName,
                           final MonoSink<RawBsonDocument> sink) {
        keyRetriever.find(cryptContext.getMongoOperation())
                .doOnSuccess(results -> {
                    for (BsonDocument result : results) {
                        cryptContext.addMongoOperationResult(result);
                    }
                    cryptContext.completeMongoOperation();
                    executeStateMachineWithSink(cryptContext, databaseName, sink);
                })
                .doOnError(t -> sink.error(MongoException.fromThrowableNonNull(t)))
                .subscribe();
    }

    private void decryptKeys(final MongoCryptContext cryptContext,
                             @Nullable final String databaseName,
                             final MonoSink<RawBsonDocument> sink) {
        MongoKeyDecryptor keyDecryptor = cryptContext.nextKeyDecryptor();
        if (keyDecryptor != null) {
            keyManagementService.decryptKey(keyDecryptor)
                    .doOnSuccess(r -> decryptKeys(cryptContext, databaseName, sink))
                    .doOnError(e -> sink.error(wrapInClientException(e)))
                    .subscribe();
        } else {
            Mono.fromRunnable(cryptContext::completeKeyDecryptors)
                    .doOnSuccess(r -> executeStateMachineWithSink(cryptContext, databaseName, sink))
                    .doOnError(e -> sink.error(wrapInClientException(e)))
                    .subscribe();
        }
    }

    private Throwable wrapInClientException(final Throwable t) {
        return new MongoClientException("Exception in encryption library: " + t.getMessage(), t);
    }

}
