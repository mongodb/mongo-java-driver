/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client.internal.vault;

import com.mongodb.ClientEncryptionSettings;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoConfigurationException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoUpdatedEncryptedFieldsException;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateEncryptedCollectionParams;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.model.vault.RewrapManyDataKeyOptions;
import com.mongodb.client.model.vault.RewrapManyDataKeyResult;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.reactivestreams.client.internal.OneShotPublisher;
import com.mongodb.reactivestreams.client.internal.crypt.Crypt;
import com.mongodb.reactivestreams.client.internal.crypt.Crypts;
import com.mongodb.reactivestreams.client.vault.ClientEncryption;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.capi.MongoCryptHelper.validateRewrapManyDataKeyOptions;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class ClientEncryptionImpl implements ClientEncryption {
    private final Crypt crypt;
    private final ClientEncryptionSettings options;
    private final MongoClient keyVaultClient;
    private final MongoCollection<BsonDocument> collection;

    public ClientEncryptionImpl(final ClientEncryptionSettings options) {
        this(MongoClients.create(options.getKeyVaultMongoClientSettings()), options);
    }

    public ClientEncryptionImpl(final MongoClient keyVaultClient, final ClientEncryptionSettings options) {
        this.keyVaultClient = keyVaultClient;
        this.crypt = Crypts.create(keyVaultClient, options);
        this.options = options;
        MongoNamespace namespace = new MongoNamespace(options.getKeyVaultNamespace());
        this.collection = keyVaultClient.getDatabase(namespace.getDatabaseName())
                .getCollection(namespace.getCollectionName(), BsonDocument.class)
                .withWriteConcern(WriteConcern.MAJORITY)
                .withReadConcern(ReadConcern.MAJORITY);
    }

    @Override
    public Publisher<BsonBinary> createDataKey(final String kmsProvider) {
        return createDataKey(kmsProvider, new DataKeyOptions());
    }

    @Override
    public Publisher<BsonBinary> createDataKey(final String kmsProvider, final DataKeyOptions dataKeyOptions) {
        return crypt.createDataKey(kmsProvider, dataKeyOptions)
                .flatMap(dataKeyDocument -> {
                    MongoNamespace namespace = new MongoNamespace(options.getKeyVaultNamespace());
                    return Mono.from(keyVaultClient.getDatabase(namespace.getDatabaseName())
                                             .getCollection(namespace.getCollectionName(), BsonDocument.class)
                                             .withWriteConcern(WriteConcern.MAJORITY)
                                             .insertOne(dataKeyDocument))
                            .map(i -> dataKeyDocument.getBinary("_id"));
                });
    }

    @Override
    public Publisher<BsonBinary> encrypt(final BsonValue value, final EncryptOptions options) {
        return crypt.encryptExplicitly(value, options);
    }

    @Override
    public Publisher<BsonDocument> encryptExpression(final Bson expression, final EncryptOptions options) {
        return crypt.encryptExpression(expression.toBsonDocument(BsonDocument.class, collection.getCodecRegistry()), options);
    }

    @Override
    public Publisher<BsonValue> decrypt(final BsonBinary value) {
        return crypt.decryptExplicitly(value);
    }

    @Override
    public Publisher<DeleteResult> deleteKey(final BsonBinary id) {
        return collection.deleteOne(Filters.eq("_id", id));
    }

    @Override
    public Publisher<BsonDocument> getKey(final BsonBinary id) {
        return collection.find(Filters.eq("_id", id)).first();
    }

    @Override
    public FindPublisher<BsonDocument> getKeys() {
        return collection.find();
    }

    @Override
    public Publisher<BsonDocument> addKeyAltName(final BsonBinary id, final String keyAltName) {
        return collection.findOneAndUpdate(Filters.eq("_id", id), Updates.addToSet("keyAltNames", keyAltName));
    }

    @Override
    public Publisher<BsonDocument> removeKeyAltName(final BsonBinary id, final String keyAltName) {
        BsonDocument updateDocument = new BsonDocument()
                .append("$set", new BsonDocument()
                        .append("keyAltNames", new BsonDocument()
                                .append("$cond", new BsonArray(asList(
                                        new BsonDocument()
                                                .append("$eq", new BsonArray(asList(
                                                        new BsonString("$keyAltNames"),
                                                        new BsonArray(singletonList(new BsonString(keyAltName)))))),
                                        new BsonString("$$REMOVE"),
                                        new BsonDocument()
                                                .append("$filter", new BsonDocument()
                                                        .append("input", new BsonString("$keyAltNames"))
                                                        .append("cond", new BsonDocument()
                                                                .append("$ne", new BsonArray(asList(
                                                                        new BsonString("$$this"),
                                                                        new BsonString(keyAltName))))))
                                )))
                        )
                );
        return collection.findOneAndUpdate(Filters.eq("_id", id), singletonList(updateDocument));
    }

    @Override
    public Publisher<BsonDocument> getKeyByAltName(final String keyAltName) {
        return collection.find(Filters.eq("keyAltNames", keyAltName)).first();
    }

    @Override
    public Publisher<RewrapManyDataKeyResult> rewrapManyDataKey(final Bson filter) {
        return rewrapManyDataKey(filter, new RewrapManyDataKeyOptions());
    }

    @Override
    public Publisher<RewrapManyDataKeyResult> rewrapManyDataKey(final Bson filter, final RewrapManyDataKeyOptions options) {
        return Mono.fromRunnable(() -> validateRewrapManyDataKeyOptions(options)).then(
                crypt.rewrapManyDataKey(filter.toBsonDocument(BsonDocument.class, collection.getCodecRegistry()), options)
                        .flatMap(results -> {
                            if (results.isEmpty()) {
                                return Mono.fromCallable(RewrapManyDataKeyResult::new);
                            }
                            List<UpdateOneModel<BsonDocument>> updateModels = results.getArray("v", new BsonArray()).stream().map(v -> {
                                BsonDocument updateDocument = v.asDocument();
                                return new UpdateOneModel<BsonDocument>(Filters.eq(updateDocument.get("_id")),
                                        Updates.combine(
                                                Updates.set("masterKey", updateDocument.get("masterKey")),
                                                Updates.set("keyMaterial", updateDocument.get("keyMaterial")),
                                                Updates.currentDate("updateDate"))
                                );
                            }).collect(Collectors.toList());
                            return Mono.from(collection.bulkWrite(updateModels)).map(RewrapManyDataKeyResult::new);
                        }));
    }

    @Override
    public Publisher<BsonDocument> createEncryptedCollection(final MongoDatabase database, final String collectionName,
            final CreateCollectionOptions createCollectionOptions, final CreateEncryptedCollectionParams createEncryptedCollectionParams) {
        notNull("collectionName", collectionName);
        notNull("createCollectionOptions", createCollectionOptions);
        notNull("createEncryptedCollectionParams", createEncryptedCollectionParams);
        // this publisher captures and mutates state, which is why it has to be one-shot
        return OneShotPublisher.from(Mono.defer(() -> {
            MongoNamespace namespace = new MongoNamespace(database.getName(), collectionName);
            Bson rawEncryptedFields = createCollectionOptions.getEncryptedFields();
            if (rawEncryptedFields == null) {
                throw new MongoConfigurationException(format("`encryptedFields` is not configured for the collection %s.", namespace));
            }
            CodecRegistry codecRegistry = options.getKeyVaultMongoClientSettings() == null
                    ? MongoClientSettings.getDefaultCodecRegistry()
                    : options.getKeyVaultMongoClientSettings().getCodecRegistry();
            BsonDocument actualEncryptedFields = rawEncryptedFields.toBsonDocument(BsonDocument.class, codecRegistry).clone();
            BsonValue fields = actualEncryptedFields.get("fields");
            Mono<Void> createCollectionMono;
            if (fields != null) {
                if (!fields.isArray()) {
                    throw new MongoConfigurationException(format("`encryptedFields` is incorrectly configured for the collection %s."
                            + " `encryptedFields.fields` must be an array, but is %s type.", namespace, fields.getBsonType()));
                }
                String kmsProvider = createEncryptedCollectionParams.getKmsProvider();
                DataKeyOptions dataKeyOptions = new DataKeyOptions();
                BsonDocument masterKey = createEncryptedCollectionParams.getMasterKey();
                if (masterKey != null) {
                    dataKeyOptions.masterKey(masterKey);
                }
                String keyIdBsonKey = "keyId";
                AtomicBoolean dataKeyMayBeCreated = new AtomicBoolean();
                Iterable<Mono<BsonDocument>> publishersOfUpdatedFields = () -> fields.asArray()
                        .stream()
                        .filter(BsonValue::isDocument)
                        .map(BsonValue::asDocument)
                        .filter(field -> field.containsKey(keyIdBsonKey))
                        .filter(field -> Objects.equals(field.get(keyIdBsonKey), BsonNull.VALUE))
                        // here we rely on the `createDataKey` publisher being cold, i.e., doing nothing until it is subscribed to
                        .map(field -> Mono.fromDirect(createDataKey(kmsProvider, dataKeyOptions))
                                // This is the closest we can do with reactive streams to setting the `dataKeyMayBeCreated` flag
                                // immediately before calling `createDataKey`.
                                .doOnSubscribe(subscription -> dataKeyMayBeCreated.set(true))
                                .doOnNext(dataKeyId -> field.put(keyIdBsonKey, dataKeyId))
                                .map(dataKeyId -> field)
                        )
                        .iterator();
                Flux<BsonDocument> publisherOfUpdatedFields = Flux.concat(publishersOfUpdatedFields);
                createCollectionMono = publisherOfUpdatedFields
                        // All write actions in `doOnNext` above happen-before the completion (`onComplete`/`onError`) signals
                        // for this publisher, because all signals are serial. `thenEmpty` further guarantees that the completion signal
                        // for this publisher happens-before the `onSubscribe` signal for the publisher passed to it
                        // (the next publisher, which creates a collection).
                        // `defer` defers calling `createCollection` until the next publisher is subscribed to.
                        // Therefore, all write actions in `doOnNext` above happen-before the invocation of `createCollection`,
                        // which means `createCollection` is guaranteed to observe all those write actions, i.e.,
                        // it is guaranteed to observe the updated document via the `actualEncryptedFields` reference.
                        // Similarly, the updated document is observed in all other places below via the `actualEncryptedFields` reference.
                        .thenEmpty(Mono.defer(() -> Mono.fromDirect(database.createCollection(collectionName,
                                createCollectionOptions.clone().encryptedFields(actualEncryptedFields))))
                        )
                        .doOnError(RuntimeException.class, e -> {
                            if (dataKeyMayBeCreated.get()) {
                                throw new MongoUpdatedEncryptedFieldsException(actualEncryptedFields, format("Failed to create %s.", namespace), e);
                            } else {
                                throw e;
                            }
                        });
            } else {
                createCollectionMono = Mono.fromDirect(database.createCollection(collectionName, createCollectionOptions));
            }
            return createCollectionMono
                    .then(Mono.defer(() -> Mono.just(actualEncryptedFields)));
        }));
    }

    @Override
    public void close() {
        keyVaultClient.close();
        crypt.close();
    }
}
