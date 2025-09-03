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

import com.mongodb.ClientEncryptionSettings;
import com.mongodb.MongoConfigurationException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoUpdatedEncryptedFieldsException;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
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
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.client.internal.TimeoutHelper.collectionWithTimeout;
import static com.mongodb.client.internal.TimeoutHelper.databaseWithTimeout;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;
import static com.mongodb.internal.capi.MongoCryptHelper.validateRewrapManyDataKeyOptions;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.bson.internal.BsonUtil.mutableDeepCopy;

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

    @VisibleForTesting(otherwise = PRIVATE)
    public ClientEncryptionImpl(final MongoClient keyVaultClient, final ClientEncryptionSettings options) {
        this.keyVaultClient = keyVaultClient;
        this.crypt = Crypts.create(keyVaultClient, options);
        this.options = options;
        MongoNamespace namespace = new MongoNamespace(options.getKeyVaultNamespace());
        this.collection = getVaultCollection(keyVaultClient, options, namespace);
    }

    private static MongoCollection<BsonDocument> getVaultCollection(final MongoClient keyVaultClient,
                                                                    final ClientEncryptionSettings options,
                                                                    final MongoNamespace namespace) {
        MongoCollection<BsonDocument> vaultCollection = keyVaultClient.getDatabase(namespace.getDatabaseName())
                .getCollection(namespace.getCollectionName(), BsonDocument.class)
                .withWriteConcern(WriteConcern.MAJORITY)
                .withReadConcern(ReadConcern.MAJORITY);

        Long timeoutMs = options.getTimeout(MILLISECONDS);
        if (timeoutMs != null){
            vaultCollection = vaultCollection.withTimeout(timeoutMs, MILLISECONDS);
        }
        return vaultCollection;
    }

    @Override
    public BsonBinary createDataKey(final String kmsProvider) {
        return createDataKey(kmsProvider, new DataKeyOptions());
    }

    @Override
    public BsonBinary createDataKey(final String kmsProvider, final DataKeyOptions dataKeyOptions) {
        Timeout operationTimeout = startTimeout();
       return createDataKey(kmsProvider, dataKeyOptions, operationTimeout);
    }

    public BsonBinary createDataKey(final String kmsProvider, final DataKeyOptions dataKeyOptions, @Nullable final Timeout operationTimeout) {
        BsonDocument dataKeyDocument = crypt.createDataKey(kmsProvider, dataKeyOptions, operationTimeout);
        collectionWithTimeout(collection, "Data key insertion exceeded the timeout limit.", operationTimeout).insertOne(dataKeyDocument);
        return dataKeyDocument.getBinary("_id");
    }

    @Override
    public BsonBinary encrypt(final BsonValue value, final EncryptOptions options) {
        Timeout operationTimeout = startTimeout();
        return crypt.encryptExplicitly(value, options, operationTimeout);
    }

    @Override
    public BsonDocument encryptExpression(final Bson expression, final EncryptOptions options) {
        Timeout operationTimeout = startTimeout();
        return crypt.encryptExpression(expression.toBsonDocument(BsonDocument.class, collection.getCodecRegistry()), options,
                operationTimeout);
    }

    @Override
    public BsonValue decrypt(final BsonBinary value) {
        Timeout operationTimeout = startTimeout();
        return crypt.decryptExplicitly(value, operationTimeout);
    }

    @Override
    public DeleteResult deleteKey(final BsonBinary id) {
        return collectionWithTimeout(collection, startTimeout()).deleteOne(Filters.eq("_id", id));
    }

    @Override
    public BsonDocument getKey(final BsonBinary id) {
        return collectionWithTimeout(collection, startTimeout()).find(Filters.eq("_id", id)).first();
    }

    @Override
    public FindIterable<BsonDocument> getKeys() {
        return collectionWithTimeout(collection, startTimeout()).find();
    }

    @Override
    public BsonDocument addKeyAltName(final BsonBinary id, final String keyAltName) {
        return collection.findOneAndUpdate(Filters.eq("_id", id), Updates.addToSet("keyAltNames", keyAltName));
    }

    @Override
    public BsonDocument removeKeyAltName(final BsonBinary id, final String keyAltName) {
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
    public BsonDocument getKeyByAltName(final String keyAltName) {
        return collection.find(Filters.eq("keyAltNames", keyAltName)).first();
    }

    @Override
    public RewrapManyDataKeyResult rewrapManyDataKey(final Bson filter) {
        return rewrapManyDataKey(filter, new RewrapManyDataKeyOptions());
    }

    @Override
    public RewrapManyDataKeyResult rewrapManyDataKey(final Bson filter, final RewrapManyDataKeyOptions options) {
        validateRewrapManyDataKeyOptions(options);
        Timeout operationTimeout = startTimeout();
        BsonDocument results = crypt.rewrapManyDataKey(filter.toBsonDocument(BsonDocument.class, collection.getCodecRegistry()),
                options, operationTimeout);
        if (results.isEmpty()) {
            return new RewrapManyDataKeyResult();
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
        BulkWriteResult bulkWriteResult = collectionWithTimeout(collection, operationTimeout).bulkWrite(updateModels);
        return new RewrapManyDataKeyResult(bulkWriteResult);
    }

    @Override
    public BsonDocument createEncryptedCollection(final MongoDatabase database, final String collectionName,
            final CreateCollectionOptions createCollectionOptions, final CreateEncryptedCollectionParams createEncryptedCollectionParams) {
        notNull("collectionName", collectionName);
        notNull("createCollectionOptions", createCollectionOptions);
        notNull("createEncryptedCollectionParams", createEncryptedCollectionParams);
        Timeout operationTimeout = startTimeout();
        MongoNamespace namespace = new MongoNamespace(database.getName(), collectionName);
        Bson rawEncryptedFields = createCollectionOptions.getEncryptedFields();
        if (rawEncryptedFields == null) {
            throw new MongoConfigurationException(format("`encryptedFields` is not configured for the collection %s.", namespace));
        }
        CodecRegistry codecRegistry = options.getKeyVaultMongoClientSettings().getCodecRegistry();
        BsonDocument encryptedFields = rawEncryptedFields.toBsonDocument(BsonDocument.class, codecRegistry);
        BsonValue fields = encryptedFields.get("fields");
        if (fields != null && fields.isArray()) {
            String kmsProvider = createEncryptedCollectionParams.getKmsProvider();
            DataKeyOptions dataKeyOptions = new DataKeyOptions();
            BsonDocument masterKey = createEncryptedCollectionParams.getMasterKey();
            if (masterKey != null) {
                dataKeyOptions.masterKey(masterKey);
            }
            String keyIdBsonKey = "keyId";
            BsonDocument maybeUpdatedEncryptedFields = mutableDeepCopy(encryptedFields);
            // only the mutability of `dataKeyMightBeCreated` is important, it does not need to be thread-safe
            AtomicBoolean dataKeyMightBeCreated = new AtomicBoolean();
            try {
                maybeUpdatedEncryptedFields.get("fields").asArray()
                        .stream()
                        .filter(BsonValue::isDocument)
                        .map(BsonValue::asDocument)
                        .filter(field -> field.containsKey(keyIdBsonKey))
                        .filter(field -> Objects.equals(field.get(keyIdBsonKey), BsonNull.VALUE))
                        .forEachOrdered(field -> {
                            // It is crucial to set the `dataKeyMightBeCreated` flag either immediately before calling `createDataKey`,
                            // or after that in a `finally` block.
                            dataKeyMightBeCreated.set(true);
                            BsonBinary dataKeyId = createDataKey(kmsProvider, dataKeyOptions, operationTimeout);
                            field.put(keyIdBsonKey, dataKeyId);
                        });
                databaseWithTimeout(database, operationTimeout).createCollection(collectionName,
                        new CreateCollectionOptions(createCollectionOptions).encryptedFields(maybeUpdatedEncryptedFields));
                return maybeUpdatedEncryptedFields;
            } catch (Exception e) {
                if (dataKeyMightBeCreated.get()) {
                    throw new MongoUpdatedEncryptedFieldsException(maybeUpdatedEncryptedFields, format("Failed to create %s.", namespace), e);
                } else {
                    throw e;
                }
            }
        } else {
            databaseWithTimeout(database, operationTimeout).createCollection(collectionName, createCollectionOptions);
            return encryptedFields;
        }
    }

    @Override
    public void close() {
        crypt.close();
        keyVaultClient.close();
    }

    @Nullable
    private Timeout startTimeout() {
        return TimeoutContext.startTimeout(options.getTimeout(MILLISECONDS));
    }
}
