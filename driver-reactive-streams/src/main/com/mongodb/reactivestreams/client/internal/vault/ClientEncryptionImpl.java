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
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
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
import com.mongodb.reactivestreams.client.internal.crypt.Crypt;
import com.mongodb.reactivestreams.client.internal.crypt.Crypts;
import com.mongodb.reactivestreams.client.vault.ClientEncryption;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

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
    public Publisher<RewrapManyDataKeyResult> rewrapManyDataKey(final BsonDocument filter) {
        return rewrapManyDataKey(filter, new RewrapManyDataKeyOptions());
    }

    @Override
    public Publisher<RewrapManyDataKeyResult> rewrapManyDataKey(final BsonDocument filter, final RewrapManyDataKeyOptions options) {
        return crypt.rewrapManyDataKey(filter, options).flatMap(results -> {
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
        });
    }

    @Override
    public void close() {
        keyVaultClient.close();
        crypt.close();
    }
}
