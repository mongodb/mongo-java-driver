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
import com.mongodb.WriteConcern;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.internal.crypt.Crypt;
import com.mongodb.reactivestreams.client.internal.crypt.Crypts;
import com.mongodb.reactivestreams.client.vault.ClientEncryption;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

public class ClientEncryptionImpl implements ClientEncryption {

    private final Crypt crypt;
    private final ClientEncryptionSettings options;
    private final MongoClient keyVaultClient;

    public ClientEncryptionImpl(final ClientEncryptionSettings options) {
        this.keyVaultClient = MongoClients.create(options.getKeyVaultMongoClientSettings());
        this.crypt = Crypts.create(keyVaultClient, options);
        this.options = options;
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
    public void close() {
        keyVaultClient.close();
        crypt.close();
    }
}
