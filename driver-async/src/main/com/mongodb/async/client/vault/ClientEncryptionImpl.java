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

package com.mongodb.async.client.vault;

import com.mongodb.ClientEncryptionSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.internal.Crypt;
import com.mongodb.async.client.internal.Crypts;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClients;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.model.vault.EncryptOptions;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;

import java.io.Closeable;

class ClientEncryptionImpl implements ClientEncryption, Closeable {
    private final Crypt crypt;
    private final ClientEncryptionSettings options;
    private final MongoClient keyVaultClient;

    ClientEncryptionImpl(final ClientEncryptionSettings options) {
        this.keyVaultClient = MongoClients.create(options.getKeyVaultMongoClientSettings());
        this.crypt = Crypts.create(keyVaultClient, options);
        this.options = options;
    }

    @Override
    public void createDataKey(final String kmsProvider, final SingleResultCallback<BsonBinary> callback) {
        createDataKey(kmsProvider, new DataKeyOptions(), callback);
    }

    @Override
    public void createDataKey(final String kmsProvider, final DataKeyOptions dataKeyOptions,
                              final SingleResultCallback<BsonBinary> callback) {
        crypt.createDataKey(kmsProvider, dataKeyOptions, new SingleResultCallback<RawBsonDocument>() {
            @Override
            public void onResult(final RawBsonDocument dataKeyDocument, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    MongoNamespace namespace = new MongoNamespace(options.getKeyVaultNamespace());
                    keyVaultClient.getDatabase(namespace.getDatabaseName())
                            .getCollection(namespace.getCollectionName(), BsonDocument.class)
                            .insertOne(dataKeyDocument, new SingleResultCallback<Void>() {
                                @Override
                                public void onResult(final Void result, final Throwable t) {
                                    if (t != null) {
                                        callback.onResult(null, t);
                                    } else {
                                        callback.onResult(dataKeyDocument.getBinary("_id"), null);
                                    }
                                }
                            });
                }
            }
        });
    }

    @Override
    public void encrypt(final BsonValue value, final EncryptOptions options, final SingleResultCallback<BsonBinary> callback) {
        crypt.encryptExplicitly(value, options, callback);
    }

    @Override
    public void decrypt(final BsonBinary value, final SingleResultCallback<BsonValue> callback) {
        crypt.decryptExplicitly(value, callback);
    }

    @Override
    public void close() {
        keyVaultClient.close();
        crypt.close();
    }
}
