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

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.MongoClientException;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.crypt.capi.MongoCrypts;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.internal.MongoClientImpl;

import javax.net.ssl.SSLContext;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import static com.mongodb.internal.capi.MongoCryptHelper.createMongoCryptOptions;

public final class Crypts {

    private Crypts() {
    }

    public static Crypt createCrypt(final MongoClientImpl client, final AutoEncryptionSettings options) {
        MongoClient internalClient = null;
        MongoClientSettings keyVaultMongoClientSettings = options.getKeyVaultMongoClientSettings();
        if (keyVaultMongoClientSettings == null || !options.isBypassAutoEncryption()) {
            MongoClientSettings settings = MongoClientSettings.builder(client.getSettings())
                    .applyToConnectionPoolSettings(builder -> builder.minSize(0))
                    .autoEncryptionSettings(null)
                    .build();
            internalClient = MongoClients.create(settings);
        }
        MongoClient collectionInfoRetrieverClient = internalClient;
        MongoClient keyVaultClient = keyVaultMongoClientSettings == null
                ? internalClient : MongoClients.create(keyVaultMongoClientSettings);
        return new Crypt(
                MongoCrypts.create(
                        createMongoCryptOptions(options.getKmsProviders(), options.getSchemaMap(), options.getEncryptedFieldsMap(),
                                options.isBypassQueryAnalysis())),
                options.getKmsProviders(),
                options.getKmsProviderPropertySuppliers(),
                options.isBypassAutoEncryption() ? null : new CollectionInfoRetriever(collectionInfoRetrieverClient),
                new CommandMarker(options.isBypassAutoEncryption(), options.getExtraOptions()),
                new KeyRetriever(keyVaultClient, new MongoNamespace(options.getKeyVaultNamespace())),
                createKeyManagementService(options.getKmsProviderSslContextMap()),
                options.isBypassAutoEncryption(),
                internalClient);
    }

    public static Crypt create(final MongoClient keyVaultClient, final ClientEncryptionSettings options) {
        return new Crypt(MongoCrypts.create(
                createMongoCryptOptions(options.getKmsProviders())),
                         options.getKmsProviders(),
                         options.getKmsProviderPropertySuppliers(),
                         new KeyRetriever(keyVaultClient, new MongoNamespace(options.getKeyVaultNamespace())),
                         createKeyManagementService(options.getKmsProviderSslContextMap()));
    }

    private static KeyManagementService createKeyManagementService(final Map<String, SSLContext> kmsProviderSslContextMap) {
        return new KeyManagementService(kmsProviderSslContextMap, 10000);
    }

    private static SSLContext getSslContext() {
        SSLContext sslContext;
        try {
            sslContext = SSLContext.getDefault();
        } catch (NoSuchAlgorithmException e) {
            throw new MongoClientException("Unable to create default SSLContext", e);
        }
        return sslContext;
    }
}
