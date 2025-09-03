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
import com.mongodb.internal.crypt.capi.MongoCrypt;
import com.mongodb.internal.crypt.capi.MongoCrypts;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;

import javax.net.ssl.SSLContext;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import static com.mongodb.internal.capi.MongoCryptHelper.createMongoCryptOptions;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class Crypts {

    private Crypts() {
    }

    public static Crypt createCrypt(final MongoClientSettings mongoClientSettings, final AutoEncryptionSettings autoEncryptionSettings) {
        MongoClient sharedInternalClient = null;
        MongoClientSettings keyVaultMongoClientSettings = autoEncryptionSettings.getKeyVaultMongoClientSettings();
        if (keyVaultMongoClientSettings == null || !autoEncryptionSettings.isBypassAutoEncryption()) {
            MongoClientSettings defaultInternalMongoClientSettings = MongoClientSettings.builder(mongoClientSettings)
                    .applyToConnectionPoolSettings(builder -> builder.minSize(0))
                    .autoEncryptionSettings(null)
                    .build();
            sharedInternalClient = MongoClients.create(defaultInternalMongoClientSettings);
        }
        MongoClient keyVaultClient = keyVaultMongoClientSettings == null
                ? sharedInternalClient : MongoClients.create(keyVaultMongoClientSettings);
        MongoCrypt mongoCrypt = MongoCrypts.create(createMongoCryptOptions(autoEncryptionSettings));
        return new Crypt(
                mongoCrypt,
                createKeyRetriever(keyVaultClient, autoEncryptionSettings.getKeyVaultNamespace()),
                createKeyManagementService(autoEncryptionSettings.getKmsProviderSslContextMap()),
                autoEncryptionSettings.getKmsProviders(),
                autoEncryptionSettings.getKmsProviderPropertySuppliers(),
                autoEncryptionSettings.isBypassAutoEncryption(),
                autoEncryptionSettings.isBypassAutoEncryption() ? null : new CollectionInfoRetriever(sharedInternalClient),
                new CommandMarker(mongoCrypt, autoEncryptionSettings),
                sharedInternalClient,
                keyVaultClient);
    }

    public static Crypt create(final MongoClient keyVaultClient, final ClientEncryptionSettings settings) {
        return new Crypt(MongoCrypts.create(createMongoCryptOptions(settings)),
                createKeyRetriever(keyVaultClient, settings.getKeyVaultNamespace()),
                createKeyManagementService(settings.getKmsProviderSslContextMap()),
                settings.getKmsProviders(),
                settings.getKmsProviderPropertySuppliers()
        );
    }

    private static KeyRetriever createKeyRetriever(final MongoClient keyVaultClient,
            final String keyVaultNamespaceString) {
        return new KeyRetriever(keyVaultClient, new MongoNamespace(keyVaultNamespaceString));
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
