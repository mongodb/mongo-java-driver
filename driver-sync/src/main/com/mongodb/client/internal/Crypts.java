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

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.internal.crypt.capi.MongoCrypt;
import com.mongodb.internal.crypt.capi.MongoCrypts;

import javax.net.ssl.SSLContext;
import java.util.Map;

import static com.mongodb.internal.capi.MongoCryptHelper.createMongoCryptOptions;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class Crypts {

    public static Crypt createCrypt(final MongoClientSettings mongoClientSettings, final AutoEncryptionSettings settings) {
        MongoClient sharedInternalClient = null;
        MongoClientSettings keyVaultMongoClientSettings = settings.getKeyVaultMongoClientSettings();
        if (keyVaultMongoClientSettings == null || !settings.isBypassAutoEncryption()) {
            MongoClientSettings defaultInternalMongoClientSettings = MongoClientSettings.builder(mongoClientSettings)
                    .applyToConnectionPoolSettings(builder -> builder.minSize(0))
                    .autoEncryptionSettings(null)
                    .build();
            sharedInternalClient = MongoClients.create(defaultInternalMongoClientSettings);
        }
        MongoClient keyVaultClient = keyVaultMongoClientSettings == null
                ? sharedInternalClient : MongoClients.create(keyVaultMongoClientSettings);
        MongoCrypt mongoCrypt = MongoCrypts.create(createMongoCryptOptions(settings));
        return new Crypt(
                mongoCrypt,
                createKeyRetriever(keyVaultClient, settings.getKeyVaultNamespace()),
                createKeyManagementService(settings.getKmsProviderSslContextMap()),
                settings.getKmsProviders(),
                settings.getKmsProviderPropertySuppliers(),
                settings.isBypassAutoEncryption(),
                settings.isBypassAutoEncryption() ? null : new CollectionInfoRetriever(sharedInternalClient),
                new CommandMarker(mongoCrypt, settings),
                sharedInternalClient, keyVaultClient);
    }

    static Crypt create(final MongoClient keyVaultClient, final ClientEncryptionSettings settings) {
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

    private Crypts() {
    }
}
