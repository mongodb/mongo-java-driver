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
import com.mongodb.KeyVaultEncryptionSettings;
import com.mongodb.MongoClientException;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.crypt.capi.MongoAwsKmsProviderOptions;
import com.mongodb.crypt.capi.MongoCryptOptions;
import com.mongodb.crypt.capi.MongoCrypts;
import com.mongodb.crypt.capi.MongoLocalKmsProviderOptions;
import org.bson.BsonDocument;

import javax.net.ssl.SSLContext;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

final class Crypts {

    static Crypt createCrypt(final MongoClient client, final AutoEncryptionSettings options) {
        return new Crypt(MongoCrypts.create(createMongoCryptOptions(options.getKmsProviders(),
                options.getNamespaceToLocalSchemaDocumentMap())),
                new CollectionInfoRetriever(client),
                new CommandMarker(options.getExtraOptions()),
                createKeyRetriever(client, options.getKeyVaultMongoClientSettings(), options.getKeyVaultNamespace()),
                createtKeyManagementService(),
                options.isBypassAutoEncryption());
    }

    static Crypt create(final MongoClient keyVaultClient, final KeyVaultEncryptionSettings options) {
        return new Crypt(MongoCrypts.create(
                createMongoCryptOptions(options.getKmsProviders(), null)),
                createKeyRetriever(keyVaultClient, false, options.getKeyVaultNamespace()),
                createtKeyManagementService());
    }

    private static MongoCryptOptions createMongoCryptOptions(final Map<String, Map<String, Object>> kmsProviders,
                                                             final Map<String, BsonDocument> namespaceToLocalSchemaDocumentMap) {
        MongoCryptOptions.Builder mongoCryptOptionsBuilder = MongoCryptOptions.builder();

        for (Map.Entry<String, Map<String, Object>> entry : kmsProviders.entrySet()) {
            if (entry.getKey().equals("aws")) {
                mongoCryptOptionsBuilder.awsKmsProviderOptions(
                        MongoAwsKmsProviderOptions.builder()
                                .accessKeyId((String) entry.getValue().get("accessKeyId"))
                                .secretAccessKey((String) entry.getValue().get("secretAccessKey"))
                                .build()
                );
            } else if (entry.getKey().equals("local")) {
                mongoCryptOptionsBuilder.localKmsProviderOptions(
                        MongoLocalKmsProviderOptions.builder()
                                .localMasterKey(ByteBuffer.wrap((byte[]) entry.getValue().get("key")))
                                .build()
                );
            } else {
                throw new MongoClientException("Unrecognized KMS provider key: " + entry.getKey());
            }
        }
        mongoCryptOptionsBuilder.localSchemaMap(namespaceToLocalSchemaDocumentMap);
        return mongoCryptOptionsBuilder.build();
    }

    private static KeyRetriever createKeyRetriever(final MongoClient defaultKeyVaultClient,
                                                   final MongoClientSettings keyVaultMongoClientSettings,
                                                   final String keyVaultNamespaceString) {
        MongoClient keyVaultClient;
        boolean keyRetrieverOwnsClient;
        if (keyVaultMongoClientSettings != null) {
            keyVaultClient = MongoClients.create(keyVaultMongoClientSettings);
            keyRetrieverOwnsClient = true;
        } else {
            keyVaultClient = defaultKeyVaultClient;
            keyRetrieverOwnsClient = false;
        }

        return createKeyRetriever(keyVaultClient, keyRetrieverOwnsClient, keyVaultNamespaceString);
    }

    private static KeyRetriever createKeyRetriever(final MongoClient keyVaultClient, final boolean keyRetrieverOwnsClient,
                                                   final String keyVaultNamespaceString) {
        return new KeyRetriever(keyVaultClient, keyRetrieverOwnsClient, new MongoNamespace(keyVaultNamespaceString));
    }

    private static KeyManagementService createtKeyManagementService() {
        return new KeyManagementService(getSslContext(), 443, 10000);
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

    private Crypts() {
    }
}
