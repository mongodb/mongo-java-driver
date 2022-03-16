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

package com.mongodb.client;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.vault.ClientEncryption;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public abstract class AbstractClientSideEncryptionAwsCredentialFromEnvironmentTest {
    protected abstract ClientEncryption createClientEncryption(ClientEncryptionSettings settings);

    protected abstract MongoClient createMongoClient(MongoClientSettings settings);

    @Test
    public void testGetCredentialsFromEnvironment() {
        assumeTrue(serverVersionAtLeast(4, 2));
        assumeTrue(System.getenv().containsKey("AWS_ACCESS_KEY_ID"));

        Map<String, Map<String, Object>> kmsProviders = new HashMap<String, Map<String, Object>>() {{
            put("aws", new HashMap<>());
        }};

        try (ClientEncryption clientEncryption = createClientEncryption(ClientEncryptionSettings.builder()
                .keyVaultNamespace("test.datakeys")
                .kmsProviders(kmsProviders)
                .keyVaultMongoClientSettings(Fixture.getMongoClientSettings())
                .build())) {

            // If this succeeds, then it means credentials have been fetched from the environment as expected
            BsonBinary dataKeyId = clientEncryption.createDataKey("aws", new DataKeyOptions().masterKey(
                    BsonDocument.parse("{"
                            + "region: \"us-east-1\", "
                            + "key: \"arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0\"}")));

            String base64DataKeyId = Base64.getEncoder().encodeToString(dataKeyId.getData());

            Map<String, BsonDocument> schemaMap = new HashMap<>();
            schemaMap.put("test.coll", BsonDocument.parse("{"
                    + "  properties: {"
                    + "    encryptedField: {"
                    + "      encrypt: {"
                    + "        keyId: [{"
                    + "          \"$binary\": {"
                    + "            \"base64\": \"" + base64DataKeyId + "\","
                    + "            \"subType\": \"04\""
                    + "          }"
                    + "        }],"
                    + "        bsonType: \"string\","
                    + "        algorithm: \"AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic\""
                    + "      }"
                    + "    }"
                    + "  },"
                    + "  \"bsonType\": \"object\""
                    + "}"));
            AutoEncryptionSettings autoEncryptionSettings = AutoEncryptionSettings.builder()
                    .kmsProviders(kmsProviders)
                    .keyVaultNamespace("test.datakeys")
                    .schemaMap(schemaMap)
                    .build();
            try (MongoClient client = createMongoClient(getMongoClientSettingsBuilder()
                    .autoEncryptionSettings(autoEncryptionSettings)
                    .build())) {
                // If this succeeds, then it means credentials have been fetched from the environment as expected
                client.getDatabase("test").getCollection("coll")
                        .insertOne(new Document("encryptedField", "encryptMe"));
            }
        }
    }
}
