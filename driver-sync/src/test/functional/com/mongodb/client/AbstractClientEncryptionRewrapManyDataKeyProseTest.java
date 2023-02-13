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

import com.mongodb.ClientEncryptionSettings;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.model.vault.RewrapManyDataKeyOptions;
import com.mongodb.client.model.vault.RewrapManyDataKeyResult;
import com.mongodb.client.vault.ClientEncryption;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.mongodb.ClusterFixture.hasEncryptionTestsEnabled;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getMongoClient;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * See <a href="https://github.com/mongodb/specifications/tree/master/source/client-side-encryption/tests#rewrap">
 * 16. Rewrap</a>.
 */
public abstract class AbstractClientEncryptionRewrapManyDataKeyProseTest {

    private static final Map<String, BsonDocument> MASTER_KEYS_BY_PROVIDER = new HashMap<>();
    static {
        MASTER_KEYS_BY_PROVIDER.put("aws", BsonDocument.parse("{\n"
                + "  \"region\": \"us-east-1\",\n"
                + "  \"key\": \"arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0\"\n"
                + "}"));
        MASTER_KEYS_BY_PROVIDER.put("azure", BsonDocument.parse("{\n"
                + "  \"keyVaultEndpoint\": \"key-vault-csfle.vault.azure.net\",\n"
                + "  \"keyName\": \"key-name-csfle\"\n"
                + "}"));
        MASTER_KEYS_BY_PROVIDER.put("gcp", BsonDocument.parse("{\n"
                + "  \"projectId\": \"devprod-drivers\",\n"
                + "  \"location\": \"global\",\n"
                + "  \"keyRing\": \"key-ring-csfle\",\n"
                + "  \"keyName\": \"key-name-csfle\"\n"
                + "}"));
        MASTER_KEYS_BY_PROVIDER.put("kmip", BsonDocument.parse("{}"));
        MASTER_KEYS_BY_PROVIDER.put("local", null);
    }

    private static final Map<String, Map<String, Object>> KMS_PROVIDERS = new HashMap<String, Map<String, Object>>() {{
        put("aws",  new HashMap<String, Object>() {{
            put("accessKeyId", System.getProperty("org.mongodb.test.awsAccessKeyId"));
            put("secretAccessKey", System.getProperty("org.mongodb.test.awsSecretAccessKey"));
        }});
        put("azure",  new HashMap<String, Object>() {{
            put("tenantId", System.getProperty("org.mongodb.test.azureTenantId"));
            put("clientId", System.getProperty("org.mongodb.test.azureClientId"));
            put("clientSecret", System.getProperty("org.mongodb.test.azureClientSecret"));
            put("identityPlatformEndpoint", "login.microsoftonline.com:443");
        }});
        put("gcp",  new HashMap<String, Object>() {{
            put("email", System.getProperty("org.mongodb.test.gcpEmail"));
            put("privateKey", System.getProperty("org.mongodb.test.gcpPrivateKey"));
            put("endpoint", "oauth2.googleapis.com:443");
        }});
        put("kmip", new HashMap<String, Object>() {{
            put("endpoint", "localhost:5698");
        }});
        put("local", new HashMap<String, Object>() {{
            put("key", "Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6NmdWMDFBM"
                    + "UN3YkQ5aXRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZGJkTXVyZG9uSjFk");
        }});
    }};

    protected abstract MongoClient createMongoClient(MongoClientSettings settings);
    public abstract ClientEncryption getClientEncryption(ClientEncryptionSettings settings);

    public static Collection<Arguments> data() {
        List<Arguments> data = new ArrayList<>();
        Set<String> types = MASTER_KEYS_BY_PROVIDER.keySet();
        for (String srcProvider : types) {
            for (String dstProvider : types) {
                data.add(Arguments.of(srcProvider, dstProvider));
            }
        }
        return data;
    }

    @ParameterizedTest
    @MethodSource("data")
    public void rewrapWithSeparateClientEncryption(final String srcProvider, final String dstProvider) {
        Assumptions.assumeTrue(serverVersionAtLeast(4, 2));
        Assumptions.assumeTrue(hasEncryptionTestsEnabled(), "Custom Endpoint tests disables");

        BsonDocument srcKey = MASTER_KEYS_BY_PROVIDER.get(srcProvider);
        BsonDocument dstKey = MASTER_KEYS_BY_PROVIDER.get(dstProvider);
        BsonString testString = new BsonString("test");

        getMongoClient().getDatabase("keyvault").getCollection("datakeys").drop();

        ClientEncryption clientEncryption1 = getClientEncryption(ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(getMongoClientSettingsBuilder().build())
                .keyVaultNamespace("keyvault.datakeys")
                .kmsProviders(KMS_PROVIDERS)
                .build());

        BsonBinary keyId = clientEncryption1.createDataKey(srcProvider, new DataKeyOptions().masterKey(srcKey));

        BsonBinary ciphertext = clientEncryption1.encrypt(
                testString,
                new EncryptOptions("AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic").keyId(keyId));

        ClientEncryption clientEncryption2 = getClientEncryption(ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(getMongoClientSettingsBuilder().build())
                .keyVaultNamespace("keyvault.datakeys")
                .kmsProviders(KMS_PROVIDERS)
                .build());

        RewrapManyDataKeyResult result = clientEncryption2.rewrapManyDataKey(
                new BsonDocument(),
                new RewrapManyDataKeyOptions().provider(dstProvider).masterKey(dstKey));
        assertEquals(1, result.getBulkWriteResult().getModifiedCount());

        assertEquals(testString, clientEncryption1.decrypt(ciphertext));
        assertEquals(testString, clientEncryption2.decrypt(ciphertext));
    }
}
