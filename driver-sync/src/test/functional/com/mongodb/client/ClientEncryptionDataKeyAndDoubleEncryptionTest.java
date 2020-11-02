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
import com.mongodb.MongoClientException;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.client.vault.ClientEncryptions;
import com.mongodb.internal.connection.TestCommandListener;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.mongodb.ClusterFixture.hasEncryptionTestsEnabled;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static com.mongodb.client.model.Filters.eq;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
public class ClientEncryptionDataKeyAndDoubleEncryptionTest {

    private final String providerName;

    private MongoClient client;
    private MongoClient clientEncrypted;
    private ClientEncryption clientEncryption;
    private TestCommandListener commandListener;

    public ClientEncryptionDataKeyAndDoubleEncryptionTest(final String providerName) {
        this.providerName = providerName;
    }


    @Before
    public void setUp() {
        assumeTrue(serverVersionAtLeast(4, 1));
        assumeTrue("Has encryption tests", hasEncryptionTestsEnabled());

        // Step 1: create unencrypted client
        commandListener = new TestCommandListener();
        client = MongoClients.create(getMongoClientSettingsBuilder().addCommandListener(commandListener).build());
        client.getDatabase("keyvault").getCollection("datakeys").drop();
        client.getDatabase("db").getCollection("coll").drop();


        // Step 2: Create encrypted client and client encryption
        Map<String, Map<String, Object>> kmsProviders = new HashMap<String, Map<String, Object>>() {{
            put("aws",  new HashMap<String, Object>() {{
                put("accessKeyId", System.getProperty("org.mongodb.test.awsAccessKeyId"));
                put("secretAccessKey", System.getProperty("org.mongodb.test.awsSecretAccessKey"));
            }});
            put("azure",  new HashMap<String, Object>() {{
                put("tenantId", System.getProperty("org.mongodb.test.azureTenantId"));
                put("clientId", System.getProperty("org.mongodb.test.azureClientId"));
                put("clientSecret", System.getProperty("org.mongodb.test.azureClientSecret"));
            }});
            put("gcp",  new HashMap<String, Object>() {{
                put("email", System.getProperty("org.mongodb.test.gcpEmail"));
                put("privateKey", System.getProperty("org.mongodb.test.gcpPrivateKey"));
            }});
            put("local", new HashMap<String, Object>() {{
                put("key", "Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6NmdWMDFBM"
                        + "UN3YkQ5aXRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZGJkTXVyZG9uSjFk");
            }});
        }};

        HashMap<String, BsonDocument> schemaMap = new HashMap<String, BsonDocument>() {{
            put("db.coll", BsonDocument.parse("{"
                    + "  \"bsonType\": \"object\","
                    + "  \"properties\": {"
                    + "    \"encrypted_placeholder\": {"
                    + "      \"encrypt\": {"
                    + "        \"keyId\": \"/placeholder\","
                    + "        \"bsonType\": \"string\","
                    + "        \"algorithm\": \"AEAD_AES_256_CBC_HMAC_SHA_512-Random\""
                    + "      }"
                    + "    }"
                    + "  }"
                    + "}"));
        }};

        String keyVaultNamespace = "keyvault.datakeys";
        clientEncrypted = MongoClients.create(getMongoClientSettingsBuilder()
                .autoEncryptionSettings(AutoEncryptionSettings.builder()
                        .keyVaultNamespace(keyVaultNamespace)
                        .kmsProviders(kmsProviders)
                        .schemaMap(schemaMap)
                .build())
                .build());

        clientEncryption = ClientEncryptions.create(
                ClientEncryptionSettings
                        .builder()
                        .keyVaultMongoClientSettings(getMongoClientSettingsBuilder().addCommandListener(commandListener).build())
                        .keyVaultNamespace(keyVaultNamespace)
                        .kmsProviders(kmsProviders)
                        .build());
    }

    @Test
    public void testProvider() {
        String keyAltName = format("%s_altname", providerName);
        BsonBinary dataKeyId = clientEncryption.createDataKey(providerName,
                new DataKeyOptions().keyAltNames(singletonList(keyAltName)).masterKey(getMasterKey()));
        assertEquals(4, dataKeyId.getType());

        ArrayList<Document> dataKeys = client
                .getDatabase("keyvault")
                .getCollection("datakeys")
                .find(eq("_id", dataKeyId))
                .into(new ArrayList<>());
        assertEquals(1, dataKeys.size());

        Document dataKey = dataKeys.get(0);
        assertEquals(providerName, dataKey.get("masterKey", new Document()).get("provider", ""));

        String insertWriteConcern = commandListener.getCommandStartedEvent("insert")
                .getCommand()
                .getDocument("writeConcern", new BsonDocument())
                .getString("w", new BsonString(""))
                .getValue();
        assertEquals("majority", insertWriteConcern);

        String stringToEncrypt = format("hello %s", providerName);
        BsonBinary encrypted = clientEncryption.encrypt(new BsonString(stringToEncrypt),
                new EncryptOptions("AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic")
                        .keyId(dataKeyId));
        assertEquals(6, encrypted.getType());

        Document insertDocument = new Document("_id", providerName);
        insertDocument.put("value", encrypted);
        clientEncrypted.getDatabase("db").getCollection("coll").insertOne(insertDocument);
        Document decryptedDocument = clientEncrypted.getDatabase("db")
                .getCollection("coll")
                .find(eq("_id", providerName))
                .first();
        assertNotNull(decryptedDocument);
        assertEquals(stringToEncrypt, decryptedDocument.get("value", ""));

        BsonBinary encryptedKeyAltName = clientEncryption.encrypt(new BsonString(stringToEncrypt),
                new EncryptOptions("AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic")
                        .keyAltName(keyAltName));
        assertEquals(encrypted, encryptedKeyAltName);

        assertThrows(MongoClientException.class, () ->
                clientEncrypted
                        .getDatabase("db")
                        .getCollection("coll")
                        .insertOne(new Document("encrypted_placeholder", encrypted))
        );
    }

    private BsonDocument getMasterKey() {
        switch (providerName) {
            case "aws":
                return BsonDocument.parse("{"
                        + "  \"region\": \"us-east-1\","
                        + "  \"key\": \"arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0\""
                        + "}");
            case "azure":
                return BsonDocument.parse("{"
                        + "  \"keyVaultEndpoint\": \"key-vault-csfle.vault.azure.net\","
                        + "  \"keyName\": \"key-name-csfle\""
                        + "}");
            case "gcp":
                return BsonDocument.parse("{"
                        + "  \"projectId\": \"devprod-drivers\","
                        + "  \"location\": \"global\", "
                        + "  \"keyRing\": \"key-ring-csfle\","
                        + "  \"keyName\": \"key-name-csfle\""
                        + "}");
            default:
                return new BsonDocument();
        }
    }


    @Parameterized.Parameters(name = "providerName: {0}")
    public static Collection<Object[]> data() {
        return asList(new Object[]{"aws"}, new Object[]{"azure"}, new Object[]{"gcp"}, new Object[]{"local"});
    }


    @After
    public void after() {
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                // ignore
            }
        }

        if (clientEncrypted != null) {
            try {
                clientEncrypted.close();
            } catch (Exception e) {
                // ignore
            }
        }

        if (clientEncryption != null) {
            try {
                clientEncryption.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

}
