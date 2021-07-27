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
import com.mongodb.MongoCredential;
import com.mongodb.MongoSecurityException;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.client.vault.ClientEncryptions;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.mongodb.ClusterFixture.isClientSideEncryptionTest;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getMongoClient;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;
import static util.JsonPoweredTestHelper.getTestDocument;

@RunWith(Parameterized.class)
public class ClientSideEncryptionExternalKeyVaultTest {
    private MongoClient client, clientEncrypted;
    private ClientEncryption clientEncryption;
    private final boolean withExternalKeyVault;

    public ClientSideEncryptionExternalKeyVaultTest(final boolean withExternalKeyVault) {
        this.withExternalKeyVault = withExternalKeyVault;
    }

    @Before
    public void setUp() throws IOException, URISyntaxException {
        assumeTrue(serverVersionAtLeast(4, 2));
        assumeTrue("Encryption test with external keyVault is disabled", isClientSideEncryptionTest());

        /* Step 1: get unencrypted client and recreate keys collection */
        client = getMongoClient();
        MongoDatabase keyvaultDatabase = client.getDatabase("keyvault");
        MongoCollection<BsonDocument> datakeys = keyvaultDatabase.getCollection("datakeys", BsonDocument.class)
                .withWriteConcern(WriteConcern.MAJORITY);
        datakeys.drop();
        datakeys.insertOne(bsonDocumentFromPath("external-key.json"));

        /* Step 2: create encryption objects. */
        Map<String, Map<String, Object>> kmsProviders = new HashMap<String, Map<String, Object>>();
        Map<String, Object> localMasterkey = new HashMap<String, Object>();
        Map<String, BsonDocument> schemaMap = new HashMap<String, BsonDocument>();

        byte[] localMasterkeyBytes = Base64.getDecoder().decode("Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6NmdWMDFBM"
                + "UN3YkQ5aXRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZGJkTXVyZG9uSjFk");
        localMasterkey.put("key", localMasterkeyBytes);
        kmsProviders.put("local", localMasterkey);
        schemaMap.put("db.coll", bsonDocumentFromPath("external-schema.json"));

        AutoEncryptionSettings.Builder autoEncryptionSettingsBuilder = AutoEncryptionSettings.builder()
                .keyVaultNamespace("keyvault.datakeys")
                .kmsProviders(kmsProviders)
                .schemaMap(schemaMap);

        MongoClientSettings externalClientSettings = null;
        if (withExternalKeyVault) {
            externalClientSettings = getMongoClientSettingsBuilder()
                    .credential(MongoCredential.createCredential("fake-user", "admin", "fake-pwd".toCharArray()))
                    .build();
            autoEncryptionSettingsBuilder.keyVaultMongoClientSettings(externalClientSettings);
        }

        AutoEncryptionSettings autoEncryptionSettings = autoEncryptionSettingsBuilder.build();

        MongoClientSettings clientSettings = getMongoClientSettingsBuilder()
                .autoEncryptionSettings(autoEncryptionSettings)
                .build();
        clientEncrypted = MongoClients.create(clientSettings);

        ClientEncryptionSettings.Builder clientEncryptionSettingsBuilder = ClientEncryptionSettings.builder().
                keyVaultMongoClientSettings(getMongoClientSettingsBuilder().build())
                .kmsProviders(kmsProviders)
                .keyVaultNamespace("keyvault.datakeys");

        if (withExternalKeyVault) {
            clientEncryptionSettingsBuilder.keyVaultMongoClientSettings(externalClientSettings);
        }

        ClientEncryptionSettings clientEncryptionSettings = clientEncryptionSettingsBuilder.build();
        clientEncryption = ClientEncryptions.create(clientEncryptionSettings);
    }

    @Test
    public void testExternal() {
        boolean authExceptionThrown = false;
        MongoCollection<BsonDocument> coll = clientEncrypted
                .getDatabase("db")
                .getCollection("coll", BsonDocument.class);
        try {
            coll.insertOne(new BsonDocument().append("encrypted", new BsonString("test")));
        } catch (MongoSecurityException mse) {
            authExceptionThrown = true;
        }
        assertEquals(authExceptionThrown, withExternalKeyVault);

        EncryptOptions encryptOptions = new EncryptOptions("AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic")
                .keyId(new BsonBinary(BsonBinarySubType.UUID_STANDARD, Base64.getDecoder().decode("LOCALAAAAAAAAAAAAAAAAA==")));
        authExceptionThrown = false;
        try {
            clientEncryption.encrypt(new BsonString("test"), encryptOptions);
        } catch (MongoSecurityException mse) {
            authExceptionThrown = true;
        }
        assertEquals(authExceptionThrown, withExternalKeyVault);
    }

    private static BsonDocument bsonDocumentFromPath(final String path) throws IOException, URISyntaxException {
        return getTestDocument(new File(ClientSideEncryptionExternalKeyVaultTest.class
                .getResource("/client-side-encryption-external/" + path).toURI()));
    }

    @Parameterized.Parameters(name = "withExternalKeyVault: {0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[]{true}, new Object[]{false});
    }

    @After
    public void after() {
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
