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
import com.mongodb.MongoClientException;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.client.vault.ClientEncryptions;
import com.mongodb.crypt.capi.MongoCryptException;
import com.mongodb.lang.Nullable;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.ClusterFixture.isNotAtLeastJava8;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getMongoClientSettings;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
public class ClientEncryptionCustomEndpointTest {

    private ClientEncryption clientEncryption;
    private BsonDocument masterKey;
    private final Class<? extends RuntimeException> exceptionClass;
    private final Class<? extends RuntimeException> wrappedExceptionClass;
    private final String messageContainedInException;

    public ClientEncryptionCustomEndpointTest(@SuppressWarnings("unused") final String name,
                                              final BsonDocument masterKey,
                                              @Nullable final Class<? extends RuntimeException> exceptionClass,
                                              @Nullable final Class<? extends RuntimeException> wrappedExceptionClass,
                                              @Nullable final String messageContainedInException) {
        this.masterKey = masterKey;
        this.exceptionClass = exceptionClass;
        this.wrappedExceptionClass = wrappedExceptionClass;
        this.messageContainedInException = messageContainedInException;
    }

    @Before
    public void setUp() {
        assumeFalse(isNotAtLeastJava8());
        assumeTrue(serverVersionAtLeast(4, 1));
        assumeTrue("Encryption test with external keyVault is disabled",
                System.getProperty("org.mongodb.test.awsAccessKeyId") != null
                        && !System.getProperty("org.mongodb.test.awsAccessKeyId").isEmpty());

        Map<String, Map<String, Object>> kmsProviders = new HashMap<String, Map<String, Object>>();
        Map<String, Object> awsCreds = new HashMap<String, Object>();
        awsCreds.put("accessKeyId", System.getProperty("org.mongodb.test.awsAccessKeyId"));
        awsCreds.put("secretAccessKey", System.getProperty("org.mongodb.test.awsSecretAccessKey"));
        kmsProviders.put("aws", awsCreds);

        ClientEncryptionSettings.Builder clientEncryptionSettingsBuilder = ClientEncryptionSettings.builder().
                keyVaultMongoClientSettings(getMongoClientSettings())
                .kmsProviders(kmsProviders)
                .keyVaultNamespace("admin.datakeys");

        ClientEncryptionSettings clientEncryptionSettings = clientEncryptionSettingsBuilder.build();
        clientEncryption = ClientEncryptions.create(clientEncryptionSettings);
    }

    @After
    public void after() {
        if (clientEncryption != null) {
            try {
                clientEncryption.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @Test
    public void testEndpoint() throws Exception {
        try {
            BsonBinary dataKeyId = clientEncryption.createDataKey("aws", new DataKeyOptions()
                    .masterKey(masterKey));

            assertNull("Expected exception, but encryption succeeded", exceptionClass);

            clientEncryption.encrypt(new BsonString("test"), new EncryptOptions("AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic")
                    .keyId(dataKeyId));
        } catch (Exception e) {
            if (exceptionClass == null) {
                throw e;
            }
            assertEquals(exceptionClass, e.getClass());
            assertEquals(wrappedExceptionClass, e.getCause().getClass());
            if (messageContainedInException != null) {
                assertTrue(e.getCause().getMessage().contains(messageContainedInException));
            }
        }
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        List<Object[]> data = new ArrayList<Object[]>();

        data.add(new Object[]{"default endpoint",
                getDefaultMasterKey(),
                null, null, null});
        data.add(new Object[]{"valid endpoint",
                getDefaultMasterKey().append("endpoint", new BsonString("kms.us-east-1.amazonaws.com")),
                null, null, null});
        data.add(new Object[]{"valid endpoint port",
                getDefaultMasterKey().append("endpoint", new BsonString("kms.us-east-1.amazonaws.com:443")),
                null, null, null});
        data.add(new Object[]{"invalid endpoint port",
                getDefaultMasterKey().append("endpoint", new BsonString("kms.us-east-1.amazonaws.com:12345")),
                MongoClientException.class, ConnectException.class, "Connection refused"});
        data.add(new Object[]{"invalid amazon region in endpoint",
                getDefaultMasterKey().append("endpoint", new BsonString("kms.us-east-2.amazonaws.com")),
                MongoClientException.class, MongoCryptException.class, "us-east-1"});
        data.add(new Object[]{"invalid endpoint host",
                getDefaultMasterKey().append("endpoint", new BsonString("example.com")),
                MongoClientException.class, MongoCryptException.class, "parse error"});

        return data;
    }

    private static BsonDocument getDefaultMasterKey() {
        return new BsonDocument()
                .append("region", new BsonString("us-east-1"))
                .append("key", new BsonString("arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"));
    }
}
