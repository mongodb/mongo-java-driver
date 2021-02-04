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

package com.mongodb.async.client;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.async.FutureResultCallback;
import org.bson.Document;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

import static com.mongodb.ClusterFixture.isNotAtLeastJava8;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;


public class ClientSideEncryptionMongocryptdSpawnBypassTest extends DatabaseTestCase {
    private final File pidFile;
    private final Map<String, Map<String, Object>> kmsProviders;
    private final MongoNamespace keyVaultNamespace = new MongoNamespace("keyvault.datakeys");

    public ClientSideEncryptionMongocryptdSpawnBypassTest() throws IOException {
        assumeFalse(isNotAtLeastJava8());
        assumeTrue(serverVersionAtLeast(4, 2));

        pidFile = new File("bypass-spawning-mongocryptd.pid");

        byte[] localMasterKey = new byte[96];
        new SecureRandom().nextBytes(localMasterKey);

        Map<String, Object> keyMap = new HashMap<String, Object>();
        keyMap.put("key", localMasterKey);
        kmsProviders = new HashMap<String, Map<String, Object>>();
        kmsProviders.put("local", keyMap);
    }


    @Test
    public void shouldNotSpawnWhenMongocryptdBypassSpawnIsTrue() {
        assumeTrue(serverVersionAtLeast(4, 1));
        Map<String, Object> extraOptions = new HashMap<String, Object>();
        extraOptions.put("mongocryptdBypassSpawn", true);
        extraOptions.put("mongocryptdSpawnArgs", asList("--pidfilepath=" + pidFile.getAbsolutePath(), "--port=27099"));

        AutoEncryptionSettings autoEncryptionSettings = AutoEncryptionSettings.builder()
                .keyVaultNamespace(keyVaultNamespace.getFullName())
                .kmsProviders(kmsProviders)
                .extraOptions(extraOptions)
                .build();

        MongoClientSettings clientSettings = Fixture.getMongoClientSettingsBuilder()
                .autoEncryptionSettings(autoEncryptionSettings)
                .build();
        MongoClient clientEncrypted = MongoClients.create(clientSettings);
        try {
            FutureResultCallback<Document> pingCallback = new FutureResultCallback<Document>();
            clientEncrypted.getDatabase("admin").runCommand(new Document("ping", 1), pingCallback);
            pingCallback.get();

            assertFalse(pidFile.exists());
        } finally {
            clientEncrypted.close();
        }
    }

    @Test
    public void shouldNotSpawnWhenBypassAutoEncryptionIsTrue() {
        assumeTrue(serverVersionAtLeast(4, 1));
        Map<String, Object> extraOptions = new HashMap<String, Object>();
        extraOptions.put("mongocryptdSpawnArgs", asList("--pidfilepath=" + pidFile.getAbsolutePath(), "--port=27099"));

        AutoEncryptionSettings autoEncryptionSettings = AutoEncryptionSettings.builder()
                .keyVaultNamespace(keyVaultNamespace.getFullName())
                .kmsProviders(kmsProviders)
                .extraOptions(extraOptions)
                .bypassAutoEncryption(true)
                .build();

        MongoClientSettings clientSettings = Fixture.getMongoClientSettingsBuilder()
                .autoEncryptionSettings(autoEncryptionSettings)
                .build();
        MongoClient clientEncrypted = MongoClients.create(clientSettings);

        try {
            FutureResultCallback<Document> pingCallback = new FutureResultCallback<Document>();
            clientEncrypted.getDatabase("admin").runCommand(new Document("ping", 1), pingCallback);
            pingCallback.get();

            assertFalse(pidFile.exists());
        } finally {
            clientEncrypted.close();
        }
    }
}
