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
import com.mongodb.KmsConnectCallback;
import com.mongodb.MongoClientException;
import com.mongodb.MongoClientSettings;
import org.junit.jupiter.api.Test;

import java.net.Socket;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link KmsConnectCallback} is implemented only for the synchronous driver, so configuring one for a reactive client
 * must fail rather than silently connecting to KMS hosts directly.
 */
final class CryptsKmsConnectCallbackNotSupportedTest {

    private static final KmsConnectCallback CALLBACK = context -> new Socket();

    @Test
    void shouldRejectACallbackOnAutoEncryptionSettings() {
        AutoEncryptionSettings settings = AutoEncryptionSettings.builder()
                .keyVaultNamespace("keyvault.datakeys")
                .kmsProviders(new HashMap<>())
                .kmsConnectCallback(CALLBACK)
                .build();

        MongoClientException e = assertThrows(MongoClientException.class,
                () -> Crypts.createCrypt(MongoClientSettings.builder().build(), settings));
        assertTrue(e.getMessage().contains("not supported for reactive clients"),
                () -> "unexpected message: " + e.getMessage());
    }

    @Test
    void shouldRejectACallbackOnClientEncryptionSettings() {
        ClientEncryptionSettings settings = ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(MongoClientSettings.builder().build())
                .keyVaultNamespace("keyvault.datakeys")
                .kmsProviders(new HashMap<>())
                .kmsConnectCallback(CALLBACK)
                .build();

        MongoClientException e = assertThrows(MongoClientException.class, () -> Crypts.create(null, settings));
        assertTrue(e.getMessage().contains("not supported for reactive clients"),
                () -> "unexpected message: " + e.getMessage());
    }

    @Test
    void shouldNotRejectSettingsWithoutACallback() {
        ClientEncryptionSettings settings = ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(MongoClientSettings.builder().build())
                .keyVaultNamespace("keyvault.datakeys")
                .kmsProviders(new HashMap<>())
                .build();

        // Without a callback the guard must not fire; construction then fails for an unrelated reason, which is not a
        // MongoClientException about callback support.
        Throwable thrown = assertThrows(Throwable.class, () -> Crypts.create(null, settings));
        assertTrue(thrown.getMessage() == null || !thrown.getMessage().contains("not supported for reactive clients"),
                () -> "the callback guard fired unexpectedly: " + thrown.getMessage());
    }
}
