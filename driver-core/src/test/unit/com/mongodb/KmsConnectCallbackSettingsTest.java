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

package com.mongodb;

import org.junit.jupiter.api.Test;

import java.net.Socket;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

final class KmsConnectCallbackSettingsTest {

    private static final KmsConnectCallback CALLBACK = context -> new Socket();

    @Test
    void autoEncryptionSettingsShouldDefaultToNoCallback() {
        AutoEncryptionSettings settings = AutoEncryptionSettings.builder()
                .keyVaultNamespace("keyvault.datakeys")
                .kmsProviders(new HashMap<>())
                .build();

        assertNull(settings.getKmsConnectCallback());
    }

    @Test
    void autoEncryptionSettingsShouldRoundTripCallback() {
        AutoEncryptionSettings settings = AutoEncryptionSettings.builder()
                .keyVaultNamespace("keyvault.datakeys")
                .kmsProviders(new HashMap<>())
                .kmsConnectCallback(CALLBACK)
                .build();

        assertSame(CALLBACK, settings.getKmsConnectCallback());
    }

    @Test
    void clientEncryptionSettingsShouldDefaultToNoCallback() {
        ClientEncryptionSettings settings = clientEncryptionSettingsBuilder().build();

        assertNull(settings.getKmsConnectCallback());
    }

    @Test
    void clientEncryptionSettingsShouldRoundTripCallback() {
        ClientEncryptionSettings settings = clientEncryptionSettingsBuilder()
                .kmsConnectCallback(CALLBACK)
                .build();

        assertSame(CALLBACK, settings.getKmsConnectCallback());
    }

    private static ClientEncryptionSettings.Builder clientEncryptionSettingsBuilder() {
        return ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(MongoClientSettings.builder().build())
                .keyVaultNamespace("keyvault.datakeys")
                .kmsProviders(new HashMap<>());
    }
}
