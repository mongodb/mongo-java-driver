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
import com.mongodb.MongoClientException;
import com.mongodb.MongoClientSettings;
import com.mongodb.connection.ProxyProtocol;
import com.mongodb.connection.ProxySettings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Routing KMS requests through a proxy is implemented only for the synchronous driver, so configuring one for a
 * reactive client must fail rather than silently connecting to KMS hosts directly.
 */
final class CryptsProxyNotSupportedTest {

    @ParameterizedTest
    @EnumSource(ProxyProtocol.class)
    void shouldRejectAKmsProxyOnAutoEncryptionSettings(final ProxyProtocol protocol) {
        AutoEncryptionSettings settings = AutoEncryptionSettings.builder()
                .keyVaultNamespace("keyvault.datakeys")
                .kmsProviders(new HashMap<>())
                .proxySettings(proxySettings(protocol))
                .build();

        MongoClientException e = assertThrows(MongoClientException.class,
                () -> Crypts.createCrypt(MongoClientSettings.builder().build(), settings));
        assertTrue(e.getMessage().contains("not supported for reactive clients"),
                () -> "unexpected message: " + e.getMessage());
    }

    @ParameterizedTest
    @EnumSource(ProxyProtocol.class)
    void shouldRejectAKmsProxyOnClientEncryptionSettings(final ProxyProtocol protocol) {
        ClientEncryptionSettings settings = ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(MongoClientSettings.builder().build())
                .keyVaultNamespace("keyvault.datakeys")
                .kmsProviders(new HashMap<>())
                .proxySettings(proxySettings(protocol))
                .build();

        MongoClientException e = assertThrows(MongoClientException.class, () -> Crypts.create(null, settings));
        assertTrue(e.getMessage().contains("not supported for reactive clients"),
                () -> "unexpected message: " + e.getMessage());
    }

    @Test
    void shouldNotRejectSettingsWithoutAProxy() {
        ClientEncryptionSettings settings = ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(MongoClientSettings.builder().build())
                .keyVaultNamespace("keyvault.datakeys")
                .kmsProviders(new HashMap<>())
                .build();

        // Without a proxy the guard must not fire; construction then fails for an unrelated reason, which is not a
        // MongoClientException about proxy support.
        Throwable thrown = assertThrows(Throwable.class, () -> Crypts.create(null, settings));
        assertTrue(thrown.getMessage() == null || !thrown.getMessage().contains("not supported for reactive clients"),
                () -> "the proxy guard fired unexpectedly: " + thrown.getMessage());
    }

    private static ProxySettings proxySettings(final ProxyProtocol protocol) {
        return ProxySettings.builder()
                .protocol(protocol)
                .host("proxy.example.com")
                .port(8080)
                .build();
    }
}
