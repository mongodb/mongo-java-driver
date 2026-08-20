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

import com.mongodb.connection.ProxyProtocol;
import com.mongodb.connection.ProxySettings;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class KmsProxySettingsTest {

    private static final ProxySettings HTTP_PROXY = ProxySettings.builder()
            .protocol(ProxyProtocol.HTTP)
            .host("proxy.example.com")
            .port(8080)
            .build();

    @Test
    void shouldDefaultToSocks5() {
        assertEquals(ProxyProtocol.SOCKS5, ProxySettings.builder().build().getProtocol());
    }

    @Test
    void shouldRequireAnExplicitPortForHttpProtocols() {
        assertThrows(IllegalStateException.class, () -> ProxySettings.builder()
                .protocol(ProxyProtocol.HTTP)
                .host("proxy.example.com")
                .build());
        assertThrows(IllegalStateException.class, () -> ProxySettings.builder()
                .protocol(ProxyProtocol.HTTPS)
                .host("proxy.example.com")
                .build());
    }

    @Test
    void shouldRoundTripThroughApplySettings() {
        ProxySettings copy = ProxySettings.builder(HTTP_PROXY).build();

        assertEquals(HTTP_PROXY, copy);
        assertEquals(ProxyProtocol.HTTP, copy.getProtocol());
        assertEquals(8080, copy.getPort());
        assertTrue(copy.isProxyEnabled());
    }

    @Test
    void shouldNotRenderCredentialsInToString() {
        String rendered = ProxySettings.builder(HTTP_PROXY)
                .username("u53rn4m3")
                .password("p4ssw0rd")
                .build()
                .toString();

        assertFalse(rendered.contains("p4ssw0rd"), () -> "password leaked: " + rendered);
        assertFalse(rendered.contains("u53rn4m3"), () -> "username leaked: " + rendered);
        assertTrue(rendered.contains("protocol=HTTP"));
    }

    @Test
    void autoEncryptionSettingsShouldDefaultToNoProxy() {
        AutoEncryptionSettings settings = AutoEncryptionSettings.builder()
                .keyVaultNamespace("keyvault.datakeys")
                .kmsProviders(new HashMap<>())
                .build();

        assertFalse(settings.getProxySettings().isProxyEnabled());
    }

    @Test
    void autoEncryptionSettingsShouldRoundTripProxySettings() {
        AutoEncryptionSettings settings = AutoEncryptionSettings.builder()
                .keyVaultNamespace("keyvault.datakeys")
                .kmsProviders(new HashMap<>())
                .proxySettings(HTTP_PROXY)
                .build();

        assertSame(HTTP_PROXY, settings.getProxySettings());
    }

    @Test
    void clientEncryptionSettingsShouldDefaultToNoProxy() {
        assertFalse(clientEncryptionSettingsBuilder().build().getProxySettings().isProxyEnabled());
    }

    @Test
    void clientEncryptionSettingsShouldRoundTripProxySettings() {
        ClientEncryptionSettings settings = clientEncryptionSettingsBuilder()
                .proxySettings(HTTP_PROXY)
                .build();

        assertSame(HTTP_PROXY, settings.getProxySettings());
    }

    private static ClientEncryptionSettings.Builder clientEncryptionSettingsBuilder() {
        return ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(MongoClientSettings.builder().build())
                .keyVaultNamespace("keyvault.datakeys")
                .kmsProviders(new HashMap<>());
    }
}
