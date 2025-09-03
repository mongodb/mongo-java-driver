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

package com.mongodb.internal.capi;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.MongoClientException;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.vault.RewrapManyDataKeyOptions;
import com.mongodb.internal.crypt.capi.MongoCryptOptions;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.mongodb.internal.capi.MongoCryptHelper.isMongocryptdSpawningDisabled;
import static com.mongodb.internal.capi.MongoCryptHelper.validateRewrapManyDataKeyOptions;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MongoCryptHelperTest {

    @Test
    public void createsExpectedMongoCryptOptionsUsingClientEncryptionSettings() {

        Map<String, Map<String, Object>> kmsProvidersRaw = new HashMap<>();
        kmsProvidersRaw.put("provider", new HashMap<String, Object>(){{
            put("test", "test");
        }});

        ClientEncryptionSettings settings = ClientEncryptionSettings
                .builder()
                .kmsProviders(kmsProvidersRaw)
                .keyVaultMongoClientSettings(MongoClientSettings.builder().build())
                .keyVaultNamespace("a.b")
                .build();
        MongoCryptOptions mongoCryptOptions = MongoCryptHelper.createMongoCryptOptions(settings);


        BsonDocument expectedKmsProviders = BsonDocument.parse("{provider: {test: 'test'}}");
        MongoCryptOptions expectedMongoCryptOptions = MongoCryptOptions
                .builder()
                .kmsProviderOptions(expectedKmsProviders)
                .needsKmsCredentialsStateEnabled(true)
                .build();

        assertMongoCryptOptions(expectedMongoCryptOptions, mongoCryptOptions);
    }

    @Test
    public void createsExpectedMongoCryptOptionsUsingAutoEncryptionSettings() {

        Map<String, Map<String, Object>> kmsProvidersRaw = new HashMap<>();
        kmsProvidersRaw.put("provider", new HashMap<String, Object>(){{
            put("test", "test");
        }});

        AutoEncryptionSettings.Builder autoEncryptionSettingsBuilder = AutoEncryptionSettings
                .builder()
                .kmsProviders(kmsProvidersRaw)
                .keyVaultNamespace("a.b");
        MongoCryptOptions mongoCryptOptions = MongoCryptHelper.createMongoCryptOptions(autoEncryptionSettingsBuilder.build());

        BsonDocument expectedKmsProviders = BsonDocument.parse("{provider: {test: 'test'}}");
        MongoCryptOptions.Builder mongoCryptOptionsBuilder = MongoCryptOptions
                .builder()
                .kmsProviderOptions(expectedKmsProviders)
                .needsKmsCredentialsStateEnabled(true)
                .encryptedFieldsMap(emptyMap())
                .localSchemaMap(emptyMap())
                .searchPaths(singletonList("$SYSTEM"));

        assertMongoCryptOptions(mongoCryptOptionsBuilder.build(), mongoCryptOptions);

        // Ensure can set key expiration
        autoEncryptionSettingsBuilder.keyExpiration(10L, TimeUnit.SECONDS);
        mongoCryptOptions = MongoCryptHelper.createMongoCryptOptions(autoEncryptionSettingsBuilder.build());
        assertMongoCryptOptions(mongoCryptOptionsBuilder.keyExpirationMS(10_000L).build(), mongoCryptOptions);

        // Ensure search Paths is empty when bypassAutoEncryption is true
        autoEncryptionSettingsBuilder.bypassAutoEncryption(true);
        mongoCryptOptions = MongoCryptHelper.createMongoCryptOptions(autoEncryptionSettingsBuilder.build());
        assertMongoCryptOptions(mongoCryptOptionsBuilder.searchPaths(emptyList()).build(), mongoCryptOptions);
    }

    @Test
    public void validateRewrapManyDataKeyOptionsTest() {
        // Happy path
        assertDoesNotThrow(() -> validateRewrapManyDataKeyOptions(new RewrapManyDataKeyOptions()));
        assertDoesNotThrow(() -> validateRewrapManyDataKeyOptions(new RewrapManyDataKeyOptions().provider("AWS")));

        // Failure
        assertThrows(MongoClientException.class, () -> validateRewrapManyDataKeyOptions(new RewrapManyDataKeyOptions().masterKey(new BsonDocument())));
    }

    @Test
    public void isMongocryptdSpawningDisabledTest() {
        assertTrue(isMongocryptdSpawningDisabled(null,
                initializeAutoEncryptionSettingsBuilder().bypassAutoEncryption(true).build()));
        assertTrue(isMongocryptdSpawningDisabled(null,
                initializeAutoEncryptionSettingsBuilder().bypassQueryAnalysis(true).build()));
        assertTrue(isMongocryptdSpawningDisabled(null,
                initializeAutoEncryptionSettingsBuilder().extraOptions(singletonMap("cryptSharedLibRequired", true)).build()));
        assertTrue(isMongocryptdSpawningDisabled("/path/to/shared/lib.so",
                initializeAutoEncryptionSettingsBuilder().build()));
        assertFalse(isMongocryptdSpawningDisabled(null,
                initializeAutoEncryptionSettingsBuilder().build()));
        assertFalse(isMongocryptdSpawningDisabled("",
                initializeAutoEncryptionSettingsBuilder().build()));
    }

    private static AutoEncryptionSettings.Builder initializeAutoEncryptionSettingsBuilder() {
        AutoEncryptionSettings.Builder builder = AutoEncryptionSettings.builder()
                .keyVaultNamespace("test.vault")
                .kmsProviders(singletonMap("local", singletonMap("key", new byte[96])));
        return builder;
    }

    void assertMongoCryptOptions(final MongoCryptOptions expected, final MongoCryptOptions actual) {
        assertEquals(expected.getAwsKmsProviderOptions(), actual.getAwsKmsProviderOptions(), "AwsKmsProviderOptions not equal");
        assertEquals(expected.getEncryptedFieldsMap(), actual.getEncryptedFieldsMap(), "EncryptedFieldsMap not equal");
        assertEquals(expected.getExtraOptions(), actual.getExtraOptions(), "ExtraOptions not equal");
        assertEquals(expected.getKmsProviderOptions(), actual.getKmsProviderOptions(), "KmsProviderOptions not equal");
        assertEquals(expected.getLocalKmsProviderOptions(), actual.getLocalKmsProviderOptions(), "LocalKmsProviderOptions not equal");
        assertEquals(expected.getLocalSchemaMap(), actual.getLocalSchemaMap(), "LocalSchemaMap not equal");
        assertEquals(expected.getSearchPaths(), actual.getSearchPaths(), "SearchPaths not equal");
        assertEquals(expected.isBypassQueryAnalysis(), actual.isBypassQueryAnalysis(), "isBypassQueryAnalysis not equal");
        assertEquals(expected.isNeedsKmsCredentialsStateEnabled(), actual.isNeedsKmsCredentialsStateEnabled(), "isNeedsKmsCredentialsStateEnabled not equal");
        assertEquals(expected.getKeyExpirationMS(), actual.getKeyExpirationMS(), "keyExpirationMS not equal");
    }
}
