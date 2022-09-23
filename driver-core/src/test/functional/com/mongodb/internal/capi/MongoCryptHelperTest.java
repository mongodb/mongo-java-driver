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
import com.mongodb.client.model.vault.RewrapManyDataKeyOptions;
import com.mongodb.crypt.capi.MongoCryptOptions;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.mongodb.internal.capi.MongoCryptHelper.validateRewrapManyDataKeyOptions;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
    }
}
