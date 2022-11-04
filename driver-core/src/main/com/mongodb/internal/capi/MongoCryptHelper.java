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
import com.mongodb.AwsCredential;
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientException;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoConfigurationException;
import com.mongodb.client.model.vault.RewrapManyDataKeyOptions;
import com.mongodb.crypt.capi.MongoCryptOptions;
import com.mongodb.internal.authentication.AwsCredentialHelper;
import com.mongodb.internal.authentication.AzureCredentialHelper;
import com.mongodb.internal.authentication.GcpCredentialHelper;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class MongoCryptHelper {

    public static MongoCryptOptions createMongoCryptOptions(final ClientEncryptionSettings settings) {
        return createMongoCryptOptions(settings.getKmsProviders(), false, emptyList(), emptyMap(), null, null);
    }

    public static MongoCryptOptions createMongoCryptOptions(final AutoEncryptionSettings settings) {
        return createMongoCryptOptions(
                settings.getKmsProviders(),
                settings.isBypassQueryAnalysis(),
                settings.isBypassAutoEncryption() ? emptyList() :  singletonList("$SYSTEM"),
                settings.getExtraOptions(),
                settings.getSchemaMap(),
                settings.getEncryptedFieldsMap());
    }

    public static void validateRewrapManyDataKeyOptions(final RewrapManyDataKeyOptions options) {
        if (options.getMasterKey() != null && options.getProvider() == null) {
            throw new MongoClientException("Missing the provider but supplied a master key in the RewrapManyDataKeyOptions");
        }
    }

    private static MongoCryptOptions createMongoCryptOptions(
            final Map<String, Map<String, Object>> kmsProviders,
            final boolean bypassQueryAnalysis,
            final List<String> searchPaths,
            @Nullable final Map<String, Object> extraOptions,
            @Nullable final Map<String, BsonDocument> localSchemaMap,
            @Nullable final Map<String, BsonDocument> encryptedFieldsMap) {
        MongoCryptOptions.Builder mongoCryptOptionsBuilder = MongoCryptOptions.builder();
        mongoCryptOptionsBuilder.kmsProviderOptions(getKmsProvidersAsBsonDocument(kmsProviders));
        mongoCryptOptionsBuilder.bypassQueryAnalysis(bypassQueryAnalysis);
        mongoCryptOptionsBuilder.searchPaths(searchPaths);
        mongoCryptOptionsBuilder.extraOptions(toBsonDocument(extraOptions));
        mongoCryptOptionsBuilder.localSchemaMap(localSchemaMap);
        mongoCryptOptionsBuilder.encryptedFieldsMap(encryptedFieldsMap);
        mongoCryptOptionsBuilder.needsKmsCredentialsStateEnabled(true);
        return mongoCryptOptionsBuilder.build();
    }
    public static BsonDocument fetchCredentials(final Map<String, Map<String, Object>> kmsProviders,
            final Map<String, Supplier<Map<String, Object>>> kmsProviderPropertySuppliers) {
        BsonDocument kmsProvidersDocument = MongoCryptHelper.getKmsProvidersAsBsonDocument(kmsProviders);
        for (Map.Entry<String, Supplier<Map<String, Object>>> entry : kmsProviderPropertySuppliers.entrySet()) {
            String kmsProviderName = entry.getKey();
            if (!kmsProvidersDocument.get(kmsProviderName).asDocument().isEmpty()) {
                continue;
            }
            Map<String, Object> kmsProviderCredential;
            try {
                kmsProviderCredential = entry.getValue().get();
            } catch (Exception e) {
                throw new MongoConfigurationException(format("Exception getting credential for kms provider %s from configured Supplier.",
                        kmsProviderName), e);
            }
            if (kmsProviderCredential == null || kmsProviderCredential.isEmpty()) {
                throw new MongoConfigurationException(format("Exception getting credential for kms provider %s from configured Supplier."
                                + " The returned value is %s.",
                        kmsProviderName, kmsProviderCredential == null ? "null" : "empty"));
            }
            kmsProvidersDocument.put(kmsProviderName, toBsonDocument(kmsProviderCredential));
        }
        if (kmsProvidersDocument.containsKey("aws") && kmsProvidersDocument.get("aws").asDocument().isEmpty()) {
            AwsCredential awsCredential = AwsCredentialHelper.obtainFromEnvironment();
            if (awsCredential != null) {
                BsonDocument awsCredentialDocument = new BsonDocument();
                awsCredentialDocument.put("accessKeyId", new BsonString(awsCredential.getAccessKeyId()));
                awsCredentialDocument.put("secretAccessKey", new BsonString(awsCredential.getSecretAccessKey()));
                if (awsCredential.getSessionToken() != null) {
                    awsCredentialDocument.put("sessionToken", new BsonString(awsCredential.getSessionToken()));
                }
                kmsProvidersDocument.put("aws", awsCredentialDocument);
            }
        }
        if (kmsProvidersDocument.containsKey("gcp") && kmsProvidersDocument.get("gcp").asDocument().isEmpty()) {
            kmsProvidersDocument.put("gcp", GcpCredentialHelper.obtainFromEnvironment());
        }
        if (kmsProvidersDocument.containsKey("azure") && kmsProvidersDocument.get("azure").asDocument().isEmpty()) {
            kmsProvidersDocument.put("azure", AzureCredentialHelper.obtainFromEnvironment());
        }

        return kmsProvidersDocument;
    }

    private static BsonDocument getKmsProvidersAsBsonDocument(final Map<String, Map<String, Object>> kmsProviders) {
        BsonDocument bsonKmsProviders = new BsonDocument();
        kmsProviders.forEach((k, v) -> bsonKmsProviders.put(k, toBsonDocument(v)));
        return bsonKmsProviders;
    }

    private static BsonDocument toBsonDocument(final Map<String, Object> optionsMap) {
        if (optionsMap == null) {
            return new BsonDocument();
        }
        return new BsonDocumentWrapper<>(new Document(optionsMap), new DocumentCodec());
    }

    public static boolean isMongocryptdSpawningDisabled(@Nullable final String cryptSharedLibVersion,
            final AutoEncryptionSettings settings) {
        boolean cryptSharedLibIsAvailable = cryptSharedLibVersion != null && !cryptSharedLibVersion.isEmpty();
        boolean cryptSharedLibRequired = (boolean) settings.getExtraOptions().getOrDefault("cryptSharedLibRequired", false);
        return settings.isBypassAutoEncryption() || settings.isBypassQueryAnalysis() || cryptSharedLibRequired || cryptSharedLibIsAvailable;
    }

    @SuppressWarnings("unchecked")
    public static List<String> createMongocryptdSpawnArgs(final Map<String, Object> options) {
        List<String> spawnArgs = new ArrayList<>();

        String path = options.containsKey("mongocryptdSpawnPath")
                ? (String) options.get("mongocryptdSpawnPath")
                : "mongocryptd";

        spawnArgs.add(path);
        if (options.containsKey("mongocryptdSpawnArgs")) {
            spawnArgs.addAll((List<String>) options.get("mongocryptdSpawnArgs"));
        }

        if (!spawnArgs.contains("--idleShutdownTimeoutSecs")) {
            spawnArgs.add("--idleShutdownTimeoutSecs");
            spawnArgs.add("60");
        }
        return spawnArgs;
    }

    public static MongoClientSettings createMongocryptdClientSettings(final String connectionString) {

        return MongoClientSettings.builder()
                .applyToClusterSettings(builder -> builder.serverSelectionTimeout(10, TimeUnit.SECONDS))
                .applyToSocketSettings(builder -> {
                    builder.readTimeout(10, TimeUnit.SECONDS);
                    builder.connectTimeout(10, TimeUnit.SECONDS);
                })
                .applyConnectionString(new ConnectionString((connectionString != null)
                        ? connectionString : "mongodb://localhost:27020"))
                .build();
    }

    public static ProcessBuilder createProcessBuilder(final Map<String, Object> options) {
        return new ProcessBuilder(createMongocryptdSpawnArgs(options));
    }

    public static void startProcess(final ProcessBuilder processBuilder) {
        try {
            processBuilder.redirectErrorStream(true);
            processBuilder.redirectOutput(new File(System.getProperty("os.name").startsWith("Windows") ? "NUL" : "/dev/null"));
            processBuilder.start();
        } catch (Throwable t) {
            throw new MongoClientException("Exception starting mongocryptd process. Is `mongocryptd` on the system path?", t);
        }
    }

    private MongoCryptHelper() {
    }
}
