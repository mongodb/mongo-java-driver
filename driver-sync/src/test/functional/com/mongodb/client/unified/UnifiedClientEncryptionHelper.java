/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.unified;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.model.vault.RewrapManyDataKeyOptions;
import com.mongodb.client.model.vault.RewrapManyDataKeyResult;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static com.mongodb.ClusterFixture.getEnv;
import static java.lang.Math.toIntExact;

public final class UnifiedClientEncryptionHelper {
    private static final BsonDocument PLACEHOLDER = BsonDocument.parse("{'$$placeholder': 1}");
    private final Entities entities;

    UnifiedClientEncryptionHelper(final Entities entities) {
        this.entities = entities;
    }

    private OperationResult resultOf(final Supplier<BsonValue> operationResult) {
        try {
            return OperationResult.of(operationResult.get());
        } catch (Exception e) {
            return OperationResult.of(e);
        }
    }

    static Map<String, Map<String, Object>> createKmsProvidersMap(final BsonDocument kmsProviders) {
        Map<String, Map<String, Object>> kmsProvidersMap = new HashMap<>();
        for (String kmsProviderKey : kmsProviders.keySet()) {
            BsonDocument kmsProviderOptions = kmsProviders.get(kmsProviderKey).asDocument();
            Map<String, Object> kmsProviderMap = new HashMap<>();
            switch (kmsProviderKey) {
                case "aws":
                case "aws:name1":
                    setKmsProviderProperty(kmsProviderMap, kmsProviderOptions, "accessKeyId", "AWS_ACCESS_KEY_ID");
                    setKmsProviderProperty(kmsProviderMap, kmsProviderOptions, "secretAccessKey", "AWS_SECRET_ACCESS_KEY");
                    break;
                case "aws:name2":
                    setKmsProviderProperty(kmsProviderMap, kmsProviderOptions, "accessKeyId", "AWS_ACCESS_KEY_ID_AWS_KMS_NAMED");
                    setKmsProviderProperty(kmsProviderMap, kmsProviderOptions, "secretAccessKey", "AWS_SECRET_ACCESS_KEY_AWS_KMS_NAMED");
                    break;
                case "awsTemporary":
                    setKmsProviderProperty(kmsProviderMap, kmsProviderOptions, "accessKeyId", "AWS_TEMP_ACCESS_KEY_ID");
                    setKmsProviderProperty(kmsProviderMap, kmsProviderOptions, "secretAccessKey", "AWS_TEMP_SECRET_ACCESS_KEY");
                    setKmsProviderProperty(kmsProviderMap, kmsProviderOptions, "sessionToken", "AWS_TEMP_SESSION_TOKEN");
                    break;
                case "awsTemporaryNoSessionToken":
                    setKmsProviderProperty(kmsProviderMap, kmsProviderOptions, "accessKeyId", "AWS_TEMP_ACCESS_KEY_ID");
                    setKmsProviderProperty(kmsProviderMap, kmsProviderOptions, "secretAccessKey", "AWS_TEMP_SECRET_ACCESS_KEY");
                    break;
                case "azure":
                case "azure:name1":
                    setKmsProviderProperty(kmsProviderMap, kmsProviderOptions, "tenantId", "AZURE_TENANT_ID");
                    setKmsProviderProperty(kmsProviderMap, kmsProviderOptions, "clientId", "AZURE_CLIENT_ID");
                    setKmsProviderProperty(kmsProviderMap, kmsProviderOptions, "clientSecret", "AZURE_CLIENT_SECRET");
                    break;
                case "gcp":
                case "gcp:name1":
                    setKmsProviderProperty(kmsProviderMap, kmsProviderOptions, "email", "GCP_EMAIL");
                    setKmsProviderProperty(kmsProviderMap, kmsProviderOptions, "privateKey", "GCP_PRIVATE_KEY");
                    break;
                case "kmip":
                case "kmip:name1":
                    setKmsProviderProperty(
                            kmsProviderMap,
                            kmsProviderOptions,
                            "endpoint",
                            () -> getEnv("org.mongodb.test.kmipEndpoint", "localhost:5698"));
                    break;
                case "local":
                case "local:name1":
                    setKmsProviderProperty(
                            kmsProviderMap,
                            kmsProviderOptions,
                            "key",
                            UnifiedClientEncryptionHelper::localKmsProviderKey);
                    break;
                case "local:name2":
                    setKmsProviderProperty(
                            kmsProviderMap,
                            kmsProviderOptions,
                            "key", () -> decodeKmsProviderString(kmsProviderOptions.getString("key").getValue()));
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported KMS provider: " + kmsProviderKey);
            }
            kmsProvidersMap.put(kmsProviderKey, kmsProviderMap);
        }
        return kmsProvidersMap;
    }

    public static byte[] localKmsProviderKey() {
        return decodeKmsProviderString("Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6NmdWMDFBMUN3YkQ5aXRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZ"
                + "GJkTXVyZG9uSjFk");
    }

    public static byte[] decodeKmsProviderString(final String key) {
        return Base64.getDecoder().decode(key);
    }

    private static void setKmsProviderProperty(final Map<String, Object> kmsProviderMap,
            final BsonDocument kmsProviderOptions, final String key, final String propertyName) {
        setKmsProviderProperty(
                kmsProviderMap,
                kmsProviderOptions,
                key,
                () -> {
                    if (getEnv(propertyName) != null) {
                        return getEnv(propertyName);
                    }
                    throw new UnsupportedOperationException("Missing system property for: " + key);
                });
    }

    private static void setKmsProviderProperty(final Map<String, Object> kmsProviderMap,
                                               final BsonDocument kmsProviderOptions, final String key,
                                               @Nullable final Supplier<Object> placeholderPropertySupplier) {
        if (kmsProviderOptions.containsKey(key)) {
            boolean isPlaceholderValue = kmsProviderOptions.get(key).equals(PLACEHOLDER);
            if (isPlaceholderValue) {
                if (placeholderPropertySupplier == null) {
                    throw new UnsupportedOperationException("Placeholder is not supported for: " + key + " :: " + kmsProviderOptions.toJson());
                }
                kmsProviderMap.put(key, placeholderPropertySupplier.get());
                return;
            }

            BsonValue kmsValue = kmsProviderOptions.get(key);
            if (kmsValue.isString()) {
                kmsProviderMap.put(key, decodeKmsProviderString(kmsValue.asString().getValue()));
            } else {
                kmsProviderMap.put(key, kmsValue);
            }
        }
    }

    OperationResult executeCreateDataKey(final BsonDocument operation) {
        ClientEncryption clientEncryption = entities.getClientEncryption(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());

        DataKeyOptions dataKeyOptions = new DataKeyOptions();
        BsonDocument options = arguments.getDocument("opts", new BsonDocument());
        for (String key : options.keySet()) {
            switch (key) {
                case "keyAltNames":
                    List<String> keyAltNames = new ArrayList<>();
                    options.getArray("keyAltNames", new BsonArray()).forEach(v -> keyAltNames.add(v.asString().getValue()));
                    dataKeyOptions.keyAltNames(keyAltNames);
                    break;
                case "masterKey":
                    dataKeyOptions.masterKey(options.getDocument("masterKey"));
                    break;
                case "keyMaterial":
                    dataKeyOptions.keyMaterial(options.getBinary("keyMaterial").getData());
                    break;
                default:
                    throw new UnsupportedOperationException("Missing key handler for: " + key + " :: " + options.toJson());
            }
        }
        return resultOf(() -> clientEncryption.createDataKey(arguments.getString("kmsProvider").getValue(), dataKeyOptions));
    }


    OperationResult executeAddKeyAltName(final BsonDocument operation) {
        ClientEncryption clientEncryption = entities.getClientEncryption(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        return resultOf(() -> clientEncryption.addKeyAltName(arguments.getBinary("id"), arguments.getString("keyAltName").getValue()));
    }

    OperationResult executeRemoveKeyAltName(final BsonDocument operation) {
        ClientEncryption clientEncryption = entities.getClientEncryption(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        return resultOf(() -> clientEncryption.removeKeyAltName(arguments.getBinary("id"), arguments.getString("keyAltName").getValue()));
    }

    OperationResult executeDeleteKey(final BsonDocument operation) {
        ClientEncryption clientEncryption = entities.getClientEncryption(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        return resultOf(() -> toExpected(clientEncryption.deleteKey(arguments.getBinary("id"))));
    }

    OperationResult executeGetKey(final BsonDocument operation) {
        ClientEncryption clientEncryption = entities.getClientEncryption(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        return resultOf(() -> clientEncryption.getKey(arguments.getBinary("id")));
    }

    OperationResult executeGetKeyByAltName(final BsonDocument operation) {
        ClientEncryption clientEncryption = entities.getClientEncryption(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        return resultOf(() -> clientEncryption.getKeyByAltName(arguments.getString("keyAltName").getValue()));
    }

    OperationResult executeGetKeys(final BsonDocument operation) {
        ClientEncryption clientEncryption = entities.getClientEncryption(operation.getString("object").getValue());
        return resultOf(() ->  new BsonArray(clientEncryption.getKeys().into(new ArrayList<>())));
    }

    OperationResult executeRewrapManyDataKey(final BsonDocument operation) {
        ClientEncryption clientEncryption = entities.getClientEncryption(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        BsonDocument options = arguments.getDocument("opts", new BsonDocument());

        BsonDocument filter = arguments.getDocument("filter");
        RewrapManyDataKeyOptions rewrapManyDataKeyOptions = new RewrapManyDataKeyOptions();
        for (String key : options.keySet()) {
            switch (key) {
                case "provider":
                    rewrapManyDataKeyOptions.provider(options.getString("provider").getValue());
                    break;
                case "masterKey":
                    rewrapManyDataKeyOptions.masterKey(options.getDocument("masterKey"));
                    break;
                default:
                    throw new UnsupportedOperationException("Missing key handler for: " + key + " :: " + options.toJson());
            }
        }
        return resultOf(() ->  toExpected(clientEncryption.rewrapManyDataKey(filter, rewrapManyDataKeyOptions)));
    }

    OperationResult executeEncrypt(final BsonDocument operation) {
        ClientEncryption clientEncryption = entities.getClientEncryption(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments");
        BsonDocument options = arguments.getDocument("opts");

        BsonString value = arguments.getString("value");
        String algorithm =  options.remove("algorithm")
                .asString()
                .getValue();

        EncryptOptions encryptOptions = new EncryptOptions(algorithm);
        for (String key : options.keySet()) {
            switch (key) {
                case "keyAltName":
                    encryptOptions.keyAltName(options.getString("keyAltName").getValue());
                    break;
                default:
                    throw new UnsupportedOperationException("Missing key handler for: " + key + " :: " + options.toJson());
            }
        }
        return resultOf(() ->  clientEncryption.encrypt(value, encryptOptions));
    }


    OperationResult executeDecrypt(final BsonDocument operation) {
        ClientEncryption clientEncryption = entities.getClientEncryption(operation.getString("object").getValue());
        BsonDocument arguments = operation.getDocument("arguments");
        BsonBinary value = arguments.getBinary("value");

        return resultOf(() ->  clientEncryption.decrypt(value));
    }

    private BsonDocument toExpected(final DeleteResult result) {
        if (result.wasAcknowledged()) {
            return new BsonDocument("deletedCount", new BsonInt32(toIntExact(result.getDeletedCount())));
        } else {
            return new BsonDocument();
        }
    }

    private BsonDocument toExpected(final RewrapManyDataKeyResult result) {
        if (result.getBulkWriteResult() != null) {
            return new BsonDocument("bulkWriteResult", toExpected(result.getBulkWriteResult()));
        }
        return new BsonDocument();
    }

    private BsonDocument toExpected(final BulkWriteResult result) {
        if (result.wasAcknowledged()) {
            BsonDocument upsertedIds = new BsonDocument();
            result.getUpserts().forEach(u -> upsertedIds.put("" + u.getIndex(), u.getId()));
            return new BsonDocument("insertedCount", new BsonInt32(result.getInsertedCount()))
                    .append("insertedCount", new BsonInt32(result.getInsertedCount()))
                    .append("matchedCount", new BsonInt32(result.getMatchedCount()))
                    .append("modifiedCount", new BsonInt32(result.getModifiedCount()))
                    .append("deletedCount", new BsonInt32(result.getDeletedCount()))
                    .append("upsertedCount", new BsonInt32(result.getUpserts().size()))
                    .append("upsertedIds", upsertedIds);
        }

        return new BsonDocument();
    }
}
