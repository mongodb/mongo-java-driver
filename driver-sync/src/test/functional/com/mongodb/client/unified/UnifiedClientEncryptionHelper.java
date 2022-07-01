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
import com.mongodb.client.model.vault.RewrapManyDataKeyOptions;
import com.mongodb.client.model.vault.RewrapManyDataKeyResult;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.vault.ClientEncryption;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

final class UnifiedClientEncryptionHelper {
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

    public static Map<String, Map<String, Object>> createKmsProvidersMap(final BsonDocument kmsProviders) {
        Map<String, Map<String, Object>> kmsProvidersMap = new HashMap<>();
        for (String kmsProviderKey : kmsProviders.keySet()) {
            BsonDocument kmsProviderOptions = kmsProviders.get(kmsProviderKey).asDocument();
            Map<String, Object> kmsProviderMap = new HashMap<>();
            switch (kmsProviderKey) {
                case "aws":
                    setKmsProviderProperty(kmsProviderMap, kmsProviderOptions, "accessKeyId", "org.mongodb.test.awsAccessKeyId");
                    setKmsProviderProperty(kmsProviderMap, kmsProviderOptions, "secretAccessKey", "org.mongodb.test.awsSecretAccessKey");
                    break;
                case "awsTemporary":
                    setKmsProviderProperty(kmsProviderMap, kmsProviderOptions, "accessKeyId", "org.mongodb.test.tmpAwsAccessKeyId");
                    setKmsProviderProperty(kmsProviderMap, kmsProviderOptions, "secretAccessKey", "org.mongodb.test.tmpAwsSecretAccessKey");
                    setKmsProviderProperty(kmsProviderMap, kmsProviderOptions, "sessionToken", "org.mongodb.test.tmpAwsSessionToken");
                    break;
                case "awsTemporaryNoSessionToken":
                    setKmsProviderProperty(kmsProviderMap, kmsProviderOptions, "accessKeyId", "org.mongodb.test.tmpAwsAccessKeyId");
                    setKmsProviderProperty(kmsProviderMap, kmsProviderOptions, "secretAccessKey", "org.mongodb.test.tmpAwsSecretAccessKey");
                    break;
                case "azure":
                    setKmsProviderProperty(kmsProviderMap, kmsProviderOptions, "tenantId", "org.mongodb.test.azureTenantId");
                    setKmsProviderProperty(kmsProviderMap, kmsProviderOptions, "clientId", "org.mongodb.test.azureClientId");
                    setKmsProviderProperty(kmsProviderMap, kmsProviderOptions, "clientSecret", "org.mongodb.test.azureClientSecret");
                    break;
                case "gcp":
                    setKmsProviderProperty(kmsProviderMap, kmsProviderOptions, "email", "org.mongodb.test.gcpEmail");
                    setKmsProviderProperty(kmsProviderMap, kmsProviderOptions, "privateKey", "org.mongodb.test.gcpPrivateKey");
                    break;
                case "kmip":
                    setKmsProviderProperty(kmsProviderMap, kmsProviderOptions, "endpoint", () ->
                            System.getProperty("org.mongodb.test.kmipEndpoint", "localhost:5698"));
                    break;
                case "local":
                    setKmsProviderProperty(kmsProviderMap, kmsProviderOptions, "key", () ->
                            "Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6NmdWMDFBM"
                            + "UN3YkQ5aXRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZGJkTXVyZG9uSjFk");
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported KMS provider: " + kmsProviderKey);
            }
            kmsProvidersMap.put(kmsProviderKey, kmsProviderMap);
        }
        return kmsProvidersMap;
    }

    private static void setKmsProviderProperty(final Map<String, Object> kmsProviderMap,
            final BsonDocument kmsProviderOptions, final String key, final String propertyName) {
        setKmsProviderProperty(kmsProviderMap, kmsProviderOptions, key, () -> {
            if (System.getProperties().containsKey(propertyName)) {
                return System.getProperty(propertyName);
            }
            throw new UnsupportedOperationException("Missing system property for: " + key);
        });
    }

    private static void setKmsProviderProperty(final Map<String, Object> kmsProviderMap,
            final BsonDocument kmsProviderOptions, final String key, final Supplier<Object> propertySupplier) {
        if (kmsProviderOptions.containsKey(key)) {
            if (kmsProviderOptions.get(key).equals(PLACEHOLDER)) {
                kmsProviderMap.put(key, propertySupplier.get());
            } else {
                throw new UnsupportedOperationException("Missing key handler for: " + key + " :: " + kmsProviderOptions.toJson());
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

    private BsonDocument toExpected(final DeleteResult result) {
        if (result.wasAcknowledged()) {
            return new BsonDocument("deletedCount", new BsonInt32((int) result.getDeletedCount()));
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
