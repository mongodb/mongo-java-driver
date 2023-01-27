/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */

package com.mongodb.client;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.MongoClientException;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.DropCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.model.vault.RangeOptions;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.test.AfterBeforeParameterResolver;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.ClusterFixture.isServerlessTest;
import static com.mongodb.ClusterFixture.isStandalone;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getDefaultDatabase;
import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.client.Fixture.getMongoClient;
import static com.mongodb.client.Fixture.getMongoClientSettings;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static util.JsonPoweredTestHelper.getTestDocument;


@ExtendWith(AfterBeforeParameterResolver.class)
public abstract class AbstractClientSideEncryptionRangeExplicitEncryptionTest {
    private MongoClient encryptedClient;
    private ClientEncryption clientEncryption;
    private BsonBinary key1Id;
    private EncryptOptions encryptOptions;
    private EncryptOptions encryptQueryOptions;
    private String encryptedField;
    private MongoCollection<BsonDocument> encryptedColl;
    protected abstract MongoClient createMongoClient(MongoClientSettings settings);

    protected abstract ClientEncryption createClientEncryption(ClientEncryptionSettings settings);

    @BeforeEach
    public void setUp(final Type type) {
        assumeTrue(serverVersionAtLeast(6, 2));
        assumeFalse(isStandalone());
        assumeFalse(isServerlessTest());

        MongoNamespace dataKeysNamespace = new MongoNamespace("keyvault.datakeys");
        BsonDocument encryptedFields = bsonDocumentFromPath("range-encryptedFields-" + type.value + ".json");
        BsonDocument key1Document = bsonDocumentFromPath("keys/key1-document.json");
        key1Id = key1Document.getBinary("_id");

        MongoDatabase explicitEncryptionDatabase = getDefaultDatabase();
        explicitEncryptionDatabase.getCollection("explicit_encryption")
                .drop(new DropCollectionOptions().encryptedFields(encryptedFields));
        explicitEncryptionDatabase.createCollection("explicit_encryption",
                new CreateCollectionOptions().encryptedFields(encryptedFields));

        MongoCollection<BsonDocument> dataKeysCollection = getMongoClient()
                .getDatabase(dataKeysNamespace.getDatabaseName())
                .getCollection(dataKeysNamespace.getCollectionName(), BsonDocument.class)
                .withWriteConcern(WriteConcern.MAJORITY);

        dataKeysCollection.drop();
        dataKeysCollection.insertOne(key1Document);

        Map<String, Map<String, Object>> kmsProviders = new HashMap<>();
        Map<String, Object> localProviderMap = new HashMap<>();
        localProviderMap.put("key",
                Base64.getDecoder().decode(
                        "Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6NmdWMDFBMUN3YkQ5aXRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZ"
                                + "GJkTXVyZG9uSjFk"));
        kmsProviders.put("local", localProviderMap);

        clientEncryption = createClientEncryption(ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(getMongoClientSettings())
                .keyVaultNamespace(dataKeysNamespace.getFullName())
                .kmsProviders(kmsProviders)
                .build());

        encryptedClient = createMongoClient(getMongoClientSettingsBuilder()
                .autoEncryptionSettings(
                        AutoEncryptionSettings.builder()
                                .keyVaultNamespace(dataKeysNamespace.getFullName())
                                .kmsProviders(kmsProviders)
                                .bypassQueryAnalysis(true)
                                .build())
                .build());

        encryptOptions = new EncryptOptions("RangePreview")
                .keyId(key1Id)
                .contentionFactor(0L)
                .rangeOptions(type.getRangeOptions());

        BsonBinary encryptedValue0 = clientEncryption.encrypt(type.convertNumber(0), encryptOptions);
        BsonBinary encryptedValue6 = clientEncryption.encrypt(type.convertNumber(6), encryptOptions);
        BsonBinary encryptedValue30 = clientEncryption.encrypt(type.convertNumber(30), encryptOptions);
        BsonBinary encryptedValue200 = clientEncryption.encrypt(type.convertNumber(200), encryptOptions);

        encryptQueryOptions = new EncryptOptions("RangePreview")
                .keyId(key1Id)
                .queryType("rangePreview")
                .contentionFactor(0L)
                .rangeOptions(type.getRangeOptions());

        encryptedColl = encryptedClient.getDatabase(getDefaultDatabaseName())
                .getCollection("explicit_encryption", BsonDocument.class);

        encryptedField = "encrypted" + type.value;
        encryptedColl.insertOne(new BsonDocument("_id", new BsonInt32(0)).append(encryptedField, encryptedValue0));
        encryptedColl.insertOne(new BsonDocument("_id", new BsonInt32(1)).append(encryptedField, encryptedValue6));
        encryptedColl.insertOne(new BsonDocument("_id", new BsonInt32(2)).append(encryptedField, encryptedValue30));
        encryptedColl.insertOne(new BsonDocument("_id", new BsonInt32(3)).append(encryptedField, encryptedValue200));
    }


    @AfterEach
    @SuppressWarnings("try")
    public void cleanUp() {
        //noinspection EmptyTryBlock
        try (ClientEncryption ignored = clientEncryption;
             MongoClient ignored1 = encryptedClient
        ) {
            // just using try-with-resources to ensure they all get closed, even in the case of exceptions
        }
    }

    @ParameterizedTest(name = "[{0}] Case 1: can decrypt a payload")
    @EnumSource(Type.class)
    void testCanDecryptAPayload(final Type type) {
        BsonValue originalValue = type.convertNumber(6);
        BsonBinary insertPayload = clientEncryption.encrypt(originalValue, encryptOptions);

        BsonValue decryptedValue = clientEncryption.decrypt(insertPayload);

        assertEquals(originalValue, decryptedValue);
    }

    @ParameterizedTest(name = "[{0}] Case 2: can find encrypted range and return the maximum")
    @EnumSource(Type.class)
    void testCanFindEncryptedRangeAndReturnTheMaximum(final Type type) {
        BsonDocument expression = Filters.and(
                Filters.gte(encryptedField, type.convertNumber(6)),
                Filters.lte(encryptedField, type.convertNumber(200))).toBsonDocument();

        BsonDocument findPayload = clientEncryption.encryptExpression(expression, encryptQueryOptions);

        List<BsonDocument> expected = asList(
                new BsonDocument(encryptedField, type.convertNumber(6)),
                new BsonDocument(encryptedField, type.convertNumber(30)),
                new BsonDocument(encryptedField, type.convertNumber(200)));

        List<BsonDocument> actual = encryptedColl.find(findPayload)
                .projection(Projections.fields(Projections.excludeId(), Projections.include(encryptedField)))
                .sort(Sorts.ascending("_id"))
                .into(new ArrayList<>());

        assertIterableEquals(expected, actual);
    }

    @ParameterizedTest(name = "[{0}] Case 3: can find encrypted range and return the minimum")
    @EnumSource(Type.class)
    void testCanFindEncryptedRangeAndReturnTheMinimum(final Type type) {
        BsonDocument expression = Filters.and(
                Filters.gte(encryptedField, type.convertNumber(0)),
                Filters.lte(encryptedField, type.convertNumber(6))).toBsonDocument();

        BsonDocument findPayload = clientEncryption.encryptExpression(expression, encryptQueryOptions);

        List<BsonDocument> expected = asList(
                new BsonDocument(encryptedField, type.convertNumber(0)),
                new BsonDocument(encryptedField, type.convertNumber(6)));

        List<BsonDocument> actual = encryptedColl.find(findPayload)
                .projection(Projections.fields(Projections.excludeId(), Projections.include(encryptedField)))
                .sort(Sorts.ascending("_id"))
                .into(new ArrayList<>());

        assertIterableEquals(expected, actual);
    }

    @ParameterizedTest(name = "[{0}] Case 4: can find encrypted range with an open range query")
    @EnumSource(Type.class)
    void testCanFindEncryptedRangeWithAnOpenRangeQuery(final Type type) {
        BsonDocument expression = Filters.and(
                Filters.gt(encryptedField, type.convertNumber(30))).toBsonDocument();

        BsonDocument findPayload = clientEncryption.encryptExpression(expression, encryptQueryOptions);

        List<BsonDocument> expected = singletonList(new BsonDocument(encryptedField, type.convertNumber(200)));

        List<BsonDocument> actual = encryptedColl.find(findPayload)
                .projection(Projections.fields(Projections.excludeId(), Projections.include(encryptedField)))
                .sort(Sorts.ascending("_id"))
                .into(new ArrayList<>());

        assertIterableEquals(expected, actual);
    }

    @ParameterizedTest(name = "[{0}] Case 5: can run an aggregation expression inside $expr")
    @EnumSource(Type.class)
    void testCanRunAnAggregationExpressionInsideExpr(final Type type) {
        BsonDocument expression = new BsonDocument("$and",
                new BsonArray(singletonList(new BsonDocument("$lt",
                        new BsonArray(asList(new BsonString("$" + encryptedField), type.convertNumber(30)))))));

        BsonDocument findPayload = clientEncryption.encryptExpression(expression, encryptQueryOptions);

        List<BsonDocument> expected = asList(
                new BsonDocument(encryptedField, type.convertNumber(0)),
                new BsonDocument(encryptedField, type.convertNumber(6)));

        List<BsonDocument> actual = encryptedColl.find(new BsonDocument("$expr", findPayload))
                .projection(Projections.fields(Projections.excludeId(), Projections.include(encryptedField)))
                .sort(Sorts.ascending("_id"))
                .into(new ArrayList<>());

        assertIterableEquals(expected, actual);
    }

    @ParameterizedTest(name = "[{0}] Case 6: encrypting a document greater than the maximum errors")
    @EnumSource(value = Type.class, mode = EnumSource.Mode.EXCLUDE, names = { "DECIMAL_NO_PRECISION", "DOUBLE_NO_PRECISION" })
    void testEncryptingADocumentGreaterThanTheMaximumErrors(final Type type) {
        BsonValue originalValue = type.convertNumber(201);

        assertThrows(MongoClientException.class, () -> clientEncryption.encrypt(originalValue, encryptOptions));
    }

    @ParameterizedTest(name = "[{0}] Case 7: encrypting a document of a different type errors")
    @EnumSource(value = Type.class, mode = EnumSource.Mode.EXCLUDE, names = { "DECIMAL_NO_PRECISION", "DOUBLE_NO_PRECISION" })
    void testEncryptingADocumentOfADifferentTypeErrors(final Type type) {
        BsonValue originalValue = type == Type.INT ? new BsonDouble(6) : new BsonInt32(6);

        assertThrows(MongoClientException.class, () -> clientEncryption.encrypt(originalValue, encryptOptions));
    }

    @ParameterizedTest(name = "[{0}] Case 8: setting precision errors if the type is not a double")
    @EnumSource(value = Type.class, mode = EnumSource.Mode.EXCLUDE,
            names = {"DECIMAL_PRECISION", "DECIMAL_NO_PRECISION", "DOUBLE_PRECISION", "DOUBLE_NO_PRECISION" })
    void testSettingPrecisionErrorsIfTheTypeIsNotADouble(final Type type) {
        BsonValue originalValue = type == Type.INT ? new BsonDouble(6) : new BsonInt32(6);

        EncryptOptions precisionEncryptOptions = new EncryptOptions("RangePreview")
                .keyId(key1Id)
                .contentionFactor(0L)
                .rangeOptions(type.getRangeOptions().precision(2));

        assertThrows(MongoClientException.class, () -> clientEncryption.encrypt(originalValue, precisionEncryptOptions));
    }

    enum Type {
        DECIMAL_PRECISION("DecimalPrecision"),
        DECIMAL_NO_PRECISION("DecimalNoPrecision"),
        DOUBLE_PRECISION("DoublePrecision"),
        DOUBLE_NO_PRECISION("DoubleNoPrecision"),
        DATE("Date"),
        INT("Int"),
        LONG("Long");
        private final String value;
        Type(final String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }

        RangeOptions getRangeOptions() {
            RangeOptions rangeOptions = new RangeOptions().sparsity(1L);
            switch (this) {
                case DECIMAL_NO_PRECISION:
                case DOUBLE_NO_PRECISION:
                    return rangeOptions;
                case DECIMAL_PRECISION:
                    return rangeOptions.precision(2)
                            .min(new BsonDecimal128(Decimal128.parse("0")))
                            .max(new BsonDecimal128(Decimal128.parse("200")));
                case DOUBLE_PRECISION:
                    return rangeOptions.precision(2).min(new BsonDouble(0)).max(new BsonDouble(200));
                case DATE:
                    return rangeOptions.min(new BsonDateTime(0)).max(new BsonDateTime(200));
                case INT:
                    return rangeOptions.min(new BsonInt32(0)).max(new BsonInt32(200));
                case LONG:
                    return rangeOptions.min(new BsonInt64(0)).max(new BsonInt64(200));
                default:
                    throw new UnsupportedOperationException("Unsupported Type " + this);
            }
        }

        BsonValue convertNumber(final int number) {
            switch (this) {
                case DECIMAL_PRECISION:
                case DECIMAL_NO_PRECISION:
                    return new BsonDecimal128(new Decimal128(number));
                case DOUBLE_PRECISION:
                case DOUBLE_NO_PRECISION:
                    return new BsonDouble(number);
                case DATE:
                    return new BsonDateTime(number);
                case INT:
                    return new BsonInt32(number);
                case LONG:
                    return new BsonInt64(number);
                default:
                    throw new UnsupportedOperationException("Unsupported Type " + this);
            }
        }
    }

    private static BsonDocument bsonDocumentFromPath(final String path) {
        try {
            return getTestDocument(new File(AbstractClientSideEncryptionRangeExplicitEncryptionTest.class
                    .getResource("/client-side-encryption-data/" + path).toURI()));
        } catch (Exception e) {
            fail("Unable to load resource", e);
            return null;
        }
    }

}
