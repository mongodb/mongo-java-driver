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

package com.mongodb.reactivestreams.client;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.reactivestreams.client.vault.ClientEncryption;
import com.mongodb.reactivestreams.client.vault.ClientEncryptions;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.UuidRepresentation;
import org.bson.codecs.UuidCodec;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;
import static com.mongodb.ClusterFixture.getEnv;
import static com.mongodb.ClusterFixture.hasEncryptionTestsEnabled;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.reactivestreams.client.Fixture.getMongoClientBuilderFromConnectionString;
import static com.mongodb.reactivestreams.client.Fixture.getMongoClientSettings;
import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assume.assumeTrue;
import static util.JsonPoweredTestHelper.getTestDocument;

// See https://github.com/mongodb/specifications/tree/master/source/client-side-encryption/corpus
@RunWith(Parameterized.class)
public class ClientSideEncryptionCorpusTest {
    private final boolean useLocalSchema;
    private MongoClient client;
    private MongoClient autoEncryptingClient;
    private ClientEncryption clientEncryption;

    public ClientSideEncryptionCorpusTest(final boolean useLocalSchema) {
        this.useLocalSchema = useLocalSchema;
    }

    @Before
    public void setUp() throws IOException, URISyntaxException {
        assumeTrue(serverVersionAtLeast(4, 2));
        assumeTrue("Corpus tests disabled", hasEncryptionTestsEnabled());

        MongoClientSettings clientSettings = getMongoClientBuilderFromConnectionString()
                .codecRegistry(fromRegistries(fromCodecs(new UuidCodec(UuidRepresentation.STANDARD)),
                        MongoClientSettings.getDefaultCodecRegistry())).build();

        // Step 1: create unencrypted client
        client = MongoClients.create(clientSettings);
        MongoDatabase db = client.getDatabase("db");

        // Step 2: Drop and recreate db.coll with schema
        BsonDocument schemaDocument = bsonDocumentFromPath("corpus-schema.json");

        Mono.from(db.getCollection("coll").drop()).block(TIMEOUT_DURATION);

        Mono.from(db.runCommand(new BsonDocument("create", new BsonString("coll"))
                    .append("validator", new BsonDocument("$jsonSchema", schemaDocument))))
                    .block(TIMEOUT_DURATION);

        // Step 3: Drop and create keyvault.datakeys
        MongoDatabase keyVaultDatabase = client.getDatabase("keyvault");
        MongoCollection<BsonDocument> dataKeysCollection = keyVaultDatabase.getCollection("datakeys", BsonDocument.class)
                .withWriteConcern(WriteConcern.MAJORITY);

        Mono.from(dataKeysCollection.drop()).block(TIMEOUT_DURATION);

        Mono.from(dataKeysCollection.insertOne(bsonDocumentFromPath("corpus-key-aws.json"))).block(TIMEOUT_DURATION);
        Mono.from(dataKeysCollection.insertOne(bsonDocumentFromPath("corpus-key-azure.json"))).block(TIMEOUT_DURATION);
        Mono.from(dataKeysCollection.insertOne(bsonDocumentFromPath("corpus-key-gcp.json"))).block(TIMEOUT_DURATION);
        Mono.from(dataKeysCollection.insertOne(bsonDocumentFromPath("corpus-key-kmip.json"))).block(TIMEOUT_DURATION);
        Mono.from(dataKeysCollection.insertOne(bsonDocumentFromPath("corpus-key-local.json"))).block(TIMEOUT_DURATION);

        // Step 4: Configure our objects
        Map<String, Map<String, Object>> kmsProviders = new HashMap<String, Map<String, Object>>() {{
            put("aws",  new HashMap<String, Object>() {{
                put("accessKeyId", getEnv("AWS_ACCESS_KEY_ID"));
                put("secretAccessKey", getEnv("AWS_SECRET_ACCESS_KEY"));
            }});
            put("azure",  new HashMap<String, Object>() {{
                put("tenantId", getEnv("AZURE_TENANT_ID"));
                put("clientId", getEnv("AZURE_CLIENT_ID"));
                put("clientSecret", getEnv("AZURE_CLIENT_SECRET"));
            }});
            put("gcp",  new HashMap<String, Object>() {{
                put("email", getEnv("GCP_EMAIL"));
                put("privateKey", getEnv("GCP_PRIVATE_KEY"));
            }});
            put("kmip",  new HashMap<String, Object>() {{
                put("endpoint", getEnv("org.mongodb.test.kmipEndpoint", "localhost:5698"));
            }});
            put("local", new HashMap<String, Object>() {{
                put("key", "Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6NmdWMDFBM"
                        + "UN3YkQ5aXRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZGJkTXVyZG9uSjFk");
            }});
        }};

        HashMap<String, BsonDocument> schemaMap = new HashMap<>();
        schemaMap.put("db.coll", schemaDocument);

        AutoEncryptionSettings.Builder autoEncryptionSettingsBuilder = AutoEncryptionSettings.builder()
                .keyVaultNamespace("keyvault.datakeys")
                .kmsProviders(kmsProviders);

        if (useLocalSchema) {
            autoEncryptionSettingsBuilder.schemaMap(schemaMap);
        }

        clientSettings = getMongoClientBuilderFromConnectionString()
                .codecRegistry(fromRegistries(
                        fromCodecs(new UuidCodec(UuidRepresentation.STANDARD)), MongoClientSettings.getDefaultCodecRegistry()))
                .autoEncryptionSettings(autoEncryptionSettingsBuilder.build())
                .build();
        autoEncryptingClient = MongoClients.create(clientSettings);

        ClientEncryptionSettings clientEncryptionSettings = ClientEncryptionSettings.builder().
                keyVaultMongoClientSettings(getMongoClientSettings()).
                kmsProviders(kmsProviders).
                keyVaultNamespace("keyvault.datakeys").build();
        clientEncryption = ClientEncryptions.create(clientEncryptionSettings);
    }

    @Test
    public void testCorpus() throws IOException, URISyntaxException {

        // Step 5: Iterate over corpus
        BsonDocument corpus = bsonDocumentFromPath("corpus.json");
        BsonDocument corpusCopied = new BsonDocument();
        for (String field : corpus.keySet()) {
            if (!corpus.get(field).isDocument()) {
                corpusCopied.append(field, corpus.get(field));
                continue;
            }

            BsonDocument fieldDocument = corpus.getDocument(field).clone();
            String kms = fieldDocument.getString("kms").getValue();
            String abbreviatedAlgorithName = fieldDocument.getString("algo").getValue();
            String method = fieldDocument.getString("method").getValue();
            String identifier = fieldDocument.getString("identifier").getValue();
            boolean allowed = fieldDocument.getBoolean("allowed").getValue();
            BsonValue value = fieldDocument.get("value");

            byte[] awsKeyId = Base64.getDecoder().decode("AWSAAAAAAAAAAAAAAAAAAA==");
            byte[] azureKeyId = Base64.getDecoder().decode("AZUREAAAAAAAAAAAAAAAAA==");
            byte[] gcpKeyId = Base64.getDecoder().decode("GCPAAAAAAAAAAAAAAAAAAA==");
            byte[] kmipKeyId = Base64.getDecoder().decode("KMIPAAAAAAAAAAAAAAAAAA==");
            byte[] localKeyId = Base64.getDecoder().decode("LOCALAAAAAAAAAAAAAAAAA==");

            if (method.equals("auto")) {
                corpusCopied.append(field, corpus.get(field));
                continue;
            }

            if (!method.equals("explicit")) {
                throw new UnsupportedOperationException("Unsupported method: " + method);
            }

            String fullAlgorithmName = "AEAD_AES_256_CBC_HMAC_SHA_512-";
            if (abbreviatedAlgorithName.equals("rand")) {
                fullAlgorithmName += "Random";
            } else if (abbreviatedAlgorithName.equals("det")) {
                fullAlgorithmName += "Deterministic";
            } else {
                throw new UnsupportedOperationException("Unsupported algorithm: " + abbreviatedAlgorithName);
            }

            EncryptOptions opts = new EncryptOptions(fullAlgorithmName);
            if (identifier.equals("id")) {
                switch (kms) {
                    case "aws":
                        opts.keyId(new BsonBinary(BsonBinarySubType.UUID_STANDARD, awsKeyId));
                        break;
                    case "azure":
                        opts.keyId(new BsonBinary(BsonBinarySubType.UUID_STANDARD, azureKeyId));
                        break;
                    case "gcp":
                        opts.keyId(new BsonBinary(BsonBinarySubType.UUID_STANDARD, gcpKeyId));
                        break;
                    case "kmip":
                        opts.keyId(new BsonBinary(BsonBinarySubType.UUID_STANDARD, kmipKeyId));
                        break;
                    case "local":
                        opts.keyId(new BsonBinary(BsonBinarySubType.UUID_STANDARD, localKeyId));
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported provider: " + kms);
                }
            } else if (identifier.equals("altname")) {
                opts.keyAltName(kms);
            } else {
                throw new UnsupportedOperationException("Unsupported identifier: " + identifier);
            }

            try {
                BsonValue encryptedValue = Mono.from(clientEncryption.encrypt(value, opts)).block(TIMEOUT_DURATION);
                fieldDocument.put("value", encryptedValue);
                corpusCopied.append(field, fieldDocument);
            } catch (MongoException e) {
                if (allowed) {
                    throw e;
                }
                corpusCopied.append(field, fieldDocument);
            }
        }

        // Step 6: insert corpusCopied
        MongoCollection<BsonDocument> encryptedCollection = autoEncryptingClient.getDatabase("db")
                .getCollection("coll", BsonDocument.class);
        Mono.from(encryptedCollection.insertOne(corpusCopied)).block(TIMEOUT_DURATION);

        // Step 7: check the auto decrypted document
        BsonDocument corpusDecrypted = Mono.from(encryptedCollection.find(new BsonDocument()).first()).block(TIMEOUT_DURATION);
        assertEquals(corpus, corpusDecrypted);

        // Step 8: check the document with an unencrypted client
        MongoCollection<BsonDocument> coll = client.getDatabase("db").getCollection("coll", BsonDocument.class);
        BsonDocument corpusEncryptedActual = Mono.from(coll.find(new BsonDocument()).first()).block(TIMEOUT_DURATION);
        BsonDocument corpusEncryptedExpected = bsonDocumentFromPath("corpus-encrypted.json");

        for (String field : corpusEncryptedExpected.keySet()) {
            if (field.equals("_id") || field.equals("altname_aws") || field.equals("altname_local")) {
                continue;
            }

            boolean allowed = corpusEncryptedActual.getDocument(field).getBoolean("allowed").getValue();
            String algorithm = corpusEncryptedActual.getDocument(field).getString("algo").getValue();
            BsonValue actualValue = corpusEncryptedActual.getDocument(field).get("value");
            BsonValue expectedValue = corpusEncryptedExpected.getDocument(field).get("value");

            if (algorithm.equals("det")) {
                assertEquals(actualValue, expectedValue);
            } else if (algorithm.equals("rand")) {
                if (allowed) {
                    assertNotEquals(actualValue, expectedValue);
                }
            } else {
                throw new UnsupportedOperationException("Unsupported algorithm type: " + algorithm);
            }

            if (allowed) {
                BsonValue decrypted = Mono.from(clientEncryption.decrypt(actualValue.asBinary())).block(TIMEOUT_DURATION);
                BsonValue expectedDecrypted = Mono.from(clientEncryption.decrypt(expectedValue.asBinary())).block(TIMEOUT_DURATION);
                assertEquals("Values should be equal for field " + field, expectedDecrypted, decrypted);
            } else {
                assertEquals("Values should be equal for field " + field, expectedValue, actualValue);
            }
        }
    }

    private static BsonDocument bsonDocumentFromPath(final String path) {
        return getTestDocument("/client-side-encryption-corpus/" + path);
    }

    @Parameterized.Parameters(name = "useLocalSchema: {0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[]{true}, new Object[]{false});
    }

    @After
    public void after() {
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                // ignore
            }
        }

        if (autoEncryptingClient != null) {
            try {
                autoEncryptingClient.close();
            } catch (Exception e) {
                // ignore
            }
        }

        if (clientEncryption != null) {
            try {
                clientEncryption.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }
}
