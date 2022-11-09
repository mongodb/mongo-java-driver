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

package com.mongodb.client;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.BSONException;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.types.Binary;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import util.Hex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.client.Fixture.getMongoClient;
import static org.bson.UuidRepresentation.JAVA_LEGACY;
import static org.bson.UuidRepresentation.STANDARD;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public abstract class AbstractUuidRepresentationTest {
    private final UuidRepresentation uuidRepresentation;
    private final BsonBinarySubType subType;
    private final UUID uuid;
    private final byte[] encodedValue;
    private final byte[] standardEncodedValue;
    private MongoCollection<Document> documentCollection;
    private MongoCollection<DBObject> dbObjectCollection;
    private MongoCollection<UuidIdPojo> uuidIdPojoCollection;
    private MongoCollection<BsonDocument> bsonDocumentCollection;


    public AbstractUuidRepresentationTest(final UuidRepresentation uuidRepresentation, final BsonBinarySubType subType,
                                          final UUID uuid,
                                          final byte[] encodedValue, final byte[] standardEncodedValue) {
        this.uuidRepresentation = uuidRepresentation;
        this.subType = subType;
        this.uuid = uuid;
        this.encodedValue = encodedValue;
        this.standardEncodedValue = standardEncodedValue;
    }

    protected abstract void createMongoClient(UuidRepresentation uuidRepresentation, CodecRegistry codecRegistry);

    protected abstract MongoDatabase getDatabase(String databaseName);


    @Before
    public void setUp() {
        PojoCodecProvider pojoCodecProvider = PojoCodecProvider.builder().automatic(true).build();
        CodecRegistry codecRegistry = fromRegistries(getDefaultCodecRegistry(), fromProviders(pojoCodecProvider));

        createMongoClient(uuidRepresentation, codecRegistry);
        MongoDatabase database = getDatabase(getDefaultDatabaseName());
        documentCollection = database.getCollection(getClass().getName());
        dbObjectCollection = documentCollection.withDocumentClass(DBObject.class);
        uuidIdPojoCollection = documentCollection.withDocumentClass(UuidIdPojo.class);

        bsonDocumentCollection = getMongoClient().getDatabase(getDefaultDatabaseName())
                .getCollection(getClass().getName())
                .withDocumentClass(BsonDocument.class);

        bsonDocumentCollection.drop();
    }

    @Test
    public void shouldEncodeDocumentWithUuidRepresentation() {

        if (uuidRepresentation == UuidRepresentation.UNSPECIFIED) {
            try {
                documentCollection.insertOne(new Document("_id", uuid));
                fail();
            } catch (CodecConfigurationException e) {
                // all good
            }
        } else {
            documentCollection.insertOne(new Document("_id", uuid));

            BsonDocument document = bsonDocumentCollection.find().first();
            assertNotNull(document);
            BsonBinary uuidAsBinary = document.getBinary("_id");
            assertEquals(subType.getValue(), uuidAsBinary.getType());
            assertArrayEquals(encodedValue, uuidAsBinary.getData());
        }
    }

    @Test
    public void shouldEncodeDbObjectWithUuidRepresentation() {

        if (uuidRepresentation == UuidRepresentation.UNSPECIFIED) {
            try {
                dbObjectCollection.insertOne(new BasicDBObject("_id", uuid));
                fail();
            } catch (CodecConfigurationException e) {
                // all good
            }
        } else {
            dbObjectCollection.insertOne(new BasicDBObject("_id", uuid));

            BsonDocument document = bsonDocumentCollection.find().first();
            assertNotNull(document);
            BsonBinary uuidAsBinary = document.getBinary("_id");
            assertEquals(subType.getValue(), uuidAsBinary.getType());
            assertEquals(subType.getValue(), uuidAsBinary.getType());
            assertArrayEquals(encodedValue, uuidAsBinary.getData());
        }
    }

    @Test
    public void shouldEncodePojoWithUuidRepresentation() {
        if (uuidRepresentation == UuidRepresentation.UNSPECIFIED) {
            try {
                uuidIdPojoCollection.insertOne(new UuidIdPojo(uuid));
                fail();
            } catch (CodecConfigurationException e) {
                // all good
            }
        } else {
            uuidIdPojoCollection.insertOne(new UuidIdPojo(uuid));

            BsonDocument document = bsonDocumentCollection.find().first();
            assertNotNull(document);
            BsonBinary uuidAsBinary = document.getBinary("_id");
            assertEquals(subType.getValue(), uuidAsBinary.getType());
            assertEquals(subType.getValue(), uuidAsBinary.getType());
            assertArrayEquals(encodedValue, uuidAsBinary.getData());
        }
    }

    @Test
    public void shouldDecodeDocumentWithUuidRepresentation() {

        bsonDocumentCollection.insertOne(new BsonDocument("standard", new BsonBinary(uuid, STANDARD))
                .append("legacy", new BsonBinary(uuid,
                        uuidRepresentation == UuidRepresentation.UNSPECIFIED || uuidRepresentation == STANDARD
                                ? UuidRepresentation.PYTHON_LEGACY
                                : uuidRepresentation)));

        Document document;
        try {
            document = documentCollection.find().first();
            assertNotNull(document);
        } catch (BSONException e) {
            if (uuidRepresentation != STANDARD && uuidRepresentation != JAVA_LEGACY) {
                throw e;
            }
            return;
        }

        if (uuidRepresentation == UuidRepresentation.UNSPECIFIED) {
            assertEquals(Binary.class, document.get("standard").getClass());
            assertEquals(new Binary(BsonBinarySubType.UUID_STANDARD, standardEncodedValue), document.get("standard"));

            assertEquals(Binary.class, document.get("legacy").getClass());
            assertEquals(new Binary(BsonBinarySubType.UUID_LEGACY, standardEncodedValue), document.get("legacy"));
        } else if (uuidRepresentation == STANDARD) {
            assertEquals(UUID.class, document.get("standard").getClass());
            assertEquals(uuid, document.get("standard"));

            assertEquals(Binary.class, document.get("legacy").getClass());
            assertEquals(new Binary(BsonBinarySubType.UUID_LEGACY, standardEncodedValue), document.get("legacy"));
        } else {
            assertEquals(Binary.class, document.get("standard").getClass());
            assertEquals(new Binary(BsonBinarySubType.UUID_STANDARD, standardEncodedValue), document.get("standard"));

            assertEquals(UUID.class, document.get("legacy").getClass());
            assertEquals(uuid, document.get("legacy"));
        }
    }

    @Test
    public void shouldDecodeDbObjectWithUuidRepresentation() {

        bsonDocumentCollection.insertOne(new BsonDocument("standard", new BsonBinary(uuid, STANDARD))
                .append("legacy", new BsonBinary(uuid,
                        uuidRepresentation == UuidRepresentation.UNSPECIFIED || uuidRepresentation == STANDARD
                                ? UuidRepresentation.PYTHON_LEGACY
                                : uuidRepresentation)));

        DBObject document;
        try {
            document = dbObjectCollection.find().first();
            assertNotNull(document);
        } catch (BSONException e) {
            if (uuidRepresentation != STANDARD && uuidRepresentation != JAVA_LEGACY) {
                throw e;
            }
            return;
        }

        if (uuidRepresentation == UuidRepresentation.UNSPECIFIED) {
            assertEquals(Binary.class, document.get("standard").getClass());
            assertEquals(new Binary(BsonBinarySubType.UUID_STANDARD, standardEncodedValue), document.get("standard"));

            assertEquals(Binary.class, document.get("legacy").getClass());
            assertEquals(new Binary(BsonBinarySubType.UUID_LEGACY, standardEncodedValue), document.get("legacy"));
        } else if (uuidRepresentation == STANDARD) {
            assertEquals(UUID.class, document.get("standard").getClass());
            assertEquals(uuid, document.get("standard"));

            assertEquals(Binary.class, document.get("legacy").getClass());
            assertEquals(new Binary(BsonBinarySubType.UUID_LEGACY, standardEncodedValue), document.get("legacy"));
        } else {
            assertEquals(Binary.class, document.get("standard").getClass());
            assertEquals(new Binary(BsonBinarySubType.UUID_STANDARD, standardEncodedValue), document.get("standard"));

            assertEquals(UUID.class, document.get("legacy").getClass());
            assertEquals(uuid, document.get("legacy"));
        }
    }

    @Test
    public void shouldDecodePojoWithStandardUuidRepresentation() {

        bsonDocumentCollection.insertOne(new BsonDocument("_id", new BsonBinary(uuid, STANDARD)));

        try {
            UuidIdPojo document = uuidIdPojoCollection.find().first();
            assertNotNull(document);
            assertEquals(uuid, document.getId());
        } catch (BSONException e) {
            assertNotEquals(STANDARD, uuidRepresentation);
        }
    }

    @Test
    public void shouldDecodePojoWithLegacyUuidRepresentation() {

        bsonDocumentCollection.insertOne(new BsonDocument("_id", new BsonBinary(uuid,
                uuidRepresentation == UuidRepresentation.UNSPECIFIED || uuidRepresentation == STANDARD
                        ? UuidRepresentation.PYTHON_LEGACY
                        : uuidRepresentation)));

        try {
            UuidIdPojo document = uuidIdPojoCollection.find().first();
            assertNotNull(document);
            assertEquals(uuid, document.getId());
        } catch (BSONException e) {
            assertNotEquals(UuidRepresentation.C_SHARP_LEGACY, uuidRepresentation);
            assertNotEquals(UuidRepresentation.PYTHON_LEGACY, uuidRepresentation);
        }
    }

    @Parameterized.Parameters(name = "{0}/{1}/{2}")
    public static Collection<Object[]> data() {
        UUID uuid = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");

        byte[] standardEncodedValue = Hex.decode("00112233445566778899AABBCCDDEEFF");

        List<Object[]> data = new ArrayList<>();
        data.add(new Object[]{
                JAVA_LEGACY,
                BsonBinarySubType.UUID_LEGACY,
                uuid,
                Hex.decode("7766554433221100FFEEDDCCBBAA9988"),
                standardEncodedValue});
        data.add(new Object[]{
                UuidRepresentation.C_SHARP_LEGACY,
                BsonBinarySubType.UUID_LEGACY,
                uuid,
                Hex.decode("33221100554477668899AABBCCDDEEFF"),
                standardEncodedValue});
        data.add(new Object[]{
                UuidRepresentation.PYTHON_LEGACY,
                BsonBinarySubType.UUID_LEGACY,
                uuid,
                standardEncodedValue,
                standardEncodedValue});
        data.add(new Object[]{
                STANDARD,
                BsonBinarySubType.UUID_STANDARD,
                uuid,
                standardEncodedValue,
                standardEncodedValue});
        data.add(new Object[]{
                UuidRepresentation.UNSPECIFIED,
                null,
                uuid,
                null,
                standardEncodedValue});
        return data;
    }

    public static class UuidIdPojo {
        private UUID id;

        @SuppressWarnings("unused")
        public UuidIdPojo() {
        }

        UuidIdPojo(final UUID id) {
            this.id = id;
        }

        public UUID getId() {
            return id;
        }

        public void setId(final UUID id) {
            this.id = id;
        }
    }
}
