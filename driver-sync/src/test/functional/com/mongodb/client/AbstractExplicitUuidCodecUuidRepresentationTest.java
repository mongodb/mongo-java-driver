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
import org.bson.codecs.UuidCodec;
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
import static org.bson.BsonBinarySubType.UUID_LEGACY;
import static org.bson.BsonBinarySubType.UUID_STANDARD;
import static org.bson.UuidRepresentation.C_SHARP_LEGACY;
import static org.bson.UuidRepresentation.JAVA_LEGACY;
import static org.bson.UuidRepresentation.PYTHON_LEGACY;
import static org.bson.UuidRepresentation.STANDARD;
import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
public abstract class AbstractExplicitUuidCodecUuidRepresentationTest {

    private final UuidRepresentation uuidRepresentationForExplicitEncoding;
    private final UuidCodec uuidCodec;
    private final UuidRepresentation uuidRepresentationForClient;
    private final BsonBinarySubType subType;
    private final UUID uuid;
    private final byte[] encodedValue;
    private final byte[] standardEncodedValue;
    private MongoCollection<Document> documentCollection;
    private MongoCollection<DBObject> dbObjectCollection;
    private MongoCollection<UuidIdPojo> uuidIdPojoCollection;
    private MongoCollection<BsonDocument> bsonDocumentCollection;

    public AbstractExplicitUuidCodecUuidRepresentationTest(final UuidRepresentation uuidRepresentationForClient,
                                                           final UuidRepresentation uuidRepresentationForExplicitEncoding,
                                                           final BsonBinarySubType subType,
                                                           final UuidCodec uuidCodec, final UUID uuid,
                                                           final byte[] encodedValue, final byte[] standardEncodedValue) {
        this.uuidRepresentationForExplicitEncoding = uuidRepresentationForExplicitEncoding;
        this.uuidRepresentationForClient = uuidRepresentationForClient;
        this.uuidCodec = uuidCodec;
        this.subType = subType;
        this.uuid = uuid;
        this.encodedValue = encodedValue;
        this.standardEncodedValue = standardEncodedValue;
    }

    protected abstract void createMongoClient(UuidRepresentation uuidRepresentation, CodecRegistry codecRegistry);

    protected abstract MongoDatabase getDatabase(String databaseName);

    @Before
    public void setUp() {
        CodecRegistry codecRegistry = fromRegistries(
                fromCodecs(uuidCodec), getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        createMongoClient(uuidRepresentationForClient, codecRegistry);
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
        documentCollection.insertOne(new Document("_id", uuid));

        BsonDocument document = bsonDocumentCollection.find().first();
        assertNotNull(document);
        BsonBinary uuidAsBinary = document.getBinary("_id");
        assertEquals(subType.getValue(), uuidAsBinary.getType());
        assertArrayEquals(encodedValue, uuidAsBinary.getData());
    }

    @Test
    public void shouldEncodeDbObjectWithUuidRepresentation() {
        dbObjectCollection.insertOne(new BasicDBObject("_id", uuid));

        BsonDocument document = bsonDocumentCollection.find().first();
        assertNotNull(document);
        BsonBinary uuidAsBinary = document.getBinary("_id");
        assertEquals(subType.getValue(), uuidAsBinary.getType());
        assertEquals(subType.getValue(), uuidAsBinary.getType());
        assertArrayEquals(encodedValue, uuidAsBinary.getData());
    }

    @Test
    public void shouldEncodePojoWithUuidRepresentation() {
        uuidIdPojoCollection.insertOne(new UuidIdPojo(uuid));

        BsonDocument document = bsonDocumentCollection.find().first();
        assertNotNull(document);
        BsonBinary uuidAsBinary = document.getBinary("_id");
        assertEquals(subType.getValue(), uuidAsBinary.getType());
        assertArrayEquals(encodedValue, uuidAsBinary.getData());
    }

    @Test
    public void shouldDecodeDocumentWithUuidRepresentation() {
        bsonDocumentCollection.insertOne(new BsonDocument("standard", new BsonBinary(uuid, STANDARD))
                .append("legacy", new BsonBinary(uuid, uuidRepresentationForExplicitEncoding)));

        Document document;
        try {
            document = documentCollection.find().first();
            assertNotNull(document);
        } catch (BSONException e) {
            if (uuidCodec.getUuidRepresentation() != STANDARD) {
                throw e;
            }
            return;
        }

        if (uuidRepresentationForClient == STANDARD) {
            assertEquals(UUID.class, document.get("standard").getClass());
            assertEquals(uuid, document.get("standard"));

            assertEquals(Binary.class, document.get("legacy").getClass());
            assertEquals(new Binary(UUID_LEGACY, encodedValue), document.get("legacy"));
        } else {
            if (uuidRepresentationForClient == JAVA_LEGACY) {
                assertEquals(UUID.class, document.get("standard").getClass());
                assertEquals(uuid, document.get("standard"));
            } else {
                assertEquals(Binary.class, document.get("standard").getClass());
                assertEquals(new Binary(UUID_STANDARD, standardEncodedValue), document.get("standard"));
            }

            assertEquals(UUID.class, document.get("legacy").getClass());
            assertEquals(uuid, document.get("legacy"));
        }
    }

    @Test
    public void shouldDecodeDBObjectWithUuidRepresentation() {
        bsonDocumentCollection.insertOne(new BsonDocument("standard", new BsonBinary(uuid, STANDARD))
                .append("legacy", new BsonBinary(uuid, uuidRepresentationForExplicitEncoding)));

        DBObject document;
        try {
            document = dbObjectCollection.find().first();
            assertNotNull(document);
        } catch (BSONException e) {
            if (uuidCodec.getUuidRepresentation() != STANDARD) {
                throw e;
            }
            return;
        }

        if (uuidRepresentationForClient == STANDARD) {
            assertEquals(UUID.class, document.get("standard").getClass());
            assertEquals(uuid, document.get("standard"));

            assertEquals(Binary.class, document.get("legacy").getClass());
            assertEquals(new Binary(UUID_LEGACY, encodedValue), document.get("legacy"));
        } else {
            if (uuidRepresentationForClient == JAVA_LEGACY) {
                assertEquals(UUID.class, document.get("standard").getClass());
                assertEquals(uuid, document.get("standard"));
            } else {
                assertEquals(Binary.class, document.get("standard").getClass());
                assertEquals(new Binary(UUID_STANDARD, standardEncodedValue), document.get("standard"));
            }

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
            if (uuidCodec.getUuidRepresentation() == uuidRepresentationForClient) {
                throw e;
            }
        }
    }

    @Test
    public void shouldDecodePojoWithLegacyUuidRepresentation() {
        bsonDocumentCollection.insertOne(new BsonDocument("_id", new BsonBinary(uuid, uuidRepresentationForExplicitEncoding)));

        try {
            UuidIdPojo document = uuidIdPojoCollection.find().first();
            assertNotNull(document);
            assertEquals(uuid, document.getId());
        } catch (BSONException e) {
            if (uuidCodec.getUuidRepresentation() == uuidRepresentationForExplicitEncoding) {
                throw e;
            }
        }
    }

    @Parameterized.Parameters(name = "{0}/{1}/{2}")
    public static Collection<Object[]> data() {
        UUID uuid = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");

        byte[] standardEncodedValue = Hex.decode("00112233445566778899AABBCCDDEEFF");

        List<Object[]> data = new ArrayList<>();
        data.add(new Object[]{
                JAVA_LEGACY,
                PYTHON_LEGACY,
                UUID_STANDARD,
                new UuidCodec(STANDARD),
                uuid,
                standardEncodedValue,
                standardEncodedValue});
        data.add(new Object[]{
                STANDARD,
                C_SHARP_LEGACY,
                UUID_LEGACY,
                new UuidCodec(C_SHARP_LEGACY),
                uuid,
                Hex.decode("33221100554477668899AABBCCDDEEFF"),
                standardEncodedValue});
        data.add(new Object[]{
                STANDARD,
                JAVA_LEGACY,
                UUID_LEGACY,
                new UuidCodec(JAVA_LEGACY),
                uuid,
                Hex.decode("7766554433221100FFEEDDCCBBAA9988"),
                standardEncodedValue});
        data.add(new Object[]{
                STANDARD,
                PYTHON_LEGACY,
                UUID_LEGACY,
                new UuidCodec(PYTHON_LEGACY),
                uuid,
                standardEncodedValue,
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
