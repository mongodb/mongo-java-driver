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

package com.mongodb.client.vector;

import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.Fixture;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.OperationTest;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.Document;
import org.bson.Float32Vector;
import org.bson.Int8Vector;
import org.bson.PackedBitVector;
import org.bson.Vector;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.types.Binary;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static org.bson.Vector.DataType.FLOAT32;
import static org.bson.Vector.DataType.INT8;
import static org.bson.Vector.DataType.PACKED_BIT;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public abstract class VectorAbstractFunctionalTest extends OperationTest {

    private static final byte VECTOR_SUBTYPE = BsonBinarySubType.VECTOR.getValue();
    private static final String FIELD_VECTOR = "vector";
    private static final CodecRegistry CODEC_REGISTRY = fromRegistries(getDefaultCodecRegistry(),
            fromProviders(PojoCodecProvider
                    .builder()
                    .automatic(true).build()));
    private MongoCollection<Document> documentCollection;

    private MongoClient mongoClient;

    @BeforeEach
    public void setUp() {
        super.beforeEach();
        mongoClient = getMongoClient(getMongoClientSettingsBuilder()
                .codecRegistry(CODEC_REGISTRY)
                .build());
        documentCollection = mongoClient
                .getDatabase(getDatabaseName())
                .getCollection(getCollectionName());
    }

    @AfterEach
    public void afterEach() {
        super.afterEach();
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    private static MongoClientSettings.Builder getMongoClientSettingsBuilder() {
        return Fixture.getMongoClientSettingsBuilder()
                .readConcern(ReadConcern.MAJORITY)
                .writeConcern(WriteConcern.MAJORITY)
                .readPreference(ReadPreference.primary());
    }

    protected abstract MongoClient getMongoClient(MongoClientSettings settings);

    @ParameterizedTest
    @ValueSource(bytes = {-1, 1, 2, 3, 4, 5, 6, 7, 8})
    void shouldThrowExceptionForInvalidPackedBitArrayPaddingWhenDecodeEmptyVector(final byte invalidPadding) {
        //given
        Binary invalidVector = new Binary(VECTOR_SUBTYPE, new byte[]{PACKED_BIT.getValue(), invalidPadding});
        documentCollection.insertOne(new Document(FIELD_VECTOR, invalidVector));

        // when & then
        Binary invalidVectorBinary = findExactlyOne(documentCollection)
                .get(FIELD_VECTOR, Binary.class);

        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class, invalidVectorBinary::asVector);
        Assertions.assertEquals("state should be: Padding must be 0 if vector is empty.", exception.getMessage());
    }

    @ParameterizedTest
    @ValueSource(bytes = {-1, 1})
    void shouldThrowExceptionForInvalidFloat32Padding(final byte invalidPadding) {
        // given
        Binary invalidVector = new Binary(VECTOR_SUBTYPE, new byte[]{FLOAT32.getValue(), invalidPadding, 10, 20, 30, 40});
        documentCollection.insertOne(new Document(FIELD_VECTOR, invalidVector));

        // when & then
        Binary invalidVectorBinary = findExactlyOne(documentCollection)
                .get(FIELD_VECTOR, Binary.class);

        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class, invalidVectorBinary::asVector);
        Assertions.assertEquals("state should be: Padding must be 0 for FLOAT32 data type.", exception.getMessage());
    }

    @ParameterizedTest
    @ValueSource(bytes = {-1, 1})
    void shouldThrowExceptionForInvalidInt8Padding(final byte invalidPadding) {
        // given
        Binary invalidVector = new Binary(VECTOR_SUBTYPE, new byte[]{INT8.getValue(), invalidPadding, 10, 20, 30, 40});
        documentCollection.insertOne(new Document(FIELD_VECTOR, invalidVector));

        // when & then
        Binary invalidVectorBinary = findExactlyOne(documentCollection)
                .get(FIELD_VECTOR, Binary.class);

        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class, invalidVectorBinary::asVector);
        Assertions.assertEquals("state should be: Padding must be 0 for INT8 data type.", exception.getMessage());
    }

    @ParameterizedTest
    @ValueSource(bytes = {-1, 8})
    void shouldThrowExceptionForInvalidPackedBitPadding(final byte invalidPadding) {
        // given
        Binary invalidVector = new Binary(VECTOR_SUBTYPE, new byte[]{PACKED_BIT.getValue(), invalidPadding, 10, 20, 30, 40});
        documentCollection.insertOne(new Document(FIELD_VECTOR, invalidVector));

        // when & then
        Binary invalidVectorBinary = findExactlyOne(documentCollection)
                .get(FIELD_VECTOR, Binary.class);

        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class, invalidVectorBinary::asVector);
        Assertions.assertEquals("state should be: Padding must be between 0 and 7 bits.", exception.getMessage());
    }

    private static Stream<Vector> provideValidVectors() {
        return Stream.of(
                Vector.floatVector(new float[]{1.1f, 2.2f, 3.3f}),
                Vector.int8Vector(new byte[]{10, 20, 30, 40}),
                Vector.packedBitVector(new byte[]{(byte) 0b10101010, (byte) 0b01010101}, (byte) 3)
        );
    }

    @ParameterizedTest
    @MethodSource("provideValidVectors")
    void shouldStoreAndRetrieveValidVector(final Vector actualVector) {
        // Given
        Document documentToInsert = new Document(FIELD_VECTOR, actualVector);
        documentCollection.insertOne(documentToInsert);

        // when & then
        Binary vectorBinary = findExactlyOne(documentCollection)
                .get(FIELD_VECTOR, Binary.class);

        Assertions.assertEquals(actualVector, vectorBinary.asVector());
    }

    @ParameterizedTest
    @MethodSource("provideValidVectors")
    void shouldStoreAndRetrieveValidVectorWithBinary(final Vector actualVector) {
        // given
        Document documentToInsert = new Document(FIELD_VECTOR, new Binary(actualVector));
        documentCollection.insertOne(documentToInsert);

        // when & then
        Binary vectorBinary = findExactlyOne(documentCollection)
                .get(FIELD_VECTOR, Binary.class);

        Assertions.assertEquals(actualVector, vectorBinary.asVector());
    }

    @ParameterizedTest
    @MethodSource("provideValidVectors")
    void shouldStoreAndRetrieveValidVectorWithBsonBinary(final Vector actualVector) {
        // Given
        Document documentToInsert = new Document(FIELD_VECTOR, new BsonBinary(actualVector));
        documentCollection.insertOne(documentToInsert);

        // when & then
        Binary vectorBinary = findExactlyOne(documentCollection)
                .get(FIELD_VECTOR, Binary.class);

        Assertions.assertEquals(actualVector, vectorBinary.asVector());
    }

    @Test
    void shouldStoreAndRetrieveValidVectorWithFloatVectorPojo() {
        // given
        MongoCollection<FloatVectorPojo> floatVectorPojoMongoCollection = mongoClient
                .getDatabase(getDatabaseName())
                .getCollection(getCollectionName()).withDocumentClass(FloatVectorPojo.class);
        Float32Vector vector = Vector.floatVector(new float[]{1.1f, 2.2f, 3.3f});

        // whe
        floatVectorPojoMongoCollection.insertOne(new FloatVectorPojo(vector));
        FloatVectorPojo floatVectorPojo = floatVectorPojoMongoCollection.find().first();

        // then
        Assertions.assertNotNull(floatVectorPojo);
        Assertions.assertEquals(vector, floatVectorPojo.getVector());
    }

    @Test
    void shouldStoreAndRetrieveValidVectorWithInt8VectorPojo() {
        // given
        MongoCollection<Int8VectorPojo> floatVectorPojoMongoCollection = mongoClient
                .getDatabase(getDatabaseName())
                .getCollection(getCollectionName()).withDocumentClass(Int8VectorPojo.class);
        Int8Vector vector = Vector.int8Vector(new byte[]{10, 20, 30, 40});

        // when
        floatVectorPojoMongoCollection.insertOne(new Int8VectorPojo(vector));
        Int8VectorPojo int8VectorPojo = floatVectorPojoMongoCollection.find().first();

        // then
        Assertions.assertNotNull(int8VectorPojo);
        Assertions.assertEquals(vector, int8VectorPojo.getVector());
    }

    @Test
    void shouldStoreAndRetrieveValidVectorWithPackedBitVectorPojo() {
        // given
        MongoCollection<PackedBitVectorPojo> floatVectorPojoMongoCollection = mongoClient
                .getDatabase(getDatabaseName())
                .getCollection(getCollectionName()).withDocumentClass(PackedBitVectorPojo.class);

        PackedBitVector vector = Vector.packedBitVector(new byte[]{(byte) 0b10101010, (byte) 0b01010101}, (byte) 3);

        // when
        floatVectorPojoMongoCollection.insertOne(new PackedBitVectorPojo(vector));
        PackedBitVectorPojo packedBitVectorPojo = floatVectorPojoMongoCollection.find().first();

        // then
        Assertions.assertNotNull(packedBitVectorPojo);
        Assertions.assertEquals(vector, packedBitVectorPojo.getVector());
    }

    @ParameterizedTest
    @MethodSource("provideValidVectors")
    void shouldStoreAndRetrieveValidVectorWithGenericVectorPojo(final Vector actualVector) {
        // given
        MongoCollection<VectorPojo> floatVectorPojoMongoCollection = mongoClient
                .getDatabase(getDatabaseName())
                .getCollection(getCollectionName()).withDocumentClass(VectorPojo.class);

        // when
        floatVectorPojoMongoCollection.insertOne(new VectorPojo(actualVector));
        VectorPojo vectorPojo = floatVectorPojoMongoCollection.find().first();

        //then
        Assertions.assertNotNull(vectorPojo);
        Assertions.assertEquals(actualVector, vectorPojo.getVector());
    }

    private Document findExactlyOne(final MongoCollection<Document> collection) {
        List<Document> documents = new ArrayList<>();
        collection.find().into(documents);
        if (documents.size() != 1) {
            throw new IllegalStateException("Expected exactly one document, but found: " + documents.size());
        }
        return documents.get(0);
    }

    public static class VectorPojo {
        private Vector vector;

        public VectorPojo() {
        }

        public VectorPojo(final Vector vector) {
            this.vector = vector;
        }

        public Vector getVector() {
            return vector;
        }

        public void setVector(final Vector vector) {
            this.vector = vector;
        }
    }

    public static class Int8VectorPojo {
        private Int8Vector vector;

        public Int8VectorPojo() {
        }

        public Int8VectorPojo(final Int8Vector vector) {
            this.vector = vector;
        }

        public Vector getVector() {
            return vector;
        }

        public void setVector(final Int8Vector vector) {
            this.vector = vector;
        }
    }

    public static class PackedBitVectorPojo {
        private PackedBitVector vector;

        public PackedBitVectorPojo() {
        }

        public PackedBitVectorPojo(final PackedBitVector vector) {
            this.vector = vector;
        }

        public Vector getVector() {
            return vector;
        }

        public void setVector(final PackedBitVector vector) {
            this.vector = vector;
        }
    }

    public static class FloatVectorPojo {
        private Float32Vector vector;

        public FloatVectorPojo() {
        }

        public FloatVectorPojo(final Float32Vector vector) {
            this.vector = vector;
        }

        public Vector getVector() {
            return vector;
        }

        public void setVector(final Float32Vector vector) {
            this.vector = vector;
        }
    }
}
