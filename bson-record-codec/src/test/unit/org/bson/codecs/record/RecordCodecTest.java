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

package org.bson.codecs.record;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWriter;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInvalidOperationException;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.record.samples.TestRecordEmbedded;
import org.bson.codecs.record.samples.TestRecordParameterized;
import org.bson.codecs.record.samples.TestRecordWithIllegalBsonCreatorOnConstructor;
import org.bson.codecs.record.samples.TestRecordWithIllegalBsonCreatorOnMethod;
import org.bson.codecs.record.samples.TestRecordWithIllegalBsonDiscriminatorOnRecord;
import org.bson.codecs.record.samples.TestRecordWithIllegalBsonExtraElementsOnAccessor;
import org.bson.codecs.record.samples.TestRecordWithIllegalBsonExtraElementsOnComponent;
import org.bson.codecs.record.samples.TestRecordWithIllegalBsonIdOnAccessor;
import org.bson.codecs.record.samples.TestRecordWithIllegalBsonIdOnCanonicalConstructor;
import org.bson.codecs.record.samples.TestRecordWithIllegalBsonIgnoreOnAccessor;
import org.bson.codecs.record.samples.TestRecordWithIllegalBsonIgnoreOnComponent;
import org.bson.codecs.record.samples.TestRecordWithIllegalBsonPropertyOnAccessor;
import org.bson.codecs.record.samples.TestRecordWithIllegalBsonPropertyOnCanonicalConstructor;
import org.bson.codecs.record.samples.TestRecordWithIllegalBsonRepresentationOnAccessor;
import org.bson.codecs.record.samples.TestRecordWithListOfListOfRecords;
import org.bson.codecs.record.samples.TestRecordWithListOfRecords;
import org.bson.codecs.record.samples.TestRecordWithMapOfListOfRecords;
import org.bson.codecs.record.samples.TestRecordWithMapOfRecords;
import org.bson.codecs.record.samples.TestRecordWithNestedParameterized;
import org.bson.codecs.record.samples.TestRecordWithNestedParameterizedRecord;
import org.bson.codecs.record.samples.TestRecordWithNullableField;
import org.bson.codecs.record.samples.TestRecordWithParameterizedRecord;
import org.bson.codecs.record.samples.TestRecordWithPojoAnnotations;
import org.bson.codecs.record.samples.TestSelfReferentialHolderRecord;
import org.bson.codecs.record.samples.TestSelfReferentialRecord;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RecordCodecTest {

    @Test
    public void testRecordWithPojoAnnotations() {
        var codec = createRecordCodec(TestRecordWithPojoAnnotations.class, Bson.DEFAULT_CODEC_REGISTRY);
        var identifier = new ObjectId();
        var testRecord = new TestRecordWithPojoAnnotations("Lucas", 14, List.of("soccer", "basketball"), identifier.toHexString());

        var document = new BsonDocument();
        var writer = new BsonDocumentWriter(document);

        // when
        codec.encode(writer, testRecord, EncoderContext.builder().build());

        // then
        assertEquals(
                new BsonDocument("_id", new BsonObjectId(identifier))
                        .append("name", new BsonString("Lucas"))
                        .append("hobbies", new BsonArray(List.of(new BsonString("soccer"), new BsonString("basketball"))))
                        .append("a", new BsonInt32(14)),
                document);
        assertEquals("_id", document.getFirstKey());

        // when
        var decoded = codec.decode(new BsonDocumentReader(document), DecoderContext.builder().build());

        // then
        assertEquals(testRecord, decoded);
    }

    @Test
    public void testRecordWithNestedListOfRecords() {
        var codec = createRecordCodec(TestRecordWithListOfRecords.class,
                fromProviders(new RecordCodecProvider(), Bson.DEFAULT_CODEC_REGISTRY));
        var identifier = new ObjectId();
        var testRecord = new TestRecordWithListOfRecords(identifier, List.of(new TestRecordEmbedded("embedded")));

        var document = new BsonDocument();
        var writer = new BsonDocumentWriter(document);

        // when
        codec.encode(writer, testRecord, EncoderContext.builder().build());

        // then
        assertEquals(
                new BsonDocument("_id", new BsonObjectId(identifier))
                        .append("nestedRecords", new BsonArray(List.of(new BsonDocument("name", new BsonString("embedded"))))),
                document);
        assertEquals("_id", document.getFirstKey());

        // when
        var decoded = codec.decode(new BsonDocumentReader(document), DecoderContext.builder().build());

        // then
        assertEquals(testRecord, decoded);
    }

    @Test
    public void testRecordWithNestedListOfListOfRecords() {
        var codec = createRecordCodec(TestRecordWithListOfListOfRecords.class,
                fromProviders(new RecordCodecProvider(), Bson.DEFAULT_CODEC_REGISTRY));
        var identifier = new ObjectId();
        var testRecord = new TestRecordWithListOfListOfRecords(identifier, List.of(List.of(new TestRecordEmbedded("embedded"))));

        var document = new BsonDocument();
        var writer = new BsonDocumentWriter(document);

        // when
        codec.encode(writer, testRecord, EncoderContext.builder().build());

        // then
        assertEquals(
                new BsonDocument("_id", new BsonObjectId(identifier))
                        .append("nestedRecords",
                                new BsonArray(List.of(new BsonArray(List.of(new BsonDocument("name", new BsonString("embedded"))))))),
                document);
        assertEquals("_id", document.getFirstKey());

        // when
        var decoded = codec.decode(new BsonDocumentReader(document), DecoderContext.builder().build());

        // then
        assertEquals(testRecord, decoded);
    }

    @Test
    public void testRecordWithNestedMapOfRecords() {
        var codec = createRecordCodec(TestRecordWithMapOfRecords.class,
                fromProviders(new RecordCodecProvider(), Bson.DEFAULT_CODEC_REGISTRY));
        var identifier = new ObjectId();
        var testRecord = new TestRecordWithMapOfRecords(identifier,
                Map.of("first", new TestRecordEmbedded("embedded")));

        var document = new BsonDocument();
        var writer = new BsonDocumentWriter(document);

        // when
        codec.encode(writer, testRecord, EncoderContext.builder().build());

        // then
        assertEquals(
                new BsonDocument("_id", new BsonObjectId(identifier))
                        .append("nestedRecords", new BsonDocument("first", new BsonDocument("name", new BsonString("embedded")))),
                document);
        assertEquals("_id", document.getFirstKey());

        // when
        var decoded = codec.decode(new BsonDocumentReader(document), DecoderContext.builder().build());

        // then
        assertEquals(testRecord, decoded);
    }

    @Test
    public void testRecordWithNestedMapOfListRecords() {
        var codec = createRecordCodec(TestRecordWithMapOfListOfRecords.class,
                fromProviders(new RecordCodecProvider(), Bson.DEFAULT_CODEC_REGISTRY));
        var identifier = new ObjectId();
        var testRecord = new TestRecordWithMapOfListOfRecords(identifier,
                Map.of("first", List.of(new TestRecordEmbedded("embedded"))));

        var document = new BsonDocument();
        var writer = new BsonDocumentWriter(document);

        // when
        codec.encode(writer, testRecord, EncoderContext.builder().build());

        // then
        assertEquals(
                new BsonDocument("_id", new BsonObjectId(identifier))
                        .append("nestedRecords",
                                new BsonDocument("first",
                                        new BsonArray(List.of(new BsonDocument("name", new BsonString("embedded")))))),
                document);
        assertEquals("_id", document.getFirstKey());

        // when
        var decoded = codec.decode(new BsonDocumentReader(document), DecoderContext.builder().build());

        // then
        assertEquals(testRecord, decoded);
    }

    @Test
    public void testRecordWithNestedParameterizedRecord() {
        var codec = createRecordCodec(TestRecordWithParameterizedRecord.class,
                fromProviders(new RecordCodecProvider(), Bson.DEFAULT_CODEC_REGISTRY));
        var identifier = new ObjectId();
        var testRecord = new TestRecordWithParameterizedRecord(identifier,
                new TestRecordParameterized<>(42.0, List.of(new TestRecordEmbedded("embedded"))));

        var document = new BsonDocument();
        var writer = new BsonDocumentWriter(document);

        // when
        codec.encode(writer, testRecord, EncoderContext.builder().build());

        // then
        assertEquals(
                new BsonDocument("_id", new BsonObjectId(identifier))
                        .append("parameterizedRecord",
                                new BsonDocument("number", new BsonDouble(42.0))
                                        .append("parameterizedList",
                                                new BsonArray(List.of(new BsonDocument("name", new BsonString("embedded")))))),
                document);
        assertEquals("_id", document.getFirstKey());

        // when
        var decoded = codec.decode(new BsonDocumentReader(document), DecoderContext.builder().build());

        // then
        assertEquals(testRecord, decoded);
    }

    @Test
    public void testRecordWithNestedParameterizedRecordWithDifferentlyOrderedTypeParameters() {
        var codec = createRecordCodec(TestRecordWithNestedParameterizedRecord.class,
                fromProviders(new RecordCodecProvider(), Bson.DEFAULT_CODEC_REGISTRY));
        var identifier = new ObjectId();
        var testRecord = new TestRecordWithNestedParameterizedRecord(identifier,
                new TestRecordWithNestedParameterized<>(
                        new TestRecordParameterized<>(42.0, List.of(new TestRecordEmbedded("p"))),
                        "o"));

        var document = new BsonDocument();
        var writer = new BsonDocumentWriter(document);

        // when
        codec.encode(writer, testRecord, EncoderContext.builder().build());

        // then
        assertEquals(
                new BsonDocument("_id", new BsonObjectId(identifier))
                        .append("nestedParameterized",
                                new BsonDocument("parameterizedRecord",
                                        new BsonDocument("number", new BsonDouble(42.0))
                                                .append("parameterizedList",
                                                        new BsonArray(List.of(new BsonDocument("name", new BsonString("p"))))))
                                        .append("other", new BsonString("o"))),
                document);

        // when
        var decoded = codec.decode(new BsonDocumentReader(document), DecoderContext.builder().build());

        // then
        assertEquals(testRecord, decoded);
    }

    @Test
    public void testRecordWithNulls() {
        var codec = createRecordCodec(TestRecordWithPojoAnnotations.class, Bson.DEFAULT_CODEC_REGISTRY);
        var identifier = new ObjectId();
        var testRecord = new TestRecordWithPojoAnnotations(null, 14, null, identifier.toHexString());

        var document = new BsonDocument();
        var writer = new BsonDocumentWriter(document);

        // when
        codec.encode(writer, testRecord, EncoderContext.builder().build());

        // then
        assertEquals(
                new BsonDocument("_id", new BsonObjectId(identifier))
                        .append("a", new BsonInt32(14)),
                document);

        // when
        var decoded = codec.decode(new BsonDocumentReader(document), DecoderContext.builder().build());

        // then
        assertEquals(testRecord, decoded);
    }

    @Test
    public void testRecordWithStoredNulls() {
        var codec = createRecordCodec(TestRecordWithNullableField.class, Bson.DEFAULT_CODEC_REGISTRY);
        var identifier = new ObjectId();
        var testRecord = new TestRecordWithNullableField(identifier, null, 42);

        var document = new BsonDocument("_id", new BsonObjectId(identifier))
                .append("name", new BsonNull())
                .append("age", new BsonInt32(42));

        // when
        var decoded = codec.decode(new BsonDocumentReader(document), DecoderContext.builder().build());

        // then
        assertEquals(testRecord, decoded);
    }

    @Test
    public void testExceptionsWithStoredNullsOnPrimitiveField() {
        var codec = createRecordCodec(TestRecordWithNullableField.class, Bson.DEFAULT_CODEC_REGISTRY);

        var document = new BsonDocument("_id", new BsonObjectId(new ObjectId()))
                .append("name", new BsonString("Felix"))
                .append("age", new BsonNull());

        assertThrows(BsonInvalidOperationException.class, () ->
                codec.decode(new BsonDocumentReader(document), DecoderContext.builder().build()));
    }

    @Test
    public void testRecordWithExtraData() {
        var codec = createRecordCodec(TestRecordWithPojoAnnotations.class, Bson.DEFAULT_CODEC_REGISTRY);
        var identifier = new ObjectId();
        var testRecord = new TestRecordWithPojoAnnotations("Felix", 13, List.of("rugby", "badminton"), identifier.toHexString());

        var document = new BsonDocument("_id", new BsonObjectId(identifier))
                .append("nationality", new BsonString("British"))
                .append("name", new BsonString("Felix"))
                .append("hobbies", new BsonArray(List.of(new BsonString("rugby"), new BsonString("badminton"))))
                .append("a", new BsonInt32(13));

        // when
        var decoded = codec.decode(new BsonDocumentReader(document), DecoderContext.builder().build());

        // then
        assertEquals(testRecord, decoded);
    }

    @Test
    public void testSelfReferentialRecords() {
        var registry = fromProviders(new RecordCodecProvider(), Bson.DEFAULT_CODEC_REGISTRY);
        var codec = registry.get(TestSelfReferentialHolderRecord.class);
        var testRecord = new TestSelfReferentialHolderRecord("0",
                new TestSelfReferentialRecord<>("1",
                new TestSelfReferentialRecord<>("2", null, null),
                new TestSelfReferentialRecord<>("3", null, null)));

        var document = new BsonDocument();

        // when
        codec.encode(new BsonDocumentWriter(document), testRecord, EncoderContext.builder().build());

        // then
        assertEquals(
                new BsonDocument("_id", new BsonString("0"))
                        .append("selfReferentialRecord",
                        new BsonDocument("name", new BsonString("1"))
                        .append("left", new BsonDocument("name", new BsonString("2")))
                        .append("right", new BsonDocument("name", new BsonString("3")))),
                document);

        // when
        var decoded = codec.decode(new BsonDocumentReader(document), DecoderContext.builder().build());

        // then
        assertEquals(testRecord, decoded);
    }

    @Test
    public void testExceptionsForAnnotationsNotOnRecordComponent() {
        assertThrows(CodecConfigurationException.class, () ->
                createRecordCodec(TestRecordWithIllegalBsonIdOnAccessor.class, Bson.DEFAULT_CODEC_REGISTRY));
        assertThrows(CodecConfigurationException.class, () ->
                createRecordCodec(TestRecordWithIllegalBsonIdOnCanonicalConstructor.class, Bson.DEFAULT_CODEC_REGISTRY));

        assertThrows(CodecConfigurationException.class, () ->
                createRecordCodec(TestRecordWithIllegalBsonPropertyOnAccessor.class, Bson.DEFAULT_CODEC_REGISTRY));
        assertThrows(CodecConfigurationException.class, () ->
                createRecordCodec(TestRecordWithIllegalBsonPropertyOnCanonicalConstructor.class, Bson.DEFAULT_CODEC_REGISTRY));

        assertThrows(CodecConfigurationException.class, () ->
                createRecordCodec(TestRecordWithIllegalBsonRepresentationOnAccessor.class, Bson.DEFAULT_CODEC_REGISTRY));
    }

    @Test
    public void testExceptionsForUnsupportedAnnotations() {
        assertThrows(CodecConfigurationException.class, () ->
                createRecordCodec(TestRecordWithIllegalBsonDiscriminatorOnRecord.class, Bson.DEFAULT_CODEC_REGISTRY));

        assertThrows(CodecConfigurationException.class, () ->
                createRecordCodec(TestRecordWithIllegalBsonCreatorOnConstructor.class, Bson.DEFAULT_CODEC_REGISTRY));
        assertThrows(CodecConfigurationException.class, () ->
                createRecordCodec(TestRecordWithIllegalBsonCreatorOnMethod.class, Bson.DEFAULT_CODEC_REGISTRY));

        assertThrows(CodecConfigurationException.class, () ->
                createRecordCodec(TestRecordWithIllegalBsonIgnoreOnComponent.class, Bson.DEFAULT_CODEC_REGISTRY));
        assertThrows(CodecConfigurationException.class, () ->
                createRecordCodec(TestRecordWithIllegalBsonIgnoreOnAccessor.class, Bson.DEFAULT_CODEC_REGISTRY));
        assertThrows(CodecConfigurationException.class, () ->
                createRecordCodec(TestRecordWithIllegalBsonExtraElementsOnComponent.class, Bson.DEFAULT_CODEC_REGISTRY));
        assertThrows(CodecConfigurationException.class, () ->
                createRecordCodec(TestRecordWithIllegalBsonExtraElementsOnAccessor.class, Bson.DEFAULT_CODEC_REGISTRY));
    }

    private static <T extends Record> RecordCodec<T> createRecordCodec(final Class<T> clazz, final CodecRegistry registry) {
        return new RecordCodec<>(clazz, List.of(), registry);
    }
}
