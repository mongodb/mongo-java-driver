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

package org.bson;

import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.RawBsonDocumentCodec;
import org.bson.io.BasicOutputBuffer;
import org.bson.json.JsonMode;
import org.bson.json.JsonReader;
import org.bson.json.JsonWriter;
import org.bson.json.JsonWriterSettings;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StringWriter;
import java.nio.ByteOrder;
import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("RawBsonDocument")
class RawBsonDocumentTest {

    private static final BsonDocument EMPTY_DOCUMENT = new BsonDocument();
    private static final RawBsonDocument EMPTY_RAW_DOCUMENT = new RawBsonDocument(EMPTY_DOCUMENT, new BsonDocumentCodec());
    private static final BsonDocument DOCUMENT = new BsonDocument()
            .append("a", new BsonInt32(1))
            .append("b", new BsonInt32(1))
            .append("c", new BsonDocument("x", BsonBoolean.TRUE))
            .append("d", new BsonArray(asList(new BsonDocument("y", BsonBoolean.FALSE), new BsonArray(asList(new BsonInt32(1))))));

    // Constructor Validation

    @Test
    @DisplayName("constructors should throw if parameters are invalid")
    void constructorsShouldThrowForInvalidParameters() {
        assertThrows(IllegalArgumentException.class, () -> new RawBsonDocument((byte[]) null));
        assertThrows(IllegalArgumentException.class, () -> new RawBsonDocument(null, 0, 5));
        assertThrows(IllegalArgumentException.class, () -> new RawBsonDocument(new byte[5], -1, 5));
        assertThrows(IllegalArgumentException.class, () -> new RawBsonDocument(new byte[5], 5, 5));
        assertThrows(IllegalArgumentException.class, () -> new RawBsonDocument(new byte[5], 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new RawBsonDocument(new byte[10], 6, 5));
        assertThrows(IllegalArgumentException.class, () -> new RawBsonDocument(null, new DocumentCodec()));
        assertThrows(IllegalArgumentException.class, () -> new RawBsonDocument(new Document(), null));
    }

    // Byte Buffer

    @ParameterizedTest
    @MethodSource("rawDocumentVariants")
    @DisplayName("byteBuffer should contain the correct bytes")
    void byteBufferShouldContainCorrectBytes(final RawBsonDocument rawDocument) {
        ByteBuf byteBuf = rawDocument.getByteBuffer();

        assertEquals(DOCUMENT, rawDocument);
        assertEquals(ByteOrder.LITTLE_ENDIAN, byteBuf.asNIO().order());
        assertEquals(66, byteBuf.remaining());

        byte[] actualBytes = new byte[66];
        byteBuf.get(actualBytes);
        assertArrayEquals(getBytesFromDocument(), actualBytes);
    }

    // Parse

    @Test
    @DisplayName("parse() should throw if parameter is invalid")
    void parseShouldThrowForInvalidParameter() {
        assertThrows(IllegalArgumentException.class, () -> RawBsonDocument.parse(null));
    }

    @Test
    @DisplayName("parse() should parse JSON")
    void parseShouldParseJson() {
        assertEquals(new BsonDocument("a", new BsonInt32(1)), RawBsonDocument.parse("{a : 1}"));
    }

    // Basic Operations

    @ParameterizedTest
    @MethodSource("rawDocumentVariants")
    @DisplayName("containsKey() throws IllegalArgumentException for null key")
    void containsKeyShouldThrowForNullKey(final RawBsonDocument rawDocument) {
        assertThrows(IllegalArgumentException.class, () -> rawDocument.containsKey(null));
    }

    @ParameterizedTest
    @MethodSource("rawDocumentVariants")
    @DisplayName("containsKey() finds existing keys")
    void containsKeyShouldFindExistingKeys(final RawBsonDocument rawDocument) {
        assertTrue(rawDocument.containsKey("a"));
        assertTrue(rawDocument.containsKey("b"));
        assertTrue(rawDocument.containsKey("c"));
        assertTrue(rawDocument.containsKey("d"));
    }

    @ParameterizedTest
    @MethodSource("rawDocumentVariants")
    @DisplayName("containsKey() does not find non-existing keys")
    void containsKeyShouldNotFindNonExistingKeys(final RawBsonDocument rawDocument) {
        assertFalse(rawDocument.containsKey("e"));
        assertFalse(rawDocument.containsKey("x"));
        assertFalse(rawDocument.containsKey("y"));
    }

    @ParameterizedTest
    @MethodSource("rawDocumentVariants")
    @DisplayName("get() returns null for non-existing keys")
    void getShouldReturnNullForNonExistingKeys(final RawBsonDocument rawDocument) {
        assertEquals(null, rawDocument.get("e"));
        assertEquals(null, rawDocument.get("x"));
        assertEquals(null, rawDocument.get("y"));
    }

    @ParameterizedTest
    @MethodSource("rawDocumentVariants")
    @DisplayName("get() returns RawBsonDocument for sub documents and RawBsonArray for arrays")
    void getShouldReturnCorrectTypes(final RawBsonDocument rawDocument) {
        assertInstanceOf(BsonInt32.class, rawDocument.get("a"));
        assertInstanceOf(BsonInt32.class, rawDocument.get("b"));
        assertInstanceOf(RawBsonDocument.class, rawDocument.get("c"));
        assertInstanceOf(RawBsonArray.class, rawDocument.get("d"));
        assertInstanceOf(RawBsonDocument.class, rawDocument.get("d").asArray().get(0));
        assertInstanceOf(RawBsonArray.class, rawDocument.get("d").asArray().get(1));

        assertTrue(rawDocument.getDocument("c").getBoolean("x").getValue());
        assertFalse(rawDocument.get("d").asArray().get(0).asDocument().getBoolean("y").getValue());
        assertEquals(1, rawDocument.get("d").asArray().get(1).asArray().get(0).asInt32().getValue());
    }

    @ParameterizedTest
    @MethodSource("rawDocumentVariants")
    @DisplayName("containsValue() finds existing values")
    void containsValueShouldFindExistingValues(final RawBsonDocument rawDocument) {
        assertTrue(rawDocument.containsValue(DOCUMENT.get("a")));
        assertTrue(rawDocument.containsValue(DOCUMENT.get("b")));
        assertTrue(rawDocument.containsValue(DOCUMENT.get("c")));
        assertTrue(rawDocument.containsValue(DOCUMENT.get("d")));
    }

    @ParameterizedTest
    @MethodSource("rawDocumentVariants")
    @DisplayName("containsValue() does not find non-existing values")
    void containsValueShouldNotFindNonExistingValues(final RawBsonDocument rawDocument) {
        assertFalse(rawDocument.containsValue(new BsonInt32(3)));
        assertFalse(rawDocument.containsValue(new BsonDocument("e", BsonBoolean.FALSE)));
        assertFalse(rawDocument.containsValue(new BsonArray(asList(new BsonInt32(2), new BsonInt32(4)))));
    }

    @ParameterizedTest
    @MethodSource("rawDocumentVariants")
    @DisplayName("isEmpty() returns false when the document is not empty")
    void isEmptyShouldReturnFalseForNonEmptyDocument(final RawBsonDocument rawDocument) {
        assertFalse(rawDocument.isEmpty());
    }

    @Test
    @DisplayName("isEmpty() returns true when the document is empty")
    void isEmptyShouldReturnTrueForEmptyDocument() {
        assertTrue(EMPTY_RAW_DOCUMENT.isEmpty());
    }

    @Test
    @DisplayName("size() returns 0 for empty document")
    void sizeShouldReturnZeroForEmptyDocument() {
        assertEquals(0, EMPTY_RAW_DOCUMENT.size());
    }

    @Test
    @DisplayName("keySet() is empty for empty document")
    void keySetShouldBeEmptyForEmptyDocument() {
        assertTrue(EMPTY_RAW_DOCUMENT.keySet().isEmpty());
    }

    @Test
    @DisplayName("values() is empty for empty document")
    void valuesShouldBeEmptyForEmptyDocument() {
        assertTrue(EMPTY_RAW_DOCUMENT.values().isEmpty());
    }

    @Test
    @DisplayName("entrySet() is empty for empty document")
    void entrySetShouldBeEmptyForEmptyDocument() {
        assertTrue(EMPTY_RAW_DOCUMENT.entrySet().isEmpty());
    }

    // Collection Views

    @ParameterizedTest
    @MethodSource("rawDocumentVariants")
    @DisplayName("size() returns correct count")
    void sizeShouldReturnCorrectCount(final RawBsonDocument rawDocument) {
        assertEquals(4, rawDocument.size());
    }

    @ParameterizedTest
    @MethodSource("rawDocumentVariants")
    @DisplayName("keySet() returns all keys")
    void keySetShouldReturnAllKeys(final RawBsonDocument rawDocument) {
        assertEquals(new HashSet<>(asList("a", "b", "c", "d")), rawDocument.keySet());
    }

    @ParameterizedTest
    @MethodSource("rawDocumentVariants")
    @DisplayName("values() returns all values")
    void valuesShouldReturnAllValues(final RawBsonDocument rawDocument) {
        assertEquals(
                asList(DOCUMENT.get("a"), DOCUMENT.get("b"), DOCUMENT.get("c"), DOCUMENT.get("d")),
                rawDocument.values());
    }

    @ParameterizedTest
    @MethodSource("rawDocumentVariants")
    @DisplayName("entrySet() returns all entries")
    void entrySetShouldReturnAllEntries(final RawBsonDocument rawDocument) {
        Set<Map.Entry<String, BsonValue>> expected = new HashSet<>(asList(
                new AbstractMap.SimpleImmutableEntry<>("a", DOCUMENT.get("a")),
                new AbstractMap.SimpleImmutableEntry<>("b", DOCUMENT.get("b")),
                new AbstractMap.SimpleImmutableEntry<>("c", DOCUMENT.get("c")),
                new AbstractMap.SimpleImmutableEntry<>("d", DOCUMENT.get("d"))
        ));
        assertEquals(expected, rawDocument.entrySet());
    }

    @ParameterizedTest
    @MethodSource("rawDocumentVariants")
    @DisplayName("getFirstKey() returns first key")
    void getFirstKeyShouldReturnFirstKey(final RawBsonDocument rawDocument) {
        assertEquals("a", rawDocument.getFirstKey());
    }

    @Test
    @DisplayName("getFirstKey() throws NoSuchElementException for empty document")
    void getFirstKeyShouldThrowForEmptyDocument() {
        assertThrows(NoSuchElementException.class, () -> EMPTY_RAW_DOCUMENT.getFirstKey());
    }

    // Conversion and Serialization

    @ParameterizedTest
    @MethodSource("rawDocumentVariants")
    @DisplayName("asBsonReader() creates valid reader")
    void asBsonReaderShouldWork(final RawBsonDocument rawDocument) {
        try (BsonReader reader = rawDocument.asBsonReader()) {
            BsonDocument decoded = new BsonDocumentCodec().decode(reader, DecoderContext.builder().build());
            assertEquals(DOCUMENT, decoded);
        }
    }

    @ParameterizedTest
    @MethodSource("rawDocumentVariants")
    @DisplayName("toJson() returns equivalent JSON")
    void toJsonShouldReturnEquivalentJson(final RawBsonDocument rawDocument) {
        RawBsonDocument reparsed = new RawBsonDocumentCodec().decode(
                new JsonReader(rawDocument.toJson()), DecoderContext.builder().build());
        assertEquals(DOCUMENT, reparsed);
    }

    @ParameterizedTest
    @MethodSource("rawDocumentVariants")
    @DisplayName("toJson() respects default JsonWriterSettings")
    void toJsonShouldRespectDefaultSettings(final RawBsonDocument rawDocument) {
        StringWriter writer = new StringWriter();
        new BsonDocumentCodec().encode(new JsonWriter(writer), DOCUMENT, EncoderContext.builder().build());
        assertEquals(writer.toString(), rawDocument.toJson());
    }

    @ParameterizedTest
    @MethodSource("rawDocumentVariants")
    @DisplayName("toJson() respects JsonWriterSettings")
    void toJsonShouldRespectCustomSettings(final RawBsonDocument rawDocument) {
        JsonWriterSettings settings = JsonWriterSettings.builder().outputMode(JsonMode.SHELL).build();
        StringWriter writer = new StringWriter();
        new RawBsonDocumentCodec().encode(new JsonWriter(writer, settings), rawDocument, EncoderContext.builder().build());
        assertEquals(writer.toString(), rawDocument.toJson(settings));
    }

    // Immutability

    @Test
    @DisplayName("All write methods throw UnsupportedOperationException")
    void writeMethodsShouldThrow() {
        RawBsonDocument rawDocument = createRawDocumentFromDocument();
        assertThrows(UnsupportedOperationException.class, () -> rawDocument.clear());
        assertThrows(UnsupportedOperationException.class, () -> rawDocument.put("x", BsonNull.VALUE));
        assertThrows(UnsupportedOperationException.class, () -> rawDocument.append("x", BsonNull.VALUE));
        assertThrows(UnsupportedOperationException.class, () -> rawDocument.putAll(new BsonDocument("x", BsonNull.VALUE)));
        assertThrows(UnsupportedOperationException.class, () -> rawDocument.remove(BsonNull.VALUE));
    }

    // Decode

    @ParameterizedTest
    @MethodSource("rawDocumentVariants")
    @DisplayName("decode() returns equivalent document")
    void decodeShouldWork(final RawBsonDocument rawDocument) {
        assertEquals(DOCUMENT, rawDocument.decode(new BsonDocumentCodec()));
    }

    // Equality and HashCode

    @ParameterizedTest
    @MethodSource("rawDocumentVariants")
    @DisplayName("hashCode() equals hash code of identical BsonDocument")
    void hashCodeShouldEqualBsonDocumentHashCode(final RawBsonDocument rawDocument) {
        assertEquals(DOCUMENT.hashCode(), rawDocument.hashCode());
    }

    @ParameterizedTest
    @MethodSource("rawDocumentVariants")
    @DisplayName("equals() works correctly")
    void equalsShouldWork(final RawBsonDocument rawDocument) {
        assertEquals(DOCUMENT, rawDocument);
        assertEquals(DOCUMENT, rawDocument);
        assertEquals(rawDocument, rawDocument);
        assertNotEquals(EMPTY_RAW_DOCUMENT, rawDocument);
    }

    // Clone

    @ParameterizedTest
    @MethodSource("rawDocumentVariants")
    @DisplayName("clone() creates deep copy")
    void cloneShouldMakeDeepCopy(final RawBsonDocument rawDocument) {
        RawBsonDocument cloned = (RawBsonDocument) rawDocument.clone();
        RawBsonDocument reference = createRawDocumentFromDocument();

        assertNotSame(cloned.getByteBuffer().array(), reference.getByteBuffer().array());
        assertEquals(rawDocument.getByteBuffer().remaining(), cloned.getByteBuffer().remaining());
        assertEquals(reference, cloned);
    }

    // Serialization

    @ParameterizedTest
    @MethodSource("rawDocumentVariants")
    @DisplayName("Java serialization works correctly")
    void serializationShouldWork(final RawBsonDocument rawDocument) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        new ObjectOutputStream(baos).writeObject(rawDocument);
        Object deserialized = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray())).readObject();
        assertEquals(DOCUMENT, deserialized);
    }

    // --- Helper Methods ---

    static Stream<RawBsonDocument> rawDocumentVariants() {
        return Stream.of(
                createRawDocumentFromDocument(),
                createRawDocumentFromByteArray(),
                createRawDocumentFromByteArrayOffsetLength()
        );
    }

    private static RawBsonDocument createRawDocumentFromDocument() {
        return new RawBsonDocument(DOCUMENT, new BsonDocumentCodec());
    }

    private static RawBsonDocument createRawDocumentFromByteArray() {
        return new RawBsonDocument(getBytesFromDocument());
    }

    private static RawBsonDocument createRawDocumentFromByteArrayOffsetLength() {
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer(1024);
        new BsonDocumentCodec().encode(new BsonBinaryWriter(outputBuffer), DOCUMENT, EncoderContext.builder().build());
        byte[] bytes = outputBuffer.getInternalBuffer();
        int size = outputBuffer.getPosition();

        byte[] unstrippedBytes = new byte[size + 2];
        System.arraycopy(bytes, 0, unstrippedBytes, 1, size);
        return new RawBsonDocument(unstrippedBytes, 1, size);
    }

    private static byte[] getBytesFromDocument() {
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer(1024);
        new BsonDocumentCodec().encode(new BsonBinaryWriter(outputBuffer), DOCUMENT, EncoderContext.builder().build());
        byte[] bytes = outputBuffer.getInternalBuffer();
        int size = outputBuffer.getPosition();

        byte[] strippedBytes = new byte[size];
        System.arraycopy(bytes, 0, strippedBytes, 0, size);
        return strippedBytes;
    }
}
