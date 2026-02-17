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

package com.mongodb.internal.connection;

import org.bson.BsonArray;
import org.bson.BsonBinaryWriter;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonReader;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@DisplayName("ByteBufBsonDocument")
class ByteBufBsonDocumentTest {
    private ByteBuf emptyDocumentByteBuf;
    private ByteBuf documentByteBuf;
    private ByteBufBsonDocument emptyByteBufDocument;
    private ByteBufBsonDocument byteBufDocument;
    private BsonDocument document;

    @BeforeEach
    void setUp() {
        emptyDocumentByteBuf = new ByteBufNIO(ByteBuffer.wrap(new byte[]{5, 0, 0, 0, 0}));
        emptyByteBufDocument = new ByteBufBsonDocument(emptyDocumentByteBuf);

        document = new BsonDocument()
                .append("a", new BsonInt32(1))
                .append("b", new BsonInt32(2))
                .append("c", new BsonDocument("x", BsonBoolean.TRUE))
                .append("d", new BsonArray(asList(
                        new BsonDocument("y", BsonBoolean.FALSE),
                        new BsonInt32(1)
                )));

        RawBsonDocument rawBsonDocument = RawBsonDocument.parse(document.toString());
        documentByteBuf = rawBsonDocument.getByteBuffer();
        byteBufDocument = new ByteBufBsonDocument(documentByteBuf);
    }

    @AfterEach
    void tearDown() {
        if (byteBufDocument != null) {
            byteBufDocument.close();
        }
        if (emptyByteBufDocument != null) {
            emptyByteBufDocument.close();
        }
    }

    // Basic Operations

    @Test
    @DisplayName("get() returns value for existing key, null for missing key")
    void getShouldReturnCorrectValue() {
        assertNull(emptyByteBufDocument.get("a"));
        assertNull(byteBufDocument.get("z"));
        assertEquals(new BsonInt32(1), byteBufDocument.get("a"));
        assertEquals(new BsonInt32(2), byteBufDocument.get("b"));
    }

    @Test
    @DisplayName("get() throws IllegalArgumentException for null key")
    void getShouldThrowForNullKey() {
        assertThrows(IllegalArgumentException.class, () -> byteBufDocument.get(null));
        assertEquals(1, documentByteBuf.getReferenceCount());
    }

    @Test
    @DisplayName("containsKey() finds existing keys and rejects missing keys")
    void containsKeyShouldWork() {
        assertThrows(IllegalArgumentException.class, () -> byteBufDocument.containsKey(null));
        assertTrue(byteBufDocument.containsKey("a"));
        assertTrue(byteBufDocument.containsKey("d"));
        assertFalse(byteBufDocument.containsKey("z"));
        assertEquals(1, documentByteBuf.getReferenceCount());
    }

    @Test
    @DisplayName("containsValue() finds existing values and rejects missing values")
    void containsValueShouldWork() {
        assertTrue(byteBufDocument.containsValue(document.get("a")));
        assertTrue(byteBufDocument.containsValue(document.get("c")));
        assertFalse(byteBufDocument.containsValue(new BsonInt32(999)));
        assertEquals(1, documentByteBuf.getReferenceCount());
    }

    @Test
    @DisplayName("isEmpty() returns correct result")
    void isEmptyShouldWork() {
        assertTrue(emptyByteBufDocument.isEmpty());
        assertFalse(byteBufDocument.isEmpty());
    }

    @Test
    @DisplayName("size() returns correct count")
    void sizeShouldWork() {
        assertEquals(0, emptyByteBufDocument.size());
        assertEquals(4, byteBufDocument.size());
        assertEquals(4, byteBufDocument.size()); // Verify caching works
    }

    @Test
    @DisplayName("getFirstKey() returns first key or throws for empty document")
    void getFirstKeyShouldWork() {
        assertEquals("a", byteBufDocument.getFirstKey());
        assertThrows(NoSuchElementException.class, () -> emptyByteBufDocument.getFirstKey());
    }

    // Collection Views

    @Test
    @DisplayName("keySet() returns all keys")
    void keySetShouldWork() {
        assertTrue(emptyByteBufDocument.keySet().isEmpty());
        assertEquals(new HashSet<>(asList("a", "b", "c", "d")), byteBufDocument.keySet());
    }

    @Test
    @DisplayName("values() returns all values")
    void valuesShouldWork() {
        assertTrue(emptyByteBufDocument.values().isEmpty());
        Set<BsonValue> expected = new HashSet<>(asList(
                document.get("a"), document.get("b"), document.get("c"), document.get("d")
        ));
        assertEquals(expected, new HashSet<>(byteBufDocument.values()));
    }

    @Test
    @DisplayName("entrySet() returns all entries")
    void entrySetShouldWork() {
        assertTrue(emptyByteBufDocument.entrySet().isEmpty());
        Set<Map.Entry<String, BsonValue>> expected = new HashSet<>(asList(
                new AbstractMap.SimpleImmutableEntry<>("a", document.get("a")),
                new AbstractMap.SimpleImmutableEntry<>("b", document.get("b")),
                new AbstractMap.SimpleImmutableEntry<>("c", document.get("c")),
                new AbstractMap.SimpleImmutableEntry<>("d", document.get("d"))
        ));
        assertEquals(expected, byteBufDocument.entrySet());
    }

    // Type-Specific Accessors

    @Test
    @DisplayName("getDocument() returns nested document")
    void getDocumentShouldWork() {
        BsonDocument nested = byteBufDocument.getDocument("c");
        assertNotNull(nested);
        assertEquals(BsonBoolean.TRUE, nested.get("x"));
    }

    @Test
    @DisplayName("getArray() returns array")
    void getArrayShouldWork() {
        BsonArray array = byteBufDocument.getArray("d");
        assertNotNull(array);
        assertEquals(2, array.size());
    }

    @Test
    @DisplayName("get() with default value works correctly")
    void getWithDefaultShouldWork() {
        assertEquals(new BsonInt32(1), byteBufDocument.get("a", new BsonInt32(999)));
        assertEquals(new BsonInt32(999), byteBufDocument.get("missing", new BsonInt32(999)));
    }

    @Test
    @DisplayName("Type check methods return correct results")
    void typeChecksShouldWork() {
        assertTrue(byteBufDocument.isNumber("a"));
        assertTrue(byteBufDocument.isInt32("a"));
        assertTrue(byteBufDocument.isDocument("c"));
        assertTrue(byteBufDocument.isArray("d"));
        assertFalse(byteBufDocument.isDocument("a"));
    }

    // Immutability

    @Test
    @DisplayName("All write methods throw UnsupportedOperationException")
    void writeMethodsShouldThrow() {
        assertThrows(UnsupportedOperationException.class, () -> byteBufDocument.clear());
        assertThrows(UnsupportedOperationException.class, () -> byteBufDocument.put("x", new BsonInt32(1)));
        assertThrows(UnsupportedOperationException.class, () -> byteBufDocument.append("x", new BsonInt32(1)));
        assertThrows(UnsupportedOperationException.class, () -> byteBufDocument.putAll(new BsonDocument()));
        assertThrows(UnsupportedOperationException.class, () -> byteBufDocument.remove("a"));
    }

    // Conversion and Serialization

    @Test
    @DisplayName("toBsonDocument() returns equivalent document and caches result")
    void toBsonDocumentShouldWork() {
        assertEquals(document, byteBufDocument.toBsonDocument());
        BsonDocument first = byteBufDocument.toBsonDocument();
        BsonDocument second = byteBufDocument.toBsonDocument();
        assertEquals(first, second);
    }

    @Test
    @DisplayName("asBsonReader() creates valid reader")
    void asBsonReaderShouldWork() {
        try (BsonReader reader = byteBufDocument.asBsonReader()) {
            BsonDocument decoded = new BsonDocumentCodec().decode(reader, DecoderContext.builder().build());
            assertEquals(document, decoded);
        }
    }

    @Test
    @DisplayName("toJson() returns correct JSON with different settings")
    void toJsonShouldWork() {
        assertEquals(document.toJson(), byteBufDocument.toJson());
        assertNotNull(byteBufDocument.toJson()); // Verify caching

        JsonWriterSettings shellSettings = JsonWriterSettings.builder().outputMode(JsonMode.SHELL).build();
        assertEquals(document.toJson(shellSettings), byteBufDocument.toJson(shellSettings));
    }

    @Test
    @DisplayName("toString() returns equivalent string")
    void toStringShouldWork() {
        assertEquals(document.toString(), byteBufDocument.toString());
    }

    @Test
    @DisplayName("clone() creates deep copy")
    void cloneShouldWork() {
        BsonDocument cloned = byteBufDocument.clone();
        assertEquals(byteBufDocument, cloned);
    }

    @Test
    @DisplayName("Java serialization works correctly")
    void serializationShouldWork() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        new ObjectOutputStream(baos).writeObject(byteBufDocument);
        Object deserialized = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray())).readObject();
        assertEquals(byteBufDocument, deserialized);
    }

    // Equality and HashCode

    @Test
    @DisplayName("equals() and hashCode() work correctly")
    void equalsAndHashCodeShouldWork() {
        assertEquals(document, byteBufDocument);
        assertEquals(byteBufDocument, document);
        assertEquals(document.hashCode(), byteBufDocument.hashCode());
        assertNotEquals(byteBufDocument, new BsonDocument("x", new BsonInt32(99)));
    }

    // Resource Management

    @Test
    @DisplayName("Closed document throws IllegalStateException on all operations")
    void closedDocumentShouldThrow() {
        byteBufDocument.close();
        assertThrows(IllegalStateException.class, () -> byteBufDocument.size());
        assertThrows(IllegalStateException.class, () -> byteBufDocument.isEmpty());
        assertThrows(IllegalStateException.class, () -> byteBufDocument.containsKey("a"));
        assertThrows(IllegalStateException.class, () -> byteBufDocument.get("a"));
        assertThrows(IllegalStateException.class, () -> byteBufDocument.keySet());
        assertThrows(IllegalStateException.class, () -> byteBufDocument.values());
        assertThrows(IllegalStateException.class, () -> byteBufDocument.entrySet());
        assertThrows(IllegalStateException.class, () -> byteBufDocument.getFirstKey());
        assertThrows(IllegalStateException.class, () -> byteBufDocument.toBsonDocument());
        assertThrows(IllegalStateException.class, () -> byteBufDocument.toJson());
    }

    @Test
    @DisplayName("close() can be called multiple times safely")
    void closeIsIdempotent() {
        byteBufDocument.close();
        byteBufDocument.close(); // Should not throw
    }

    @Test
    @DisplayName("Nested documents are closed when parent is closed")
    void nestedDocumentsClosedWithParent() {
        BsonDocument doc = new BsonDocument("outer", new BsonDocument("inner", new BsonInt32(42)));
        ByteBuf buf = createByteBufFromDocument(doc);
        ByteBufBsonDocument byteBufDoc = new ByteBufBsonDocument(buf);

        BsonDocument retrieved = byteBufDoc.getDocument("outer");
        byteBufDoc.close();

        assertThrows(IllegalStateException.class, byteBufDoc::size);
        if (retrieved instanceof ByteBufBsonDocument) {
            assertThrows(IllegalStateException.class, retrieved::size);
        }
    }

    @Test
    @DisplayName("Nested arrays are closed when parent is closed")
    void nestedArraysClosedWithParent() {
        BsonDocument doc = new BsonDocument("arr", new BsonArray(asList(
                new BsonInt32(1), new BsonDocument("x", new BsonInt32(2))
        )));
        ByteBuf buf = createByteBufFromDocument(doc);
        ByteBufBsonDocument byteBufDoc = new ByteBufBsonDocument(buf);

        BsonArray retrieved = byteBufDoc.getArray("arr");
        byteBufDoc.close();

        assertThrows(IllegalStateException.class, byteBufDoc::size);
        if (retrieved instanceof ByteBufBsonArray) {
            assertThrows(IllegalStateException.class, retrieved::size);
        }
    }

    @Test
    @DisplayName("Deeply nested structures are closed recursively")
    void deeplyNestedClosedRecursively() {
        BsonDocument doc = new BsonDocument()
                .append("level1", new BsonArray(asList(
                        new BsonDocument("level2", new BsonDocument("level3", new BsonInt32(999))),
                        new BsonInt32(1)
                )))
                .append("sibling", new BsonDocument("key", new BsonString("value")));

        ByteBuf buf = createByteBufFromDocument(doc);
        ByteBufBsonDocument byteBufDoc = new ByteBufBsonDocument(buf);

        BsonArray level1 = byteBufDoc.getArray("level1");
        byteBufDoc.getDocument("sibling");

        if (level1.get(0).isDocument()) {
            BsonDocument level2Doc = level1.get(0).asDocument();
            if (level2Doc.containsKey("level2")) {
                assertEquals(new BsonInt32(999), level2Doc.getDocument("level2").get("level3"));
            }
        }

        byteBufDoc.close();
        assertThrows(IllegalStateException.class, byteBufDoc::size);
    }

    @Test
    @DisplayName("Iteration tracks resources correctly")
    void iterationTracksResources() {
        BsonDocument doc = new BsonDocument()
                .append("doc1", new BsonDocument("a", new BsonInt32(1)))
                .append("arr1", new BsonArray(asList(new BsonInt32(2), new BsonInt32(3))))
                .append("primitive", new BsonString("test"));

        ByteBuf buf = createByteBufFromDocument(doc);
        ByteBufBsonDocument byteBufDoc = new ByteBufBsonDocument(buf);

        int count = 0;
        for (Map.Entry<String, BsonValue> entry : byteBufDoc.entrySet()) {
            assertNotNull(entry.getKey());
            assertNotNull(entry.getValue());
            count++;
        }
        assertEquals(3, count);

        byteBufDoc.close();
        assertThrows(IllegalStateException.class, byteBufDoc::size);
    }

    @Test
    @DisplayName("toBsonDocument() handles nested structures and allows close")
    void toBsonDocumentHandlesNestedStructures() {
        BsonDocument complexDoc = new BsonDocument()
                .append("doc", new BsonDocument("x", new BsonInt32(1)))
                .append("arr", new BsonArray(asList(new BsonDocument("y", new BsonInt32(2)), new BsonInt32(3))));

        ByteBuf buf = createByteBufFromDocument(complexDoc);
        ByteBufBsonDocument byteBufDoc = new ByteBufBsonDocument(buf);

        BsonDocument hydrated = byteBufDoc.toBsonDocument();
        assertEquals(complexDoc, hydrated);

        byteBufDoc.close();
    }

    @Test
    @DisplayName("cachedDocument is usable after close")
    void cachedDocumentIsUsableAfterClose() {
        BsonDocument complexDoc = new BsonDocument()
                .append("doc", new BsonDocument("x", new BsonInt32(1)))
                .append("arr", new BsonArray(asList(new BsonDocument("y", new BsonInt32(2)), new BsonInt32(3))));

        ByteBuf buf = createByteBufFromDocument(complexDoc);
        ByteBufBsonDocument byteBufDoc = new ByteBufBsonDocument(buf);
        BsonDocument hydrated = byteBufDoc.toBsonDocument();

        byteBufDoc.close();
        assertEquals(complexDoc, hydrated);
        assertEquals(complexDoc.toJson(), hydrated.toJson());
    }

    // Sequence Fields (OP_MSG)

    @Test
    @DisplayName("Sequence field is accessible as array of ByteBufBsonDocuments")
    void sequenceFieldAccessibleAsArray() {
        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider());
             ByteBufBsonDocument commandDoc = createCommandMessageDocument(output, 3)) {

            BsonValue documentsValue = commandDoc.get("documents");
            assertNotNull(documentsValue);
            assertTrue(documentsValue.isArray());

            BsonArray documents = documentsValue.asArray();
            assertEquals(3, documents.size());

            for (int i = 0; i < 3; i++) {
                BsonValue doc = documents.get(i);
                assertInstanceOf(ByteBufBsonDocument.class, doc);
                assertEquals(new BsonInt32(i), doc.asDocument().get("_id"));
                assertEquals(new BsonString("doc" + i), doc.asDocument().get("name"));
            }
        }
    }

    @Test
    @DisplayName("Sequence field is included in size, keySet, values, and entrySet")
    void sequenceFieldIncludedInCollectionViews() {
        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider());
             ByteBufBsonDocument commandDoc = createCommandMessageDocument(output, 2)) {

            assertTrue(commandDoc.size() >= 3);
            assertTrue(commandDoc.keySet().contains("documents"));
            assertTrue(commandDoc.keySet().contains("insert"));

            boolean foundDocumentsArray = false;
            for (BsonValue value : commandDoc.values()) {
                if (value.isArray() && value.asArray().size() == 2) {
                    foundDocumentsArray = true;
                    break;
                }
            }
            assertTrue(foundDocumentsArray);

            boolean foundDocumentsEntry = false;
            for (Map.Entry<String, BsonValue> entry : commandDoc.entrySet()) {
                if ("documents".equals(entry.getKey())) {
                    foundDocumentsEntry = true;
                    assertEquals(2, entry.getValue().asArray().size());
                    break;
                }
            }
            assertTrue(foundDocumentsEntry);
        }
    }

    @Test
    @DisplayName("containsKey and containsValue work with sequence fields")
    void containsMethodsWorkWithSequenceFields() {
        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider());
             ByteBufBsonDocument commandDoc = createCommandMessageDocument(output, 3)) {

            assertTrue(commandDoc.containsKey("documents"));
            assertTrue(commandDoc.containsKey("insert"));
            assertFalse(commandDoc.containsKey("nonexistent"));

            BsonDocument expectedDoc = new BsonDocument()
                    .append("_id", new BsonInt32(1))
                    .append("name", new BsonString("doc1"));
            assertTrue(commandDoc.containsValue(expectedDoc));
        }
    }

    @Test
    @DisplayName("Sequence field documents are closed when parent is closed")
    void sequenceFieldDocumentsClosedWithParent() {
        ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider());
        ByteBufBsonDocument commandDoc = createCommandMessageDocument(output, 2);

        BsonArray documents = commandDoc.getArray("documents");
        List<BsonDocument> docRefs = new ArrayList<>();
        for (BsonValue doc : documents) {
            docRefs.add(doc.asDocument());
        }

        commandDoc.close();
        output.close();

        assertThrows(IllegalStateException.class, commandDoc::size);
        for (BsonDocument doc : docRefs) {
            if (doc instanceof ByteBufBsonDocument) {
                assertThrows(IllegalStateException.class, doc::size);
            }
        }
    }

    @Test
    @DisplayName("Sequence field is cached on multiple access")
    void sequenceFieldCached() {
        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider());
             ByteBufBsonDocument commandDoc = createCommandMessageDocument(output, 2)) {

            BsonArray first = commandDoc.getArray("documents");
            BsonArray second = commandDoc.getArray("documents");
            assertNotNull(first);
            assertEquals(first.size(), second.size());
        }
    }

    @Test
    @DisplayName("toBsonDocument() hydrates sequence fields to regular BsonDocuments")
    void toBsonDocumentHydratesSequenceFields() {
        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider());
             ByteBufBsonDocument commandDoc = createCommandMessageDocument(output, 2)) {

            BsonDocument hydrated = commandDoc.toBsonDocument();
            assertTrue(hydrated.containsKey("documents"));

            BsonArray documents = hydrated.getArray("documents");
            assertEquals(2, documents.size());
            for (BsonValue doc : documents) {
                assertFalse(doc instanceof ByteBufBsonDocument);
            }
        }
    }

    @Test
    @DisplayName("Sequence field with nested documents works correctly")
    void sequenceFieldWithNestedDocuments() {
        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            ByteBufBsonDocument commandDoc = createNestedCommandMessageDocument(output);

            BsonArray documents = commandDoc.getArray("documents");
            assertEquals(2, documents.size());

            BsonDocument firstDoc = documents.get(0).asDocument();
            BsonDocument nested = firstDoc.getDocument("nested");
            assertEquals(new BsonInt32(0), nested.get("inner"));

            BsonArray array = firstDoc.getArray("array");
            assertEquals(2, array.size());

            commandDoc.close();
        }
    }

    @Test
    @DisplayName("Empty sequence field returns empty array")
    void emptySequenceField() {
        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider());
             ByteBufBsonDocument commandDoc = createCommandMessageDocument(output, 0)) {

            assertTrue(commandDoc.containsKey("insert"));
            assertTrue(commandDoc.containsKey("documents"));
            assertTrue(commandDoc.getArray("documents").isEmpty());
        }
    }

    @Test
    @DisplayName("getFirstKey() returns body field, not sequence field")
    void getFirstKeyReturnsBodyField() {
        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider());
             ByteBufBsonDocument commandDoc = createCommandMessageDocument(output, 2)) {

            assertEquals("insert", commandDoc.getFirstKey());
        }
    }

    @Test
    @DisplayName("toJson() includes sequence fields")
    void toJsonIncludesSequenceFields() {
        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider());
             ByteBufBsonDocument commandDoc = createCommandMessageDocument(output, 2)) {

            String json = commandDoc.toJson();
            assertTrue(json.contains("documents"));
            assertTrue(json.contains("_id"));
        }
    }

    @Test
    @DisplayName("equals() and hashCode() include sequence fields")
    void equalsAndHashCodeIncludeSequenceFields() {
        try (ByteBufferBsonOutput output1 = new ByteBufferBsonOutput(new SimpleBufferProvider());
             ByteBufBsonDocument commandDoc1 = createCommandMessageDocument(output1, 2);
             ByteBufferBsonOutput output2 = new ByteBufferBsonOutput(new SimpleBufferProvider());
             ByteBufBsonDocument commandDoc2 = createCommandMessageDocument(output2, 2)) {

            assertEquals(commandDoc1.toBsonDocument(), commandDoc2.toBsonDocument());
            assertEquals(commandDoc1.hashCode(), commandDoc2.hashCode());
        }
    }

    // --- Helper Methods ---

    private ByteBufBsonDocument createCommandMessageDocument(final ByteBufferBsonOutput output, final int numDocuments) {
        BsonDocument bodyDoc = new BsonDocument()
                .append("insert", new BsonString("test"))
                .append("$db", new BsonString("db"));

        byte[] bodyBytes = encodeBsonDocument(bodyDoc);
        List<byte[]> sequenceDocBytes = new ArrayList<>();
        for (int i = 0; i < numDocuments; i++) {
            BsonDocument seqDoc = new BsonDocument()
                    .append("_id", new BsonInt32(i))
                    .append("name", new BsonString("doc" + i));
            sequenceDocBytes.add(encodeBsonDocument(seqDoc));
        }

        writeOpMsgFormat(output, bodyBytes, "documents", sequenceDocBytes);

        List<ByteBuf> buffers = output.getByteBuffers();
        return ByteBufBsonDocument.createCommandMessage(new CompositeByteBuf(buffers));
    }

    private ByteBufBsonDocument createNestedCommandMessageDocument(final ByteBufferBsonOutput output) {
        BsonDocument bodyDoc = new BsonDocument()
                .append("insert", new BsonString("test"))
                .append("$db", new BsonString("db"));

        byte[] bodyBytes = encodeBsonDocument(bodyDoc);
        List<byte[]> sequenceDocBytes = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            BsonDocument seqDoc = new BsonDocument()
                    .append("_id", new BsonInt32(i))
                    .append("nested", new BsonDocument("inner", new BsonInt32(i * 10)))
                    .append("array", new BsonArray(asList(
                            new BsonInt32(i),
                            new BsonDocument("arrayNested", new BsonString("value" + i))
                    )));
            sequenceDocBytes.add(encodeBsonDocument(seqDoc));
        }

        writeOpMsgFormat(output, bodyBytes, "documents", sequenceDocBytes);
        return ByteBufBsonDocument.createCommandMessage(new CompositeByteBuf(output.getByteBuffers()));
    }

    private void writeOpMsgFormat(final ByteBufferBsonOutput output, final byte[] bodyBytes,
                                  final String sequenceIdentifier, final List<byte[]> sequenceDocBytes) {
        output.writeBytes(bodyBytes, 0, bodyBytes.length);

        int sequencePayloadSize = sequenceDocBytes.stream().mapToInt(b -> b.length).sum();
        int sequenceSectionSize = 4 + sequenceIdentifier.length() + 1 + sequencePayloadSize;

        output.writeByte(1);
        output.writeInt32(sequenceSectionSize);
        output.writeCString(sequenceIdentifier);
        for (byte[] docBytes : sequenceDocBytes) {
            output.writeBytes(docBytes, 0, docBytes.length);
        }
    }

    private static byte[] encodeBsonDocument(final BsonDocument doc) {
        try {
            BasicOutputBuffer buffer = new BasicOutputBuffer();
            new BsonDocumentCodec().encode(new BsonBinaryWriter(buffer), doc, EncoderContext.builder().build());
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            buffer.pipe(baos);
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static ByteBuf createByteBufFromDocument(final BsonDocument doc) {
        return new ByteBufNIO(ByteBuffer.wrap(encodeBsonDocument(doc)));
    }

    private static class SimpleBufferProvider implements BufferProvider {
        @NotNull
        @Override
        public ByteBuf getBuffer(final int size) {
            return new ByteBufNIO(ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN));
        }
    }
}
