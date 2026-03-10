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
import org.bson.BsonBinary;
import org.bson.BsonBinaryWriter;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDbPointer;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonJavaScript;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonSymbol;
import org.bson.BsonTimestamp;
import org.bson.BsonUndefined;
import org.bson.BsonValue;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.bson.BsonBoolean.FALSE;
import static org.bson.BsonBoolean.TRUE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("ByteBufBsonArray")
class ByteBufBsonArrayTest {

    // Basic Operations

    @Test
    @DisplayName("getValues() returns array values")
    void testGetValues() {
        List<BsonInt32> values = asList(new BsonInt32(0), new BsonInt32(1), new BsonInt32(2));
        try (ByteBufBsonArray bsonArray = fromBsonValues(values)) {
            assertEquals(values, bsonArray.getValues());
        }
    }

    @Test
    @DisplayName("size() returns correct count")
    void testSize() {
        try (ByteBufBsonArray bsonArray = fromBsonValues(emptyList())) {
            assertEquals(0, bsonArray.size());
        }
        try (ByteBufBsonArray bsonArray = fromBsonValues(singletonList(TRUE))) {
            assertEquals(1, bsonArray.size());
        }
        try (ByteBufBsonArray bsonArray = fromBsonValues(asList(TRUE, TRUE))) {
            assertEquals(2, bsonArray.size());
        }
    }

    @Test
    @DisplayName("isEmpty() returns correct result")
    void testIsEmpty() {
        try (ByteBufBsonArray bsonArray = fromBsonValues(emptyList())) {
            assertTrue(bsonArray.isEmpty());
        }
        try (ByteBufBsonArray bsonArray = fromBsonValues(singletonList(TRUE))) {
            assertFalse(bsonArray.isEmpty());
        }
        try (ByteBufBsonArray bsonArray = fromBsonValues(asList(TRUE, TRUE))) {
            assertFalse(bsonArray.isEmpty());
        }
    }

    @Test
    @DisplayName("contains() finds existing values and rejects missing values")
    void testContains() {
        try (ByteBufBsonArray bsonArray = fromBsonValues(emptyList())) {
            assertFalse(bsonArray.contains(TRUE));
        }
        try (ByteBufBsonArray bsonArray = fromBsonValues(singletonList(TRUE))) {
            assertTrue(bsonArray.contains(TRUE));
        }
        try (ByteBufBsonArray bsonArray = fromBsonValues(asList(FALSE, TRUE))) {
            assertTrue(bsonArray.contains(TRUE));
        }
        try (ByteBufBsonArray bsonArray = fromBsonValues(singletonList(FALSE))) {
            assertFalse(bsonArray.contains(TRUE));
        }
        try (ByteBufBsonArray bsonArray = fromBsonValues(asList(FALSE, FALSE))) {
            assertFalse(bsonArray.contains(TRUE));
        }
    }

    @Test
    @DisplayName("iterator() navigates through all elements")
    void testIterator() {
        try (ByteBufBsonArray bsonArray = fromBsonValues(emptyList())) {
            Iterator<BsonValue> iterator = bsonArray.iterator();
            assertFalse(iterator.hasNext());
            assertThrows(NoSuchElementException.class, iterator::next);
        }

        try (ByteBufBsonArray bsonArray = fromBsonValues(singletonList(TRUE))) {
            Iterator<BsonValue> iterator = bsonArray.iterator();
            assertTrue(iterator.hasNext());
            assertEquals(TRUE, iterator.next());
            assertFalse(iterator.hasNext());
            assertThrows(NoSuchElementException.class, iterator::next);
        }

        try (ByteBufBsonArray bsonArray = fromBsonValues(asList(TRUE, FALSE))) {
            Iterator<BsonValue> iterator = bsonArray.iterator();
            assertTrue(iterator.hasNext());
            assertEquals(TRUE, iterator.next());
            assertTrue(iterator.hasNext());
            assertEquals(FALSE, iterator.next());
            assertFalse(iterator.hasNext());
            assertThrows(NoSuchElementException.class, iterator::next);
        }
    }

    @Test
    @DisplayName("Iterators ensure the resource is still open")
    void iteratorsEnsureResourceIsStillOpen() {
        ByteBufBsonArray bsonArray = fromBsonValues(singletonList(TRUE));
        Iterator<BsonValue> arrayIterator = bsonArray.iterator();

        assertDoesNotThrow(arrayIterator::hasNext);

        bsonArray.close();
        assertThrows(IllegalStateException.class, arrayIterator::hasNext);
    }

    @Test
    @DisplayName("toArray() converts array to Object array")
    void testToArray() {
        try (ByteBufBsonArray bsonArray = fromBsonValues(asList(TRUE, FALSE))) {
            assertArrayEquals(new BsonValue[]{TRUE, FALSE}, bsonArray.toArray());
        }
        try (ByteBufBsonArray bsonArray = fromBsonValues(asList(TRUE, FALSE))) {
            assertArrayEquals(new BsonValue[]{TRUE, FALSE}, bsonArray.toArray(new BsonValue[0]));
        }
    }

    @Test
    @DisplayName("containsAll() checks if all elements are present")
    void testContainsAll() {
        try (ByteBufBsonArray bsonArray = fromBsonValues(asList(TRUE, FALSE))) {
            assertTrue(bsonArray.containsAll(asList(TRUE, FALSE)));
        }
        try (ByteBufBsonArray bsonArray = fromBsonValues(asList(TRUE, TRUE))) {
            assertFalse(bsonArray.containsAll(asList(TRUE, FALSE)));
        }
    }

    @Test
    @DisplayName("get() retrieves element at index and throws for out of bounds")
    void testGet() {
        try (ByteBufBsonArray bsonArray = fromBsonValues(asList(TRUE, FALSE))) {
            assertEquals(TRUE, bsonArray.get(0));
            assertEquals(FALSE, bsonArray.get(1));
            assertThrows(IndexOutOfBoundsException.class, () -> bsonArray.get(-1));
            assertThrows(IndexOutOfBoundsException.class, () -> bsonArray.get(2));
        }
    }

    @Test
    @DisplayName("indexOf() finds element position or returns -1")
    void testIndexOf() {
        try (ByteBufBsonArray bsonArray = fromBsonValues(asList(TRUE, FALSE))) {
            assertEquals(0, bsonArray.indexOf(TRUE));
            assertEquals(1, bsonArray.indexOf(FALSE));
            assertEquals(-1, bsonArray.indexOf(BsonNull.VALUE));
        }
    }

    @Test
    @DisplayName("lastIndexOf() finds last element position or returns -1")
    void testLastIndexOf() {
        try (ByteBufBsonArray bsonArray = fromBsonValues(asList(TRUE, FALSE, TRUE, FALSE))) {
            assertEquals(2, bsonArray.lastIndexOf(TRUE));
            assertEquals(3, bsonArray.lastIndexOf(FALSE));
            assertEquals(-1, bsonArray.lastIndexOf(BsonNull.VALUE));
        }
    }

    @Test
    @DisplayName("listIterator() supports bidirectional iteration")
    void testListIterator() {
        // implementation is delegated to ArrayList, so not much testing is needed
        try (ByteBufBsonArray bsonArray = fromBsonValues(emptyList())) {
            ListIterator<BsonValue> iterator = bsonArray.listIterator();
            assertFalse(iterator.hasNext());
            assertFalse(iterator.hasPrevious());
        }
    }

    @Test
    @DisplayName("subList() returns subset of array elements")
    void testSubList() {
        try (ByteBufBsonArray bsonArray = fromBsonValues(asList(new BsonInt32(0), new BsonInt32(1), new BsonInt32(2)))) {
            assertEquals(emptyList(), bsonArray.subList(0, 0));
            assertEquals(singletonList(new BsonInt32(0)), bsonArray.subList(0, 1));
            assertEquals(singletonList(new BsonInt32(2)), bsonArray.subList(2, 3));
            assertThrows(IndexOutOfBoundsException.class, () -> bsonArray.subList(-1, 1));
            assertThrows(IllegalArgumentException.class, () -> bsonArray.subList(3, 2));
            assertThrows(IndexOutOfBoundsException.class, () -> bsonArray.subList(2, 4));
        }
    }

    // Equality and HashCode

    @Test
    @DisplayName("equals() and hashCode() work correctly")
    void testEquals() {
        try (ByteBufBsonArray bsonArray = fromBsonValues(asList(TRUE, FALSE))) {
            assertEquals(new BsonArray(asList(TRUE, FALSE)), bsonArray);
        }
        try (ByteBufBsonArray bsonArray = fromBsonValues(asList(TRUE, FALSE))) {
            assertEquals(bsonArray, new BsonArray(asList(TRUE, FALSE)));
        }

        try (ByteBufBsonArray bsonArray = fromBsonValues(asList(FALSE, TRUE))) {
            assertNotEquals(new BsonArray(asList(TRUE, FALSE)), bsonArray);
        }
        try (ByteBufBsonArray bsonArray = fromBsonValues(asList(TRUE, FALSE))) {
            assertNotEquals(bsonArray, new BsonArray(asList(FALSE, TRUE)));
        }

        try (ByteBufBsonArray bsonArray = fromBsonValues(asList(TRUE, FALSE, TRUE))) {
            assertNotEquals(new BsonArray(asList(TRUE, FALSE)), bsonArray);
        }
        try (ByteBufBsonArray bsonArray = fromBsonValues(asList(TRUE, FALSE))) {
            assertNotEquals(bsonArray, new BsonArray(asList(TRUE, FALSE, TRUE)));
        }
        try (ByteBufBsonArray bsonArray = fromBsonValues(asList(TRUE, FALSE, TRUE))) {
            assertNotEquals(bsonArray, new BsonArray(asList(TRUE, FALSE)));
        }
    }

    @Test
    @DisplayName("hashCode() is consistent with equals()")
    void testHashCode() {
        try (ByteBufBsonArray bsonArray = fromBsonValues(asList(TRUE, FALSE))) {
            assertEquals(new BsonArray(asList(TRUE, FALSE)).hashCode(), bsonArray.hashCode());
        }
    }

    @Test
    @DisplayName("toString() returns equivalent string")
    void testToString() {
        try (ByteBufBsonArray bsonArray = fromBsonValues(asList(TRUE, FALSE))) {
            assertEquals(new BsonArray(asList(TRUE, FALSE)).toString(), bsonArray.toString());
        }
    }

    // Type Support

    @Test
    @DisplayName("All BSON types are supported")
    void testAllBsonTypes() {
        BsonValue bsonNull = new BsonNull();
        BsonValue bsonInt32 = new BsonInt32(42);
        BsonValue bsonInt64 = new BsonInt64(52L);
        BsonValue bsonDecimal128 = new BsonDecimal128(Decimal128.parse("1.0"));
        BsonValue bsonBoolean = new BsonBoolean(true);
        BsonValue bsonDateTime = new BsonDateTime(new Date().getTime());
        BsonValue bsonDouble = new BsonDouble(62.0);
        BsonValue bsonString = new BsonString("the fox ...");
        BsonValue minKey = new BsonMinKey();
        BsonValue maxKey = new BsonMaxKey();
        BsonValue javaScript = new BsonJavaScript("int i = 0;");
        BsonValue objectId = new BsonObjectId(new ObjectId());
        BsonValue scope = new BsonJavaScriptWithScope("int x = y", new BsonDocument("y", new BsonInt32(1)));
        BsonValue regularExpression = new BsonRegularExpression("^test.*regex.*xyz$", "i");
        BsonValue symbol = new BsonSymbol("ruby stuff");
        BsonValue timestamp = new BsonTimestamp(0x12345678, 5);
        BsonValue undefined = new BsonUndefined();
        BsonValue binary = new BsonBinary((byte) 80, new byte[] {5, 4, 3, 2, 1});
        BsonValue array = new BsonArray();
        BsonValue document = new BsonDocument("a", new BsonInt32(1));
        BsonValue dbPointer = new BsonDbPointer("db.coll", new ObjectId());

        try (ByteBufBsonArray bsonArray = fromBsonValues(asList(
                bsonNull, bsonInt32, bsonInt64, bsonDecimal128, bsonBoolean, bsonDateTime, bsonDouble, bsonString, minKey, maxKey,
                javaScript, objectId, scope, regularExpression, symbol, timestamp, undefined, binary, array, document, dbPointer))) {
            assertEquals(bsonNull, bsonArray.get(0));
            assertEquals(bsonInt32, bsonArray.get(1));
            assertEquals(bsonInt64, bsonArray.get(2));
            assertEquals(bsonDecimal128, bsonArray.get(3));
            assertEquals(bsonBoolean, bsonArray.get(4));
            assertEquals(bsonDateTime, bsonArray.get(5));
            assertEquals(bsonDouble, bsonArray.get(6));
            assertEquals(bsonString, bsonArray.get(7));
            assertEquals(minKey, bsonArray.get(8));
            assertEquals(maxKey, bsonArray.get(9));
            assertEquals(javaScript, bsonArray.get(10));
            assertEquals(objectId, bsonArray.get(11));
            assertEquals(scope, bsonArray.get(12));
            assertEquals(regularExpression, bsonArray.get(13));
            assertEquals(symbol, bsonArray.get(14));
            assertEquals(timestamp, bsonArray.get(15));
            assertEquals(undefined, bsonArray.get(16));
            assertEquals(binary, bsonArray.get(17));
            assertEquals(array, bsonArray.get(18));
            assertEquals(document, bsonArray.get(19));
            assertEquals(dbPointer, bsonArray.get(20));
        }
    }

    static ByteBufBsonArray fromBsonValues(final List<? extends BsonValue> values) {
        BsonDocument document = new BsonDocument("a", new BsonArray(values));
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        new BsonDocumentCodec().encode(new BsonBinaryWriter(buffer), document, EncoderContext.builder().build());
        byte[] bytes = new byte[buffer.getPosition()];
        System.arraycopy(buffer.getInternalBuffer(), 0, bytes, 0, bytes.length);
        // Skip past the outer document header to the array value bytes.
        // Document format: [4-byte size][type byte (0x04)][field name "a\0"][array bytes...][0x00]
        int arrayOffset = 4 + 1 + 2; // doc size + type byte + "a" + null terminator
        int arraySize = (bytes[arrayOffset] & 0xFF)
                | ((bytes[arrayOffset + 1] & 0xFF) << 8)
                | ((bytes[arrayOffset + 2] & 0xFF) << 16)
                | ((bytes[arrayOffset + 3] & 0xFF) << 24);
        ByteBuf arrayByteBuf = new ByteBufNIO(ByteBuffer.wrap(bytes, arrayOffset, arraySize));
        return new ByteBufBsonArray(arrayByteBuf);
    }
}
