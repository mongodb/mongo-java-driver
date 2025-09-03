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
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ByteBufBsonArrayTest {

    @Test
    void testGetValues() {
        List<BsonInt32> values = asList(new BsonInt32(0), new BsonInt32(1), new BsonInt32(2));
        ByteBufBsonArray bsonArray = fromBsonValues(values);
        assertEquals(values, bsonArray.getValues());
    }

    @Test
    void testSize() {
        assertEquals(0, fromBsonValues(emptyList()).size());
        assertEquals(1, fromBsonValues(singletonList(TRUE)).size());
        assertEquals(2, fromBsonValues(asList(TRUE, TRUE)).size());
    }

    @Test
    void testIsEmpty() {
        assertTrue(fromBsonValues(emptyList()).isEmpty());
        assertFalse(fromBsonValues(singletonList(TRUE)).isEmpty());
        assertFalse(fromBsonValues(asList(TRUE, TRUE)).isEmpty());
    }

    @Test
    void testContains() {
        assertFalse(fromBsonValues(emptyList()).contains(TRUE));
        assertTrue(fromBsonValues(singletonList(TRUE)).contains(TRUE));
        assertTrue(fromBsonValues(asList(FALSE, TRUE)).contains(TRUE));
        assertFalse(fromBsonValues(singletonList(FALSE)).contains(TRUE));
        assertFalse(fromBsonValues(asList(FALSE, FALSE)).contains(TRUE));
    }

    @Test
    void testIterator() {
        Iterator<BsonValue> iterator = fromBsonValues(emptyList()).iterator();
        assertFalse(iterator.hasNext());
        assertThrows(NoSuchElementException.class, iterator::next);

        iterator = fromBsonValues(singletonList(TRUE)).iterator();
        assertTrue(iterator.hasNext());
        assertEquals(TRUE, iterator.next());
        assertFalse(iterator.hasNext());
        assertThrows(NoSuchElementException.class, iterator::next);

        iterator = fromBsonValues(asList(TRUE, FALSE)).iterator();
        assertTrue(iterator.hasNext());
        assertEquals(TRUE, iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(FALSE, iterator.next());
        assertFalse(iterator.hasNext());
        assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Test
    void testToArray() {
        assertArrayEquals(new BsonValue[]{TRUE, FALSE}, fromBsonValues(asList(TRUE, FALSE)).toArray());
        assertArrayEquals(new BsonValue[]{TRUE, FALSE}, fromBsonValues(asList(TRUE, FALSE)).toArray(new BsonValue[0]));
    }

    @Test
    void testContainsAll() {
        assertTrue(fromBsonValues(asList(TRUE, FALSE)).containsAll(asList(TRUE, FALSE)));
        assertFalse(fromBsonValues(asList(TRUE, TRUE)).containsAll(asList(TRUE, FALSE)));
    }

    @Test
    void testGet() {
        ByteBufBsonArray bsonArray = fromBsonValues(asList(TRUE, FALSE));
        assertEquals(TRUE, bsonArray.get(0));
        assertEquals(FALSE, bsonArray.get(1));
        assertThrows(IndexOutOfBoundsException.class, () -> bsonArray.get(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> bsonArray.get(2));
    }

    @Test
    void testIndexOf() {
        ByteBufBsonArray bsonArray = fromBsonValues(asList(TRUE, FALSE));
        assertEquals(0, bsonArray.indexOf(TRUE));
        assertEquals(1, bsonArray.indexOf(FALSE));
        assertEquals(-1, bsonArray.indexOf(BsonNull.VALUE));
    }

    @Test
    void testLastIndexOf() {
        ByteBufBsonArray bsonArray = fromBsonValues(asList(TRUE, FALSE, TRUE, FALSE));
        assertEquals(2, bsonArray.lastIndexOf(TRUE));
        assertEquals(3, bsonArray.lastIndexOf(FALSE));
        assertEquals(-1, bsonArray.lastIndexOf(BsonNull.VALUE));
    }

    @Test
    void testListIterator() {
        // implementation is delegated to ArrayList, so not much testing is needed
        ListIterator<BsonValue> iterator = fromBsonValues(emptyList()).listIterator();
        assertFalse(iterator.hasNext());
        assertFalse(iterator.hasPrevious());
    }

    @Test
    void testSubList() {
        ByteBufBsonArray bsonArray = fromBsonValues(asList(new BsonInt32(0), new BsonInt32(1), new BsonInt32(2)));
        assertEquals(emptyList(), bsonArray.subList(0, 0));
        assertEquals(singletonList(new BsonInt32(0)), bsonArray.subList(0, 1));
        assertEquals(singletonList(new BsonInt32(2)), bsonArray.subList(2, 3));
        assertThrows(IndexOutOfBoundsException.class, () -> bsonArray.subList(-1, 1));
        assertThrows(IllegalArgumentException.class, () -> bsonArray.subList(3, 2));
        assertThrows(IndexOutOfBoundsException.class, () -> bsonArray.subList(2, 4));
    }

    @Test
    void testEquals() {
        assertEquals(new BsonArray(asList(TRUE, FALSE)), fromBsonValues(asList(TRUE, FALSE)));
        assertEquals(fromBsonValues(asList(TRUE, FALSE)), new BsonArray(asList(TRUE, FALSE)));

        assertNotEquals(new BsonArray(asList(TRUE, FALSE)), fromBsonValues(asList(FALSE, TRUE)));
        assertNotEquals(fromBsonValues(asList(TRUE, FALSE)), new BsonArray(asList(FALSE, TRUE)));

        assertNotEquals(new BsonArray(asList(TRUE, FALSE)), fromBsonValues(asList(TRUE, FALSE, TRUE)));
        assertNotEquals(fromBsonValues(asList(TRUE, FALSE)), new BsonArray(asList(TRUE, FALSE, TRUE)));
        assertNotEquals(fromBsonValues(asList(TRUE, FALSE, TRUE)), new BsonArray(asList(TRUE, FALSE)));
    }

    @Test
    void testHashCode() {
        assertEquals(new BsonArray(asList(TRUE, FALSE)).hashCode(), fromBsonValues(asList(TRUE, FALSE)).hashCode());
    }

    @Test
    void testToString() {
        assertEquals(new BsonArray(asList(TRUE, FALSE)).toString(), fromBsonValues(asList(TRUE, FALSE)).toString());
    }

    @Test
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

        ByteBufBsonArray bsonArray = fromBsonValues(asList(
                bsonNull, bsonInt32, bsonInt64, bsonDecimal128, bsonBoolean, bsonDateTime, bsonDouble, bsonString, minKey, maxKey,
                javaScript, objectId, scope, regularExpression, symbol, timestamp, undefined, binary, array, document, dbPointer));
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

    static ByteBufBsonArray fromBsonValues(final List<? extends BsonValue> values) {
        BsonDocument document = new BsonDocument()
                .append("a", new BsonArray(values));
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        new BsonDocumentCodec().encode(new BsonBinaryWriter(buffer), document, EncoderContext.builder().build());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            buffer.pipe(baos);
        } catch (IOException e) {
            throw new RuntimeException("impossible!");
        }
        ByteBuf documentByteBuf = new ByteBufNIO(ByteBuffer.wrap(baos.toByteArray()));
        return (ByteBufBsonArray) new ByteBufBsonDocument(documentByteBuf).entrySet().iterator().next().getValue();
    }
}
