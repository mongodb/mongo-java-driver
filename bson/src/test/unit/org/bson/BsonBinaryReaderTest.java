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

import org.bson.io.ByteBufferBsonInput;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public class BsonBinaryReaderTest {

    @Test
    public void testReadDBPointer() {
        BsonBinaryReader reader = createReaderForBytes(new byte[]{26, 0, 0, 0, 12, 97, 0, 2, 0, 0, 0, 98, 0, 82, 9, 41, 108,
                                                                  -42, -60, -29, -116, -7, 111, -1, -36, 0});

        reader.readStartDocument();
        assertThat(reader.readBsonType(), is(BsonType.DB_POINTER));
        BsonDbPointer dbPointer = reader.readDBPointer();
        assertThat(dbPointer.getNamespace(), is("b"));
        assertThat(dbPointer.getId(), is(new ObjectId("5209296cd6c4e38cf96fffdc")));
        reader.readEndDocument();
        reader.close();
    }

    @Test
    public void testInvalidBsonType() {
        BsonBinaryReader reader = createReaderForBytes(new byte[]{26, 0, 0, 0, 22, 97, 0, 2, 0, 0, 0, 98, 0, 82, 9, 41, 108,
                -42, -60, -29, -116, -7, 111, -1, -36, 0});

        reader.readStartDocument();
        try {
            reader.readBsonType();
            fail("Should have thrown BsonSerializationException");
        } catch (BsonSerializationException e) {
            assertEquals("Detected unknown BSON type \"\\x16\" for fieldname \"a\". Are you using the latest driver version?", e.getMessage());
        }
    }

    @Test
    public void testInvalidBsonTypeFollowedByInvalidCString() {
        BsonBinaryReader reader = createReaderForBytes(new byte[]{26, 0, 0, 0, 22, 97, 98});

        reader.readStartDocument();
        try {
            reader.readBsonType();
            fail("Should have thrown BsonSerializationException");
        } catch (BsonSerializationException e) {
            assertEquals("Found a BSON string that is not null-terminated", e.getMessage());
        }
    }

    // String rejection tests - skipValue() rejects size=0

    @Test
    public void testSkipValueRejectsZeroLengthString() {
        // {"a": <string with declared size 0>}
        // doc size=12: 4(size) + 1(type 0x02) + 2(name "a\0") + 4(string size=0) + 1(doc term)
        assertSkipValueThrows(new byte[]{
                12, 0, 0, 0,    // document size
                typeByte(BsonType.STRING),
                0x61, 0x00,     // name: "a\0"
                0, 0, 0, 0,     // string size: 0 (invalid)
                0x00            // document terminator
        });
    }

    @Test
    public void testSkipValueRejectsZeroLengthSymbol() {
        // {"a": <symbol with declared size 0>}
        // doc size=12: 4(size) + 1(type 0x0E) + 2(name "a\0") + 4(symbol size=0) + 1(doc term)
        assertSkipValueThrows(new byte[]{
                12, 0, 0, 0,    // document size
                typeByte(BsonType.SYMBOL),
                0x61, 0x00,     // name: "a\0"
                0, 0, 0, 0,     // symbol size: 0 (invalid)
                0x00            // document terminator
        });
    }

    @Test
    public void testSkipValueRejectsZeroLengthJavaScript() {
        // {"a": <javascript with declared size 0>}
        // doc size=12: 4(size) + 1(type 0x0D) + 2(name "a\0") + 4(js size=0) + 1(doc term)
        assertSkipValueThrows(new byte[]{
                12, 0, 0, 0,    // document size
                typeByte(BsonType.JAVASCRIPT),
                0x61, 0x00,     // name: "a\0"
                0, 0, 0, 0,     // javascript size: 0 (invalid)
                0x00            // document terminator
        });
    }

    @Test
    public void testSkipValueRejectsZeroLengthDbPointer() {
        // {"a": <dbpointer with declared string size 0 + 12 bytes objectid>}
        // doc size=24: 4(size) + 1(type 0x0C) + 2(name "a\0") + 4(string size=0) + 12(objectid) + 1(doc term)
        assertSkipValueThrows(new byte[]{
                24, 0, 0, 0,    // document size
                typeByte(BsonType.DB_POINTER),
                0x61, 0x00,     // name: "a\0"
                0, 0, 0, 0,     // string size: 0 (invalid)
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // objectid (12 bytes)
                0x00            // document terminator
        });
    }

    @Test
    public void testSkipValueRejectsOverflowingBinarySize() {
        // {"a": <binary with size=Integer.MAX_VALUE, which overflows when adding the subtype byte>}
        assertSkipValueThrows(new byte[]{
                12, 0, 0, 0,    // document size
                typeByte(BsonType.BINARY),
                0x61, 0x00,     // name: "a\0"
                -1, -1, -1, 0x7F, // binary size: Integer.MAX_VALUE
                0x00            // document terminator
        });
    }

    @Test
    public void testSkipValueRejectsOverflowingDbPointerSize() {
        // {"a": <dbpointer with string size=Integer.MAX_VALUE, which overflows when adding ObjectId bytes>}
        assertSkipValueThrows(new byte[]{
                12, 0, 0, 0,    // document size
                typeByte(BsonType.DB_POINTER),
                0x61, 0x00,     // name: "a\0"
                -1, -1, -1, 0x7F, // string size: Integer.MAX_VALUE
                0x00            // document terminator
        });
    }

    // Container rejection tests - skipValue() rejects undersized containers

    @Test
    public void testSkipValueRejectsUndersizedDocument() {
        // {"a": <embedded document with declared size 3, below minimum 5>}
        // doc size=12: 4(size) + 1(type 0x03) + 2(name "a\0") + 4(embedded doc size=3) + 1(doc term)
        assertSkipValueThrows(new byte[]{
                12, 0, 0, 0,    // document size
                typeByte(BsonType.DOCUMENT),
                0x61, 0x00,     // name: "a\0"
                3, 0, 0, 0,     // embedded document size: 3 (invalid, minimum is 5)
                0x00            // document terminator
        });
    }

    @Test
    public void testSkipValueRejectsUndersizedArray() {
        // {"a": <array with declared size 0>}
        // doc size=12: 4(size) + 1(type 0x04) + 2(name "a\0") + 4(array size=0) + 1(doc term)
        assertSkipValueThrows(new byte[]{
                12, 0, 0, 0,    // document size
                typeByte(BsonType.ARRAY),
                0x61, 0x00,     // name: "a\0"
                0, 0, 0, 0,     // array size: 0 (invalid, minimum is 5)
                0x00            // document terminator
        });
    }

    @Test
    public void testSkipValueRejectsUndersizedJavaScriptWithScope() {
        // {"a": <javascript_with_scope with declared size 13, below minimum 14>}
        // doc size=24: 4(size) + 1(type 0x0F) + 2(name "a\0") + 4(js_ws size=13) + 12(filler) + 1(doc term)
        assertSkipValueThrows(new byte[]{
                24, 0, 0, 0,    // document size
                typeByte(BsonType.JAVASCRIPT_WITH_SCOPE),
                0x61, 0x00,     // name: "a\0"
                13, 0, 0, 0,    // javascript_with_scope size: 13 (invalid, minimum is 14)
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // filler
                0x00            // document terminator
        });
    }

    @Test
    public void testSkipValueRejectsJavaScriptWithScopeWithZeroLengthCode() {
        // {"a": <javascript_with_scope with valid total size but code string size 0>}
        // doc size=22: 4(size) + 1(type 0x0F) + 2(name "a\0") + 14(js_ws value) + 1(doc term)
        assertSkipValueThrows(new byte[]{
                22, 0, 0, 0,    // document size
                typeByte(BsonType.JAVASCRIPT_WITH_SCOPE),
                0x61, 0x00,     // name: "a\0"
                14, 0, 0, 0,    // javascript_with_scope total size
                0, 0, 0, 0,     // code string size: 0 (invalid)
                0, 0, 0, 0, 0, 0, // filler
                0x00            // document terminator
        });
    }

    @Test
    public void testSkipValueRejectsJavaScriptWithScopeWithOversizedCode() {
        // {"a": <javascript_with_scope where total size=15 but code string size=3 leaves no room for scope doc>}
        // totalSize=15, codeSize=3: codeSize (3) > totalSize - 13 (2), so the intermediate guard fires.
        // doc size=23: 4(size) + 1(type 0x0F) + 2(name "a\0") + 15(js_ws value) + 1(doc term)
        assertSkipValueThrows(new byte[]{
                23, 0, 0, 0,    // document size
                typeByte(BsonType.JAVASCRIPT_WITH_SCOPE),
                0x61, 0x00,     // name: "a\0"
                15, 0, 0, 0,    // javascript_with_scope total size: 15
                3, 0, 0, 0,     // code string size: 3 (invalid - too large; max is totalSize-13=2)
                0, 0, 0, 0, 0, 0, 0, 0, // filler
                0x00            // document terminator
        });
    }

    @Test
    public void testSkipValueRejectsJavaScriptWithScopeWithMismatchedTotalSize() {
        // {"a": <javascript_with_scope where every individual size passes its guard but parts don't sum to totalSize>}
        // totalSize=20, codeSize=1, scopeSize=5: parts sum to 4+4+1+5=14, which != totalSize 20.
        // Exercises the final expectedSize != size consistency check.
        // doc size=28: 4(size) + 1(type 0x0F) + 2(name "a\0") + 20(js_ws value) + 1(doc term)
        assertSkipValueThrows(new byte[]{
                28, 0, 0, 0,    // document size
                typeByte(BsonType.JAVASCRIPT_WITH_SCOPE),
                0x61, 0x00,     // name: "a\0"
                20, 0, 0, 0,    // javascript_with_scope total size: 20 (inconsistent with parts)
                1, 0, 0, 0,     // code string size: 1
                0x00,           // code string content: "\0"
                5, 0, 0, 0,     // scope document size: 5 (valid minimum)
                0x00,           // scope document terminator
                0, 0, 0, 0, 0, 0, // filler so outer doc reaches declared total
                0x00            // outer document terminator
        });
    }

    @Test
    public void testSkipValueRejectsJavaScriptWithScopeWithUndersizedScopeDocument() {
        // {"a": <javascript_with_scope with valid total size but scope document size 4>}
        // doc size=22: 4(size) + 1(type 0x0F) + 2(name "a\0") + 14(js_ws value) + 1(doc term)
        assertSkipValueThrows(new byte[]{
                22, 0, 0, 0,    // document size
                typeByte(BsonType.JAVASCRIPT_WITH_SCOPE),
                0x61, 0x00,     // name: "a\0"
                14, 0, 0, 0,    // javascript_with_scope total size
                1, 0, 0, 0,     // code string size: 1
                0x00,           // code string content: "\0"
                4, 0, 0, 0,     // scope document size: 4 (invalid, minimum is 5)
                0x00,           // filler
                0x00            // document terminator
        });
    }

    // Regression guards - valid minimal values still work

    @Test
    public void testSkipValueAcceptsMinimalString() {
        // {"a": ""} - string with size=1 (just the null terminator)
        // doc size=13: 4(size) + 1(type 0x02) + 2(name "a\0") + 4(string size=1) + 1('\0') + 1(doc term)
        assertSkipValueSucceeds(new byte[]{
                13, 0, 0, 0,    // document size
                typeByte(BsonType.STRING),
                0x61, 0x00,     // name: "a\0"
                1, 0, 0, 0,     // string size: 1 (valid minimum - just null terminator)
                0x00,           // string content: "\0"
                0x00            // document terminator
        });
    }

    @Test
    public void testSkipValueAcceptsEmptyBinary() {
        // {"a": <binary with size=0, subtype 0x00>}
        // doc size=13: 4(size) + 1(type 0x05) + 2(name "a\0") + 4(binary size=0) + 1(subtype) + 1(doc term)
        assertSkipValueSucceeds(new byte[]{
                13, 0, 0, 0,    // document size
                typeByte(BsonType.BINARY),
                0x61, 0x00,     // name: "a\0"
                0, 0, 0, 0,     // binary size: 0 (valid - empty binary)
                0x00,           // subtype: generic
                0x00            // document terminator
        });
    }

    @Test
    public void testSkipValueAcceptsMinimalDocument() {
        // {"a": {}} - embedded document with size=5 (minimum valid: int32 size + '\x00')
        // doc size=13: 4(docsize) + 1(type 0x03) + 2(name "a\0") + 5(embedded {}) + 1(doc term)
        assertSkipValueSucceeds(new byte[]{
                13, 0, 0, 0,    // document size
                typeByte(BsonType.DOCUMENT),
                0x61, 0x00,     // name: "a\0"
                5, 0, 0, 0,     // embedded document size: 5 (valid minimum)
                0x00,           // embedded document terminator
                0x00            // outer document terminator
        });
    }

    @Test
    public void testSkipValueAcceptsMinimalArray() {
        // {"a": []} - embedded array with size=5 (minimum valid: int32 size + '\x00')
        // doc size=13: 4(docsize) + 1(type 0x04) + 2(name "a\0") + 5(embedded []) + 1(doc term)
        assertSkipValueSucceeds(new byte[]{
                13, 0, 0, 0,    // document size
                typeByte(BsonType.ARRAY),
                0x61, 0x00,     // name: "a\0"
                5, 0, 0, 0,     // embedded array size: 5 (valid minimum)
                0x00,           // embedded array terminator
                0x00            // outer document terminator
        });
    }

    @Test
    public void testSkipValueAcceptsMinimalJavaScriptWithScope() {
        // {"a": <javascript_with_scope with total size=14>}
        // doc size=22: 4(size) + 1(type 0x0F) + 2(name "a\0") + 14(js_ws value) + 1(doc term)
        assertSkipValueSucceeds(new byte[]{
                22, 0, 0, 0,    // document size
                typeByte(BsonType.JAVASCRIPT_WITH_SCOPE),
                0x61, 0x00,     // name: "a\0"
                14, 0, 0, 0,    // javascript_with_scope total size
                1, 0, 0, 0,     // code string size: 1
                0x00,           // code string content: "\0"
                5, 0, 0, 0,     // scope document size: 5 (valid minimum)
                0x00,           // scope document terminator
                0x00            // outer document terminator
        });
    }

    /**
     * Wraps the given bytes in a BSON reader, advances past the document start, type, and field name,
     * and asserts that {@code skipValue()} throws {@link BsonSerializationException}.
     */
    private static void assertSkipValueThrows(final byte[] bytes) {
        try (BsonBinaryReader reader = createReaderForBytes(bytes)) {
            reader.readStartDocument();
            reader.readBsonType();
            reader.readName();
            assertThrows(BsonSerializationException.class, reader::skipValue);
        }
    }

    /**
     * Wraps the given bytes in a BSON reader, advances past the document start, type, and field name,
     * skips the value, and asserts the document terminates cleanly.
     */
    private static void assertSkipValueSucceeds(final byte[] bytes) {
        try (BsonBinaryReader reader = createReaderForBytes(bytes)) {
            reader.readStartDocument();
            reader.readBsonType();
            reader.readName();
            reader.skipValue();
            reader.readEndDocument();
        }
    }

    private static BsonBinaryReader createReaderForBytes(final byte[] bytes) {
        return new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(bytes))));
    }

    private static byte typeByte(final BsonType type) {
        return (byte) type.getValue();
    }
}
