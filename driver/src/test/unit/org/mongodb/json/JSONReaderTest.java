/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.json;

import org.bson.BSONBinarySubType;
import org.bson.BSONReader;
import org.bson.BSONType;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.DBPointer;
import org.bson.types.ObjectId;
import org.bson.types.RegularExpression;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


public class JSONReaderTest {
    private BSONReader bsonReader;

    @Test
    public void testArrayEmpty() {
        String json = "[]";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.ARRAY, bsonReader.readBSONType());
        bsonReader.readStartArray();
        assertEquals(BSONType.END_OF_DOCUMENT, bsonReader.readBSONType());
        bsonReader.readEndArray();
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testArrayOneElement() {
        String json = "[1]";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.ARRAY, bsonReader.readBSONType());
        bsonReader.readStartArray();
        assertEquals(BSONType.INT32, bsonReader.readBSONType());
        assertEquals(1, bsonReader.readInt32());
        assertEquals(BSONType.END_OF_DOCUMENT, bsonReader.readBSONType());
        bsonReader.readEndArray();
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testArrayTwoElements() {
        String json = "[1, 2]";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.ARRAY, bsonReader.readBSONType());
        bsonReader.readStartArray();
        assertEquals(BSONType.INT32, bsonReader.readBSONType());
        assertEquals(1, bsonReader.readInt32());
        assertEquals(BSONType.INT32, bsonReader.readBSONType());
        assertEquals(2, bsonReader.readInt32());
        assertEquals(BSONType.END_OF_DOCUMENT, bsonReader.readBSONType());
        bsonReader.readEndArray();
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testBooleanFalse() {
        String json = "false";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.BOOLEAN, bsonReader.readBSONType());
        assertEquals(false, bsonReader.readBoolean());
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testBooleanTrue() {
        String json = "true";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.BOOLEAN, bsonReader.readBSONType());
        assertEquals(true, bsonReader.readBoolean());
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDateTimeMinBson() {
        String json = "new Date(-9223372036854775808)";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.DATE_TIME, bsonReader.readBSONType());
        assertEquals(-9223372036854775808L, bsonReader.readDateTime());
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDateTimeMaxBson() {
        String json = "new Date(9223372036854775807)";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.DATE_TIME, bsonReader.readBSONType());
        long k = bsonReader.readDateTime();
        assertEquals(9223372036854775807L, k);
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDateTimeShell() {
        String json = "ISODate(\"1970-01-01T00:00:00Z\")";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.DATE_TIME, bsonReader.readBSONType());
        assertEquals(0, bsonReader.readDateTime());
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDateTimeStrict() {
        String json = "{ \"$date\" : 0 }";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.DATE_TIME, bsonReader.readBSONType());
        assertEquals(0, bsonReader.readDateTime());
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDateTimeTengen() {
        String json = "new Date(0)";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.DATE_TIME, bsonReader.readBSONType());
        assertEquals(0, bsonReader.readDateTime());
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDocumentEmpty() {
        String json = "{ }";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.DOCUMENT, bsonReader.readBSONType());
        bsonReader.readStartDocument();
        assertEquals(BSONType.END_OF_DOCUMENT, bsonReader.readBSONType());
        bsonReader.readEndDocument();
    }

    @Test
    public void testDocumentNested() {
        String json = "{ \"a\" : { \"x\" : 1 }, \"y\" : 2 }";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.DOCUMENT, bsonReader.readBSONType());
        bsonReader.readStartDocument();
        assertEquals(BSONType.DOCUMENT, bsonReader.readBSONType());
        assertEquals("a", bsonReader.readName());
        bsonReader.readStartDocument();
        assertEquals(BSONType.INT32, bsonReader.readBSONType());
        assertEquals("x", bsonReader.readName());
        assertEquals(1, bsonReader.readInt32());
        assertEquals(BSONType.END_OF_DOCUMENT, bsonReader.readBSONType());
        bsonReader.readEndDocument();
        assertEquals(BSONType.INT32, bsonReader.readBSONType());
        assertEquals("y", bsonReader.readName());
        assertEquals(2, bsonReader.readInt32());
        assertEquals(BSONType.END_OF_DOCUMENT, bsonReader.readBSONType());
        bsonReader.readEndDocument();
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDocumentOneElement() {
        String json = "{ \"x\" : 1 }";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.DOCUMENT, bsonReader.readBSONType());
        bsonReader.readStartDocument();
        assertEquals(BSONType.INT32, bsonReader.readBSONType());
        assertEquals("x", bsonReader.readName());
        assertEquals(1, bsonReader.readInt32());
        assertEquals(BSONType.END_OF_DOCUMENT, bsonReader.readBSONType());
        bsonReader.readEndDocument();
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDocumentTwoElements() {
        String json = "{ \"x\" : 1, \"y\" : 2 }";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.DOCUMENT, bsonReader.readBSONType());
        bsonReader.readStartDocument();
        assertEquals(BSONType.INT32, bsonReader.readBSONType());
        assertEquals("x", bsonReader.readName());
        assertEquals(1, bsonReader.readInt32());
        assertEquals(BSONType.INT32, bsonReader.readBSONType());
        assertEquals("y", bsonReader.readName());
        assertEquals(2, bsonReader.readInt32());
        assertEquals(BSONType.END_OF_DOCUMENT, bsonReader.readBSONType());
        bsonReader.readEndDocument();
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDouble() {
        String json = "1.5";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.DOUBLE, bsonReader.readBSONType());
        assertEquals(1.5, bsonReader.readDouble(), 0);
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testHexData() {
        byte[] expectedBytes = new byte[]{0x01, 0x23};
        String json = "HexData(0, \"0123\")";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.BINARY, bsonReader.readBSONType());
        Binary binary = bsonReader.readBinaryData();
        byte[] bytes = binary.getData();
        assertArrayEquals(expectedBytes, binary.getData());
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testHexDataWithNew() {
        byte[] expectedBytes = new byte[]{0x01, 0x23};
        String json = "new HexData(0, \"0123\")";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.BINARY, bsonReader.readBSONType());
        Binary binary = bsonReader.readBinaryData();
        byte[] bytes = binary.getData();
        assertArrayEquals(expectedBytes, binary.getData());
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testInt32() {
        String json = "123";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.INT32, bsonReader.readBSONType());
        assertEquals(123, bsonReader.readInt32());
        assertEquals(BSONReader.State.DONE, bsonReader.getState());

    }

    @Test
    public void testInt64() {
        String json = String.valueOf(Long.MAX_VALUE);
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.INT64, bsonReader.readBSONType());
        assertEquals(Long.MAX_VALUE, bsonReader.readInt64());
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testNumberLong() {
        String json = "NumberLong(123)";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.INT64, bsonReader.readBSONType());
        assertEquals(123, bsonReader.readInt64());
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testNumberLongWithNew() {
        String json = "new NumberLong(123)";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.INT64, bsonReader.readBSONType());
        assertEquals(123, bsonReader.readInt64());
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testJavaScript() {
        String json = "{ \"$code\" : \"function f() { return 1; }\" }";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.JAVASCRIPT, bsonReader.readBSONType());
        assertEquals("function f() { return 1; }", bsonReader.readJavaScript());
        assertEquals(BSONReader.State.DONE, bsonReader.getState());

    }

    @Test
    public void testJavaScriptWithScope() {
        String json = "{ \"$code\" : \"function f() { return n; }\", \"$scope\" : { \"n\" : 1 } }";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.JAVASCRIPT_WITH_SCOPE, bsonReader.readBSONType());
        assertEquals("function f() { return n; }", bsonReader.readJavaScriptWithScope());
        bsonReader.readStartDocument();
        assertEquals(BSONType.INT32, bsonReader.readBSONType());
        assertEquals("n", bsonReader.readName());
        assertEquals(1, bsonReader.readInt32());
        bsonReader.readEndDocument();
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testMaxKey() {
        String json = "{ \"$maxkey\" : 1 }";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.MAX_KEY, bsonReader.readBSONType());
        bsonReader.readMaxKey();
        assertEquals(BSONReader.State.DONE, bsonReader.getState());

    }

    @Test
    public void testMinKey() {
        String json = "{ \"$minkey\" : 1 }";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.MIN_KEY, bsonReader.readBSONType());
        bsonReader.readMinKey();
        assertEquals(BSONReader.State.DONE, bsonReader.getState());

    }

    @Test
    public void testNestedArray() {
        String json = "{ \"a\" : [1, 2] }";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.DOCUMENT, bsonReader.readBSONType());
        bsonReader.readStartDocument();
        assertEquals(BSONType.ARRAY, bsonReader.readBSONType());
        assertEquals("a", bsonReader.readName());
        bsonReader.readStartArray();
        assertEquals(1, bsonReader.readInt32());
        assertEquals(2, bsonReader.readInt32());
        bsonReader.readEndArray();
        bsonReader.readEndDocument();
        assertEquals(BSONReader.State.DONE, bsonReader.getState());

    }

    @Test
    public void testNestedDocument() {
        String json = "{ \"a\" : { \"b\" : 1, \"c\" : 2 } }";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.DOCUMENT, bsonReader.readBSONType());
        bsonReader.readStartDocument();
        assertEquals(BSONType.DOCUMENT, bsonReader.readBSONType());
        assertEquals("a", bsonReader.readName());
        bsonReader.readStartDocument();
        assertEquals("b", bsonReader.readName());
        assertEquals(1, bsonReader.readInt32());
        assertEquals("c", bsonReader.readName());
        assertEquals(2, bsonReader.readInt32());
        bsonReader.readEndDocument();
        bsonReader.readEndDocument();
        assertEquals(BSONReader.State.DONE, bsonReader.getState());

    }

    @Test
    public void testNull() {
        String json = "null";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.NULL, bsonReader.readBSONType());
        bsonReader.readNull();
        assertEquals(BSONReader.State.DONE, bsonReader.getState());

    }

    @Test
    public void testObjectIdShell() {
        String json = "ObjectId(\"4d0ce088e447ad08b4721a37\")";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.OBJECT_ID, bsonReader.readBSONType());
        ObjectId objectId = bsonReader.readObjectId();
        assertEquals("4d0ce088e447ad08b4721a37", objectId.toString());
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testObjectIdWithNew() {
        String json = "new ObjectId(\"4d0ce088e447ad08b4721a37\")";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.OBJECT_ID, bsonReader.readBSONType());
        ObjectId objectId = bsonReader.readObjectId();
        assertEquals("4d0ce088e447ad08b4721a37", objectId.toString());
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testObjectIdStrict() {
        String json = "{ \"$oid\" : \"4d0ce088e447ad08b4721a37\" }";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.OBJECT_ID, bsonReader.readBSONType());
        ObjectId objectId = bsonReader.readObjectId();
        assertEquals("4d0ce088e447ad08b4721a37", objectId.toString());
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testObjectIdTenGen() {
        String json = "ObjectId(\"4d0ce088e447ad08b4721a37\")";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.OBJECT_ID, bsonReader.readBSONType());
        ObjectId objectId = bsonReader.readObjectId();
        assertEquals("4d0ce088e447ad08b4721a37", objectId.toString());
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testRegularExpressionShell() {
        String json = "/pattern/imxs";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.REGULAR_EXPRESSION, bsonReader.readBSONType());
        RegularExpression regex = bsonReader.readRegularExpression();
        assertEquals("pattern", regex.getPattern());
        assertEquals("imxs", regex.getOptions());
        assertEquals(BSONReader.State.DONE, bsonReader.getState());

    }

    @Test
    public void testRegularExpressionStrict() {
        String json = "{ \"$regex\" : \"pattern\", \"$options\" : \"imxs\" }";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.REGULAR_EXPRESSION, bsonReader.readBSONType());
        RegularExpression regex = bsonReader.readRegularExpression();
        assertEquals("pattern", regex.getPattern());
        assertEquals("imxs", regex.getOptions());
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
        JSONWriterSettings settings = new JSONWriterSettings(JSONMode.STRICT);

    }

    @Test
    public void testString() {
        String json = "\"abc\"";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.STRING, bsonReader.readBSONType());
        assertEquals("abc", bsonReader.readString());
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testStringEmpty() {
        String json = "\"\"";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.STRING, bsonReader.readBSONType());
        assertEquals("", bsonReader.readString());
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testSymbol() {
        String json = "{ \"$symbol\" : \"symbol\" }";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.SYMBOL, bsonReader.readBSONType());
        assertEquals("symbol", bsonReader.readSymbol());
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testTimestamp() {
        String json = "{ \"$timestamp\" : NumberLong(1234) }";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.TIMESTAMP, bsonReader.readBSONType());
        assertEquals(new BSONTimestamp(1234, 1), bsonReader.readTimestamp());
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testUndefined() {
        String json = "undefined";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.UNDEFINED, bsonReader.readBSONType());
        bsonReader.readUndefined();
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }


    @Test(expected = IllegalStateException.class)
    public void testClosedState() {
        bsonReader = new JSONReader("");
        bsonReader.close();
        bsonReader.readBinaryData();
    }

    //TODO Together with next text this is just an indicator that our behavior is not very correct.
    @Test(expected = JSONParseException.class)
    public void testEndOfFile0() {
        String json = "{";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.DOCUMENT, bsonReader.readBSONType());
        bsonReader.readStartDocument();
        bsonReader.readBSONType();
    }

    @Test
    public void testEndOfFile1() {
        String json = "{ test : ";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.DOCUMENT, bsonReader.readBSONType());
        bsonReader.readStartDocument();
        assertEquals(BSONType.END_OF_DOCUMENT, bsonReader.readBSONType());
    }

    @Test
    public void testBinary() {
        String json = "{ \"$binary\" : \"AQID\", \"$type\" : \"0\" }";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.BINARY, bsonReader.readBSONType());
        Binary binary = bsonReader.readBinaryData();
        assertEquals(0, binary.getType());
        assertArrayEquals(new byte[]{1, 2, 3}, binary.getData());
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testUserDefinedBinary() {
        String json = "{ \"$binary\" : \"AQID\", \"$type\" : \"80\" }";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.BINARY, bsonReader.readBSONType());
        Binary binary = bsonReader.readBinaryData();
        assertEquals(BSONBinarySubType.USER_DEFINED.getValue(), binary.getType());
        assertArrayEquals(new byte[]{1, 2, 3}, binary.getData());
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testInfinity() {
        String json = "{ \"value\" : Infinity }";
        bsonReader = new JSONReader(json);
        bsonReader.readStartDocument();
        assertEquals(BSONType.DOUBLE, bsonReader.readBSONType());
        bsonReader.readName();
        assertEquals(Double.POSITIVE_INFINITY, bsonReader.readDouble(), 0.0001);
    }

    @Test
    public void testNaN() {
        String json = "{ \"value\" : NaN }";
        bsonReader = new JSONReader(json);
        bsonReader.readStartDocument();
        assertEquals(BSONType.DOUBLE, bsonReader.readBSONType());
        bsonReader.readName();
        assertEquals(Double.NaN, bsonReader.readDouble(), 0.0001);
    }

    @Test
    public void testBinData() {
        String json = "{ \"a\" : BinData(3, AQID) }";
        bsonReader = new JSONReader(json);
        bsonReader.readStartDocument();
        assertEquals(BSONType.BINARY, bsonReader.readBSONType());
        Binary binary = bsonReader.readBinaryData();
        assertEquals(3, binary.getType());
        assertArrayEquals(new byte[]{1, 2, 3}, binary.getData());
        bsonReader.readEndDocument();
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testBinDataUserDefined() {
        String json = "{ \"a\" : BinData(128, AQID) }";
        bsonReader = new JSONReader(json);
        bsonReader.readStartDocument();
        assertEquals(BSONType.BINARY, bsonReader.readBSONType());
        Binary binary = bsonReader.readBinaryData();
        assertEquals(BSONBinarySubType.USER_DEFINED.getValue(), binary.getType());
        assertArrayEquals(new byte[]{1, 2, 3}, binary.getData());
        bsonReader.readEndDocument();
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testBinDataWithNew() {
        String json = "{ \"a\" : new BinData(3, AQID) }";
        bsonReader = new JSONReader(json);
        bsonReader.readStartDocument();
        assertEquals(BSONType.BINARY, bsonReader.readBSONType());
        Binary binary = bsonReader.readBinaryData();
        assertEquals(3, binary.getType());
        assertArrayEquals(new byte[]{1, 2, 3}, binary.getData());
        bsonReader.readEndDocument();
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDateWithNumbers() {
        String json = "new Date(1988, 06, 13 , 22 , 1)";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.DATE_TIME, bsonReader.readBSONType());
        assertEquals(584834460000L, bsonReader.readDateTime());
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDateTimeConstructorWithNew() {
        String json = "new Date(\"Sat Jul 13 2013 11:10:05 UTC\")";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.DATE_TIME, bsonReader.readBSONType());
        assertEquals(1373713805000L, bsonReader.readDateTime());
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testEmptyDateTimeConstructorWithNew() {
        String json = "new Date()";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.DATE_TIME, bsonReader.readBSONType());
        bsonReader.readDateTime();
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testEmptyDateTimeConstructor() {
        String json = "Date()";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.DATE_TIME, bsonReader.readBSONType());
        bsonReader.readDateTime();
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }


    @Test
    public void testRegExp() {
        String json = "RegExp(\"abc\",\"im\")";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.REGULAR_EXPRESSION, bsonReader.readBSONType());
        RegularExpression regularExpression = bsonReader.readRegularExpression();
        assertEquals("abc", regularExpression.getPattern());
        assertEquals("im", regularExpression.getOptions());
    }

    @Test
    public void testRegExpWithNew() {
        String json = "new RegExp(\"abc\",\"im\")";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.REGULAR_EXPRESSION, bsonReader.readBSONType());
        RegularExpression regularExpression = bsonReader.readRegularExpression();
        assertEquals("abc", regularExpression.getPattern());
        assertEquals("im", regularExpression.getOptions());
    }

    @Test
    public void testSkip() {
        String json = "{ \"a\" : 2 }";
        bsonReader = new JSONReader(json);
        bsonReader.readStartDocument();
        bsonReader.readBSONType();
        bsonReader.skipName();
        bsonReader.skipValue();
        assertEquals(BSONType.END_OF_DOCUMENT, bsonReader.readBSONType());
        bsonReader.readEndDocument();
        assertEquals(BSONReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDBPointer() {
        String json = "DBPointer(\"b\",\"5209296cd6c4e38cf96fffdc\")";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.DB_POINTER, bsonReader.readBSONType());
        DBPointer dbPointer = bsonReader.readDBPointer();
        assertEquals("b", dbPointer.getNamespace());
        assertEquals(new ObjectId("5209296cd6c4e38cf96fffdc"), dbPointer.getId());
    }

    @Test
    public void testDBPointerWithNew() {
        String json = "new DBPointer(\"b\",\"5209296cd6c4e38cf96fffdc\")";
        bsonReader = new JSONReader(json);
        assertEquals(BSONType.DB_POINTER, bsonReader.readBSONType());
        DBPointer dbPointer = bsonReader.readDBPointer();
        assertEquals("b", dbPointer.getNamespace());
        assertEquals(new ObjectId("5209296cd6c4e38cf96fffdc"), dbPointer.getId());
    }
}
