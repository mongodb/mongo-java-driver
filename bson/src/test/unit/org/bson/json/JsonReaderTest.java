/*
 * Copyright (c) 2008-2016 MongoDB, Inc.
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

package org.bson.json;

import org.bson.AbstractBsonReader;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonDbPointer;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.BsonType;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class JsonReaderTest {
    private AbstractBsonReader bsonReader;

    @Test
    public void testArrayEmpty() {
        String json = "[]";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.ARRAY, bsonReader.readBsonType());
        bsonReader.readStartArray();
        assertEquals(BsonType.END_OF_DOCUMENT, bsonReader.readBsonType());
        bsonReader.readEndArray();
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testArrayOneElement() {
        String json = "[1]";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.ARRAY, bsonReader.readBsonType());
        bsonReader.readStartArray();
        assertEquals(BsonType.INT32, bsonReader.readBsonType());
        assertEquals(1, bsonReader.readInt32());
        assertEquals(BsonType.END_OF_DOCUMENT, bsonReader.readBsonType());
        bsonReader.readEndArray();
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testArrayTwoElements() {
        String json = "[1, 2]";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.ARRAY, bsonReader.readBsonType());
        bsonReader.readStartArray();
        assertEquals(BsonType.INT32, bsonReader.readBsonType());
        assertEquals(1, bsonReader.readInt32());
        assertEquals(BsonType.INT32, bsonReader.readBsonType());
        assertEquals(2, bsonReader.readInt32());
        assertEquals(BsonType.END_OF_DOCUMENT, bsonReader.readBsonType());
        bsonReader.readEndArray();
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testBooleanFalse() {
        String json = "false";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.BOOLEAN, bsonReader.readBsonType());
        assertEquals(false, bsonReader.readBoolean());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testBooleanTrue() {
        String json = "true";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.BOOLEAN, bsonReader.readBsonType());
        assertEquals(true, bsonReader.readBoolean());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDateTimeMinBson() {
        String json = "new Date(-9223372036854775808)";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.DATE_TIME, bsonReader.readBsonType());
        assertEquals(-9223372036854775808L, bsonReader.readDateTime());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDateTimeMaxBson() {
        String json = "new Date(9223372036854775807)";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.DATE_TIME, bsonReader.readBsonType());
        long k = bsonReader.readDateTime();
        assertEquals(9223372036854775807L, k);
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDateTimeShell() {
        String json = "ISODate(\"1970-01-01T00:00:00Z\")";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.DATE_TIME, bsonReader.readBsonType());
        assertEquals(0, bsonReader.readDateTime());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDateTimeShellWith24HourTimeSpecification() {
        String json = "ISODate(\"2013-10-04T12:07:30.443Z\")";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.DATE_TIME, bsonReader.readBsonType());
        assertEquals(1380888450443L, bsonReader.readDateTime());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDateTimeStrict() {
        String json = "{ \"$date\" : 0 }";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.DATE_TIME, bsonReader.readBsonType());
        assertEquals(0, bsonReader.readDateTime());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDateTimeISOString() {
        String json = "{ \"$date\" : \"2015-04-16T14:55:57.626Z\" }";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.DATE_TIME, bsonReader.readBsonType());
        assertEquals(1429196157626L, bsonReader.readDateTime());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDateTimeISOStringWithTimeOffset() {
        String json = "{ \"$date\" : \"2015-04-16T16:55:57.626+02:00\" }";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.DATE_TIME, bsonReader.readBsonType());
        assertEquals(1429196157626L, bsonReader.readDateTime());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test(expected = JsonParseException.class)
    public void testInvalidDateTimeISOString1() {
        String json = "{ \"$date\" : \"2015-04-16T16:55:57.626+02:0000\" }";
        bsonReader = new JsonReader(json);
        bsonReader.readBsonType();
    }

    @Test(expected = JsonParseException.class)
    public void testInvalidDateTimeISOString2() {
        String json = "{ \"$date\" : \"2015-04-16T16:55:57.626Z invalid string\" }";
        bsonReader = new JsonReader(json);
        bsonReader.readBsonType();
    }

    @Test(expected = JsonParseException.class)
    public void testInvalidDateTimeValue() {
        String json = "{ \"$date\" : {} }";
        bsonReader = new JsonReader(json);
        bsonReader.readBsonType();
    }

    @Test
    public void testDateTimeTengen() {
        String json = "new Date(0)";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.DATE_TIME, bsonReader.readBsonType());
        assertEquals(0, bsonReader.readDateTime());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDocumentEmpty() {
        String json = "{ }";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.DOCUMENT, bsonReader.readBsonType());
        bsonReader.readStartDocument();
        assertEquals(BsonType.END_OF_DOCUMENT, bsonReader.readBsonType());
        bsonReader.readEndDocument();
    }

    @Test
    public void testDocumentNested() {
        String json = "{ \"a\" : { \"x\" : 1 }, \"y\" : 2 }";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.DOCUMENT, bsonReader.readBsonType());
        bsonReader.readStartDocument();
        assertEquals(BsonType.DOCUMENT, bsonReader.readBsonType());
        assertEquals("a", bsonReader.readName());
        bsonReader.readStartDocument();
        assertEquals(BsonType.INT32, bsonReader.readBsonType());
        assertEquals("x", bsonReader.readName());
        assertEquals(1, bsonReader.readInt32());
        assertEquals(BsonType.END_OF_DOCUMENT, bsonReader.readBsonType());
        bsonReader.readEndDocument();
        assertEquals(BsonType.INT32, bsonReader.readBsonType());
        assertEquals("y", bsonReader.readName());
        assertEquals(2, bsonReader.readInt32());
        assertEquals(BsonType.END_OF_DOCUMENT, bsonReader.readBsonType());
        bsonReader.readEndDocument();
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDocumentOneElement() {
        String json = "{ \"x\" : 1 }";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.DOCUMENT, bsonReader.readBsonType());
        bsonReader.readStartDocument();
        assertEquals(BsonType.INT32, bsonReader.readBsonType());
        assertEquals("x", bsonReader.readName());
        assertEquals(1, bsonReader.readInt32());
        assertEquals(BsonType.END_OF_DOCUMENT, bsonReader.readBsonType());
        bsonReader.readEndDocument();
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDocumentTwoElements() {
        String json = "{ \"x\" : 1, \"y\" : 2 }";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.DOCUMENT, bsonReader.readBsonType());
        bsonReader.readStartDocument();
        assertEquals(BsonType.INT32, bsonReader.readBsonType());
        assertEquals("x", bsonReader.readName());
        assertEquals(1, bsonReader.readInt32());
        assertEquals(BsonType.INT32, bsonReader.readBsonType());
        assertEquals("y", bsonReader.readName());
        assertEquals(2, bsonReader.readInt32());
        assertEquals(BsonType.END_OF_DOCUMENT, bsonReader.readBsonType());
        bsonReader.readEndDocument();
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDouble() {
        String json = "1.5";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.DOUBLE, bsonReader.readBsonType());
        assertEquals(1.5, bsonReader.readDouble(), 0);
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testHexData() {
        byte[] expectedBytes = new byte[]{0x01, 0x23};
        String json = "HexData(0, \"0123\")";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.BINARY, bsonReader.readBsonType());
        BsonBinary binary = bsonReader.readBinaryData();
        byte[] bytes = binary.getData();
        assertArrayEquals(expectedBytes, binary.getData());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testHexDataWithNew() {
        byte[] expectedBytes = new byte[]{0x01, 0x23};
        String json = "new HexData(0, \"0123\")";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.BINARY, bsonReader.readBsonType());
        BsonBinary binary = bsonReader.readBinaryData();
        byte[] bytes = binary.getData();
        assertArrayEquals(expectedBytes, binary.getData());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testInt32() {
        String json = "123";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.INT32, bsonReader.readBsonType());
        assertEquals(123, bsonReader.readInt32());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());

    }

    @Test
    public void testInt64() {
        String json = String.valueOf(Long.MAX_VALUE);
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.INT64, bsonReader.readBsonType());
        assertEquals(Long.MAX_VALUE, bsonReader.readInt64());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testNumberLong() {
        String json = "NumberLong(123)";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.INT64, bsonReader.readBsonType());
        assertEquals(123, bsonReader.readInt64());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testNumberLongExtendedJson() {
        String json = "{\"$numberLong\":\"123\"}";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.INT64, bsonReader.readBsonType());
        assertEquals(123, bsonReader.readInt64());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testNumberLongWithNew() {
        String json = "new NumberLong(123)";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.INT64, bsonReader.readBsonType());
        assertEquals(123, bsonReader.readInt64());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDecimal128StringConstructor() {
        String json = "NumberDecimal(\"314E-2\")";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.DECIMAL128, bsonReader.readBsonType());
        assertEquals(Decimal128.parse("314E-2"), bsonReader.readDecimal128());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDecimal128Int32Constructor() {
        String json = "NumberDecimal(" + Integer.MAX_VALUE + ")";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.DECIMAL128, bsonReader.readBsonType());
        assertEquals(new Decimal128(Integer.MAX_VALUE), bsonReader.readDecimal128());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDecimal128Int64Constructor() {
        String json = "NumberDecimal(" + Long.MAX_VALUE + ")";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.DECIMAL128, bsonReader.readBsonType());
        assertEquals(new Decimal128(Long.MAX_VALUE), bsonReader.readDecimal128());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDecimal128DoubleConstructor() {
        String json = "NumberDecimal(" + 1.0 + ")";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.DECIMAL128, bsonReader.readBsonType());
        assertEquals(Decimal128.parse("1"), bsonReader.readDecimal128());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDecimal128BooleanConstructor() {
        String json = "NumberDecimal(true)";
        bsonReader = new JsonReader(json);
        try {
            bsonReader.readBsonType();
            fail("Should fail to parse NumberDecimal constructor with a string");
        } catch (JsonParseException e) {
            // all good
        }
    }

    @Test
    public void testDecimal128WithNew() {
        String json = "new NumberDecimal(\"314E-2\")";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.DECIMAL128, bsonReader.readBsonType());
        assertEquals(Decimal128.parse("314E-2"), bsonReader.readDecimal128());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDecimal128ExtendedJson() {
        String json = "{\"$numberDecimal\":\"314E-2\"}";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.DECIMAL128, bsonReader.readBsonType());
        assertEquals(Decimal128.parse("314E-2"), bsonReader.readDecimal128());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDecimal128ExtendedJsonWithBoolean() {
        String json = "{\"$numberDecimal\": true}";
        bsonReader = new JsonReader(json);
        try {
            bsonReader.readBsonType();
            fail("Should fail to parse NumberDecimal constructor with a string");
        } catch (JsonParseException e) {
            // all good
        }
    }

    @Test
    public void testJavaScript() {
        String json = "{ \"$code\" : \"function f() { return 1; }\" }";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.JAVASCRIPT, bsonReader.readBsonType());
        assertEquals("function f() { return 1; }", bsonReader.readJavaScript());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());

    }

    @Test
    public void testJavaScriptWithScope() {
        String json = "{\"codeWithScope\": { \"$code\" : \"function f() { return n; }\", \"$scope\" : { \"n\" : 1 } } }";
        bsonReader = new JsonReader(json);
        bsonReader.readStartDocument();
        assertEquals(BsonType.JAVASCRIPT_WITH_SCOPE, bsonReader.readBsonType());
        assertEquals("codeWithScope", bsonReader.readName());
        assertEquals("function f() { return n; }", bsonReader.readJavaScriptWithScope());
        bsonReader.readStartDocument();
        assertEquals(BsonType.INT32, bsonReader.readBsonType());
        assertEquals("n", bsonReader.readName());
        assertEquals(1, bsonReader.readInt32());
        bsonReader.readEndDocument();
        bsonReader.readEndDocument();
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testMaxKey() {
        String json = "{ \"$maxKey\" : 1 }";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.MAX_KEY, bsonReader.readBsonType());
        bsonReader.readMaxKey();
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());

    }

    @Test
    public void testMinKey() {
        String json = "{ \"$minKey\" : 1 }";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.MIN_KEY, bsonReader.readBsonType());
        bsonReader.readMinKey();
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());

    }

    @Test
    public void testNestedArray() {
        String json = "{ \"a\" : [1, 2] }";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.DOCUMENT, bsonReader.readBsonType());
        bsonReader.readStartDocument();
        assertEquals(BsonType.ARRAY, bsonReader.readBsonType());
        assertEquals("a", bsonReader.readName());
        bsonReader.readStartArray();
        assertEquals(1, bsonReader.readInt32());
        assertEquals(2, bsonReader.readInt32());
        bsonReader.readEndArray();
        bsonReader.readEndDocument();
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());

    }

    @Test
    public void testNestedDocument() {
        String json = "{ \"a\" : { \"b\" : 1, \"c\" : 2 } }";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.DOCUMENT, bsonReader.readBsonType());
        bsonReader.readStartDocument();
        assertEquals(BsonType.DOCUMENT, bsonReader.readBsonType());
        assertEquals("a", bsonReader.readName());
        bsonReader.readStartDocument();
        assertEquals("b", bsonReader.readName());
        assertEquals(1, bsonReader.readInt32());
        assertEquals("c", bsonReader.readName());
        assertEquals(2, bsonReader.readInt32());
        bsonReader.readEndDocument();
        bsonReader.readEndDocument();
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());

    }

    @Test
    public void testNull() {
        String json = "null";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.NULL, bsonReader.readBsonType());
        bsonReader.readNull();
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());

    }

    @Test
    public void testObjectIdShell() {
        String json = "ObjectId(\"4d0ce088e447ad08b4721a37\")";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.OBJECT_ID, bsonReader.readBsonType());
        ObjectId objectId = bsonReader.readObjectId();
        assertEquals("4d0ce088e447ad08b4721a37", objectId.toString());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testObjectIdWithNew() {
        String json = "new ObjectId(\"4d0ce088e447ad08b4721a37\")";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.OBJECT_ID, bsonReader.readBsonType());
        ObjectId objectId = bsonReader.readObjectId();
        assertEquals("4d0ce088e447ad08b4721a37", objectId.toString());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testObjectIdStrict() {
        String json = "{ \"$oid\" : \"4d0ce088e447ad08b4721a37\" }";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.OBJECT_ID, bsonReader.readBsonType());
        ObjectId objectId = bsonReader.readObjectId();
        assertEquals("4d0ce088e447ad08b4721a37", objectId.toString());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testObjectIdTenGen() {
        String json = "ObjectId(\"4d0ce088e447ad08b4721a37\")";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.OBJECT_ID, bsonReader.readBsonType());
        ObjectId objectId = bsonReader.readObjectId();
        assertEquals("4d0ce088e447ad08b4721a37", objectId.toString());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testRegularExpressionShell() {
        String json = "/pattern/imxs";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.REGULAR_EXPRESSION, bsonReader.readBsonType());
        BsonRegularExpression regex = bsonReader.readRegularExpression();
        assertEquals("pattern", regex.getPattern());
        assertEquals("imxs", regex.getOptions());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());

    }

    @Test
    public void testRegularExpressionStrict() {
        String json = "{ \"$regex\" : \"pattern\", \"$options\" : \"imxs\" }";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.REGULAR_EXPRESSION, bsonReader.readBsonType());
        BsonRegularExpression regex = bsonReader.readRegularExpression();
        assertEquals("pattern", regex.getPattern());
        assertEquals("imxs", regex.getOptions());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
        JsonWriterSettings settings = new JsonWriterSettings(JsonMode.STRICT);

    }

    @Test
    public void testString() {
        String str = "abc";
        String json = '"' + str + '"';
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.STRING, bsonReader.readBsonType());
        assertEquals(str, bsonReader.readString());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());

        str = "\ud806\udc5c";
        json = '"' + str + '"';
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.STRING, bsonReader.readBsonType());
        assertEquals(str, bsonReader.readString());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());

        str = "\\ud806\\udc5c";
        json = '"' + str + '"';
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.STRING, bsonReader.readBsonType());
        assertEquals("\ud806\udc5c", bsonReader.readString());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());

        str = "꼢𑡜ᳫ鉠鮻罖᧭䆔瘉";
        json = '"' + str + '"';
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.STRING, bsonReader.readBsonType());
        assertEquals(str, bsonReader.readString());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testStringEmpty() {
        String json = "\"\"";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.STRING, bsonReader.readBsonType());
        assertEquals("", bsonReader.readString());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testSymbol() {
        String json = "{ \"$symbol\" : \"symbol\" }";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.SYMBOL, bsonReader.readBsonType());
        assertEquals("symbol", bsonReader.readSymbol());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testTimestampStrict() {
        String json = "{ \"$timestamp\" : { \"t\" : 1234, \"i\" : 1 } }";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.TIMESTAMP, bsonReader.readBsonType());
        assertEquals(new BsonTimestamp(1234, 1), bsonReader.readTimestamp());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testTimestampStrictWithOutOfOrderFields() {
        String json = "{ \"$timestamp\" : { \"i\" : 1, \"t\" : 1234 } }";
        bsonReader = new JsonReader(json);

        try {
            bsonReader.readBsonType();
            fail("Should have failed to read timestamp with fields not in expected order");
        } catch (JsonParseException e) {
            // all good
        }
    }

    @Test
    public void testTimestampShell() {
        String json = "Timestamp(1234, 1)";
        bsonReader = new JsonReader(json);

        assertEquals(BsonType.TIMESTAMP, bsonReader.readBsonType());
        assertEquals(new BsonTimestamp(1234, 1), bsonReader.readTimestamp());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testUndefined() {
        String json = "undefined";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.UNDEFINED, bsonReader.readBsonType());
        bsonReader.readUndefined();
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testUndefinedExtended() {
        String json = "{ \"$undefined\" : true }";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.UNDEFINED, bsonReader.readBsonType());
        bsonReader.readUndefined();
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test(expected = JsonParseException.class)
    public void testUndefinedExtendedInvalid() {
        String json = "{ \"$undefined\" : false }";
        bsonReader = new JsonReader(json);
        bsonReader.readUndefined();
    }

    @Test(expected = IllegalStateException.class)
    public void testClosedState() {
        bsonReader = new JsonReader("");
        bsonReader.close();
        bsonReader.readBinaryData();
    }

    @Test(expected = JsonParseException.class)
    public void testEndOfFile0() {
        String json = "{";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.DOCUMENT, bsonReader.readBsonType());
        bsonReader.readStartDocument();
        bsonReader.readBsonType();
    }

    @Test(expected = JsonParseException.class)
    public void testEndOfFile1() {
        String json = "{ test : ";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.DOCUMENT, bsonReader.readBsonType());
        bsonReader.readStartDocument();
        bsonReader.readBsonType();
    }

    @Test
    public void testBinary() {
        String json = "{ \"$binary\" : \"AQID\", \"$type\" : \"0\" }";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.BINARY, bsonReader.readBsonType());
        BsonBinary binary = bsonReader.readBinaryData();
        assertEquals(0, binary.getType());
        assertArrayEquals(new byte[]{1, 2, 3}, binary.getData());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testUserDefinedBinary() {
        String json = "{ \"$binary\" : \"AQID\", \"$type\" : \"80\" }";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.BINARY, bsonReader.readBsonType());
        BsonBinary binary = bsonReader.readBinaryData();
        assertEquals(BsonBinarySubType.USER_DEFINED.getValue(), binary.getType());
        assertArrayEquals(new byte[]{1, 2, 3}, binary.getData());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testInfinity() {
        String json = "{ \"value\" : Infinity }";
        bsonReader = new JsonReader(json);
        bsonReader.readStartDocument();
        assertEquals(BsonType.DOUBLE, bsonReader.readBsonType());
        bsonReader.readName();
        assertEquals(Double.POSITIVE_INFINITY, bsonReader.readDouble(), 0.0001);
    }

    @Test
    public void testNaN() {
        String json = "{ \"value\" : NaN }";
        bsonReader = new JsonReader(json);
        bsonReader.readStartDocument();
        assertEquals(BsonType.DOUBLE, bsonReader.readBsonType());
        bsonReader.readName();
        assertEquals(Double.NaN, bsonReader.readDouble(), 0.0001);
    }

    @Test
    public void testBinData() {
        String json = "{ \"a\" : BinData(3, AQID) }";
        bsonReader = new JsonReader(json);
        bsonReader.readStartDocument();
        assertEquals(BsonType.BINARY, bsonReader.readBsonType());
        BsonBinary binary = bsonReader.readBinaryData();
        assertEquals(3, binary.getType());
        assertArrayEquals(new byte[]{1, 2, 3}, binary.getData());
        bsonReader.readEndDocument();
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testBinDataUserDefined() {
        String json = "{ \"a\" : BinData(128, AQID) }";
        bsonReader = new JsonReader(json);
        bsonReader.readStartDocument();
        assertEquals(BsonType.BINARY, bsonReader.readBsonType());
        BsonBinary binary = bsonReader.readBinaryData();
        assertEquals(BsonBinarySubType.USER_DEFINED.getValue(), binary.getType());
        assertArrayEquals(new byte[]{1, 2, 3}, binary.getData());
        bsonReader.readEndDocument();
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testBinDataWithNew() {
        String json = "{ \"a\" : new BinData(3, AQID) }";
        bsonReader = new JsonReader(json);
        bsonReader.readStartDocument();
        assertEquals(BsonType.BINARY, bsonReader.readBsonType());
        BsonBinary binary = bsonReader.readBinaryData();
        assertEquals(3, binary.getType());
        assertArrayEquals(new byte[]{1, 2, 3}, binary.getData());
        bsonReader.readEndDocument();
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDateWithNumbers() {
        String json = "new Date(1988, 06, 13 , 22 , 1)";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.DATE_TIME, bsonReader.readBsonType());
        assertEquals(584834460000L, bsonReader.readDateTime());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDateTimeConstructorWithNew() {
        String json = "new Date(\"Sat Jul 13 2013 11:10:05 UTC\")";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.DATE_TIME, bsonReader.readBsonType());
        assertEquals(1373713805000L, bsonReader.readDateTime());
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testEmptyDateTimeConstructorWithNew() {
        long currentTime = new Date().getTime();
        String json = "new Date()";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.DATE_TIME, bsonReader.readBsonType());
        assertTrue(bsonReader.readDateTime() >= currentTime);
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDateTimeWithOutNew() {
        long currentTime = currentTimeWithoutMillis();
        String json = "Date()";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.STRING, bsonReader.readBsonType());
        assertTrue(dateStringToTime(bsonReader.readString()) >= currentTime);
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDateTimeWithOutNewContainingJunk() {
        long currentTime = currentTimeWithoutMillis();
        String json = "Date({ok: 1}, 1234)";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.STRING, bsonReader.readBsonType());
        assertTrue(dateStringToTime(bsonReader.readString()) >= currentTime);
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testEmptyISODateTimeConstructorWithNew() {
        long currentTime = new Date().getTime();
        String json = "new ISODate()";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.DATE_TIME, bsonReader.readBsonType());
        assertTrue(bsonReader.readDateTime() >= currentTime);
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testEmptyISODateTimeConstructor() {
        long currentTime = new Date().getTime();
        String json = "ISODate()";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.DATE_TIME, bsonReader.readBsonType());
        assertTrue(bsonReader.readDateTime() >= currentTime);
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testRegExp() {
        String json = "RegExp(\"abc\",\"im\")";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.REGULAR_EXPRESSION, bsonReader.readBsonType());
        BsonRegularExpression regularExpression = bsonReader.readRegularExpression();
        assertEquals("abc", regularExpression.getPattern());
        assertEquals("im", regularExpression.getOptions());
    }

    @Test
    public void testRegExpWithNew() {
        String json = "new RegExp(\"abc\",\"im\")";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.REGULAR_EXPRESSION, bsonReader.readBsonType());
        BsonRegularExpression regularExpression = bsonReader.readRegularExpression();
        assertEquals("abc", regularExpression.getPattern());
        assertEquals("im", regularExpression.getOptions());
    }

    @Test
    public void testSkip() {
        String json = "{ \"a\" : 2 }";
        bsonReader = new JsonReader(json);
        bsonReader.readStartDocument();
        bsonReader.readBsonType();
        bsonReader.skipName();
        bsonReader.skipValue();
        assertEquals(BsonType.END_OF_DOCUMENT, bsonReader.readBsonType());
        bsonReader.readEndDocument();
        assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
    }

    @Test
    public void testDBPointer() {
        String json = "DBPointer(\"b\",\"5209296cd6c4e38cf96fffdc\")";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.DB_POINTER, bsonReader.readBsonType());
        BsonDbPointer dbPointer = bsonReader.readDBPointer();
        assertEquals("b", dbPointer.getNamespace());
        assertEquals(new ObjectId("5209296cd6c4e38cf96fffdc"), dbPointer.getId());
    }

    @Test
    public void testDBPointerWithNew() {
        String json = "new DBPointer(\"b\",\"5209296cd6c4e38cf96fffdc\")";
        bsonReader = new JsonReader(json);
        assertEquals(BsonType.DB_POINTER, bsonReader.readBsonType());
        BsonDbPointer dbPointer = bsonReader.readDBPointer();
        assertEquals("b", dbPointer.getNamespace());
        assertEquals(new ObjectId("5209296cd6c4e38cf96fffdc"), dbPointer.getId());
    }

    private long dateStringToTime(final String date) {
        SimpleDateFormat df = new SimpleDateFormat("EEE MMM dd yyyy HH:mm:ss z", Locale.ENGLISH);
        return df.parse(date, new ParsePosition(0)).getTime();
    }

    private long currentTimeWithoutMillis() {
        long currentTime = new Date().getTime();
        return currentTime - (currentTime % 1000);
    }

}
