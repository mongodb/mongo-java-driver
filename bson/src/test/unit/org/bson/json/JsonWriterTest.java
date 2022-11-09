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

package org.bson.json;

import org.bson.BsonBinary;
import org.bson.BsonDbPointer;
import org.bson.BsonInvalidOperationException;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;

import java.io.StringWriter;
import java.util.Date;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("unchecked")
public class JsonWriterTest {
    private StringWriter stringWriter;
    private JsonWriter writer;

    @Before
    public void before() {
        stringWriter = new StringWriter();
        writer = new JsonWriter(stringWriter, JsonWriterSettings.builder().build());
    }

    private static class TestData<T> {
        private final T value;
        private final String expected;

        TestData(final T value, final String expected) {
            this.value = value;
            this.expected = expected;
        }
    }

    @Test(expected = BsonInvalidOperationException.class)
    public void shouldThrowExceptionForBooleanWhenWritingBeforeStartingDocument() {
        writer.writeBoolean("b1", true);
    }

    @Test(expected = BsonInvalidOperationException.class)
    public void shouldThrowExceptionForNameWhenWritingBeforeStartingDocument() {
        writer.writeName("name");
    }

    @Test(expected = BsonInvalidOperationException.class)
    public void shouldThrowExceptionForStringWhenStateIsValue() {
        writer.writeStartDocument();
        writer.writeString("SomeString");
    }

    @Test(expected = BsonInvalidOperationException.class)
    public void shouldThrowExceptionWhenEndingAnArrayWhenStateIsValue() {
        writer.writeStartDocument();
        writer.writeEndArray();
    }

    @Test(expected = BsonInvalidOperationException.class)
    public void shouldThrowExceptionWhenWritingASecondName() {
        writer.writeStartDocument();
        writer.writeName("f1");
        writer.writeName("i2");
    }

    @Test(expected = BsonInvalidOperationException.class)
    public void shouldThrowExceptionWhenEndingADocumentBeforeValueIsWritten() {
        writer.writeStartDocument();
        writer.writeName("f1");
        writer.writeEndDocument();
    }

    @Test(expected = BsonInvalidOperationException.class)
    public void shouldThrowAnExceptionWhenTryingToWriteASecondValue() {
        writer.writeDouble(100);
        writer.writeString("i2");
    }

    @Test(expected = BsonInvalidOperationException.class)
    public void shouldThrowAnExceptionWhenTryingToWriteJavaScript() {
        writer.writeDouble(100);
        writer.writeJavaScript("var i");
    }

    @Test(expected = BsonInvalidOperationException.class)
    public void shouldThrowAnExceptionWhenWritingANameInAnArray() {
        writer.writeStartDocument();
        writer.writeStartArray("f2");
        writer.writeName("i3");
    }

    @Test(expected = BsonInvalidOperationException.class)
    public void shouldThrowAnExceptionWhenEndingDocumentInTheMiddleOfWritingAnArray() {
        writer.writeStartDocument();
        writer.writeStartArray("f2");
        writer.writeEndDocument();
    }

    @Test(expected = BsonInvalidOperationException.class)
    public void shouldThrowAnExceptionWhenEndingAnArrayInASubDocument() {
        writer.writeStartDocument();
        writer.writeStartArray("f2");
        writer.writeStartDocument();
        writer.writeEndArray();
    }

    @Test(expected = BsonInvalidOperationException.class)
    public void shouldThrowAnExceptionWhenWritingANameInAnArrayEvenWhenSubDocumentExistsInArray() {
        writer.writeStartDocument();
        writer.writeStartArray("f2");
        writer.writeStartDocument();
        writer.writeEndDocument();
        writer.writeName("i3");
    }

    @Test(expected = BsonInvalidOperationException.class)
    public void shouldThrowAnExceptionWhenAttemptingToEndAnArrayThatWasNotStarted() {
        writer.writeStartDocument();
        writer.writeStartArray("f2");
        writer.writeEndArray();
        writer.writeEndArray();
    }

    @Test(expected = BsonInvalidOperationException.class)
    public void shouldThrowAnErrorIfTryingToWriteNameIntoAJavascriptScope() {
        writer.writeStartDocument();
        writer.writeJavaScriptWithScope("js1", "var i = 1");

        writer.writeName("b1");
    }

    @Test(expected = BsonInvalidOperationException.class)
    public void shouldThrowAnErrorIfTryingToWriteValueIntoAJavascriptScope() {
        writer.writeStartDocument();
        writer.writeJavaScriptWithScope("js1", "var i = 1");

        writer.writeBinaryData(new BsonBinary(new byte[]{0, 0, 1, 0}));
    }

    @Test(expected = BsonInvalidOperationException.class)
    public void shouldThrowAnErrorIfTryingToWriteArrayIntoAJavascriptScope() {
        writer.writeStartDocument();
        writer.writeJavaScriptWithScope("js1", "var i = 1");

        writer.writeStartArray();
    }

    @Test(expected = BsonInvalidOperationException.class)
    public void shouldThrowAnErrorIfTryingToWriteEndDocumentIntoAJavascriptScope() {
        writer.writeStartDocument();
        writer.writeJavaScriptWithScope("js1", "var i = 1");

        writer.writeEndDocument();
    }

    @Test
    public void testEmptyDocument() {
        writer.writeStartDocument();
        writer.writeEndDocument();
        String expected = "{}";
        assertEquals(expected, stringWriter.toString());
    }

    @Test
    public void testSingleElementDocument() {
        writer.writeStartDocument();
        writer.writeName("s");
        writer.writeString("str");
        writer.writeEndDocument();
        String expected = "{\"s\": \"str\"}";
        assertEquals(expected, stringWriter.toString());
    }

    @Test
    public void testTwoElementDocument() {
        writer.writeStartDocument();
        writer.writeName("s");
        writer.writeString("str");
        writer.writeName("d");
        writer.writeString("str2");
        writer.writeEndDocument();
        String expected = "{\"s\": \"str\", \"d\": \"str2\"}";
        assertEquals(expected, stringWriter.toString());
    }

    @Test
    public void testNestedDocument() {
        writer.writeStartDocument();
        writer.writeName("doc");
        writer.writeStartDocument();
        writer.writeName("doc");
        writer.writeStartDocument();
        writer.writeName("s");
        writer.writeString("str");
        writer.writeEndDocument();
        writer.writeEndDocument();
        writer.writeEndDocument();
        String expected = "{\"doc\": {\"doc\": {\"s\": \"str\"}}}";
        assertEquals(expected, stringWriter.toString());
    }

    @Test
    public void testSingleString() {
        writer.writeStartDocument();
        writer.writeString("abc", "xyz");
        writer.writeEndDocument();
        String expected = "{\"abc\": \"xyz\"}";
        assertEquals(expected, stringWriter.toString());
    }


    @Test
    public void testBoolean() {
        writer.writeStartDocument();
        writer.writeBoolean("abc", true);
        writer.writeEndDocument();
        String expected = "{\"abc\": true}";
        assertEquals(expected, stringWriter.toString());
    }

    @Test
    public void testDouble() {
        List<TestData<Double>> tests = asList(new TestData<>(0.0, "0.0"), new TestData<>(0.0005, "5.0E-4"),
                new TestData<>(0.5, "0.5"), new TestData<>(1.0, "1.0"),
                new TestData<>(1.5, "1.5"), new TestData<>(1.5E+40, "1.5E40"),
                new TestData<>(1.5E-40, "1.5E-40"),
                new TestData<>(1234567890.1234568E+123, "1.2345678901234568E132"),
                new TestData<>(Double.MAX_VALUE, "1.7976931348623157E308"),
                new TestData<>(Double.MIN_VALUE, "4.9E-324"),

                new TestData<>(-0.0005, "-5.0E-4"),
                new TestData<>(-0.5, "-0.5"),
                new TestData<>(-1.0, "-1.0"),
                new TestData<>(-1.5, "-1.5"),
                new TestData<>(-1.5E+40, "-1.5E40"),
                new TestData<>(-1.5E-40, "-1.5E-40"),
                new TestData<>(-1234567890.1234568E+123, "-1.2345678901234568E132"),

                new TestData<>(Double.NaN, "NaN"),
                new TestData<>(Double.NEGATIVE_INFINITY, "-Infinity"),
                new TestData<>(Double.POSITIVE_INFINITY, "Infinity"));
        for (final TestData<Double> cur : tests) {
            stringWriter = new StringWriter();
            writer = new JsonWriter(stringWriter, JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build());
            writer.writeStartDocument();
            writer.writeDouble("d", cur.value);
            writer.writeEndDocument();
            String expected = "{\"d\": {\"$numberDouble\": \"" + cur.expected + "\"}}";
            assertEquals(expected, stringWriter.toString());
        }
    }

    @Test
    public void testInt64Shell() {
        List<TestData<Long>> tests = asList(new TestData<>(Long.MIN_VALUE, "NumberLong(\"-9223372036854775808\")"),
                new TestData<>(Integer.MIN_VALUE - 1L, "NumberLong(\"-2147483649\")"),
                new TestData<>((long) Integer.MIN_VALUE, "NumberLong(-2147483648)"),
                new TestData<>(0L, "NumberLong(0)"),
                new TestData<>((long) Integer.MAX_VALUE, "NumberLong(2147483647)"),
                new TestData<>(Integer.MAX_VALUE + 1L, "NumberLong(\"2147483648\")"),
                new TestData<>(Long.MAX_VALUE, "NumberLong(\"9223372036854775807\")"));
        for (final TestData<Long> cur : tests) {
            stringWriter = new StringWriter();
            writer = new JsonWriter(stringWriter, JsonWriterSettings.builder().outputMode(JsonMode.SHELL).build());
            writer.writeStartDocument();
            writer.writeInt64("l", cur.value);
            writer.writeEndDocument();
            String expected = "{\"l\": " + cur.expected + "}";
            assertEquals(expected, stringWriter.toString());
        }
    }

    @Test
    public void testInt64Relaxed() {
        List<TestData<Long>> tests = asList(new TestData<>(Long.MIN_VALUE, "-9223372036854775808"),
                new TestData<>(Integer.MIN_VALUE - 1L, "-2147483649"),
                new TestData<>((long) Integer.MIN_VALUE, "-2147483648"),
                new TestData<>(0L, "0"),
                new TestData<>((long) Integer.MAX_VALUE, "2147483647"),
                new TestData<>(Integer.MAX_VALUE + 1L, "2147483648"),
                new TestData<>(Long.MAX_VALUE, "9223372036854775807"));

        for (final TestData<Long> cur : tests) {
            stringWriter = new StringWriter();
            writer = new JsonWriter(stringWriter, JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build());
            writer.writeStartDocument();
            writer.writeInt64("l", cur.value);
            writer.writeEndDocument();
            String expected = "{\"l\": " + cur.expected + "}";
            assertEquals(expected, stringWriter.toString());
        }
    }

    @Test
    public void testDecimal128SShell() {
        List<TestData<Decimal128>> tests = asList(
                new TestData<>(Decimal128.parse("1.0"), "1.0"),
                new TestData<>(Decimal128.POSITIVE_INFINITY, Decimal128.POSITIVE_INFINITY.toString()));


        for (final TestData<Decimal128> cur : tests) {
            stringWriter = new StringWriter();
            writer = new JsonWriter(stringWriter, JsonWriterSettings.builder().outputMode(JsonMode.SHELL).build());
            writer.writeStartDocument();
            writer.writeDecimal128("d", cur.value);
            writer.writeEndDocument();
            String expected = "{\"d\": NumberDecimal(\"" + cur.expected + "\")}";
            assertEquals(expected, stringWriter.toString());
        }
    }

    @Test
    public void testDecimal128Relaxed() {
        List<TestData<Decimal128>> tests = asList(
                new TestData<>(Decimal128.parse("1.0"), "1.0"),
                new TestData<>(Decimal128.POSITIVE_INFINITY, Decimal128.POSITIVE_INFINITY.toString()));


        for (final TestData<Decimal128> cur : tests) {
            stringWriter = new StringWriter();
            writer = new JsonWriter(stringWriter, JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build());
            writer.writeStartDocument();
            writer.writeDecimal128("d", cur.value);
            writer.writeEndDocument();
            String expected = "{\"d\": {\"$numberDecimal\": \"" + cur.expected + "\"}}";
            assertEquals(expected, stringWriter.toString());
        }
    }

    @Test
    public void testArray() {
        writer.writeStartDocument();
        writer.writeStartArray("array");
        writer.writeInt32(1);
        writer.writeInt32(2);
        writer.writeInt32(3);
        writer.writeEndArray();
        writer.writeEndDocument();
        String expected = "{\"array\": [1, 2, 3]}";
        assertEquals(expected, stringWriter.toString());
    }

    @Test
    public void testBinaryRelaxed() {
        List<TestData<BsonBinary>> tests = asList(new TestData<>(new BsonBinary(new byte[0]),
                        "{\"$binary\": {\"base64\": \"\", "
                                + "\"subType\": \"00\"}}"),
                new TestData<>(new BsonBinary(new byte[]{1}),
                        "{\"$binary\": {\"base64\": \"AQ==\", "
                                + "\"subType\": \"00\"}}"),
                new TestData<>(new BsonBinary(new byte[]{1, 2}),
                        "{\"$binary\": {\"base64\": \"AQI=\", "
                                + "\"subType\": \"00\"}}"),
                new TestData<>(new BsonBinary(new byte[]{1, 2, 3}),
                        "{\"$binary\": {\"base64\": \"AQID\", "
                                + "\"subType\": \"00\"}}"),
                new TestData<>(new BsonBinary((byte) 0x80, new byte[]{1, 2, 3}),
                        "{\"$binary\": {\"base64\": \"AQID\", "
                                + "\"subType\": \"80\"}}"));
        for (final TestData<BsonBinary> cur : tests) {
            stringWriter = new StringWriter();
            writer = new JsonWriter(stringWriter, JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build());
            writer.writeStartDocument();
            writer.writeBinaryData("binary", cur.value);
            writer.writeEndDocument();
            String expected = "{\"binary\": " + cur.expected + "}";
            assertEquals(expected, stringWriter.toString());
        }
    }

    @Test
    public void testBinaryShell() {
        List<TestData<BsonBinary>> tests = asList(new TestData<>(new BsonBinary(new byte[0]), "new BinData(0, \"\")"),
                new TestData<>(new BsonBinary(new byte[]{1}), "new BinData(0, \"AQ==\")"),
                new TestData<>(new BsonBinary(new byte[]{1, 2}), "new BinData(0, \"AQI=\")"),
                new TestData<>(new BsonBinary(new byte[]{1, 2, 3}), "new BinData(0, \"AQID\")"),
                new TestData<>(new BsonBinary((byte) 0x80, new byte[]{1, 2, 3}),
                        "new BinData(128, \"AQID\")"));
        for (final TestData<BsonBinary> cur : tests) {
            stringWriter = new StringWriter();
            writer = new JsonWriter(stringWriter, JsonWriterSettings.builder().outputMode(JsonMode.SHELL).build());
            writer.writeStartDocument();
            writer.writeBinaryData("binary", cur.value);
            writer.writeEndDocument();
            String expected = "{\"binary\": " + cur.expected + "}";
            assertEquals(expected, stringWriter.toString());
        }
    }

    @Test
    public void testDateTimeRelaxed() {
        List<TestData<Date>> tests = asList(new TestData<>(new Date(0), "{\"$date\": \"1970-01-01T00:00:00Z\"}"),
                new TestData<>(new Date(Long.MAX_VALUE), "{\"$date\": {\"$numberLong\": \"9223372036854775807\"}}"),
                new TestData<>(new Date(Long.MIN_VALUE), "{\"$date\": {\"$numberLong\": \"-9223372036854775808\"}}"));
        for (final TestData<Date> cur : tests) {
            stringWriter = new StringWriter();
            writer = new JsonWriter(stringWriter, JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build());
            writer.writeStartDocument();
            writer.writeDateTime("date", cur.value.getTime());
            writer.writeEndDocument();
            String expected = "{\"date\": " + cur.expected + "}";
            assertEquals(expected, stringWriter.toString());
        }
    }

    @Test
    public void testDateTimeShell() {
        List<TestData<Date>> tests = asList(new TestData<>(new Date(0), "ISODate(\"1970-01-01T00:00:00.000Z\")"),
                new TestData<>(new Date(1), "ISODate(\"1970-01-01T00:00:00.001Z\")"),
                new TestData<>(new Date(-1), "ISODate(\"1969-12-31T23:59:59.999Z\")"),
                new TestData<>(new Date(Long.MAX_VALUE), "new Date(9223372036854775807)"),
                new TestData<>(new Date(Long.MIN_VALUE), "new Date(-9223372036854775808)"));
        for (final TestData<Date> cur : tests) {
            stringWriter = new StringWriter();
            writer = new JsonWriter(stringWriter, JsonWriterSettings.builder().outputMode(JsonMode.SHELL).build());
            writer.writeStartDocument();
            writer.writeDateTime("date", cur.value.getTime());
            writer.writeEndDocument();
            String expected = "{\"date\": " + cur.expected + "}";
            assertEquals(expected, stringWriter.toString());
        }
    }

    @Test
    public void testJavaScript() {
        writer.writeStartDocument();
        writer.writeJavaScript("f", "function f() { return 1; }");
        writer.writeEndDocument();
        String expected = "{\"f\": {\"$code\": \"function f() { return 1; }\"}}";
        assertEquals(expected, stringWriter.toString());
    }

    @Test
    public void testJavaScriptWithScope() {
        writer.writeStartDocument();
        writer.writeJavaScriptWithScope("f", "function f() { return n; }");
        writer.writeStartDocument();
        writer.writeInt32("n", 1);
        writer.writeEndDocument();
        writer.writeEndDocument();
        String expected =
                "{\"f\": {\"$code\": \"function f() { return n; }\", " + "\"$scope\": {\"n\": 1}}}";
        assertEquals(expected, stringWriter.toString());
    }

    @Test
    public void testMaxKeyStrict() {
        writer.writeStartDocument();
        writer.writeMaxKey("maxkey");
        writer.writeEndDocument();
        String expected = "{\"maxkey\": {\"$maxKey\": 1}}";
        assertEquals(expected, stringWriter.toString());
    }

    @Test
    public void testMinKeyStrict() {
        writer.writeStartDocument();
        writer.writeMinKey("minkey");
        writer.writeEndDocument();
        String expected = "{\"minkey\": {\"$minKey\": 1}}";
        assertEquals(expected, stringWriter.toString());
    }


    @Test
    public void testMaxKeyShell() {
        writer = new JsonWriter(stringWriter, JsonWriterSettings.builder().outputMode(JsonMode.SHELL).build());
        writer.writeStartDocument();
        writer.writeMaxKey("maxkey");
        writer.writeEndDocument();
        String expected = "{\"maxkey\": MaxKey}";
        assertEquals(expected, stringWriter.toString());
    }

    @Test
    public void testMinKeyShell() {
        writer = new JsonWriter(stringWriter, JsonWriterSettings.builder().outputMode(JsonMode.SHELL).build());
        writer.writeStartDocument();
        writer.writeMinKey("minkey");
        writer.writeEndDocument();
        String expected = "{\"minkey\": MinKey}";
        assertEquals(expected, stringWriter.toString());
    }

    @Test
    public void testNull() {
        writer.writeStartDocument();
        writer.writeNull("null");
        writer.writeEndDocument();
        String expected = "{\"null\": null}";
        assertEquals(expected, stringWriter.toString());
    }

    @Test
    public void testObjectIdShell() {
        writer = new JsonWriter(stringWriter, JsonWriterSettings.builder().outputMode(JsonMode.SHELL).build());
        ObjectId objectId = new ObjectId("4d0ce088e447ad08b4721a37");

        writer.writeStartDocument();
        writer.writeObjectId("_id", objectId);
        writer.writeEndDocument();

        String expected = "{\"_id\": ObjectId(\"4d0ce088e447ad08b4721a37\")}";
        assertEquals(expected, stringWriter.toString());
    }

    @Test
    public void testObjectIdStrict() {
        ObjectId objectId = new ObjectId("4d0ce088e447ad08b4721a37");

        writer.writeStartDocument();
        writer.writeObjectId("_id", objectId);
        writer.writeEndDocument();

        String expected = "{\"_id\": {\"$oid\": \"4d0ce088e447ad08b4721a37\"}}";
        assertEquals(expected, stringWriter.toString());
    }

    @Test
    public void testRegularExpressionShell() {
        List<TestData<BsonRegularExpression>> tests;
        tests = asList(new TestData<>(new BsonRegularExpression(""), "/(?:)/"),
                new TestData<>(new BsonRegularExpression("a"), "/a/"),
                new TestData<>(new BsonRegularExpression("a/b"), "/a\\/b/"),
                new TestData<>(new BsonRegularExpression("a\\b"), "/a\\b/"),
                new TestData<>(new BsonRegularExpression("a", "i"), "/a/i"),
                new TestData<>(new BsonRegularExpression("a", "m"), "/a/m"),
                new TestData<>(new BsonRegularExpression("a", "x"), "/a/x"),
                new TestData<>(new BsonRegularExpression("a", "s"), "/a/s"),
                new TestData<>(new BsonRegularExpression("a", "imxs"), "/a/imsx"));
        for (final TestData<BsonRegularExpression> cur : tests) {
            stringWriter = new StringWriter();
            writer = new JsonWriter(stringWriter, JsonWriterSettings.builder().outputMode(JsonMode.SHELL).build());
            writer.writeStartDocument();
            writer.writeRegularExpression("regex", cur.value);
            writer.writeEndDocument();
            String expected = "{\"regex\": " + cur.expected + "}";
            assertEquals(expected, stringWriter.toString());
        }
    }

    @Test
    public void testRegularExpressionRelaxed() {
        List<TestData<BsonRegularExpression>> tests;
        tests = asList(new TestData<>(new BsonRegularExpression(""),
                        "{\"$regularExpression\": {\"pattern\": \"\", \"options\": \"\"}}"),
                new TestData<>(new BsonRegularExpression("a"),
                        "{\"$regularExpression\": {\"pattern\": \"a\", \"options\": \"\"}}"),
                new TestData<>(new BsonRegularExpression("a/b"),
                        "{\"$regularExpression\": {\"pattern\": \"a/b\", \"options\": \"\"}}"),
                new TestData<>(new BsonRegularExpression("a\\b"),
                        "{\"$regularExpression\": {\"pattern\": \"a\\\\b\", \"options\": \"\"}}"),
                new TestData<>(new BsonRegularExpression("a", "i"),
                        "{\"$regularExpression\": {\"pattern\": \"a\", \"options\": \"i\"}}"),
                new TestData<>(new BsonRegularExpression("a", "m"),
                        "{\"$regularExpression\": {\"pattern\": \"a\", \"options\": \"m\"}}"),
                new TestData<>(new BsonRegularExpression("a", "x"),
                        "{\"$regularExpression\": {\"pattern\": \"a\", \"options\": \"x\"}}"),
                new TestData<>(new BsonRegularExpression("a", "s"),
                        "{\"$regularExpression\": {\"pattern\": \"a\", \"options\": \"s\"}}"),
                new TestData<>(new BsonRegularExpression("a", "imxs"),
                        "{\"$regularExpression\": {\"pattern\": \"a\", \"options\": \"imsx\"}}"));
        for (final TestData<BsonRegularExpression> cur : tests) {
            stringWriter = new StringWriter();
            writer = new JsonWriter(stringWriter, JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build());
            writer.writeStartDocument();
            writer.writeRegularExpression("regex", cur.value);
            writer.writeEndDocument();
            String expected = "{\"regex\": " + cur.expected + "}";
            assertEquals(expected, stringWriter.toString());
        }
    }

    @Test
    public void testSymbol() {
        writer.writeStartDocument();
        writer.writeSymbol("symbol", "name");
        writer.writeEndDocument();
        String expected = "{\"symbol\": {\"$symbol\": \"name\"}}";
        assertEquals(expected, stringWriter.toString());
    }

    //
    @Test
    public void testTimestampStrict() {
        writer.writeStartDocument();
        writer.writeTimestamp("timestamp", new BsonTimestamp(1000, 1));
        writer.writeEndDocument();
        String expected = "{\"timestamp\": {\"$timestamp\": {\"t\": 1000, \"i\": 1}}}";
        assertEquals(expected, stringWriter.toString());
    }

    @Test
    public void testTimestampShell() {
        writer = new JsonWriter(stringWriter, JsonWriterSettings.builder().outputMode(JsonMode.SHELL).build());
        writer.writeStartDocument();
        writer.writeTimestamp("timestamp", new BsonTimestamp(1000, 1));
        writer.writeEndDocument();
        String expected = "{\"timestamp\": Timestamp(1000, 1)}";
        assertEquals(expected, stringWriter.toString());
    }

    @Test
    public void testUndefinedRelaxed() {
        writer = new JsonWriter(stringWriter, JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build());
        writer.writeStartDocument();
        writer.writeUndefined("undefined");
        writer.writeEndDocument();
        String expected = "{\"undefined\": {\"$undefined\": true}}";
        assertEquals(expected, stringWriter.toString());
    }

    @Test
    public void testUndefinedShell() {
        writer = new JsonWriter(stringWriter, JsonWriterSettings.builder().outputMode(JsonMode.SHELL).build());
        writer.writeStartDocument();
        writer.writeUndefined("undefined");
        writer.writeEndDocument();
        String expected = "{\"undefined\": undefined}";
        assertEquals(expected, stringWriter.toString());
    }

    @Test
    public void testDBPointer() {
        writer.writeStartDocument();
        writer.writeDBPointer("dbPointer", new BsonDbPointer("my.test", new ObjectId("4d0ce088e447ad08b4721a37")));
        writer.writeEndDocument();
        String expected = "{\"dbPointer\": {\"$ref\": \"my.test\", \"$id\": {\"$oid\": \"4d0ce088e447ad08b4721a37\"}}}";
        assertEquals(expected, stringWriter.toString());
    }
}
