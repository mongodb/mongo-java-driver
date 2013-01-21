/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.bson.types.RegularExpression;
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
        writer = new JsonWriter(stringWriter, new JsonWriterSettings());
    }

    private class TestData<T> {
        private final T value;
        private final String expected;

        public TestData(final T value, final String expected) {
            this.value = value;
            this.expected = expected;
        }
    }

    @Test
    public void testEmptyDocument() {
        writer.writeStartDocument();
        writer.writeEndDocument();
        final String expected = "{ }";
        assertEquals(expected, stringWriter.toString());
    }

    @Test
    public void testSingleString() {
        writer.writeStartDocument();
        writer.writeString("abc", "xyz");
        writer.writeEndDocument();
        final String expected = "{ \"abc\" : \"xyz\" }";
        assertEquals(expected, stringWriter.toString());
    }

    @Test
    public void testIndentedEmptyDocument() {
        writer = new JsonWriter(stringWriter, new JsonWriterSettings(true));
        writer.writeStartDocument();
        writer.writeEndDocument();
        final String expected = "{ }";
        assertEquals(expected, stringWriter.toString());
    }

    @Test
    public void testIndentedOneElement() {
        writer = new JsonWriter(stringWriter, new JsonWriterSettings(true));
        writer.writeStartDocument();
        writer.writeString("name", "value");
        writer.writeEndDocument();
        final String expected = "{\n  \"name\" : \"value\"\n}";
        assertEquals(expected, stringWriter.toString());
    }

    @Test
    public void testIndentedTwoElements() {
        writer = new JsonWriter(stringWriter, new JsonWriterSettings(true));
        writer.writeStartDocument();
        writer.writeString("a", "x");
        writer.writeString("b", "y");
        writer.writeEndDocument();
        final String expected = "{\n  \"a\" : \"x\",\n  \"b\" : \"y\"\n}";
        assertEquals(expected, stringWriter.toString());
    }

    @Test
    public void testDouble() {
        final List<TestData<Double>> tests = asList(new TestData<Double>(0.0, "0.0"), new TestData<Double>(0.0005,
                                                                                                           "5.0E-4"),
                                                    new TestData<Double>(0.5, "0.5"), new TestData<Double>(1.0,
                                                                                                           "1.0"),
                                                    new TestData<Double>(1.5, "1.5"), new TestData<Double>(1.5E+40,
                                                                                                           "1.5E40"),
                                                    new TestData<Double>(1.5E-40, "1.5E-40"),
                                                    new TestData<Double>(1234567890.1234568E+123,
                                                                         "1.2345678901234568E132"),
                                                    new TestData<Double>(Double.MAX_VALUE, "1.7976931348623157E308"),
                                                    new TestData<Double>(Double.MIN_VALUE, "4.9E-324"),

                                                    new TestData<Double>(-0.0005, "-5.0E-4"),
                                                    new TestData<Double>(-0.5, "-0.5"), new TestData<Double>(-1.0,
                                                                                                             "-1.0"),
                                                    new TestData<Double>(-1.5, "-1.5"),
                                                    new TestData<Double>(-1.5E+40, "-1.5E40"),
                                                    new TestData<Double>(-1.5E-40, "-1.5E-40"),
                                                    new TestData<Double>(-1234567890.1234568E+123,
                                                                         "-1.2345678901234568E132"),

                                                    new TestData<Double>(Double.NaN, "NaN"),
                                                    new TestData<Double>(Double.NEGATIVE_INFINITY, "-Infinity"),
                                                    new TestData<Double>(Double.POSITIVE_INFINITY, "Infinity"));
        for (final TestData<Double> cur : tests) {
            stringWriter = new StringWriter();
            writer = new JsonWriter(stringWriter, new JsonWriterSettings());
            writer.writeStartDocument();
            writer.writeDouble("d", cur.value);
            writer.writeEndDocument();
            final String expected = "{ \"d\" : " + cur.expected + " }";
            assertEquals(expected, stringWriter.toString());
        }
    }

    @Test
    public void testInt64Shell() {
        final List<TestData<Long>> tests = asList(new TestData<Long>(Long.MIN_VALUE,
                                                                     "NumberLong(\"-9223372036854775808\")"),
                                                  new TestData<Long>(
                Integer.MIN_VALUE - 1L, "NumberLong(\"-2147483649\")"), new TestData<Long>(Integer.MIN_VALUE
                                                                                                   + 0L,
                                                                                           "NumberLong(-2147483648)")
                , new TestData<Long>(0L, "NumberLong(0)"), new TestData<Long>(
                Integer.MAX_VALUE + 0L, "NumberLong(2147483647)"), new TestData<Long>(Integer.MAX_VALUE
                                                                                              + 1L,
                                                                                      "NumberLong(\"2147483648\")"),
                                                  new TestData<Long>(Long.MAX_VALUE,
                                                                     "NumberLong(\"9223372036854775807\")"));
        for (final TestData<Long> cur : tests) {
            stringWriter = new StringWriter();
            writer = new JsonWriter(stringWriter, new JsonWriterSettings(JsonOutputMode.Shell));
            writer.writeStartDocument();
            writer.writeInt64("l", cur.value);
            writer.writeEndDocument();
            final String expected = "{ \"l\" : " + cur.expected + " }";
            assertEquals(expected, stringWriter.toString());
        }
    }

    @Test
    public void testInt64Strict() {
        final List<TestData<Long>> tests = asList(new TestData<Long>(Long.MIN_VALUE, "-9223372036854775808"),
                                                  new TestData<Long>(
                Integer.MIN_VALUE - 1L, "-2147483649"), new TestData<Long>(
                Integer.MIN_VALUE - 0L, "-2147483648"), new TestData<Long>(0L, "0"), new TestData<Long>(
                Integer.MAX_VALUE + 0L, "2147483647"), new TestData<Long>(
                Integer.MAX_VALUE + 1L, "2147483648"), new TestData<Long>(Long.MAX_VALUE, "9223372036854775807"));

        for (final TestData<Long> cur : tests) {
            stringWriter = new StringWriter();
            writer = new JsonWriter(stringWriter, new JsonWriterSettings(JsonOutputMode.Strict));
            writer.writeStartDocument();
            writer.writeInt64("l", cur.value);
            writer.writeEndDocument();
            final String expected = "{ \"l\" : " + cur.expected + " }";
            assertEquals(expected, stringWriter.toString());
        }
    }

    @Test
    public void testEmbeddedDocument() {
        writer.writeStartDocument();
        writer.writeStartDocument("doc");
        writer.writeInt32("a", 1);
        writer.writeInt32("b", 2);
        writer.writeEndDocument();
        writer.writeEndDocument();
        final String expected = "{ \"doc\" : { \"a\" : 1, \"b\" : 2 } }";
        assertEquals(expected, stringWriter.toString());
    }


    @Test
    public void testIndentedEmbeddedDocument() {
        writer = new JsonWriter(stringWriter, new JsonWriterSettings(true));
        writer.writeStartDocument();
        writer.writeStartDocument("doc");
        writer.writeInt32("a", 1);
        writer.writeInt32("b", 2);
        writer.writeEndDocument();
        writer.writeEndDocument();
        final String expected = "{\n  \"doc\" : {\n    \"a\" : 1,\n    \"b\" : 2\n  }\n}";
        assertEquals(expected, stringWriter.toString());
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
        final String expected = "{ \"array\" : [1, 2, 3] }";
        assertEquals(expected, stringWriter.toString());
    }

    @Test
    public void testBinaryStrict() {
        final List<TestData<Binary>> tests = asList(new TestData<Binary>(new Binary(new byte[0]),
                                                                         "{ \"$binary\" : \"\", "
                                                                         + "\"$type\" : \"0\" }"),
                                                    new TestData<Binary>(new Binary(new byte[]{1}),
                                                                         "{ \"$binary\" : \"AQ==\", "
                                                                         + "\"$type\" : \"0\" }"),
                                                    new TestData<Binary>(new Binary(new byte[]{1, 2}),
                                                                         "{ \"$binary\" : \"AQI=\", "
                                                                         + "\"$type\" : \"0\" }"),
                                                    new TestData<Binary>(new Binary(new byte[]{1, 2, 3}),
                                                                         "{ \"$binary\" : \"AQID\", "
                                                                         + "\"$type\" : \"0\" }"),
                                                    new TestData<Binary>(new Binary((byte) 10, new byte[]{1, 2, 3}),
                                                                         "{ \"$binary\" : \"AQID\", "
                                                                         + "\"$type\" : \"a\" }"));
        for (final TestData<Binary> cur : tests) {
            stringWriter = new StringWriter();
            writer = new JsonWriter(stringWriter, new JsonWriterSettings(JsonOutputMode.Strict));
            writer.writeStartDocument();
            writer.writeBinaryData("binary", cur.value);
            writer.writeEndDocument();
            final String expected = "{ \"binary\" : " + cur.expected + " }";
            assertEquals(expected, stringWriter.toString());
        }
    }

    @Test
    public void testBinaryShell() {
        final List<TestData<Binary>> tests = asList(new TestData<Binary>(new Binary(new byte[0]),
                                                                         "new BinData(0, \"\")"),
                                                    new TestData<Binary>(new Binary(new byte[]{1}),
                                                                         "new BinData(0, \"AQ==\")"),
                                                    new TestData<Binary>(new Binary(new byte[]{1, 2}),
                                                                         "new BinData(0, \"AQI=\")"),
                                                    new TestData<Binary>(new Binary(new byte[]{1, 2, 3}),
                                                                         "new BinData(0, \"AQID\")"),
                                                    new TestData<Binary>(new Binary((byte) 10, new byte[]{1, 2, 3}),
                                                                         "new BinData(a, \"AQID\")"));
        for (final TestData<Binary> cur : tests) {
            stringWriter = new StringWriter();
            writer = new JsonWriter(stringWriter, new JsonWriterSettings(JsonOutputMode.Shell));
            writer.writeStartDocument();
            writer.writeBinaryData("binary", cur.value);
            writer.writeEndDocument();
            final String expected = "{ \"binary\" : " + cur.expected + " }";
            assertEquals(expected, stringWriter.toString());
        }
    }

    @Test
    public void testDateTimeStrict() {
        final List<TestData<Date>> tests = asList(new TestData<Date>(new Date(0), "{ \"$date\" : 0 }"),
                                                  new TestData<Date>(new Date(Long.MAX_VALUE),
                                                                     "{ \"$date\" : 9223372036854775807 }"),
                                                  new TestData<Date>(new Date(Long.MIN_VALUE),
                                                                     "{ \"$date\" : -9223372036854775808 }"));
        for (final TestData<Date> cur : tests) {
            stringWriter = new StringWriter();
            writer = new JsonWriter(stringWriter, new JsonWriterSettings(JsonOutputMode.Strict));
            writer.writeStartDocument();
            writer.writeDateTime("date", cur.value.getTime());
            writer.writeEndDocument();
            final String expected = "{ \"date\" : " + cur.expected + " }";
            assertEquals(expected, stringWriter.toString());
        }
    }

    @Test
    public void testDateTimeShell() {
        final List<TestData<Date>> tests = asList(new TestData<Date>(new Date(0),
                                                                     "ISODate(\"1970-01-01T00:00:00.000Z\")"),
                                                  new TestData<Date>(new Date(1),
                                                                     "ISODate(\"1970-01-01T00:00:00.001Z\")"),
                                                  new TestData<Date>(new Date(-1),
                                                                     "ISODate(\"1969-12-31T23:59:59.999Z\")"),
                                                  new TestData<Date>(new Date(Long.MAX_VALUE),
                                                                     "new Date(9223372036854775807)"),
                                                  new TestData<Date>(new Date(Long.MIN_VALUE),
                                                                     "new Date(-9223372036854775808)"));
        for (final TestData<Date> cur : tests) {
            stringWriter = new StringWriter();
            writer = new JsonWriter(stringWriter, new JsonWriterSettings(JsonOutputMode.Shell));
            writer.writeStartDocument();
            writer.writeDateTime("date", cur.value.getTime());
            writer.writeEndDocument();
            final String expected = "{ \"date\" : " + cur.expected + " }";
            assertEquals(expected, stringWriter.toString());
        }
    }


    @Test
    public void testDateTimeTenGen() {
        final List<TestData<Date>> tests = asList(new TestData<Date>(new Date(0), "new Date(0)"),
                                                  new TestData<Date>(new Date(Long.MAX_VALUE),
                                                                     "new Date(9223372036854775807)"),
                                                  new TestData<Date>(new Date(Long.MIN_VALUE),
                                                                     "new Date(-9223372036854775808)"));
        for (final TestData<Date> cur : tests) {
            stringWriter = new StringWriter();
            writer = new JsonWriter(stringWriter, new JsonWriterSettings(JsonOutputMode.TenGen));
            writer.writeStartDocument();
            writer.writeDateTime("date", cur.value.getTime());
            writer.writeEndDocument();
            final String expected = "{ \"date\" : " + cur.expected + " }";
            assertEquals(expected, stringWriter.toString());
        }
    }

    @Test
    public void testJavaScript() {
        writer.writeStartDocument();
        writer.writeJavaScript("f", "function f() { return 1; }");
        writer.writeEndDocument();
        final String expected = "{ \"f\" : { \"$code\" : \"function f() { return 1; }\" } }";
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
        final String expected =
                "{ \"f\" : { \"$code\" : \"function f() { return n; }\", " + "\"$scope\" : { \"n\" : 1 } } }";
        assertEquals(expected, stringWriter.toString());
    }

    @Test
    public void testMaxKey() {
        writer.writeStartDocument();
        writer.writeMaxKey("maxkey");
        writer.writeEndDocument();
        final String expected = "{ \"maxkey\" : { \"$maxkey\" : 1 } }";
        assertEquals(expected, stringWriter.toString());
    }

    @Test
    public void testMinKey() {
        writer.writeStartDocument();
        writer.writeMinKey("minkey");
        writer.writeEndDocument();
        final String expected = "{ \"minkey\" : { \"$minkey\" : 1 } }";
        assertEquals(expected, stringWriter.toString());
    }

    @Test
    public void testNull() {
        writer.writeStartDocument();
        writer.writeNull("null");
        writer.writeEndDocument();
        final String expected = "{ \"null\" : null }";
        assertEquals(expected, stringWriter.toString());
    }

    @Test
    public void testObjectIdShell() {
        writer = new JsonWriter(stringWriter, new JsonWriterSettings(JsonOutputMode.Shell));
        final ObjectId objectId = new ObjectId("4d0ce088e447ad08b4721a37");

        writer.writeStartDocument();
        writer.writeObjectId("_id", objectId);
        writer.writeEndDocument();

        final String expected = "{ \"_id\" : ObjectId(\"4d0ce088e447ad08b4721a37\") }";
        assertEquals(expected, stringWriter.toString());
    }

    @Test
    public void testObjectIdStrict() {
        final ObjectId objectId = new ObjectId("4d0ce088e447ad08b4721a37");

        writer.writeStartDocument();
        writer.writeObjectId("_id", objectId);
        writer.writeEndDocument();

        final String expected = "{ \"_id\" : { \"$oid\" : \"4d0ce088e447ad08b4721a37\" } }";
        assertEquals(expected, stringWriter.toString());
    }

    @Test
    public void testRegularExpressionShell() {
        final List<TestData<RegularExpression>> tests;
        tests = asList(new TestData<RegularExpression>(new RegularExpression(""), "/(?:)/"),
                      new TestData<RegularExpression>(new RegularExpression("a"), "/a/"),
                      new TestData<RegularExpression>(new RegularExpression("a/b"), "/a\\/b/"),
                      new TestData<RegularExpression>(new RegularExpression("a\\b"), "/a\\b/"),
                      new TestData<RegularExpression>(new RegularExpression("a", "i"), "/a/i"),
                      new TestData<RegularExpression>(new RegularExpression("a", "m"), "/a/m"),
                      new TestData<RegularExpression>(new RegularExpression("a", "x"), "/a/x"),
                      new TestData<RegularExpression>(new RegularExpression("a", "s"), "/a/s"),
                      new TestData<RegularExpression>(new RegularExpression("a", "imxs"), "/a/imxs"));
        for (final TestData<RegularExpression> cur : tests) {
            stringWriter = new StringWriter();
            writer = new JsonWriter(stringWriter, new JsonWriterSettings(JsonOutputMode.Shell));
            writer.writeStartDocument();
            writer.writeRegularExpression("regex", cur.value);
            writer.writeEndDocument();
            final String expected = "{ \"regex\" : " + cur.expected + " }";
            assertEquals(expected, stringWriter.toString());
        }
    }

    @Test
    public void testRegularExpressionStrict() {
        final List<TestData<RegularExpression>> tests;
        tests = asList(new TestData<RegularExpression>(new RegularExpression(""), "{ \"$regex\" : \"\", "
                                                                                  + "\"$options\" : \"\" "
                                                                                  + "}"),
                      new TestData<RegularExpression>(new RegularExpression("a"), "{ \"$regex\" : \"a\","
                                                                                  + " \"$options\" : \"\" "
                                                                                  + "}"),
                      new TestData<RegularExpression>(new RegularExpression("a/b"), "{ \"$regex\" : "
                                                                                    + "\"a/b\", "
                                                                                    + "\"$options\" : \"\" "
                                                                                    + "}"),
                      new TestData<RegularExpression>(new RegularExpression("a\\b"), "{ \"$regex\" : "
                                                                                     + "\"a\\\\b\", "
                                                                                     + "\"$options\" : \"\" "
                                                                                     + "}"),
                      new TestData<RegularExpression>(new RegularExpression("a", "i"), "{ \"$regex\" : \"a\","
                                                                                       + " \"$options\" : \"i\""
                                                                                       + " }"),
                      new TestData<RegularExpression>(new RegularExpression("a", "m"), "{ \"$regex\" : \"a\","
                                                                                       + " \"$options\" : \"m\""
                                                                                       + " }"),
                      new TestData<RegularExpression>(new RegularExpression("a", "x"), "{ \"$regex\" : \"a\","
                                                                                       + " \"$options\" : \"x\""
                                                                                       + " }"),
                      new TestData<RegularExpression>(new RegularExpression("a", "s"), "{ \"$regex\" : \"a\","
                                                                                       + " \"$options\" : \"s\""
                                                                                       + " }"),
                      new TestData<RegularExpression>(new RegularExpression("a", "imxs"),
                                                     "{ \"$regex\" : \"a\"," + " \"$options\" : \"imxs\" }"));
        for (final TestData<RegularExpression> cur : tests) {
            stringWriter = new StringWriter();
            writer = new JsonWriter(stringWriter, new JsonWriterSettings(JsonOutputMode.Strict));
            writer.writeStartDocument();
            writer.writeRegularExpression("regex", cur.value);
            writer.writeEndDocument();
            final String expected = "{ \"regex\" : " + cur.expected + " }";
            assertEquals(expected, stringWriter.toString());
        }
    }

    @Test
    public void testString() {
        final List<TestData<String>> tests;
        tests = asList(new TestData<String>("", "\"\""), new TestData<String>(" ", "\" \""),
                      new TestData<String>("a", "\"a\""), new TestData<String>("ab", "\"ab\""),
                      new TestData<String>("abc", "\"abc\""),
                      new TestData<String>("abc\u0000def", "\"abc\\u0000def\""),
                      new TestData<String>("\\", "\"\\\\\""), new TestData<String>("\'", "\"'\""),
                      new TestData<String>("\"", "\"\\\"\""), new TestData<String>("\0", "\"\\u0000\""),
                      new TestData<String>("\b", "\"\\b\""), new TestData<String>("\f", "\"\\f\""),
                      new TestData<String>("\n", "\"\\n\""), new TestData<String>("\r", "\"\\r\""),
                      new TestData<String>("\t", "\"\\t\""), new TestData<String>("\u0080", "\"\\u0080\""),
                      new TestData<String>("\u0080\u0081", "\"\\u0080\\u0081\""),
                      new TestData<String>("\u0080\u0081\u0082", "\"\\u0080\\u0081\\u0082\""));
        for (final TestData<String> cur : tests) {
            stringWriter = new StringWriter();
            writer = new JsonWriter(stringWriter, new JsonWriterSettings(JsonOutputMode.Strict));
            writer.writeStartDocument();
            writer.writeString("str", cur.value);
            writer.writeEndDocument();
            final String expected = "{ \"str\" : " + cur.expected + " }";
            assertEquals(expected, stringWriter.toString());
        }
    }

    @Test
    public void testSymbol() {
        writer.writeStartDocument();
        writer.writeSymbol("symbol", "name");
        writer.writeEndDocument();
        final String expected = "{ \"symbol\" : { \"$symbol\" : \"name\" } }";
        assertEquals(expected, stringWriter.toString());
    }

    //
    @Test
    public void testTimestampStrict() {
        writer.writeStartDocument();
        writer.writeTimestamp("timestamp", new BSONTimestamp(1000, 1));
        writer.writeEndDocument();
        final String expected = "{ \"timestamp\" : { \"$timestamp\" : { \"t\" : 1000, \"i\" : 1 } } }";
        assertEquals(expected, stringWriter.toString());
    }

    @Test
    public void testTimestampShell() {
        writer = new JsonWriter(stringWriter, new JsonWriterSettings(JsonOutputMode.Shell));
        writer.writeStartDocument();
        writer.writeTimestamp("timestamp", new BSONTimestamp(1000, 1));
        writer.writeEndDocument();
        final String expected = "{ \"timestamp\" : Timestamp(1000, 1) }";
        assertEquals(expected, stringWriter.toString());
    }

    @Test
    public void testUndefined() {
        writer.writeStartDocument();
        writer.writeUndefined("undefined");
        writer.writeEndDocument();
        final String expected = "{ \"undefined\" : undefined }";
        assertEquals(expected, stringWriter.toString());
    }
}
