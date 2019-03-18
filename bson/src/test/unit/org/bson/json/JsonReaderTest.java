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

import org.bson.AbstractBsonReader;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonDbPointer;
import org.bson.BsonReaderMark;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.BsonType;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class JsonReaderTest {
    @Test
    public void testArrayEmpty() {
        String json = "[]";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.ARRAY, bsonReader.readBsonType());
                bsonReader.readStartArray();
                assertEquals(BsonType.END_OF_DOCUMENT, bsonReader.readBsonType());
                bsonReader.readEndArray();
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testArrayOneElement() {
        String json = "[1]";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.ARRAY, bsonReader.readBsonType());
                bsonReader.readStartArray();
                assertEquals(BsonType.INT32, bsonReader.readBsonType());
                assertEquals(1, bsonReader.readInt32());
                assertEquals(BsonType.END_OF_DOCUMENT, bsonReader.readBsonType());
                bsonReader.readEndArray();
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testArrayTwoElements() {
        String json = "[1, 2]";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.ARRAY, bsonReader.readBsonType());
                bsonReader.readStartArray();
                assertEquals(BsonType.INT32, bsonReader.readBsonType());
                assertEquals(1, bsonReader.readInt32());
                assertEquals(BsonType.INT32, bsonReader.readBsonType());
                assertEquals(2, bsonReader.readInt32());
                assertEquals(BsonType.END_OF_DOCUMENT, bsonReader.readBsonType());
                bsonReader.readEndArray();
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testBooleanFalse() {
        String json = "false";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.BOOLEAN, bsonReader.readBsonType());
                assertFalse(bsonReader.readBoolean());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testBooleanTrue() {
        String json = "true";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.BOOLEAN, bsonReader.readBsonType());
                assertTrue(bsonReader.readBoolean());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testDateTimeMinBson() {
        String json = "new Date(-9223372036854775808)";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.DATE_TIME, bsonReader.readBsonType());
                assertEquals(-9223372036854775808L, bsonReader.readDateTime());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testDateTimeMaxBson() {
        String json = "new Date(9223372036854775807)";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.DATE_TIME, bsonReader.readBsonType());
                long k = bsonReader.readDateTime();
                assertEquals(9223372036854775807L, k);
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testDateTimeShell() {
        String json = "ISODate(\"1970-01-01T00:00:00Z\")";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.DATE_TIME, bsonReader.readBsonType());
                assertEquals(0, bsonReader.readDateTime());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testDateTimeShellWith24HourTimeSpecification() {
        String json = "ISODate(\"2013-10-04T12:07:30.443Z\")";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.DATE_TIME, bsonReader.readBsonType());
                assertEquals(1380888450443L, bsonReader.readDateTime());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testDateTimeStrict() {
        String json = "{ \"$date\" : 0 }";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.DATE_TIME, bsonReader.readBsonType());
                assertEquals(0, bsonReader.readDateTime());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testNestedDateTimeStrict() {
        String json = "{d1 : { \"$date\" : 0 }, d2 : { \"$date\" : 1 } }";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                bsonReader.readStartDocument();
                assertEquals(0L, bsonReader.readDateTime("d1"));
                assertEquals(1L, bsonReader.readDateTime("d2"));
                bsonReader.readEndDocument();
                return null;
            }
        });
    }

    @Test
    public void testDateTimeISOString() {
        String json = "{ \"$date\" : \"2015-04-16T14:55:57.626Z\" }";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.DATE_TIME, bsonReader.readBsonType());
                assertEquals(1429196157626L, bsonReader.readDateTime());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testDateTimeISOStringWithTimeOffset() {
        String json = "{ \"$date\" : \"2015-04-16T16:55:57.626+02:00\" }";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.DATE_TIME, bsonReader.readBsonType());
                assertEquals(1429196157626L, bsonReader.readDateTime());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testDateTimeTengen() {
        String json = "new Date(0)";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.DATE_TIME, bsonReader.readBsonType());
                assertEquals(0, bsonReader.readDateTime());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testDocumentEmpty() {
        String json = "{ }";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.DOCUMENT, bsonReader.readBsonType());
                bsonReader.readStartDocument();
                assertEquals(BsonType.END_OF_DOCUMENT, bsonReader.readBsonType());
                bsonReader.readEndDocument();
                return null;
            }
        });
    }

    @Test
    public void testDocumentNested() {
        String json = "{ \"a\" : { \"x\" : 1 }, \"y\" : 2 }";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
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
                return null;
            }
        });
    }

    @Test
    public void testDocumentOneElement() {
        String json = "{ \"x\" : 1 }";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.DOCUMENT, bsonReader.readBsonType());
                bsonReader.readStartDocument();
                assertEquals(BsonType.INT32, bsonReader.readBsonType());
                assertEquals("x", bsonReader.readName());
                assertEquals(1, bsonReader.readInt32());
                assertEquals(BsonType.END_OF_DOCUMENT, bsonReader.readBsonType());
                bsonReader.readEndDocument();
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testDocumentTwoElements() {
        String json = "{ \"x\" : 1, \"y\" : 2 }";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
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
                return null;
            }
        });
    }

    @Test
    public void testDouble() {
        String json = "1.5";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.DOUBLE, bsonReader.readBsonType());
                assertEquals(1.5, bsonReader.readDouble(), 0);
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testHexData() {
        final byte[] expectedBytes = new byte[]{0x01, 0x23};
        String json = "HexData(0, \"0123\")";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.BINARY, bsonReader.readBsonType());
                BsonBinary binary = bsonReader.readBinaryData();
                assertArrayEquals(expectedBytes, binary.getData());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testHexDataWithNew() {
        final byte[] expectedBytes = new byte[]{0x01, 0x23};
        String json = "new HexData(0, \"0123\")";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.BINARY, bsonReader.readBsonType());
                BsonBinary binary = bsonReader.readBinaryData();
                assertArrayEquals(expectedBytes, binary.getData());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testInt32() {
        String json = "123";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.INT32, bsonReader.readBsonType());
                assertEquals(123, bsonReader.readInt32());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });

    }

    @Test
    public void testInt64() {
        String json = String.valueOf(Long.MAX_VALUE);
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.INT64, bsonReader.readBsonType());
                assertEquals(Long.MAX_VALUE, bsonReader.readInt64());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testNumberLongExtendedJson() {
        String json = "{\"$numberLong\":\"123\"}";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.INT64, bsonReader.readBsonType());
                assertEquals(123, bsonReader.readInt64());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testNumberLong() {
        List<String> jsonTexts = asList(
                "NumberLong(123)",
                "NumberLong(\"123\")",
                "new NumberLong(123)",
                "new NumberLong(\"123\")");
        for (String json : jsonTexts) {
            testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
                @Override
                public Void apply(final AbstractBsonReader bsonReader) {
                    assertEquals(BsonType.INT64, bsonReader.readBsonType());
                    assertEquals(123, bsonReader.readInt64());
                    assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                    return null;
                }
            });
        }
    }

    @Test
    public void testNumberInt() {
        List<String> jsonTexts = asList(
                "NumberInt(123)",
                "NumberInt(\"123\")",
                "new NumberInt(123)",
                "new NumberInt(\"123\")");
        for (String json : jsonTexts) {
            testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
                @Override
                public Void apply(final AbstractBsonReader bsonReader) {
                    assertEquals(BsonType.INT32, bsonReader.readBsonType());
                    assertEquals(123, bsonReader.readInt32());
                    assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                    return null;
                }
            });
        }
    }

    @Test
    public void testDecimal128StringConstructor() {
        String json = "NumberDecimal(\"314E-2\")";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.DECIMAL128, bsonReader.readBsonType());
                assertEquals(Decimal128.parse("314E-2"), bsonReader.readDecimal128());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testDecimal128Int32Constructor() {
        String json = "NumberDecimal(" + Integer.MAX_VALUE + ")";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.DECIMAL128, bsonReader.readBsonType());
                assertEquals(new Decimal128(Integer.MAX_VALUE), bsonReader.readDecimal128());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testDecimal128Int64Constructor() {
        String json = "NumberDecimal(" + Long.MAX_VALUE + ")";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.DECIMAL128, bsonReader.readBsonType());
                assertEquals(new Decimal128(Long.MAX_VALUE), bsonReader.readDecimal128());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testDecimal128DoubleConstructor() {
        String json = "NumberDecimal(" + 1.0 + ")";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.DECIMAL128, bsonReader.readBsonType());
                assertEquals(Decimal128.parse("1"), bsonReader.readDecimal128());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testDecimal128BooleanConstructor() {
        String json = "NumberDecimal(true)";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                try {
                    bsonReader.readBsonType();
                    fail("Should fail to parse NumberDecimal constructor with a string");
                } catch (JsonParseException e) {
                    // all good
                }
                return null;
            }
        });
    }

    @Test
    public void testDecimal128WithNew() {
        String json = "new NumberDecimal(\"314E-2\")";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.DECIMAL128, bsonReader.readBsonType());
                assertEquals(Decimal128.parse("314E-2"), bsonReader.readDecimal128());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testDecimal128ExtendedJson() {
        String json = "{\"$numberDecimal\":\"314E-2\"}";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.DECIMAL128, bsonReader.readBsonType());
                assertEquals(Decimal128.parse("314E-2"), bsonReader.readDecimal128());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testJavaScript() {
        String json = "{ \"$code\" : \"function f() { return 1; }\" }";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.JAVASCRIPT, bsonReader.readBsonType());
                assertEquals("function f() { return 1; }", bsonReader.readJavaScript());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testJavaScriptWithScope() {
        String json = "{\"codeWithScope\": { \"$code\" : \"function f() { return n; }\", \"$scope\" : { \"n\" : 1 } } }";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
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
                return null;
            }
        });
    }

    @Test
    public void testMaxKey() {
        for (String maxKeyJson : asList("{ \"$maxKey\" : 1 }", "MaxKey", "MaxKey()", "new MaxKey", "new MaxKey()")) {
            String json = "{ maxKey : " + maxKeyJson + " }";
            testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
                @Override
                public Void apply(final AbstractBsonReader bsonReader) {
                    bsonReader.readStartDocument();
                    assertEquals("maxKey", bsonReader.readName());
                    assertEquals(BsonType.MAX_KEY, bsonReader.getCurrentBsonType());
                    bsonReader.readMaxKey();
                    bsonReader.readEndDocument();
                    assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                    return null;
                }
            });
        }
    }

    @Test
    public void testMinKey() {
        for (String minKeyJson : asList("{ \"$minKey\" : 1 }", "MinKey", "MinKey()", "new MinKey", "new MinKey()")) {
            String json = "{ minKey : " + minKeyJson + " }";
            testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
                @Override
                public Void apply(final AbstractBsonReader bsonReader) {
                    bsonReader.readStartDocument();
                    assertEquals("minKey", bsonReader.readName());
                    assertEquals(BsonType.MIN_KEY, bsonReader.getCurrentBsonType());
                    bsonReader.readMinKey();
                    bsonReader.readEndDocument();
                    assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                    return null;
                }
            });
        }
    }

    @Test
    public void testNestedArray() {
        String json = "{ \"a\" : [1, 2] }";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
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
                return null;
            }
        });
    }

    @Test
    public void testNestedDocument() {
        String json = "{ \"a\" : { \"b\" : 1, \"c\" : 2 } }";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
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
                return null;
            }
        });
    }

    @Test
    public void testNull() {
        String json = "null";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.NULL, bsonReader.readBsonType());
                bsonReader.readNull();
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testObjectIdShell() {
        String json = "ObjectId(\"4d0ce088e447ad08b4721a37\")";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.OBJECT_ID, bsonReader.readBsonType());
                ObjectId objectId = bsonReader.readObjectId();
                assertEquals("4d0ce088e447ad08b4721a37", objectId.toString());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testObjectIdWithNew() {
        String json = "new ObjectId(\"4d0ce088e447ad08b4721a37\")";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.OBJECT_ID, bsonReader.readBsonType());
                ObjectId objectId = bsonReader.readObjectId();
                assertEquals("4d0ce088e447ad08b4721a37", objectId.toString());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testObjectIdStrict() {
        String json = "{ \"$oid\" : \"4d0ce088e447ad08b4721a37\" }";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.OBJECT_ID, bsonReader.readBsonType());
                ObjectId objectId = bsonReader.readObjectId();
                assertEquals("4d0ce088e447ad08b4721a37", objectId.toString());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testObjectIdTenGen() {
        String json = "ObjectId(\"4d0ce088e447ad08b4721a37\")";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.OBJECT_ID, bsonReader.readBsonType());
                ObjectId objectId = bsonReader.readObjectId();
                assertEquals("4d0ce088e447ad08b4721a37", objectId.toString());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testRegularExpressionShell() {
        String json = "/pattern/imxs";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.REGULAR_EXPRESSION, bsonReader.readBsonType());
                BsonRegularExpression regex = bsonReader.readRegularExpression();
                assertEquals("pattern", regex.getPattern());
                assertEquals("imsx", regex.getOptions());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testRegularExpressionStrict() {
        String json = "{ \"$regex\" : \"pattern\", \"$options\" : \"imxs\" }";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.REGULAR_EXPRESSION, bsonReader.readBsonType());
                BsonRegularExpression regex = bsonReader.readRegularExpression();
                assertEquals("pattern", regex.getPattern());
                assertEquals("imsx", regex.getOptions());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testRegularExpressionCanonical() {
        String json = "{ \"$regularExpression\" : { \"pattern\" : \"pattern\", \"options\" : \"imxs\" }}";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.REGULAR_EXPRESSION, bsonReader.readBsonType());
                BsonRegularExpression regex = bsonReader.readRegularExpression();
                assertEquals("pattern", regex.getPattern());
                assertEquals("imsx", regex.getOptions());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testRegularExpressionQuery() {
        String json = "{ \"$regex\" : { \"$regularExpression\" : { \"pattern\" : \"pattern\", \"options\" : \"imxs\" }}}";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                bsonReader.readStartDocument();
                BsonRegularExpression regex = bsonReader.readRegularExpression("$regex");
                assertEquals("pattern", regex.getPattern());
                assertEquals("imsx", regex.getOptions());
                bsonReader.readEndDocument();
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testRegularExpressionQueryShell() {
        String json = "{ \"$regex\" : /pattern/imxs}";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                bsonReader.readStartDocument();
                BsonRegularExpression regex = bsonReader.readRegularExpression("$regex");
                assertEquals("pattern", regex.getPattern());
                assertEquals("imsx", regex.getOptions());
                bsonReader.readEndDocument();
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testString() {
        final String str = "abc";
        final String json = '"' + str + '"';
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.STRING, bsonReader.readBsonType());
                assertEquals(str, bsonReader.readString());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });

        final String str2 = "\ud806\udc5c";
        final String json2 = '"' + str2 + '"';
        testStringAndStream(json2, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.STRING, bsonReader.readBsonType());
                assertEquals(str2, bsonReader.readString());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });

        final String str3 = "\\ud806\\udc5c";
        final String json3 = '"' + str3 + '"';
        testStringAndStream(json3, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.STRING, bsonReader.readBsonType());
                assertEquals("\ud806\udc5c", bsonReader.readString());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });

        final String str4 = "꼢𑡜ᳫ鉠鮻罖᧭䆔瘉";
        final String json4 = '"' + str4 + '"';
        testStringAndStream(json4, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.STRING, bsonReader.readBsonType());
                assertEquals(str4, bsonReader.readString());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testStringEmpty() {
        String json = "\"\"";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.STRING, bsonReader.readBsonType());
                assertEquals("", bsonReader.readString());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testSymbol() {
        String json = "{ \"$symbol\" : \"symbol\" }";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.SYMBOL, bsonReader.readBsonType());
                assertEquals("symbol", bsonReader.readSymbol());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testTimestampStrict() {
        String json = "{ \"$timestamp\" : { \"t\" : 1234, \"i\" : 1 } }";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.TIMESTAMP, bsonReader.readBsonType());
                assertEquals(new BsonTimestamp(1234, 1), bsonReader.readTimestamp());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testTimestampStrictWithOutOfOrderFields() {
        String json = "{ \"$timestamp\" : { \"i\" : 1, \"t\" : 1234 } }";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.TIMESTAMP, bsonReader.readBsonType());
                assertEquals(new BsonTimestamp(1234, 1), bsonReader.readTimestamp());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testTimestampShell() {
        String json = "Timestamp(1234, 1)";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.TIMESTAMP, bsonReader.readBsonType());
                assertEquals(new BsonTimestamp(1234, 1), bsonReader.readTimestamp());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testUndefined() {
        String json = "undefined";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.UNDEFINED, bsonReader.readBsonType());
                bsonReader.readUndefined();
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testUndefinedExtended() {
        String json = "{ \"$undefined\" : true }";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.UNDEFINED, bsonReader.readBsonType());
                bsonReader.readUndefined();
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test(expected = IllegalStateException.class)
    public void testClosedState() {
        final AbstractBsonReader bsonReader = new JsonReader("");
        bsonReader.close();
        bsonReader.readBinaryData();
    }

    @Test
    public void testEndOfFile0() {
        String json = "{";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.DOCUMENT, bsonReader.readBsonType());
                bsonReader.readStartDocument();
                bsonReader.readBsonType();
                return null;
            }
        }, JsonParseException.class);
    }

    @Test
    public void testEndOfFile1() {
        String json = "{ test : ";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.DOCUMENT, bsonReader.readBsonType());
                bsonReader.readStartDocument();
                bsonReader.readBsonType();
                return null;
            }
        }, JsonParseException.class);
    }

    @Test
    public void testLegacyBinary() {
        String json = "{ \"$binary\" : \"AQID\", \"$type\" : \"0\" }";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.BINARY, bsonReader.readBsonType());
                BsonBinary binary = bsonReader.readBinaryData();
                assertEquals(BsonBinarySubType.BINARY.getValue(), binary.getType());
                assertArrayEquals(new byte[]{1, 2, 3}, binary.getData());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testLegacyBinaryWithNumericType() {
        String json = "{ \"$binary\" : \"AQID\", \"$type\" : 0 }";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.BINARY, bsonReader.readBsonType());
                BsonBinary binary = bsonReader.readBinaryData();
                assertEquals(BsonBinarySubType.BINARY.getValue(), binary.getType());
                assertArrayEquals(new byte[]{1, 2, 3}, binary.getData());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testLegacyUserDefinedBinary() {
        String json = "{ \"$binary\" : \"AQID\", \"$type\" : \"80\" }";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.BINARY, bsonReader.readBsonType());
                BsonBinary binary = bsonReader.readBinaryData();
                assertEquals(BsonBinarySubType.USER_DEFINED.getValue(), binary.getType());
                assertArrayEquals(new byte[]{1, 2, 3}, binary.getData());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testLegacyUserDefinedBinaryWithKeyOrderReversed() {
        String json = "{ \"$type\" : \"80\", \"$binary\" : \"AQID\" }";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.BINARY, bsonReader.readBsonType());
                BsonBinary binary = bsonReader.readBinaryData();
                assertEquals(BsonBinarySubType.USER_DEFINED.getValue(), binary.getType());
                assertArrayEquals(new byte[]{1, 2, 3}, binary.getData());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testLegacyUserDefinedBinaryWithNumericType() {
        String json = "{ \"$binary\" : \"AQID\", \"$type\" : 128 }";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.BINARY, bsonReader.readBsonType());
                BsonBinary binary = bsonReader.readBinaryData();
                assertEquals(BsonBinarySubType.USER_DEFINED.getValue(), binary.getType());
                assertArrayEquals(new byte[]{1, 2, 3}, binary.getData());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testCanonicalExtendedJsonBinary() {
        String json = "{ \"$binary\" : { \"base64\" : \"AQID\", \"subType\" : \"80\" } }";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.BINARY, bsonReader.readBsonType());
                BsonBinary binary = bsonReader.readBinaryData();
                assertEquals(BsonBinarySubType.USER_DEFINED.getValue(), binary.getType());
                assertArrayEquals(new byte[]{1, 2, 3}, binary.getData());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testCanonicalExtendedJsonBinaryWithKeysReversed() {
        String json = "{ \"$binary\" : { \"subType\" : \"80\", \"base64\" : \"AQID\" } }";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.BINARY, bsonReader.readBsonType());
                BsonBinary binary = bsonReader.readBinaryData();
                assertEquals(BsonBinarySubType.USER_DEFINED.getValue(), binary.getType());
                assertArrayEquals(new byte[]{1, 2, 3}, binary.getData());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testCanonicalExtendedJsonBinaryWithIncorrectFirstKey() {
        String json = "{ \"$binary\" : { \"badKey\" : \"80\", \"base64\" : \"AQID\" } }";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.BINARY, bsonReader.readBsonType());
                return null;
            }
        }, JsonParseException.class);
    }

    @Test
    public void testInfinity() {
        String json = "{ \"value\" : Infinity }";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                bsonReader.readStartDocument();
                assertEquals(BsonType.DOUBLE, bsonReader.readBsonType());
                bsonReader.readName();
                assertEquals(Double.POSITIVE_INFINITY, bsonReader.readDouble(), 0.0001);
                return null;
            }
        });
    }

    @Test
    public void testNaN() {
        String json = "{ \"value\" : NaN }";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                bsonReader.readStartDocument();
                assertEquals(BsonType.DOUBLE, bsonReader.readBsonType());
                bsonReader.readName();
                assertEquals(Double.NaN, bsonReader.readDouble(), 0.0001);
                return null;
            }
        });
    }

    @Test
    public void testBinData() {
        String json = "{ \"a\" : BinData(3, AQID) }";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                bsonReader.readStartDocument();
                assertEquals(BsonType.BINARY, bsonReader.readBsonType());
                BsonBinary binary = bsonReader.readBinaryData();
                assertEquals(3, binary.getType());
                assertArrayEquals(new byte[]{1, 2, 3}, binary.getData());
                bsonReader.readEndDocument();
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testBinDataUserDefined() {
        String json = "{ \"a\" : BinData(128, AQID) }";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                bsonReader.readStartDocument();
                assertEquals(BsonType.BINARY, bsonReader.readBsonType());
                BsonBinary binary = bsonReader.readBinaryData();
                assertEquals(BsonBinarySubType.USER_DEFINED.getValue(), binary.getType());
                assertArrayEquals(new byte[]{1, 2, 3}, binary.getData());
                bsonReader.readEndDocument();
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testBinDataWithNew() {
        String json = "{ \"a\" : new BinData(3, AQID) }";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                bsonReader.readStartDocument();
                assertEquals(BsonType.BINARY, bsonReader.readBsonType());
                BsonBinary binary = bsonReader.readBinaryData();
                assertEquals(3, binary.getType());
                assertArrayEquals(new byte[]{1, 2, 3}, binary.getData());
                bsonReader.readEndDocument();
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testBinDataQuoted() {
        String json = "{ \"a\" : BinData(3, \"AQIDBA==\") }";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                bsonReader.readStartDocument();
                assertEquals(BsonType.BINARY, bsonReader.readBsonType());
                BsonBinary binary = bsonReader.readBinaryData();
                assertEquals(3, binary.getType());
                assertArrayEquals(new byte[]{1, 2, 3, 4}, binary.getData());
                bsonReader.readEndDocument();
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testDateWithNumbers() {
        String json = "new Date(1988, 06, 13 , 22 , 1)";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.DATE_TIME, bsonReader.readBsonType());
                assertEquals(584834460000L, bsonReader.readDateTime());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testDateTimeConstructorWithNew() {
        String json = "new Date(\"Sat Jul 13 2013 11:10:05 UTC\")";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.DATE_TIME, bsonReader.readBsonType());
                assertEquals(1373713805000L, bsonReader.readDateTime());
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testEmptyDateTimeConstructorWithNew() {
        final long currentTime = new Date().getTime();
        String json = "new Date()";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.DATE_TIME, bsonReader.readBsonType());
                assertTrue(bsonReader.readDateTime() >= currentTime);
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testDateTimeWithOutNew() {
        final long currentTime = currentTimeWithoutMillis();
        String json = "Date()";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.STRING, bsonReader.readBsonType());
                assertTrue(dateStringToTime(bsonReader.readString()) >= currentTime);
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testDateTimeWithOutNewContainingJunk() {
        final long currentTime = currentTimeWithoutMillis();
        String json = "Date({ok: 1}, 1234)";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.STRING, bsonReader.readBsonType());
                assertTrue(dateStringToTime(bsonReader.readString()) >= currentTime);
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testEmptyISODateTimeConstructorWithNew() {
        final long currentTime = new Date().getTime();
        String json = "new ISODate()";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.DATE_TIME, bsonReader.readBsonType());
                assertTrue(bsonReader.readDateTime() >= currentTime);
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testEmptyISODateTimeConstructor() {
        final long currentTime = new Date().getTime();
        String json = "ISODate()";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.DATE_TIME, bsonReader.readBsonType());
                assertTrue(bsonReader.readDateTime() >= currentTime);
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testRegExp() {
        String json = "RegExp(\"abc\",\"im\")";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.REGULAR_EXPRESSION, bsonReader.readBsonType());
                BsonRegularExpression regularExpression = bsonReader.readRegularExpression();
                assertEquals("abc", regularExpression.getPattern());
                assertEquals("im", regularExpression.getOptions());
                return null;
            }
        });
    }

    @Test
    public void testRegExpWithNew() {
        String json = "new RegExp(\"abc\",\"im\")";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.REGULAR_EXPRESSION, bsonReader.readBsonType());
                BsonRegularExpression regularExpression = bsonReader.readRegularExpression();
                assertEquals("abc", regularExpression.getPattern());
                assertEquals("im", regularExpression.getOptions());
                return null;
            }
        });
    }

    @Test
    public void testSkip() {
        String json = "{ \"a\" : 2 }";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                bsonReader.readStartDocument();
                bsonReader.readBsonType();
                bsonReader.skipName();
                bsonReader.skipValue();
                assertEquals(BsonType.END_OF_DOCUMENT, bsonReader.readBsonType());
                bsonReader.readEndDocument();
                assertEquals(AbstractBsonReader.State.DONE, bsonReader.getState());
                return null;
            }
        });
    }

    @Test
    public void testDBPointer() {
        String json = "DBPointer(\"b\",\"5209296cd6c4e38cf96fffdc\")";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.DB_POINTER, bsonReader.readBsonType());
                BsonDbPointer dbPointer = bsonReader.readDBPointer();
                assertEquals("b", dbPointer.getNamespace());
                assertEquals(new ObjectId("5209296cd6c4e38cf96fffdc"), dbPointer.getId());
                return null;
            }
        });
    }

    @Test
    public void testDBPointerWithNew() {
        String json = "new DBPointer(\"b\",\"5209296cd6c4e38cf96fffdc\")";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                assertEquals(BsonType.DB_POINTER, bsonReader.readBsonType());
                BsonDbPointer dbPointer = bsonReader.readDBPointer();
                assertEquals("b", dbPointer.getNamespace());
                assertEquals(new ObjectId("5209296cd6c4e38cf96fffdc"), dbPointer.getId());
                return null;
            }
        });
    }

    @Test
    public void testMultipleMarks() {
        String json = "{a : { b : 1 }}";
        testStringAndStream(json, new Function<AbstractBsonReader, Void>() {
            @Override
            public Void apply(final AbstractBsonReader bsonReader) {
                bsonReader.readStartDocument();
                BsonReaderMark markOne = bsonReader.getMark();
                bsonReader.readName("a");
                bsonReader.readStartDocument();
                BsonReaderMark markTwo = bsonReader.getMark();
                bsonReader.readName("b");
                bsonReader.readInt32();
                bsonReader.readEndDocument();
                markTwo.reset();
                bsonReader.readName("b");
                markOne.reset();
                bsonReader.readName("a");
                return null;
            }
        });
    }

    @Test
    public void testTwoDocuments() {
        Reader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream("{a : 1}{b : 1}".getBytes())));

        JsonReader jsonReader = new JsonReader(reader);
        jsonReader.readStartDocument();
        jsonReader.readName("a");
        jsonReader.readInt32();
        jsonReader.readEndDocument();

        jsonReader = new JsonReader(reader);
        jsonReader.readStartDocument();
        jsonReader.readName("b");
        jsonReader.readInt32();
        jsonReader.readEndDocument();
    }

    private void testStringAndStream(final String json, final Function<AbstractBsonReader, Void> testFunc,
                                     final Class<? extends RuntimeException> exClass) {
        try {
            testFunc.apply(new JsonReader(json));
        } catch (final RuntimeException e) {
            if (exClass == null) {
                throw e;
            }
            assertEquals(exClass, e.getClass());
        }
        try {
            testFunc.apply(new JsonReader(new InputStreamReader(new ByteArrayInputStream(json.getBytes()))));
        } catch (final RuntimeException e) {
            if (exClass == null) {
                throw e;
            }
            assertEquals(exClass, e.getClass());
        }
    }

    private void testStringAndStream(final String json, final Function<AbstractBsonReader, Void> testFunc) {
        testStringAndStream(json, testFunc, null);
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
