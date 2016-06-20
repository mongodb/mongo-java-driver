/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import org.bson.BsonRegularExpression;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class JsonScannerTest {

    @Test
    public void testEndOfFile() {
        String json = "\t ";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.END_OF_FILE, token.getType());
        assertEquals("<eof>", token.getValue());
        assertEquals(-1, buffer.read());
    }

    @Test
    public void testBeginObject() {
        String json = "\t {x";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.BEGIN_OBJECT, token.getType());
        assertEquals("{", token.getValue());
        assertEquals('x', buffer.read());
    }

    @Test
    public void testEndObject() {
        String json = "\t }x";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.END_OBJECT, token.getType());
        assertEquals("}", token.getValue());
        assertEquals('x', buffer.read());
    }

    @Test
    public void testBeginArray() {
        String json = "\t [x";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.BEGIN_ARRAY, token.getType());
        assertEquals("[", token.getValue());
        assertEquals('x', buffer.read());
    }

    @Test
    public void testEndArray() {
        String json = "\t ]x";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.END_ARRAY, token.getType());
        assertEquals("]", token.getValue());
        assertEquals('x', buffer.read());
    }

    @Test
    public void testParentheses() {
        String json = "\t (jj)x";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.LEFT_PAREN, token.getType());
        assertEquals("(", token.getValue());
        token = scanner.nextToken();
        token = scanner.nextToken();
        assertEquals(JsonTokenType.RIGHT_PAREN, token.getType());
        assertEquals('x', buffer.read());
    }


    @Test
    public void testNameSeparator() {
        String json = "\t :x";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.COLON, token.getType());
        assertEquals(":", token.getValue());
        assertEquals('x', buffer.read());
    }

    @Test
    public void testValueSeparator() {
        String json = "\t ,x";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.COMMA, token.getType());
        assertEquals(",", token.getValue());
        assertEquals('x', buffer.read());
    }

    @Test
    public void testEmptyString() {
        String json = "\t \"\"x";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.STRING, token.getType());
        assertEquals("", token.getValue());
        assertEquals('x', buffer.read());
    }

    @Test
    public void test1CharacterString() {
        String json = "\t \"1\"x";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.STRING, token.getType());
        assertEquals("1", token.getValue());
        assertEquals('x', buffer.read());
    }

    @Test
    public void test2CharacterString() {
        String json = "\t \"12\"x";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.STRING, token.getType());
        assertEquals("12", token.getValue());
        assertEquals('x', buffer.read());
    }

    @Test
    public void test3CharacterString() {
        String json = "\t \"123\"x";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.STRING, token.getType());
        assertEquals("123", token.getValue());
        assertEquals('x', buffer.read());
    }

    @Test
    public void testEscapeSequences() {
        String json = "\t \"x\\\"\\\\\\/\\b\\f\\n\\r\\t\\u0030y\"x";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.STRING, token.getType());
        assertEquals("x\"\\/\b\f\n\r\t0y", token.getValue());
        assertEquals('x', buffer.read());
    }


    @Test
    public void testTrue() {
        String json = "\t true,";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.UNQUOTED_STRING, token.getType());
        assertEquals("true", token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusInfinity() {
        String json = "\t -Infinity]";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.DOUBLE, token.getType());
        assertEquals(Double.NEGATIVE_INFINITY, token.getValue());
        assertEquals(']', buffer.read());
    }

    @Test
    public void testFalse() {
        String json = "\t false,";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.UNQUOTED_STRING, token.getType());
        assertEquals("false", token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testNull() {
        String json = "\t null,";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.UNQUOTED_STRING, token.getType());
        assertEquals("null", token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testUndefined() {
        String json = "\t undefined,";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.UNQUOTED_STRING, token.getType());
        assertEquals("undefined", token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testUnquotedStringWithSeparator() {
        String json = "\t name123:1";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.UNQUOTED_STRING, token.getType());
        assertEquals("name123", token.getValue());
        assertEquals(':', buffer.read());
    }

    @Test
    public void testUnquotedString() {
        String json = "name123";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.UNQUOTED_STRING, token.getType());
        assertEquals("name123", token.getValue());
        assertEquals(-1, buffer.read());
    }

    @Test
    public void testZero() {
        String json = "\t 0,";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.INT32, token.getType());
        assertEquals(0, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusZero() {
        String json = "\t -0,";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.INT32, token.getType());
        assertEquals(-0, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testOne() {
        String json = "\t 1,";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.INT32, token.getType());
        assertEquals(1, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusOne() {
        String json = "\t -1,";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.INT32, token.getType());
        assertEquals(-1, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testTwelve() {
        String json = "\t 12,";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.INT32, token.getType());
        assertEquals(12, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusTwelve() {
        String json = "\t -12,";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.INT32, token.getType());
        assertEquals(-12, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testZeroPointZero() {
        String json = "\t 0.0,";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.DOUBLE, token.getType());
        assertEquals(0.0, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusZeroPointZero() {
        String json = "\t -0.0,";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.DOUBLE, token.getType());
        assertEquals(-0.0, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testZeroExponentOne() {
        String json = "\t 0e1,";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.DOUBLE, token.getType());
        assertEquals(0e1, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusZeroExponentOne() {
        String json = "\t -0e1,";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.DOUBLE, token.getType());
        assertEquals(-0e1, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testZeroExponentMinusOne() {
        String json = "\t 0e-1,";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.DOUBLE, token.getType());
        assertEquals(0e-1, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusZeroExponentMinusOne() {
        String json = "\t -0e-1,";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.DOUBLE, token.getType());
        assertEquals(-0e-1, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testOnePointTwo() {
        String json = "\t 1.2,";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.DOUBLE, token.getType());
        assertEquals(1.2, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusOnePointTwo() {
        String json = "\t -1.2,";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.DOUBLE, token.getType());
        assertEquals(-1.2, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testOneExponentTwelve() {
        String json = "\t 1e12,";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.DOUBLE, token.getType());
        assertEquals(1e12, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusZeroExponentTwelve() {
        String json = "\t -1e12,";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.DOUBLE, token.getType());
        assertEquals(-1e12, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testOneExponentMinuesTwelve() {
        String json = "\t 1e-12,";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.DOUBLE, token.getType());
        assertEquals(1e-12, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusZeroExponentMinusTwelve() {
        String json = "\t -1e-12,";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.DOUBLE, token.getType());
        assertEquals(-1e-12, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testRegularExpressionEmpty() {
        String json = "\t //,";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.REGULAR_EXPRESSION, token.getType());

        BsonRegularExpression regularExpression = token.getValue(BsonRegularExpression.class);

        assertEquals("", regularExpression.getPattern());
        assertEquals("", regularExpression.getOptions());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testRegularExpressionPattern() {
        String json = "\t /pattern/,";

        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.REGULAR_EXPRESSION, token.getType());
        assertEquals("pattern", token.getValue(BsonRegularExpression.class).getPattern());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testRegularExpressionPatternAndOptions() {
        String json = "\t /pattern/im,";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.REGULAR_EXPRESSION, token.getType());

        BsonRegularExpression regularExpression = token.getValue(BsonRegularExpression.class);
        assertEquals("pattern", regularExpression.getPattern());
        assertEquals("im", regularExpression.getOptions());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testRegularExpressionPatternAndEscapeSequence() {
        String json = "\t /patte\\.n/,";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
        assertEquals(JsonTokenType.REGULAR_EXPRESSION, token.getType());
        assertEquals("patte\\.n", token.getValue(BsonRegularExpression.class).getPattern());
        assertEquals(',', buffer.read());
    }

    @Test(expected = JsonParseException.class)
    public void testInvalidRegularExpression() {
        String json = "\t /pattern/nsk,";
        JsonBuffer buffer = new JsonBuffer(json);
        JsonScanner scanner = new JsonScanner(buffer);
        JsonToken token = scanner.nextToken();
    }

    @Test(expected = JsonParseException.class)
    public void testInvalidInput() {
        String json = "\t &&";
        JsonScanner scanner = new JsonScanner(json);
        scanner.nextToken();
    }

    @Test(expected = JsonParseException.class)
    public void testInvalidNumber() {
        String json = "\t 123a]";
        JsonScanner scanner = new JsonScanner(json);
        scanner.nextToken();
    }

    @Test(expected = JsonParseException.class)
    public void testInvalidInfinity() {
        String json = "\t -Infinnity]";
        JsonScanner scanner = new JsonScanner(json);
        scanner.nextToken();
    }
}

