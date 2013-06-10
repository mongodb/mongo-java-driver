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

import org.bson.types.RegularExpression;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class JSONScannerTest {

    @Test
    public void testEndOfFile() {
        final String json = "\t ";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.END_OF_FILE, token.getType());
        assertEquals("<eof>", token.getValue());
        assertEquals(-1, buffer.read());
    }

    @Test
    public void testBeginObject() {
        final String json = "\t {x";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.BEGIN_OBJECT, token.getType());
        assertEquals("{", token.getValue());
        assertEquals('x', buffer.read());
    }

    @Test
    public void testEndObject() {
        final String json = "\t }x";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.END_OBJECT, token.getType());
        assertEquals("}", token.getValue());
        assertEquals('x', buffer.read());
    }

    @Test
    public void testBeginArray() {
        final String json = "\t [x";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.BEGIN_ARRAY, token.getType());
        assertEquals("[", token.getValue());
        assertEquals('x', buffer.read());
    }

    @Test
    public void testEndArray() {
        final String json = "\t ]x";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.END_ARRAY, token.getType());
        assertEquals("]", token.getValue());
        assertEquals('x', buffer.read());
    }

    @Test
    public void testParentheses() {
        final String json = "\t (jj)x";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.LEFT_PAREN, token.getType());
        assertEquals("(", token.getValue());
        token = scanner.nextToken();
        token = scanner.nextToken();
        assertEquals(JSONTokenType.RIGHT_PAREN, token.getType());
        assertEquals('x', buffer.read());
    }


    @Test
    public void testNameSeparator() {
        final String json = "\t :x";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.COLON, token.getType());
        assertEquals(":", token.getValue());
        assertEquals('x', buffer.read());
    }

    @Test
    public void testValueSeparator() {
        final String json = "\t ,x";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.COMMA, token.getType());
        assertEquals(",", token.getValue());
        assertEquals('x', buffer.read());
    }

    @Test
    public void testEmptyString() {
        final String json = "\t \"\"x";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.STRING, token.getType());
        assertEquals("", token.getValue());
        assertEquals('x', buffer.read());
    }

    @Test
    public void test1CharacterString() {
        final String json = "\t \"1\"x";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.STRING, token.getType());
        assertEquals("1", token.getValue());
        assertEquals('x', buffer.read());
    }

    @Test
    public void test2CharacterString() {
        final String json = "\t \"12\"x";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.STRING, token.getType());
        assertEquals("12", token.getValue());
        assertEquals('x', buffer.read());
    }

    @Test
    public void test3CharacterString() {
        final String json = "\t \"123\"x";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.STRING, token.getType());
        assertEquals("123", token.getValue());
        assertEquals('x', buffer.read());
    }

    @Test
    public void testEscapeSequences() {
        final String json = "\t \"x\\\"\\\\\\/\\b\\f\\n\\r\\t\\u0030y\"x";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.STRING, token.getType());
        assertEquals("x\"\\/\b\f\n\r\t0y", token.getValue());
        assertEquals('x', buffer.read());
    }


    @Test
    public void testTrue() {
        final String json = "\t true,";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.UNQUOTED_STRING, token.getType());
        assertEquals("true", token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusInfinity() {
        final String json = "\t -Infinity]";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.DOUBLE, token.getType());
        assertEquals(Double.NEGATIVE_INFINITY, token.getValue());
        assertEquals(']', buffer.read());
    }

    @Test
    public void testFalse() {
        final String json = "\t false,";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.UNQUOTED_STRING, token.getType());
        assertEquals("false", token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testNull() {
        final String json = "\t null,";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.UNQUOTED_STRING, token.getType());
        assertEquals("null", token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testUndefined() {
        final String json = "\t undefined,";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.UNQUOTED_STRING, token.getType());
        assertEquals("undefined", token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testUnquotedStringWithSeparator() {
        final String json = "\t name123:1";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.UNQUOTED_STRING, token.getType());
        assertEquals("name123", token.getValue());
        assertEquals(':', buffer.read());
    }

    @Test
    public void testUnquotedString() {
        final String json = "name123";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.UNQUOTED_STRING, token.getType());
        assertEquals("name123", token.getValue());
        assertEquals(-1, buffer.read());
    }

    @Test
    public void testZero() {
        final String json = "\t 0,";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.INT32, token.getType());
        assertEquals(0, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusZero() {
        final String json = "\t -0,";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.INT32, token.getType());
        assertEquals(-0, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testOne() {
        final String json = "\t 1,";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.INT32, token.getType());
        assertEquals(1, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusOne() {
        final String json = "\t -1,";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.INT32, token.getType());
        assertEquals(-1, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testTwelve() {
        final String json = "\t 12,";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.INT32, token.getType());
        assertEquals(12, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusTwelve() {
        final String json = "\t -12,";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.INT32, token.getType());
        assertEquals(-12, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testZeroPointZero() {
        final String json = "\t 0.0,";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.DOUBLE, token.getType());
        assertEquals(0.0, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusZeroPointZero() {
        final String json = "\t -0.0,";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.DOUBLE, token.getType());
        assertEquals(-0.0, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testZeroExponentOne() {
        final String json = "\t 0e1,";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.DOUBLE, token.getType());
        assertEquals(0e1, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusZeroExponentOne() {
        final String json = "\t -0e1,";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.DOUBLE, token.getType());
        assertEquals(-0e1, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testZeroExponentMinusOne() {
        final String json = "\t 0e-1,";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.DOUBLE, token.getType());
        assertEquals(0e-1, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusZeroExponentMinusOne() {
        final String json = "\t -0e-1,";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.DOUBLE, token.getType());
        assertEquals(-0e-1, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testOnePointTwo() {
        final String json = "\t 1.2,";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.DOUBLE, token.getType());
        assertEquals(1.2, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusOnePointTwo() {
        final String json = "\t -1.2,";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.DOUBLE, token.getType());
        assertEquals(-1.2, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testOneExponentTwelve() {
        final String json = "\t 1e12,";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.DOUBLE, token.getType());
        assertEquals(1e12, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusZeroExponentTwelve() {
        final String json = "\t -1e12,";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.DOUBLE, token.getType());
        assertEquals(-1e12, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testOneExponentMinuesTwelve() {
        final String json = "\t 1e-12,";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.DOUBLE, token.getType());
        assertEquals(1e-12, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusZeroExponentMinusTwelve() {
        final String json = "\t -1e-12,";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.DOUBLE, token.getType());
        assertEquals(-1e-12, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testRegularExpressionEmpty() {
        final String json = "\t //,";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.REGULAR_EXPRESSION, token.getType());

        final RegularExpression regularExpression = token.getValue(RegularExpression.class);

        assertEquals("", regularExpression.getPattern());
        assertEquals("", regularExpression.getOptions());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testRegularExpressionPattern() {
        final String json = "\t /pattern/,";

        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.REGULAR_EXPRESSION, token.getType());
        assertEquals("pattern", token.getValue(RegularExpression.class).getPattern());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testRegularExpressionPatternAndOptions() {
        final String json = "\t /pattern/im,";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.REGULAR_EXPRESSION, token.getType());

        final RegularExpression regularExpression = token.getValue(RegularExpression.class);
        assertEquals("pattern", regularExpression.getPattern());
        assertEquals("im", regularExpression.getOptions());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testRegularExpressionPatternAndEscapeSequence() {
        final String json = "\t /patte\\.n/,";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.REGULAR_EXPRESSION, token.getType());
        assertEquals("patte\\.n", token.getValue(RegularExpression.class).getPattern());
        assertEquals(',', buffer.read());
    }

    @Test(expected = JSONParseException.class)
    public void testInvalidRegularExpression() {
        final String json = "\t /pattern/nsk,";
        final JSONBuffer buffer = new JSONBuffer(json);
        final JSONScanner scanner = new JSONScanner(buffer);
        final JSONToken token = scanner.nextToken();
    }

    @Test(expected = JSONParseException.class)
    public void testInvalidInput() {
        final String json = "\t &&";
        final JSONScanner scanner = new JSONScanner(json);
        scanner.nextToken();
    }

    @Test(expected = JSONParseException.class)
    public void testInvalidNumber() {
        final String json = "\t 123a]";
        final JSONScanner scanner = new JSONScanner(json);
        scanner.nextToken();
    }

    @Test(expected = JSONParseException.class)
    public void testInvalidInfinity() {
        final String json = "\t -Infinnity]";
        final JSONScanner scanner = new JSONScanner(json);
        scanner.nextToken();
    }
}

