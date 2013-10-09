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
        String json = "\t ";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.END_OF_FILE, token.getType());
        assertEquals("<eof>", token.getValue());
        assertEquals(-1, buffer.read());
    }

    @Test
    public void testBeginObject() {
        String json = "\t {x";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.BEGIN_OBJECT, token.getType());
        assertEquals("{", token.getValue());
        assertEquals('x', buffer.read());
    }

    @Test
    public void testEndObject() {
        String json = "\t }x";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.END_OBJECT, token.getType());
        assertEquals("}", token.getValue());
        assertEquals('x', buffer.read());
    }

    @Test
    public void testBeginArray() {
        String json = "\t [x";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.BEGIN_ARRAY, token.getType());
        assertEquals("[", token.getValue());
        assertEquals('x', buffer.read());
    }

    @Test
    public void testEndArray() {
        String json = "\t ]x";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.END_ARRAY, token.getType());
        assertEquals("]", token.getValue());
        assertEquals('x', buffer.read());
    }

    @Test
    public void testParentheses() {
        String json = "\t (jj)x";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
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
        String json = "\t :x";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.COLON, token.getType());
        assertEquals(":", token.getValue());
        assertEquals('x', buffer.read());
    }

    @Test
    public void testValueSeparator() {
        String json = "\t ,x";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.COMMA, token.getType());
        assertEquals(",", token.getValue());
        assertEquals('x', buffer.read());
    }

    @Test
    public void testEmptyString() {
        String json = "\t \"\"x";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.STRING, token.getType());
        assertEquals("", token.getValue());
        assertEquals('x', buffer.read());
    }

    @Test
    public void test1CharacterString() {
        String json = "\t \"1\"x";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.STRING, token.getType());
        assertEquals("1", token.getValue());
        assertEquals('x', buffer.read());
    }

    @Test
    public void test2CharacterString() {
        String json = "\t \"12\"x";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.STRING, token.getType());
        assertEquals("12", token.getValue());
        assertEquals('x', buffer.read());
    }

    @Test
    public void test3CharacterString() {
        String json = "\t \"123\"x";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.STRING, token.getType());
        assertEquals("123", token.getValue());
        assertEquals('x', buffer.read());
    }

    @Test
    public void testEscapeSequences() {
        String json = "\t \"x\\\"\\\\\\/\\b\\f\\n\\r\\t\\u0030y\"x";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.STRING, token.getType());
        assertEquals("x\"\\/\b\f\n\r\t0y", token.getValue());
        assertEquals('x', buffer.read());
    }


    @Test
    public void testTrue() {
        String json = "\t true,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.UNQUOTED_STRING, token.getType());
        assertEquals("true", token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusInfinity() {
        String json = "\t -Infinity]";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.DOUBLE, token.getType());
        assertEquals(Double.NEGATIVE_INFINITY, token.getValue());
        assertEquals(']', buffer.read());
    }

    @Test
    public void testFalse() {
        String json = "\t false,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.UNQUOTED_STRING, token.getType());
        assertEquals("false", token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testNull() {
        String json = "\t null,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.UNQUOTED_STRING, token.getType());
        assertEquals("null", token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testUndefined() {
        String json = "\t undefined,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.UNQUOTED_STRING, token.getType());
        assertEquals("undefined", token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testUnquotedStringWithSeparator() {
        String json = "\t name123:1";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.UNQUOTED_STRING, token.getType());
        assertEquals("name123", token.getValue());
        assertEquals(':', buffer.read());
    }

    @Test
    public void testUnquotedString() {
        String json = "name123";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.UNQUOTED_STRING, token.getType());
        assertEquals("name123", token.getValue());
        assertEquals(-1, buffer.read());
    }

    @Test
    public void testZero() {
        String json = "\t 0,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.INT32, token.getType());
        assertEquals(0, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusZero() {
        String json = "\t -0,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.INT32, token.getType());
        assertEquals(-0, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testOne() {
        String json = "\t 1,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.INT32, token.getType());
        assertEquals(1, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusOne() {
        String json = "\t -1,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.INT32, token.getType());
        assertEquals(-1, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testTwelve() {
        String json = "\t 12,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.INT32, token.getType());
        assertEquals(12, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusTwelve() {
        String json = "\t -12,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.INT32, token.getType());
        assertEquals(-12, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testZeroPointZero() {
        String json = "\t 0.0,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.DOUBLE, token.getType());
        assertEquals(0.0, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusZeroPointZero() {
        String json = "\t -0.0,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.DOUBLE, token.getType());
        assertEquals(-0.0, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testZeroExponentOne() {
        String json = "\t 0e1,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.DOUBLE, token.getType());
        assertEquals(0e1, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusZeroExponentOne() {
        String json = "\t -0e1,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.DOUBLE, token.getType());
        assertEquals(-0e1, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testZeroExponentMinusOne() {
        String json = "\t 0e-1,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.DOUBLE, token.getType());
        assertEquals(0e-1, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusZeroExponentMinusOne() {
        String json = "\t -0e-1,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.DOUBLE, token.getType());
        assertEquals(-0e-1, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testOnePointTwo() {
        String json = "\t 1.2,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.DOUBLE, token.getType());
        assertEquals(1.2, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusOnePointTwo() {
        String json = "\t -1.2,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.DOUBLE, token.getType());
        assertEquals(-1.2, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testOneExponentTwelve() {
        String json = "\t 1e12,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.DOUBLE, token.getType());
        assertEquals(1e12, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusZeroExponentTwelve() {
        String json = "\t -1e12,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.DOUBLE, token.getType());
        assertEquals(-1e12, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testOneExponentMinuesTwelve() {
        String json = "\t 1e-12,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.DOUBLE, token.getType());
        assertEquals(1e-12, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusZeroExponentMinusTwelve() {
        String json = "\t -1e-12,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.DOUBLE, token.getType());
        assertEquals(-1e-12, token.getValue());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testRegularExpressionEmpty() {
        String json = "\t //,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.REGULAR_EXPRESSION, token.getType());

        RegularExpression regularExpression = token.getValue(RegularExpression.class);

        assertEquals("", regularExpression.getPattern());
        assertEquals("", regularExpression.getOptions());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testRegularExpressionPattern() {
        String json = "\t /pattern/,";

        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.REGULAR_EXPRESSION, token.getType());
        assertEquals("pattern", token.getValue(RegularExpression.class).getPattern());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testRegularExpressionPatternAndOptions() {
        String json = "\t /pattern/im,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.REGULAR_EXPRESSION, token.getType());

        RegularExpression regularExpression = token.getValue(RegularExpression.class);
        assertEquals("pattern", regularExpression.getPattern());
        assertEquals("im", regularExpression.getOptions());
        assertEquals(',', buffer.read());
    }

    @Test
    public void testRegularExpressionPatternAndEscapeSequence() {
        String json = "\t /patte\\.n/,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
        assertEquals(JSONTokenType.REGULAR_EXPRESSION, token.getType());
        assertEquals("patte\\.n", token.getValue(RegularExpression.class).getPattern());
        assertEquals(',', buffer.read());
    }

    @Test(expected = JSONParseException.class)
    public void testInvalidRegularExpression() {
        String json = "\t /pattern/nsk,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.nextToken();
    }

    @Test(expected = JSONParseException.class)
    public void testInvalidInput() {
        String json = "\t &&";
        JSONScanner scanner = new JSONScanner(json);
        scanner.nextToken();
    }

    @Test(expected = JSONParseException.class)
    public void testInvalidNumber() {
        String json = "\t 123a]";
        JSONScanner scanner = new JSONScanner(json);
        scanner.nextToken();
    }

    @Test(expected = JSONParseException.class)
    public void testInvalidInfinity() {
        String json = "\t -Infinnity]";
        JSONScanner scanner = new JSONScanner(json);
        scanner.nextToken();
    }
}

