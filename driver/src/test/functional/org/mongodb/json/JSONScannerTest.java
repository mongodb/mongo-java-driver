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

import junit.framework.Assert;
import org.junit.Test;
import org.mongodb.json.*;

import java.nio.CharBuffer;
import java.nio.charset.Charset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class JSONScannerTest {

    @Test
    public void testEndOfFile() {
        String json = "\t ";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        assertEquals(JSONTokenType.END_OF_FILE, token.getType());
        assertEquals("<eof>", token.getLexeme());
        assertFalse(scanner.hasNext());
        assertEquals(-1, buffer.read());
    }

    @Test
    public void testBeginObject() {
        String json = "\t {x";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        assertEquals(JSONTokenType.BEGIN_OBJECT, token.getType());
        assertEquals("{", token.getLexeme());
        assertEquals('x', buffer.read());
    }

    @Test
    public void testEndObject()
    {
        String json = "\t }x";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.END_OBJECT, token.getType());
        Assert.assertEquals("}", token.getLexeme());
        Assert.assertEquals('x', buffer.read());
    }

    @Test
    public void testBeginArray()
    {
        String json = "\t [x";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.BEGIN_ARRAY, token.getType());
        Assert.assertEquals("[", token.getLexeme());
        Assert.assertEquals('x', buffer.read());
    }

    @Test
    public void testEndArray()
    {
        String json = "\t ]x";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.END_ARRAY, token.getType());
        Assert.assertEquals("]", token.getLexeme());
        Assert.assertEquals('x', buffer.read());
    }

    @Test
    public void testNameSeparator()
    {
        String json = "\t :x";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.COLON, token.getType());
        Assert.assertEquals(":", token.getLexeme());
        Assert.assertEquals('x', buffer.read());
    }

    @Test
    public void testValueSeparator()
    {
        String json = "\t ,x";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.COMMA, token.getType());
        Assert.assertEquals(",", token.getLexeme());
        Assert.assertEquals('x', buffer.read());
    }

    @Test
    public void testEmptyString()
    {
        String json = "\t \"\"x";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.STRING, token.getType());
        Assert.assertEquals("", token.asString());
        Assert.assertEquals('x', buffer.read());
    }

    @Test
    public void test1CharacterString()
    {
        String json = "\t \"1\"x";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.STRING, token.getType());
        Assert.assertEquals("1", token.asString());
        Assert.assertEquals('x', buffer.read());
    }

    @Test
    public void test2CharacterString()
    {
        String json = "\t \"12\"x";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.STRING, token.getType());
        Assert.assertEquals("12", token.asString());
        Assert.assertEquals('x', buffer.read());
    }

    @Test
    public void test3CharacterString()
    {
        String json = "\t \"123\"x";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.STRING, token.getType());
        Assert.assertEquals("123", token.asString());
        Assert.assertEquals('x', buffer.read());
    }

    @Test
    public void testEscapeSequences()
    {
        String json = "\t \"x\\\"\\\\\\/\\b\\f\\n\\r\\t\\u0030y\"x";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.STRING, token.getType());
        Assert.assertEquals("x\"\\/\b\f\n\r\t0y", token.asString());
        Assert.assertEquals('x', buffer.read());
    }

    @Test
    public void testTrue()
    {
        String json = "\t true,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.UNQUOTED_STRING, token.getType());
        Assert.assertEquals("true", token.asString());
        Assert.assertEquals(',', buffer.read());
    }

    @Test
    public void testFalse()
    {
        String json = "\t false,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.UNQUOTED_STRING, token.getType());
        Assert.assertEquals("false", token.asString());
        Assert.assertEquals(',', buffer.read());
    }

    @Test
    public void testNull()
    {
        String json = "\t null,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.UNQUOTED_STRING, token.getType());
        Assert.assertEquals("null", token.asString());
        Assert.assertEquals(',', buffer.read());
    }

    @Test
    public void testUndefined()
    {
        String json = "\t undefined,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.UNQUOTED_STRING, token.getType());
        Assert.assertEquals("undefined", token.asString());
        Assert.assertEquals(',', buffer.read());
    }

    @Test
    public void testUnquotedString()
    {
        String json = "\t name123:1";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.UNQUOTED_STRING, token.getType());
        Assert.assertEquals("name123", token.asString());
        Assert.assertEquals(':', buffer.read());
    }

    @Test
    public void testZero()
    {
        String json = "\t 0,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.INT32, token.getType());
        Assert.assertEquals("0", token.getLexeme());
        Assert.assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusZero()
    {
        String json = "\t -0,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.INT32, token.getType());
        Assert.assertEquals("-0", token.getLexeme());
        Assert.assertEquals(',', buffer.read());
    }

    @Test
    public void testOne()
    {
        String json = "\t 1,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.INT32, token.getType());
        Assert.assertEquals("1", token.getLexeme());
        Assert.assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusOne()
    {
        String json = "\t -1,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.INT32, token.getType());
        Assert.assertEquals("-1", token.getLexeme());
        Assert.assertEquals(',', buffer.read());
    }

    @Test
    public void testTwelve()
    {
        String json = "\t 12,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.INT32, token.getType());
        Assert.assertEquals("12", token.getLexeme());
        Assert.assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusTwelve()
    {
        String json = "\t -12,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.INT32, token.getType());
        Assert.assertEquals("-12", token.getLexeme());
        Assert.assertEquals(',', buffer.read());
    }

    @Test
    public void testZeroPointZero()
    {
        String json = "\t 0.0,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.DOUBLE, token.getType());
        Assert.assertEquals("0.0", token.getLexeme());
        Assert.assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusZeroPointZero()
    {
        String json = "\t -0.0,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.DOUBLE, token.getType());
        Assert.assertEquals("-0.0", token.getLexeme());
        Assert.assertEquals(',', buffer.read());
    }

    @Test
    public void testZeroExponentOne()
    {
        String json = "\t 0e1,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.DOUBLE, token.getType());
        Assert.assertEquals("0e1", token.getLexeme());
        Assert.assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusZeroExponentOne()
    {
        String json = "\t -0e1,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.DOUBLE, token.getType());
        Assert.assertEquals("-0e1", token.getLexeme());
        Assert.assertEquals(',', buffer.read());
    }

    @Test
    public void testZeroExponentMinusOne()
    {
        String json = "\t 0e-1,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.DOUBLE, token.getType());
        Assert.assertEquals("0e-1", token.getLexeme());
        Assert.assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusZeroExponentMinusOne()
    {
        String json = "\t -0e-1,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.DOUBLE, token.getType());
        Assert.assertEquals("-0e-1", token.getLexeme());
        Assert.assertEquals(',', buffer.read());
    }

    @Test
    public void testOnePointTwo()
    {
        String json = "\t 1.2,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.DOUBLE, token.getType());
        Assert.assertEquals("1.2", token.getLexeme());
        Assert.assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusOnePointTwo()
    {
        String json = "\t -1.2,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.DOUBLE, token.getType());
        Assert.assertEquals("-1.2", token.getLexeme());
        Assert.assertEquals(',', buffer.read());
    }

    @Test
    public void testOneExponentTwelve()
    {
        String json = "\t 1e12,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.DOUBLE, token.getType());
        Assert.assertEquals("1e12", token.getLexeme());
        Assert.assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusZeroExponentTwelve()
    {
        String json = "\t -1e12,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.DOUBLE, token.getType());
        Assert.assertEquals("-1e12", token.getLexeme());
        Assert.assertEquals(',', buffer.read());
    }

    @Test
    public void testOneExponentMinuesTwelve()
    {
        String json = "\t 1e-12,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.DOUBLE, token.getType());
        Assert.assertEquals("1e-12", token.getLexeme());
        Assert.assertEquals(',', buffer.read());
    }

    @Test
    public void testMinusZeroExponentMinusTwelve()
    {
        String json = "\t -1e-12,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.DOUBLE, token.getType());
        Assert.assertEquals("-1e-12", token.getLexeme());
        Assert.assertEquals(',', buffer.read());
    }

    @Test
    public void testRegularExpressionEmpty()
    {
        String json = "\t //,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.REGULAR_EXPRESSION, token.getType());
        Assert.assertEquals("//", token.getLexeme());
        Assert.assertEquals(',', buffer.read());
    }

    @Test
    public void testRegularExpressionPattern()
    {
        String json = "\t /pattern/,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.REGULAR_EXPRESSION, token.getType());
        Assert.assertEquals("/pattern/", token.getLexeme());
        Assert.assertEquals(',', buffer.read());
    }

    @Test
    public void testRegularExpressionPatternAndOptions()
    {
        String json = "\t /pattern/imxs,";
        JSONBuffer buffer = new JSONBuffer(json);
        JSONScanner scanner = new JSONScanner(buffer);
        JSONToken token = scanner.next();
        Assert.assertEquals(JSONTokenType.REGULAR_EXPRESSION, token.getType());
        Assert.assertEquals("/pattern/imxs", token.getLexeme());
        Assert.assertEquals(',', buffer.read());
    }
}

