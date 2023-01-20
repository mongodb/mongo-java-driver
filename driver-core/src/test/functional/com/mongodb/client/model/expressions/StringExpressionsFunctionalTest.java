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

package com.mongodb.client.model.expressions;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static com.mongodb.client.model.expressions.Expressions.of;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings({"ConstantConditions"})
class StringExpressionsFunctionalTest extends AbstractExpressionsFunctionalTest {
    // https://www.mongodb.com/docs/manual/reference/operator/aggregation/#string-expression-operators

    private final String jalapeno = "jalape\u00F1o";
    private final String sushi = "\u5BFF\u53F8";
    private final String fish = "\uD83D\uDC1F";

    @Test
    public void literalsTest() {
        assertExpression("", of(""), "''");
        assertExpression("abc", of("abc"), "'abc'");
        assertThrows(IllegalArgumentException.class, () -> of((String) null));
        assertExpression(fish, of(fish), "'" + fish + "'");
    }

    @Test
    public void concatTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/concat/
        assertExpression(
                "abc".concat("de"),
                of("abc").concat(of("de")),
                "{'$concat': ['abc', 'de']}");
    }

    @Test
    public void toLowerTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/toLower/
        assertExpression(
                "ABC".toLowerCase(),
                of("ABC").toLower(),
                "{'$toLower': 'ABC'}");
    }

    @Test
    public void toUpperTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/toUpper/
        assertExpression(
                "abc".toUpperCase(),
                of("abc").toUpper(),
                "{'$toUpper': 'abc'}");
    }

    @Test
    public void strLenTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/strLenCP/
        assertExpression(
                "abc".codePointCount(0, 3),
                of("abc").strLen(),
                "{'$strLenCP': 'abc'}");

        // unicode
        assertExpression(
                jalapeno.codePointCount(0, jalapeno.length()),
                of(jalapeno).strLen(),
                "{'$strLenCP': '" + jalapeno + "'}");
        assertExpression(
                sushi.codePointCount(0, sushi.length()),
                of(sushi).strLen(),
                "{'$strLenCP': '" + sushi + "'}");
        assertExpression(
                fish.codePointCount(0, fish.length()),
                of(fish).strLen(),
                "{'$strLenCP': '" + fish + "'}");
    }

    @Test
    public void strLenBytesTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/strLenBytes/
        assertExpression(
                "abc".getBytes(StandardCharsets.UTF_8).length,
                of("abc").strLenBytes(),
                "{'$strLenBytes': 'abc'}");

        // unicode
        assertExpression(
                jalapeno.getBytes(StandardCharsets.UTF_8).length,
                of(jalapeno).strLenBytes(),
                "{'$strLenBytes': '" + jalapeno + "'}");
        assertExpression(
                sushi.getBytes(StandardCharsets.UTF_8).length,
                of(sushi).strLenBytes(),
                "{'$strLenBytes': '" + sushi + "'}");
        assertExpression(
                fish.getBytes(StandardCharsets.UTF_8).length,
                of(fish).strLenBytes(),
                "{'$strLenBytes': '" + fish + "'}");

        // comparison
        assertExpression(8, of(jalapeno).strLen());
        assertExpression(9, of(jalapeno).strLenBytes());
        assertExpression(2, of(sushi).strLen());
        assertExpression(6, of(sushi).strLenBytes());
        assertExpression(1, of(fish).strLen());
        assertExpression(4, of(fish).strLenBytes());
    }

    @Test
    public void substrTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/substr/
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/substrCP/
        // substr is deprecated, an alias for bytes
        assertExpression(
                "abc".substring(1, 1 + 1),
                of("abc").substr(of(1), of(1)),
                "{'$substrCP': ['abc', 1, 1]}");
        assertExpression(
                "bc",
                of("abc").substr(of(1), of(100)),
                "{'$substrCP': ['abc', 1, 100]}");

        // unicode
        assertExpression(
                jalapeno.substring(5, 5 + 3),
                of(jalapeno).substr(of(5), of(3)),
                "{'$substrCP': ['" + jalapeno + "', 5, 3]}");
        assertExpression(
                "e\u00F1o",
                of(jalapeno).substr(of(5), of(3)));

        // bounds; convenience
        assertExpression("abc", of("abc").substr(0, 99));
        assertExpression("ab", of("abc").substr(0, 2));
        assertExpression("b", of("abc").substr(1, 1));
        assertExpression("", of("abc").substr(1, 0));
    }

    @Test
    public void substrBytesTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/substrBytes/
        assertExpression(
                "b",
                of("abc").substrBytes(of(1), of(1)),
                "{'$substrBytes': ['abc', 1, 1]}");

        // unicode
        byte[] bytes = Arrays.copyOfRange(sushi.getBytes(StandardCharsets.UTF_8), 0, 3);
        String expected = new String(bytes, StandardCharsets.UTF_8);
        assertExpression(expected,
                of(sushi).substrBytes(of(0), of(3)));
        // server returns "starting index is a UTF-8 continuation byte" error when substrBytes(1, 1)

        // convenience
        assertExpression("b", of("abc").substrBytes(1, 1));
    }
}
