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

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.client.model.expressions.Expressions.of;
import static com.mongodb.client.model.expressions.Expressions.ofArray;
import static com.mongodb.client.model.expressions.Expressions.ofBooleanArray;
import static com.mongodb.client.model.expressions.Expressions.ofDateArray;
import static com.mongodb.client.model.expressions.Expressions.ofIntegerArray;
import static com.mongodb.client.model.expressions.Expressions.ofNumberArray;
import static com.mongodb.client.model.expressions.Expressions.ofStringArray;

@SuppressWarnings({"ConstantConditions", "Convert2MethodRef"})
class ArrayExpressionsFunctionalTest extends AbstractExpressionsFunctionalTest {
    // https://www.mongodb.com/docs/manual/reference/operator/aggregation/#array-expression-operators
    // (Incomplete)

    private final ArrayExpression<IntegerExpression> array123 = ofIntegerArray(1, 2, 3);
    private final ArrayExpression<BooleanExpression> arrayTTF = ofBooleanArray(true, true, false);

    @Test
    public void literalsTest() {
        // Boolean
        assertExpression(
                Arrays.asList(true, true, false),
                arrayTTF,
                "[true, true, false]");
        // Integer
        assertExpression(
                Arrays.asList(1, 2, 3),
                array123,
                "[1, 2, 3]");
        assertExpression(
                Arrays.asList(1L, 2L, 3L),
                ofIntegerArray(1L, 2L, 3L),
                "[{'$numberLong': '1'}, {'$numberLong': '2'}, {'$numberLong': '3'}]");
        // Number
        assertExpression(
                Arrays.asList(1.0, 2.0, 3.0),
                ofNumberArray(1.0, 2.0, 3.0),
                "[1.0, 2.0, 3.0]");
        assertExpression(
                Arrays.asList(BigDecimal.valueOf(1.0)),
                ofNumberArray(BigDecimal.valueOf(1.0)),
                "[{'$numberDecimal': '1.0'}]");
        // String
        assertExpression(
                Arrays.asList("a", "b", "c"),
                ofStringArray("a", "b", "c"),
                "['a', 'b', 'c']");
        // Date
        assertExpression(
                Arrays.asList(Instant.parse("2007-12-03T10:15:30.00Z")),
                ofDateArray(Instant.parse("2007-12-03T10:15:30.00Z")),
                "[{'$date': '2007-12-03T10:15:30.00Z'}]");
        // Document
        // ...

        // Array
        ArrayExpression<ArrayExpression<Expression>> arrays = ofArray(ofArray(), ofArray());
        assertExpression(
                Arrays.asList(Collections.emptyList(), Collections.emptyList()), arrays,
                "[[], []]");

        // Mixed
        ArrayExpression<Expression> expression = ofArray(of(1), of(true), ofArray(of(1.0), of(1)));
        assertExpression(
                Arrays.asList(1, true, Arrays.asList(1.0, 1)),
                expression,
                "[1, true, [1.0, 1]]");
    }

    @Test
    public void filterTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/filter/
        assertExpression(
                Stream.of(true, true, false)
                        .filter(v -> v).collect(Collectors.toList()),
                arrayTTF.filter(v -> v),
                // MQL:
                "{'$filter': {'input': [true, true, false], 'cond': '$$this'}}");
    }

    @Test
    public void mapTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/map/
        assertExpression(
                Stream.of(true, true, false)
                        .map(v -> !v).collect(Collectors.toList()),
                arrayTTF.map(v -> v.not()),
                // MQL:
                "{'$map': {'input': [true, true, false], 'in': {'$not': '$$this'}}}");
    }

    @Test
    public void reduceTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/reduce/
        assertExpression(
                Stream.of(true, true, false)
                        .reduce(false, (a, b) -> a || b),
                arrayTTF.reduce(of(false), (a, b) -> a.or(b)),
                // MQL:
                "{'$reduce': {'input': [true, true, false], 'initialValue': false, 'in': {'$or': ['$$value', '$$this']}}}");
        assertExpression(
                Stream.of(true, true, false)
                        .reduce(true, (a, b) -> a && b),
                arrayTTF.reduce(of(true), (a, b) -> a.and(b)),
                // MQL:
                "{'$reduce': {'input': [true, true, false], 'initialValue': true, 'in': {'$and': ['$$value', '$$this']}}}");
        // empty array
        assertExpression(
                Stream.<Boolean>empty().reduce(true, (a, b) -> a && b),
                ofBooleanArray().reduce(of(true), (a, b) -> a.and(b)),
                // MQL:
                "{'$reduce': {'input': [], 'initialValue': true, 'in': {'$and': ['$$value', '$$this']}}}");
        // constant result
        assertExpression(
                Stream.of(true, true, false)
                        .reduce(true, (a, b) -> true),
                arrayTTF.reduce(of(true), (a, b) -> of(true)),
                // MQL:
                "{'$reduce': {'input': [true, true, false], 'initialValue': true, 'in': true}}");
        // non-commutative
        assertExpression(
                "abc",
                ofStringArray("a", "b", "c").reduce(of(""), (a, b) -> a.concat(b)),
                // MQL:
                "{'$reduce': {'input': ['a', 'b', 'c'], 'initialValue': '', 'in': {'$concat': ['$$value', '$$this']}}}");

    }

    @Test
    public void sizeTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/size/
        assertExpression(
                Arrays.asList(1, 2, 3).size(),
                array123.size(),
                // MQL:
                "{'$size': [[1, 2, 3]]}");
    }

    @Test
    public void elementAtTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/arrayElemAt/
        assertExpression(
                Arrays.asList(1, 2, 3).get(0),
                array123.elementAt((IntegerExpression) of(0.0)),
                // MQL:
                "{'$arrayElemAt': [[1, 2, 3], 0.0]}");

        assertExpression(
                Arrays.asList(1, 2, 3).get(3 - 1),
                array123.elementAt(-1));

        assertExpression(
                true,
                ofRem().eq(array123.elementAt(99)));
        assertExpression(
                true,
                ofRem().eq(array123.elementAt(-99)));
    }

    @Test
    public void firstTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/first/
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/first-array-element/
        assertExpression(
                Arrays.asList(1, 2, 3).get(0),
                array123.first(),
                // MQL:
                "{'$first': [[1, 2, 3]]}");
    }

    @Test
    public void lastTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/last-array-element/
        assertExpression(
                Arrays.asList(1, 2, 3).get(2),
                array123.last(),
                // MQL:
                "{'$last': [[1, 2, 3]]}");
    }

    @Test
    public void containsTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/in/
        // The parameters of this expression are flipped
        assertExpression(
                Arrays.asList(1, 2, 3).contains(2),
                array123.contains(of(2)),
                // MQL:
                "{'$in': [2, [1, 2, 3]]}");
    }

    @Test
    public void concatTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/concatArrays/
        assertExpression(
                Stream.concat(Stream.of(1, 2, 3), Stream.of(1, 2, 3))
                        .collect(Collectors.toList()),
                array123.concat(array123),
                // MQL:
                "{'$concatArrays': [[1, 2, 3], [1, 2, 3]]}");
    }

    @Test
    public void sliceTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/slice/
        assertExpression(
                Arrays.asList(1, 2, 3).subList(1, 3),
                array123.slice(1, 10),
                // MQL:
                "{'$slice': [[1, 2, 3], 1, 10]}");

        ArrayExpression<IntegerExpression> array12345 = ofIntegerArray(1, 2, 3, 4, 5);
        // sub-array: skipFirstN + firstN
        assertExpression(
                Arrays.asList(2, 3),
                array12345.slice(1, 2));
        // lastN + firstN
        assertExpression(
                Arrays.asList(5),
                array12345.slice(-1, 100));
        assertExpression(
                Arrays.asList(1, 2, 3, 4, 5),
                array12345.slice(-100, 100));
    }

    @Test
    public void setUnionTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/setUnion/ (40)
        assertExpression(
                Arrays.asList(1, 2, 3),
                array123.setUnion(array123),
                // MQL:
                "{'$setUnion': [[1, 2, 3], [1, 2, 3]]}");
        // convenience
        assertExpression(
                Arrays.asList(1, 2, 3),
                ofIntegerArray(1, 2, 1, 3, 3).distinct(),
                // MQL:
                "{'$setUnion': [[1, 2, 1, 3, 3]]}");
    }

}
