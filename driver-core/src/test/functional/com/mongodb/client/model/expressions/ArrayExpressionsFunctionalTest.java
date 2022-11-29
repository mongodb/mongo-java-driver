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

import com.mongodb.MongoCommandException;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.client.model.expressions.Expressions.of;
import static com.mongodb.client.model.expressions.Expressions.ofArray;
import static com.mongodb.client.model.expressions.Expressions.ofBooleanArray;
import static com.mongodb.client.model.expressions.Expressions.ofDateArray;
import static com.mongodb.client.model.expressions.Expressions.ofIntegerArray;
import static com.mongodb.client.model.expressions.Expressions.ofNumberArray;
import static com.mongodb.client.model.expressions.Expressions.ofStringArray;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
                Arrays.asList(Decimal128.parse("1.0")),
                ofNumberArray(Decimal128.parse("1.0")),
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
        ArrayExpression<DocumentExpression> documentArray = ofArray(
                of(Document.parse("{a: 1}")),
                of(Document.parse("{b: 2}")));
        assertExpression(
                Arrays.asList(Document.parse("{a: 1}"), Document.parse("{b: 2}")),
                documentArray,
                "[{'$literal': {'a': 1}}, {'$literal': {'b': 2}}]");

        // Array
        ArrayExpression<ArrayExpression<Expression>> arrayArray = ofArray(ofArray(), ofArray());
        assertExpression(
                Arrays.asList(Collections.emptyList(), Collections.emptyList()),
                arrayArray,
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
        assertExpression(
                0,
                ofIntegerArray().size(),
                // MQL:
                "{'$size': [[]]}");
    }

    @Test
    public void elementAtTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/arrayElemAt/
        assertExpression(
                Arrays.asList(1, 2, 3).get(0),
                // 0.0 is a valid integer value
                array123.elementAt(of(0.0).isIntegerOr(of(-1))),
                // MQL:
                "{'$arrayElemAt': [[1, 2, 3], {'$cond': [{'$cond': "
                        + "[{'$isNumber': [0.0]}, {'$eq': [0.0, {'$round': 0.0}]}, false]}, 0.0, -1]}]}");
        // negatives
        assertExpression(
                Arrays.asList(1, 2, 3).get(3 - 1),
                array123.elementAt(-1));
        // underlying long
        assertExpression(
                2,
                array123.elementAt(of(1L)));

        assertExpression(
                MISSING,
                array123.elementAt(99));

        assertExpression(
                MISSING,
                array123.elementAt(-99));

        // long values are considered entirely out of bounds; server error
        assertThrows(MongoCommandException.class, () -> assertExpression(
                MISSING,
                array123.elementAt(of(Long.MAX_VALUE))));
    }

    @Test
    public void firstTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/first/
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/first-array-element/
        assertExpression(
                new LinkedList<>(Arrays.asList(1, 2, 3)).getFirst(),
                array123.first(),
                // MQL:
                "{'$first': [[1, 2, 3]]}");
    }

    @Test
    public void lastTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/last-array-element/
        assertExpression(
                new LinkedList<>(Arrays.asList(1, 2, 3)).getLast(),
                array123.last(),
                // MQL:
                "{'$last': [[1, 2, 3]]}");

        assertExpression(
                MISSING,
                ofIntegerArray().last(),
                // MQL:
                "{'$last': [[]]}");
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
                ofIntegerArray(1, 2, 3).concat(ofIntegerArray(1, 2, 3)),
                // MQL:
                "{'$concatArrays': [[1, 2, 3], [1, 2, 3]]}");
        // mixed types:
        assertExpression(
                Arrays.asList(1.0, 1, 2, 3),
                ofNumberArray(1.0).concat(ofIntegerArray(1, 2, 3)));
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
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/setUnion/
        assertExpression(
                Arrays.asList(1, 2, 3),
                array123.union(array123),
                // MQL:
                "{'$setUnion': [[1, 2, 3], [1, 2, 3]]}");

        // mixed types:
        assertExpression(
                Arrays.asList(1, 2.0, 3),
                // above is a set; in case of flakiness, below should `sort` (not implemented at time of test creation)
                ofNumberArray(2.0).union(ofIntegerArray(1, 2, 3)));
        // convenience
        assertExpression(
                Arrays.asList(1, 2, 3),
                ofIntegerArray(1, 2, 1, 3, 3).distinct(),
                // MQL:
                "{'$setUnion': [[1, 2, 1, 3, 3]]}");
    }

}
