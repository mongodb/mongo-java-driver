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

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.client.model.expressions.Expressions.of;
import static com.mongodb.client.model.expressions.Expressions.ofBooleanArray;

@SuppressWarnings({"ConstantConditions", "Convert2MethodRef"})
class ArrayExpressionsFunctionalTest extends AbstractExpressionsFunctionalTest {
    // https://www.mongodb.com/docs/manual/reference/operator/aggregation/#array-expression-operators
    // (Incomplete)

    private final ArrayExpression<BooleanExpression> arrayTTF = ofBooleanArray(true, true, false);

    @Test
    public void literalsTest() {
        assertExpression(Arrays.asList(true, true, false), arrayTTF, "[true, true, false]");
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
                "{'$reduce': {'input': [true, true, false], 'initialValue': false, 'in': {'$or': ['$$this', '$$value']}}}");
        assertExpression(
                Stream.of(true, true, false)
                        .reduce(true, (a, b) -> a && b),
                arrayTTF.reduce(of(true), (a, b) -> a.and(b)),
                // MQL:
                "{'$reduce': {'input': [true, true, false], 'initialValue': true, 'in': {'$and': ['$$this', '$$value']}}}");
        // empty array
        assertExpression(
                Stream.<Boolean>empty().reduce(true, (a, b) -> a && b),
                ofBooleanArray().reduce(of(true), (a, b) -> a.and(b)),
                // MQL:
                "{'$reduce': {'input': [], 'initialValue': true, 'in': {'$and': ['$$this', '$$value']}}}");
        // constant result
        assertExpression(
                Stream.of(true, true, false)
                        .reduce(true, (a, b) -> true),
                arrayTTF.reduce(of(true), (a, b) -> of(true)),
                // MQL:
                "{'$reduce': {'input': [true, true, false], 'initialValue': true, 'in': true}}");
    }
}
