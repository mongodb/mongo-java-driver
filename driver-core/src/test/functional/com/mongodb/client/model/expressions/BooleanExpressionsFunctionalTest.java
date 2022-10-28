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

@SuppressWarnings({"PointlessBooleanExpression", "ConstantConditions", "ConstantConditionalExpression"})
class BooleanExpressionsFunctionalTest extends AbstractExpressionsFunctionalTest {
    // https://www.mongodb.com/docs/manual/reference/operator/aggregation/#boolean-expression-operators
    // (Complete as of 6.0)

    private final BooleanExpression tru = Expressions.of(true);
    private final BooleanExpression fal = Expressions.of(false);

    @Test
    public void literalsTest() {
        assertExpression(true, tru, "true");
        assertExpression(false, fal, "false");
    }

    @Test
    public void orTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/or/
        assertExpression(true || false, tru.or(fal), "{'$or': [true, false]}");
        assertExpression(false || true, fal.or(tru), "{'$or': [false, true]}");
    }

    @Test
    public void andTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/and/
        assertExpression(true && false, tru.and(fal), "{'$and': [true, false]}");
        assertExpression(false && true, fal.and(tru), "{'$and': [false, true]}");
    }

    @Test
    public void notTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/not/
        assertExpression(!true, tru.not(), "{'$not': true}");
        assertExpression(!false, fal.not(), "{'$not': false}");
    }

    @Test
    public void condTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/cond/
        StringExpression abc = Expressions.of("abc");
        StringExpression xyz = Expressions.of("xyz");
        NumberExpression nnn = Expressions.of(123);
        assertExpression(
                true && false ? "abc" : "xyz",
                tru.and(fal).cond(abc, xyz),
                "{'$cond': [{'$and': [true, false]}, 'abc', 'xyz']}");
        assertExpression(
                true || false ? "abc" : "xyz",
                tru.or(fal).cond(abc, xyz),
                "{'$cond': [{'$or': [true, false]}, 'abc', 'xyz']}");
        assertExpression(
                false ? "abc" : 123,
                fal.cond(abc, nnn),
                "{'$cond': [false, 'abc', 123]}");
    }
}
