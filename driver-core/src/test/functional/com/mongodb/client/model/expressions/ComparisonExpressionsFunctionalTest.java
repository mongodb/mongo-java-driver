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

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.codecs.BsonValueCodecProvider;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.expressions.Expressions.of;
import static com.mongodb.client.model.expressions.Expressions.ofBooleanArray;
import static com.mongodb.client.model.expressions.Expressions.ofIntegerArray;
import static com.mongodb.client.model.expressions.Expressions.ofNull;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.junit.jupiter.api.Assertions.fail;

@SuppressWarnings({"ConstantConditions"})
class ComparisonExpressionsFunctionalTest extends AbstractExpressionsFunctionalTest {
    // https://www.mongodb.com/docs/manual/reference/operator/aggregation/#comparison-expression-operators
    // (Complete as of 6.0)
    // Comparison expressions are part of the generic Expression class.

    // https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/#std-label-bson-types-comparison-order
    private final List<Expression> sampleValues = Arrays.asList(
            MqlExpression.ofRem(),
            ofNull(),
            of(0),
            of(1),
            of(""),
            of("str"),
            of(BsonDocument.parse("{}")),
            of(BsonDocument.parse("{a: 1}")),
            of(BsonDocument.parse("{a: 2}")),
            of(BsonDocument.parse("{a: 2, b: 1}")),
            of(BsonDocument.parse("{b: 1, a: 2}")),
            of(BsonDocument.parse("{'':''}")),
            ofIntegerArray(0),
            ofIntegerArray(1),
            ofBooleanArray(true),
            of(false),
            of(true),
            of(Instant.now())
    );

    @Test
    public void literalsTest() {
        // special values
        assertExpression(null, ofNull(), "null");
        // the "missing" value is obtained via getField.
        // the "$$REMOVE" value is intentionally not exposed. It is used internally.
        // the "undefined" value is deprecated.
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/literal/
        // $literal is intentionally not exposed. It is used internally.
    }

    @Test
    public void eqTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/eq/
        assertExpression(
                1 == 2,
                of(1).eq(of(2)),
                "{'$eq': [1, 2]}");
        assertExpression(
                false,
                of(BsonDocument.parse("{}")).eq(ofIntegerArray()),
                "{'$eq': [{'$literal': {}}, []]}");

        // numbers are equal, even though of different types
        assertExpression(
                1 == 1.0,
                of(1).eq(of(1.0)),
                "{'$eq': [1, 1.0]}");
        assertExpression(
                1 == 1L,
                of(1).eq(of(1L)),
                "{'$eq': [1, { '$numberLong': '1' }]}");

        // ensure that no two samples are equal to each other
        for (int i = 0; i < sampleValues.size(); i++) {
            for (int j = 0; j < sampleValues.size(); j++) {
                if (i == j) {
                    continue;
                }
                Expression first = sampleValues.get(i);
                Expression second = sampleValues.get(j);
                BsonValue evaluate = evaluate(first.eq(second));
                if (evaluate.asBoolean().getValue()) {
                    BsonValue v1 = ((MqlExpression<?>) first).toBsonValue(fromProviders(new BsonValueCodecProvider()));
                    BsonValue v2 = ((MqlExpression<?>) second).toBsonValue(fromProviders(new BsonValueCodecProvider()));
                    fail(i + "," + j + " --" + v1 + " and " + v2 + " should not equal");
                }
            }
        }
    }

    @Test
    public void neTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/ne/
        assertExpression(
                1 != 2,
                of(1).ne(of(2)),
                "{'$ne': [1, 2]}");
    }

    @Test
    public void ltTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/lt/
        assertExpression(
                -1 < 1,
                of(-1).lt(of(1)),
                "{'$lt': [-1, 1]}");
        assertExpression(
                0 < 0,
                of(0).lt(of(0)),
                "{'$lt': [0, 0]}");

        assertExpression(
                true,
                ofNull().lt(of(0)),
                "{'$lt': [null, 0]}");

        for (int i = 0; i < sampleValues.size() - 1; i++) {
            for (int j = i + 1; j < sampleValues.size(); j++) {
                Expression first = sampleValues.get(i);
                Expression second = sampleValues.get(j);
                BsonValue evaluate = evaluate(first.lt(second));
                if (!evaluate.asBoolean().getValue()) {
                    BsonValue v1 = ((MqlExpression<?>) first).toBsonValue(fromProviders(new BsonValueCodecProvider()));
                    BsonValue v2 = ((MqlExpression<?>) second).toBsonValue(fromProviders(new BsonValueCodecProvider()));
                    fail(i + "," + j + " --" + v1 + " < " + v2 + " should be true");
                }
            }
        }
    }

    @Test
    public void lteTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/lte/
        assertExpression(
                -1 <= 1,
                of(-1).lte(of(1)),
                "{'$lte': [-1, 1]}");
        assertExpression(
                0 <= 0,
                of(0).lte(of(0)),
                "{'$lte': [0, 0]}");
    }

    @Test
    public void gtTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/gt/
        assertExpression(
                -1 > 1,
                of(-1).gt(of(1)),
                "{'$gt': [-1, 1]}");
        assertExpression(
                0 > 0,
                of(0).gt(of(0)),
                "{'$gt': [0, 0]}");
    }

    @Test
    public void gteTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/gte/
        assertExpression(
                -1 >= 1,
                of(-1).gte(of(1)),
                "{'$gte': [-1, 1]}");
        assertExpression(
                0 >= 0,
                of(0).gte(of(0)),
                "{'$gte': [0, 0]}");
    }

}
