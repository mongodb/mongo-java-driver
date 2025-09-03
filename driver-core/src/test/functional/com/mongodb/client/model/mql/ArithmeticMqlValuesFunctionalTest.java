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

package com.mongodb.client.model.mql;

import org.bson.types.Decimal128;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static com.mongodb.client.model.mql.MqlValues.numberToMqlNumber;
import static com.mongodb.client.model.mql.MqlValues.of;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings("ConstantConditions")
class ArithmeticMqlValuesFunctionalTest extends AbstractMqlValuesFunctionalTest {
    // https://www.mongodb.com/docs/manual/reference/operator/aggregation/#arithmetic-expression-operators

    @Test
    public void literalsTest() {
        assertExpression(1, of(1), "1");
        assertExpression(1L, of(1L));
        assertExpression(1.0, of(1.0));
        assertExpression(Decimal128.parse("1.0"), of(Decimal128.parse("1.0")));
        assertThrows(IllegalArgumentException.class, () -> of((Decimal128) null));

        // expression equality differs from bson equality
        assertExpression(true, of(1L).eq(of(1.0)));
        assertExpression(true, of(1L).eq(of(1)));

        // bson equality; underlying type is preserved
        // this behaviour is not defined by the API, but tested for clarity
        assertEquals(toBsonValue(1), evaluate(of(1)));
        assertEquals(toBsonValue(1L), evaluate(of(1L)));
        assertEquals(toBsonValue(1.0), evaluate(of(1.0)));
        assertNotEquals(toBsonValue(1), evaluate(of(1L)));
        assertNotEquals(toBsonValue(1.0), evaluate(of(1L)));

        // Number conversions; used internally
        assertExpression(1, numberToMqlNumber(1));
        assertExpression(1L, numberToMqlNumber(1L));
        assertExpression(1.0, numberToMqlNumber(1.0));
        assertExpression(Decimal128.parse("1.0"), numberToMqlNumber(Decimal128.parse("1.0")));
        assertThrows(IllegalArgumentException.class,
                () -> assertExpression("n/a", numberToMqlNumber(BigDecimal.valueOf(1))));
    }

    @Test
    public void multiplyTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/multiply/
        assertExpression(
                2.0 * 2,
                of(2.0).multiply(of(2)),
                "{'$multiply': [2.0, 2]}");

        // mixing integers and numbers
        MqlInteger oneInt = of(1);
        MqlNumber oneNum = of(1.0);
        MqlInteger resultInt = oneInt.multiply(oneInt);
        MqlNumber resultNum = oneNum.multiply(oneNum);
        // compile time error if these were IntegerExpressions:
        MqlNumber r2 = oneNum.multiply(oneInt);
        MqlNumber r3 = oneInt.multiply(oneNum);
        assertExpression(1, resultInt);
        // 1 is also a valid expected value in our API
        assertExpression(1.0, resultNum);
        assertExpression(1.0, r2);
        assertExpression(1.0, r3);

        // convenience
        assertExpression(2.0, of(1.0).multiply(2.0));
        assertExpression(2L, of(1).multiply(2L));
        assertExpression(2, of(1).multiply(2));
    }

    @SuppressWarnings("PointlessArithmeticExpression")
    @Test
    public void divideTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/divide/
        assertExpression(
                2.0 / 1.0,
                of(2.0).divide(of(1.0)),
                "{'$divide': [2.0, 1.0]}");

        // division always converts to a double:
        assertExpression(
                2.0, // not: 2 / 1
                of(2).divide(of(1)),
                "{'$divide': [2, 1]}");

        // this means that unlike Java's 1/2==0, dividing any underlying
        // BSON number type always yields an equal result:
        assertExpression(
                1.0 / 2.0,
                of(1.0).divide(of(2.0)),
                "{'$divide': [1.0, 2.0]}");
        assertExpression(
                0.5,
                of(1).divide(of(2)),
                "{'$divide': [1, 2]}");

        // however, there are differences in evaluation between numbers
        // represented using Decimal128 and double:
        assertExpression(
                2.5242187499999997,
                of(3.231).divide(of(1.28)));
        assertExpression(
                Decimal128.parse("2.52421875"),
                of(Decimal128.parse("3.231")).divide(of(Decimal128.parse("1.28"))));
        assertExpression(
                Decimal128.parse("2.52421875"),
                of(Decimal128.parse("3.231")).divide(of(1.28)));
        assertExpression(
                Decimal128.parse("2.524218750000"),
                of(3.231).divide(of(Decimal128.parse("1.28"))));
        // this is not simply because the Java literal used has no corresponding
        // double value - it is the same value as-written:
        assertEquals("3.231", "" + 3.231);
        assertEquals("1.28", "" + 1.28);


        // convenience
        assertExpression(0.5, of(1.0).divide(2.0));
        assertExpression(0.5, of(1).divide(2.0));
        assertExpression(0.5, of(1).divide(2L));
        assertExpression(0.5, of(1).divide(2));

        // divide always returns a Number, so the method is not on IntegerExpression
    }

    @Test
    public void addTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/add/
        MqlInteger actual = of(2).add(of(2));
        assertExpression(
                2 + 2, actual,
                "{'$add': [2, 2]}");
        assertExpression(
                2.0 + 2,
                of(2.0).add(of(2)),
                "{'$add': [2.0, 2]}");

        // overflows into a supported underlying type
        assertExpression(
                Integer.MAX_VALUE + 2L,
                of(Integer.MAX_VALUE).add(of(2)));
        assertExpression(
                Long.MAX_VALUE + 2.0,
                of(Long.MAX_VALUE).add(of(2)));
        assertExpression(
                Double.POSITIVE_INFINITY,
                of(Double.MAX_VALUE).add(of(Double.MAX_VALUE)));

        // convenience
        assertExpression(3.0, of(1.0).add(2.0));
        assertExpression(3L, of(1).add(2L));
        assertExpression(3, of(1).add(2));

        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/sum/
        // sum's alternative behaviour exists for purposes of reduction, but is
        // inconsistent with multiply, and potentially confusing. Unimplemented.
    }

    @Test
    public void subtractTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/subtract/
        MqlInteger actual = of(2).subtract(of(2));
        assertExpression(
                0,
                actual,
                "{'$subtract': [2, 2]} ");
        assertExpression(
                2.0 - 2,
                of(2.0).subtract(of(2)),
                "{'$subtract': [2.0, 2]} ");

        // convenience
        assertExpression(-1.0, of(1.0).subtract(2.0));
        assertExpression(-1, of(1).subtract(2));
    }

    @Test
    public void maxTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/max/
        MqlInteger actual = of(-2).max(of(2));
        assertExpression(
                Math.max(-2, 2),
                actual,
                "{'$max': [-2, 2]}");
        assertExpression(
                Math.max(-2.0, 2.0),
                of(-2.0).max(of(2.0)),
                "{'$max': [-2.0, 2.0]}");
    }

    @Test
    public void minTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/min/
        MqlInteger actual = of(-2).min(of(2));
        assertExpression(
                Math.min(-2, 2),
                actual,
                "{'$min': [-2, 2]}");
        assertExpression(
                Math.min(-2.0, 2.0),
                of(-2.0).min(of(2.0)),
                "{'$min': [-2.0, 2.0]}");
    }

    @Test
    public void roundTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/round/
        MqlInteger actual = of(5.5).round();
        assertExpression(
                6.0,
                actual,
                "{'$round': 5.5} ");
        MqlNumber actualNum = of(5.5).round(of(0));
        assertExpression(
                new BigDecimal("5.5").setScale(0, RoundingMode.HALF_EVEN).doubleValue(),
                actualNum,
                "{'$round': [5.5, 0]} ");
        // unlike Java, uses banker's rounding (half_even)
        assertExpression(
                2.0,
                of(2.5).round(),
                "{'$round': 2.5} ");
        assertExpression(
                new BigDecimal("-5.5").setScale(0, RoundingMode.HALF_EVEN).doubleValue(),
                of(-5.5).round());
        // to place
        assertExpression(
                555.55,
                of(555.555).round(of(2)),
                "{'$round': [555.555, 2]} ");
        assertExpression(
                600.0,
                of(555.555).round(of(-2)),
                "{'$round': [555.555, -2]} ");
        // underlying type rounds to same underlying type
        assertExpression(
                5L,
                of(5L).round());
        assertExpression(
                5.0,
                of(5.0).round());
        assertExpression(
                Decimal128.parse("1234"),
                of(Decimal128.parse("1234.2")).round());
    }

    @Test
    public void absTest() {
        // https://www.mongodb.com/docs/manual/reference/operator/aggregation/round/
        assertExpression(
                Math.abs(-2.0),
                of(-2.0).abs(),
                "{'$abs': -2.0}");
        // integer
        MqlInteger abs = of(-2).abs();
        assertExpression(
                Math.abs(-2), abs,
                "{'$abs': -2}");
    }
}
