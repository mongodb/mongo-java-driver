package com.mongodb.client.model.expressions;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings({"PointlessBooleanExpression", "ConstantConditions"})
class BooleanExpressionsFunctionalTest extends ExpressionsFunctionalTest {
    // https://www.mongodb.com/docs/manual/reference/operator/aggregation/#boolean-expression-operators
    // (Complete as of 6.0)

    private final BooleanExpression tru = Expressions.ofTrue();
    private final BooleanExpression fal = Expressions.ofFalse();

    @Test
    public void literalsTest() {
        assertExpression(true, tru, "true");
        assertExpression(false, fal, "false");
        assertTrue(tru instanceof BooleanExpression);
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
        StringExpression abc = Expressions.ofString("abc");
        StringExpression xyz = Expressions.ofString("xyz");
        assertExpression(
                true && false ? "abc" : "xyz",
                tru.and(fal).cond(abc, xyz),
                "{'$cond': [{'$and': [true, false]}, 'abc', 'xyz']}");
        assertExpression(
                true || false ? "abc" : "xyz",
                tru.or(fal).cond(abc, xyz),
                "{'$cond': [{'$or': [true, false]}, 'abc', 'xyz']}");
    }
}
