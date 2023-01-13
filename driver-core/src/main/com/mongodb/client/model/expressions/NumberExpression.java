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

import java.util.function.Function;

/**
 * A number {@linkplain Expression value} in the context of the MongoDB Query
 * Language (MQL). {@linkplain IntegerExpression Integers} are a subset of
 * numbers, and so, for example, the integer 0 and the number 0 are
 * {@linkplain #eq(Expression) equal}.
 */
public interface NumberExpression extends Expression {

    /**
     * The product of multiplying {@code this} and the {@code other} value.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    NumberExpression multiply(NumberExpression other);

    /**
     * The product of multiplying {@code this} and the {@code other} value.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    default NumberExpression multiply(final Number other) {
        return this.multiply(Expressions.numberToExpression(other));
    }

    /**
     * The quotient of dividing {@code this} value by the {@code other} value.
     * This is not integer division: dividing {@code 1} by {@code 2} will
     * always yield {@code 0.5}.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    NumberExpression divide(NumberExpression other);

    /**
     * The quotient of dividing {@code this} value by the {@code other} value.
     * This is not integer division: dividing {@code 1} by {@code 2} will
     * always yield {@code 0.5}.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    default NumberExpression divide(final Number other) {
        return this.divide(Expressions.numberToExpression(other));
    }

    /**
     * The sum of adding {@code this} and the {@code other} value.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    NumberExpression add(NumberExpression other);

    /**
     * The sum of adding {@code this} and the {@code other} value.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    default NumberExpression add(final Number other) {
        return this.add(Expressions.numberToExpression(other));
    }

    /**
     * The difference of subtracting the {@code other} value from {@code this}.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    NumberExpression subtract(NumberExpression other);

    /**
     * The difference of subtracting the {@code other} value from {@code this}.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    default NumberExpression subtract(final Number other) {
        return this.subtract(Expressions.numberToExpression(other));
    }

    /**
     * The {@linkplain #gt(Expression) larger} value of {@code this}
     * and the {@code other} value.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    NumberExpression max(NumberExpression other);

    /**
     * The {@linkplain #lt(Expression) smaller} value of {@code this}
     * and the {@code other} value.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    NumberExpression min(NumberExpression other);

    /**
     * The integer result of rounding {@code this} to the nearest even value.
     *
     * @return the resulting value.
     */
    IntegerExpression round();

    /**
     * The result of rounding {@code this} to the nearest even {@code place}.
     *
     * @param place the decimal place to round to, from -20 to 100, exclusive.
     *              Positive values specify the place to the right of the
     *              decimal point, while negative values, to the left.
     * @return the resulting value.
     */
    NumberExpression round(IntegerExpression place);

    /**
     * The absolute value of {@code this} value.
     *
     * @return the resulting value.
     */
    NumberExpression abs();

    <R extends Expression> R passNumberTo(Function<? super NumberExpression, ? extends R> f);
    <R extends Expression> R switchNumberOn(Function<Branches<NumberExpression>, ? extends BranchesTerminal<NumberExpression, ? extends R>> on);
}
