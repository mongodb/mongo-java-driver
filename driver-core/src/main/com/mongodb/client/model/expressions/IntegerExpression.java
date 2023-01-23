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
 * An integer {@linkplain Expression value} in the context of the MongoDB Query
 * Language (MQL). Integers are a subset of {@linkplain NumberExpression numbers},
 * and so, for example, the integer 0 and the number 0 are
 * {@linkplain #eq(Expression) equal}.
 */
public interface IntegerExpression extends NumberExpression {

    /**
     * The product of multiplying {@code this} and the {@code other} value.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    IntegerExpression multiply(IntegerExpression other);

    /**
     * The product of multiplying {@code this} and the {@code other} value.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    default IntegerExpression multiply(final int other) {
        return this.multiply(Expressions.of(other));
    }

    /**
     * The sum of adding {@code this} and the {@code other} value.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    IntegerExpression add(IntegerExpression other);

    /**
     * The sum of adding {@code this} and the {@code other} value.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    default IntegerExpression add(final int other) {
        return this.add(Expressions.of(other));
    }

    /**
     * The difference of subtracting the {@code other} value from {@code this}.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    IntegerExpression subtract(IntegerExpression other);

    /**
     * The difference of subtracting the {@code other} value from {@code this}.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    default IntegerExpression subtract(final int other) {
        return this.subtract(Expressions.of(other));
    }

    /**
     * The {@linkplain #gt(Expression) larger} value of {@code this}
     * and the {@code other} value.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    IntegerExpression max(IntegerExpression other);

    /**
     * The {@linkplain #lt(Expression) smaller} value of {@code this}
     * and the {@code other} value.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    IntegerExpression min(IntegerExpression other);

    /**
     * The absolute value of {@code this} value.
     *
     * @return the resulting value.
     */
    IntegerExpression abs();

    /**
     * The {@linkplain DateExpression date} corresponding to {@code this} value
     * when taken to be the number of milliseconds since the Unix epoch.
     *
     * @mongodb.server.release 4.0
     * @return the resulting value.
     */
    DateExpression millisecondsToDate();

    /**
     * The result of passing {@code this} value to the provided function.
     * Equivalent to {@code f.apply(this)}, and allows lambdas and static,
     * user-defined functions to use the chaining syntax.
     *
     * @see Expression#passTo
     * @param f the function to apply.
     * @return the resulting value.
     * @param <R> the type of the resulting value.
     */
    <R extends Expression> R passIntegerTo(Function<? super IntegerExpression, ? extends R> f);

    /**
     * The result of applying the provided switch mapping to {@code this} value.
     *
     * @see Expression#switchOn
     * @param mapping the switch mapping.
     * @return the resulting value.
     * @param <R> the type of the resulting value.
     */
    <R extends Expression> R switchIntegerOn(Function<Branches<IntegerExpression>, ? extends BranchesTerminal<IntegerExpression, ? extends R>> mapping);
}
