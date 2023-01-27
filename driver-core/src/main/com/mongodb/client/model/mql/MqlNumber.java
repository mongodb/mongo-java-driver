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

import com.mongodb.annotations.Beta;
import com.mongodb.annotations.Sealed;
import com.mongodb.assertions.Assertions;

import java.util.function.Function;

/**
 * A number {@linkplain MqlValue value} in the context of the MongoDB Query
 * Language (MQL). {@linkplain MqlInteger Integers} are a subset of
 * numbers, and so, for example, the integer 0 and the number 0 are
 * {@linkplain #eq(MqlValue) equal}.
 *
 * @since 4.9.0
 */
@Sealed
@Beta(Beta.Reason.CLIENT)
public interface MqlNumber extends MqlValue {

    /**
     * The product of multiplying {@code this} and the {@code other} value.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    MqlNumber multiply(MqlNumber other);

    /**
     * The product of multiplying {@code this} and the {@code other} value.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    default MqlNumber multiply(final Number other) {
        Assertions.notNull("other", other);
        return this.multiply(MqlValues.numberToExpression(other));
    }

    /**
     * The quotient of dividing {@code this} value by the {@code other} value.
     * This is not integer division: dividing {@code 1} by {@code 2} will
     * always yield {@code 0.5}.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    MqlNumber divide(MqlNumber other);

    /**
     * The quotient of dividing {@code this} value by the {@code other} value.
     * This is not integer division: dividing {@code 1} by {@code 2} will
     * always yield {@code 0.5}.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    default MqlNumber divide(final Number other) {
        Assertions.notNull("other", other);
        return this.divide(MqlValues.numberToExpression(other));
    }

    /**
     * The sum of adding {@code this} and the {@code other} value.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    MqlNumber add(MqlNumber other);

    /**
     * The sum of adding {@code this} and the {@code other} value.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    default MqlNumber add(final Number other) {
        Assertions.notNull("other", other);
        return this.add(MqlValues.numberToExpression(other));
    }

    /**
     * The difference of subtracting the {@code other} value from {@code this}.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    MqlNumber subtract(MqlNumber other);

    /**
     * The difference of subtracting the {@code other} value from {@code this}.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    default MqlNumber subtract(final Number other) {
        Assertions.notNull("other", other);
        return this.subtract(MqlValues.numberToExpression(other));
    }

    /**
     * The {@linkplain #gt(MqlValue) larger} value of {@code this}
     * and the {@code other} value.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    MqlNumber max(MqlNumber other);

    /**
     * The {@linkplain #lt(MqlValue) smaller} value of {@code this}
     * and the {@code other} value.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    MqlNumber min(MqlNumber other);

    /**
     * The integer result of rounding {@code this} to the nearest even value.
     *
     * @mongodb.server.release 4.2
     * @return the resulting value.
     */
    MqlInteger round();

    /**
     * The result of rounding {@code this} to {@code place} decimal places
     * using the "half to even" approach.
     *
     * @param place the decimal place to round to, from -20 to 100, exclusive.
     *              Positive values specify the place to the right of the
     *              decimal point, while negative values, to the left.
     * @return the resulting value.
     */
    MqlNumber round(MqlInteger place);

    /**
     * The absolute value of {@code this} value.
     *
     * @return the resulting value.
     */
    MqlNumber abs();

    /**
     * The result of passing {@code this} value to the provided function.
     * Equivalent to {@code f.apply(this)}, and allows lambdas and static,
     * user-defined functions to use the chaining syntax.
     *
     * @see MqlValue#passTo
     * @param f the function to apply.
     * @return the resulting value.
     * @param <R> the type of the resulting value.
     */
    <R extends MqlValue> R passNumberTo(Function<? super MqlNumber, ? extends R> f);

    /**
     * The result of applying the provided switch mapping to {@code this} value.
     *
     * @see MqlValue#switchOn
     * @param mapping the switch mapping.
     * @return the resulting value.
     * @param <R> the type of the resulting value.
     */
    <R extends MqlValue> R switchNumberOn(Function<Branches<MqlNumber>, ? extends BranchesTerminal<MqlNumber, ? extends R>> mapping);
}
