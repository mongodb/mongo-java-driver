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

import java.util.function.Function;

/**
 * An integer {@linkplain MqlValue value} in the context of the MongoDB Query
 * Language (MQL). Integers are a subset of {@linkplain MqlNumber numbers},
 * and so, for example, the integer 0 and the number 0 are
 * {@linkplain #eq(MqlValue) equal}.
 *
 * @since 4.9.0
 */
@Sealed
@Beta(Beta.Reason.CLIENT)
public interface MqlInteger extends MqlNumber {

    /**
     * The product of multiplying {@code this} and the {@code other} value.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    MqlInteger multiply(MqlInteger other);

    /**
     * The product of multiplying {@code this} and the {@code other} value.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    default MqlInteger multiply(final int other) {
        return this.multiply(MqlValues.of(other));
    }

    /**
     * The sum of adding {@code this} and the {@code other} value.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    MqlInteger add(MqlInteger other);

    /**
     * The sum of adding {@code this} and the {@code other} value.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    default MqlInteger add(final int other) {
        return this.add(MqlValues.of(other));
    }

    /**
     * The difference of subtracting the {@code other} value from {@code this}.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    MqlInteger subtract(MqlInteger other);

    /**
     * The difference of subtracting the {@code other} value from {@code this}.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    default MqlInteger subtract(final int other) {
        return this.subtract(MqlValues.of(other));
    }

    /**
     * The {@linkplain #gt(MqlValue) larger} value of {@code this}
     * and the {@code other} value.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    MqlInteger max(MqlInteger other);

    /**
     * The {@linkplain #lt(MqlValue) smaller} value of {@code this}
     * and the {@code other} value.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    MqlInteger min(MqlInteger other);

    /**
     * The absolute value of {@code this} value.
     *
     * @return the resulting value.
     */
    MqlInteger abs();

    /**
     * The {@linkplain MqlDate date} corresponding to {@code this} value
     * when taken to be the number of milliseconds since the Unix epoch.
     *
     * @mongodb.server.release 4.0
     * @return the resulting value.
     */
    MqlDate millisecondsToDate();

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
    <R extends MqlValue> R passIntegerTo(Function<? super MqlInteger, ? extends R> f);

    /**
     * The result of applying the provided switch mapping to {@code this} value.
     *
     * @see MqlValue#switchOn
     * @param mapping the switch mapping.
     * @return the resulting value.
     * @param <R> the type of the resulting value.
     */
    <R extends MqlValue> R switchIntegerOn(Function<Branches<MqlInteger>, ? extends BranchesTerminal<MqlInteger, ? extends R>> mapping);
}
