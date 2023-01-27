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
 * A boolean {@linkplain MqlValue value} in the context of the
 * MongoDB Query Language (MQL).
 *
 * @since 4.9.0
 */
@Sealed
@Beta(Beta.Reason.CLIENT)
public interface MqlBoolean extends MqlValue {

    /**
     * The logical negation of {@code this} value.
     *
     * @return the resulting value.
     */
    MqlBoolean not();

    /**
     * The logical conjunction of {@code this} and the {@code other} value.
     *
     * @param other the other boolean value.
     * @return the resulting value.
     */
    MqlBoolean or(MqlBoolean other);

    /**
     * The logical disjunction of {@code this} and the {@code other} value.
     *
     * @param other the other boolean value.
     * @return the resulting value.
     */
    MqlBoolean and(MqlBoolean other);

    /**
     * The {@code ifTrue} value when {@code this} is true,
     * and the {@code ifFalse} value otherwise.
     *
     * @param ifTrue the ifTrue value.
     * @param ifFalse the ifFalse value.
     * @return the resulting value.
     * @param <T> The type of the resulting expression.
     */
    <T extends MqlValue> T cond(T ifTrue, T ifFalse);

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
    <R extends MqlValue> R passBooleanTo(Function<? super MqlBoolean, ? extends R> f);

    /**
     * The result of applying the provided switch mapping to {@code this} value.
     *
     * @see MqlValue#switchOn
     * @param mapping the switch mapping.
     * @return the resulting value.
     * @param <R> the type of the resulting value.
     */
    <R extends MqlValue> R switchBooleanOn(Function<Branches<MqlBoolean>, ? extends BranchesTerminal<MqlBoolean, ? extends R>> mapping);
}
