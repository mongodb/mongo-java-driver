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
 * A logical boolean value, either true or false.
 */
public interface BooleanExpression extends Expression {

    /**
     * The logical negation of {@code this} value.
     *
     * @return the resulting value.
     */
    BooleanExpression not();

    /**
     * The logical conjunction of {@code this} and the {@code other} value.
     *
     * @param other the other boolean value.
     * @return the resulting value.
     */
    BooleanExpression or(BooleanExpression other);

    /**
     * The logical disjunction of {@code this} and the {@code other} value.
     *
     * @param other the other boolean value.
     * @return the resulting value.
     */
    BooleanExpression and(BooleanExpression other);
    // TODO-END check the evaluation semantics of and/or

    /**
     * The {@code left} branch when {@code this} is true,
     * and the {@code right} branch otherwise.
     *
     * @param left the left branch.
     * @param right the right branch.
     * @return the resulting value.
     * @param <T> The type of the resulting expression.
     */
    <T extends Expression> T cond(T left, T right);

    <R extends Expression> R passBooleanTo(Function<? super BooleanExpression, ? extends R> f);

    <R extends Expression> R switchBooleanOn(Function<Branches<BooleanExpression>, ? extends BranchesTerminal<BooleanExpression, ? extends R>> on);
}
