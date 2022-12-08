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
 * Expresses a boolean value.
 */
public interface BooleanExpression extends Expression {

    /**
     * Returns logical true if this expression evaluates to logical false.
     * Returns logical false if this expression evaluates to logical true.
     *
     * @return True if false; false if true.
     */
    BooleanExpression not();

    /**
     * Returns logical true if this or the other expression evaluates to logical
     * true. Returns logical false if both evaluate to logical false.
     *
     * @param or the other boolean expression.
     * @return True if either true, false if both false.
     */
    BooleanExpression or(BooleanExpression or);

    /**
     * Returns logical true if both this and the other expression evaluate to
     * logical true. Returns logical false if either evaluate to logical false.
     *
     * @param and the other boolean expression.
     * @return true if both true, false if either false.
     */
    BooleanExpression and(BooleanExpression and);

    /**
     * If this expression evaluates to logical true, returns the result of the
     * evaluated left branch expression. If this evaluates to logical false,
     * returns the result of the evaluated right branch expression.
     *
     * @param left the left branch expression
     * @param right the right branch expression
     * @return left if true, right if false.
     * @param <T> The type of the resulting expression.
     */
    <T extends Expression> T cond(T left, T right);

    <R extends Expression> R passBooleanTo(Function<? super BooleanExpression, R> f);

    <R extends Expression> R switchBooleanOn(Function<Branches, ? extends BranchesTerminal<? super BooleanExpression, R>> on);
}
