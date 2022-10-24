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

public interface BooleanExpression extends Expression {

    /**
     * Evaluates this expression and returns logical true if the result is
     * logical false. Otherwise, false.
     *
     * <p>If this expression does not evaluate to logical true or false, then
     * the result is undefined.
     *
     * @return True if false; false if true.
     */
    BooleanExpression not();

    /**
     * Returns logical true if this or the other expression evaluate to logical
     * true. Otherwise, false.
     *
     * <p>If this expression does not evaluate to logical true or false, then
     * the result is undefined. The order of expression evaluation, and whether
     * both are evaluated, is undefined.
     *
     * @param or the other boolean expression.
     * @return True if either true, false otherwise.
     */
    BooleanExpression or(BooleanExpression or);

    /**
     * Evaluates this and the other expression and returns logical true if both
     * this and the other expression evaluate to logical true. Otherwise, false.
     *
     * <p>If this expression does not evaluate to logical true or false, then
     * the result is undefined. The order of expression evaluation is undefined.
     *
     * @param and the other boolean expression.
     * @return True if both true, false otherwise.
     */
    BooleanExpression and(BooleanExpression and);

    /**
     * Evaluates this expression. If true, returns the result of the evaluated
     * left branch expression. Otherwise, returns the result of the evaluated
     * right branch expression.
     *
     * <p>If this expression does not evaluate to logical true or false, then
     * the result is undefined. The order of expression evaluation, and whether
     * both are evaluated, is undefined.
     *
     * @param left the left branch expression, evaluated if this is true.
     * @param right the right branch expression, evaluated if this is false.
     * @return Left if true, right if false.
     * @param <T> The type of the resulting expression.
     */
    <T extends Expression> T cond(T left, T right);
}
