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

import com.mongodb.annotations.Evolving;

import java.util.function.Function;

/**
 * Expressions express values that may be represented in (or computations that
 * may be performed within) a MongoDB server. Each expression evaluates to some
 * value, much like any Java expression evaluates to some value. Expressions may
 * be thought of as boxed values. Evaluation of an expression will usually occur
 * on a MongoDB server.
 *
 * <p>Users should treat these interfaces as sealed, and must not implement any
 * expression interfaces.
 *
 * <p>Expressions are typed. It is possible to execute expressions against data
 * that is of the wrong type, such as by applying the "not" boolean expression
 * to a document field that is an integer, null, or missing. This API does not
 * define the output in such cases (though the output may be defined within the
 * execution context - the server - where the expression is evaluated). Users of
 * this API must mitigate any risk of applying an expression to some type where
 * resulting behaviour is not defined by this API (for example, by checking for
 * null, by ensuring that field types are correctly specified). Likewise, unless
 * otherwise specified, this API does not define the order of evaluation for all
 * arguments, and whether all arguments to some expression will be evaluated.
 *
 * @see Expressions
 */
@Evolving
public interface Expression {

    /**
     * Returns logical true if the value of this expression is equal to the
     * value of the other expression. Otherwise, false.
     *
     * @param eq the other expression
     * @return true if equal, false if not equal
     */
    BooleanExpression eq(Expression eq);

    /**
     * Returns logical true if the value of this expression is not equal to the
     * value of the other expression. Otherwise, false.
     *
     * @param ne the other expression
     * @return true if equal, false otherwise
     */
    BooleanExpression ne(Expression ne);

    /**
     * Returns logical true if the value of this expression is greater than the
     * value of the other expression. Otherwise, false.
     *
     * @param gt the other expression
     * @return true if greater than, false otherwise
     */
    BooleanExpression gt(Expression gt);

    /**
     * Returns logical true if the value of this expression is greater than or
     * equal to the value of the other expression. Otherwise, false.
     *
     * @param gte the other expression
     * @return true if greater than or equal to, false otherwise
     */
    BooleanExpression gte(Expression gte);

    /**
     * Returns logical true if the value of this expression is less than the
     * value of the other expression. Otherwise, false.
     *
     * @param lt the other expression
     * @return true if less than, false otherwise
     */
    BooleanExpression lt(Expression lt);

    /**
     * Returns logical true if the value of this expression is less than or
     * equal to the value of the other expression. Otherwise, false.
     *
     * @param lte the other expression
     * @return true if less than or equal to, false otherwise
     */
    BooleanExpression lte(Expression lte);

    /**
     * also checks for nulls
     * @param other
     * @return
     */
    BooleanExpression isBooleanOr(BooleanExpression other);
    NumberExpression isNumberOr(NumberExpression other);
    IntegerExpression isIntegerOr(IntegerExpression other);
    StringExpression isStringOr(StringExpression other);
    DateExpression isDateOr(DateExpression other);
    <T extends Expression> ArrayExpression<T> isArrayOr(ArrayExpression<? extends T> other);
    <T extends DocumentExpression> T isDocumentOr(T other);

    <T extends Expression> MapExpression<T> isMapOr(MapExpression<? extends T> other);

    StringExpression asString();

    <R extends Expression> R passTo(Function<? super Expression, R> f);

    <R extends Expression> R switchOn(Function<Branches, ? extends BranchesTerminal<? super Expression, R>> on);

}
