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
 * <p>Users should treat these interfaces as sealed, and must not implement any
 * sub-interfaces.
 *
 * TODO-END: 'cause an error', 'execution context', wrong types/unsafe operations
 * TODO-END: types and how missing/null are not part of any type.
 * TODO-END: behaviour of equals
 *
 * @see Expressions
 */
@Evolving
public interface Expression {

    /**
     * The method {@link Expression#eq} should be used to compare values for
     * equality.
     */
    @Deprecated // TODO-END (?) <p>Marked deprecated to prevent erroneous use.
    @Override
    boolean equals(Object other);

    /**
     * Whether {@code this} value is equal to the {@code other} value.
     *
     * <p>The result does not correlate with {@link Expression#equals(Object)}.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    BooleanExpression eq(Expression other);

    /**
     * Whether {@code this} value is not equal to the {@code other} value.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    BooleanExpression ne(Expression other);

    /**
     * Whether {@code this} value is greater than the {@code other} value.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    BooleanExpression gt(Expression other);

    /**
     * Whether {@code this} value is greater than or equal to the {@code other}
     * value.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    BooleanExpression gte(Expression other);

    /**
     * Whether {@code this} value is less than the {@code other} value.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    BooleanExpression lt(Expression other);

    /**
     * Whether {@code this} value is less than or equal to the {@code other}
     * value.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    BooleanExpression lte(Expression other);

    /**
     * {@code this} value as a {@link BooleanExpression boolean} if
     * {@code this} is a boolean, or the {@code other} boolean value if
     * {@code this} is null, or is missing, or is of any other non-boolean type.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    BooleanExpression isBooleanOr(BooleanExpression other);

    /**
     * {@code this} value as a {@link NumberExpression number} if
     * {@code this} is a number, or the {@code other} number value if
     * {@code this} is null, or is missing, or is of any other non-number type.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    NumberExpression isNumberOr(NumberExpression other);

    /**
     * {@code this} value as an {@link IntegerExpression integer} if
     * {@code this} is an integer, or the {@code other} integer value if
     * {@code this} is null, or is missing, or is of any other non-integer type.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    IntegerExpression isIntegerOr(IntegerExpression other);

    /**
     * {@code this} value as a {@link StringExpression string} if
     * {@code this} is a string, or the {@code other} string value if
     * {@code this} is null, or is missing, or is of any other non-string type.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    StringExpression isStringOr(StringExpression other);

    /**
     * {@code this} value as a {@link DateExpression boolean} if
     * {@code this} is a date, or the {@code other} date value if
     * {@code this} is null, or is missing, or is of any other non-date type.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    DateExpression isDateOr(DateExpression other);


    /**
     * {@code this} value as a {@link ArrayExpression array} if
     * {@code this} is an array, or the {@code other} array value if
     * {@code this} is null, or is missing, or is of any other non-array type.
     *
     * <p>Warning: this operation does not guarantee type safety. While this
     * operation is guaranteed to produce an array, the type of the elements of
     * that array are not guaranteed by the API. The specification of a type by
     * the user is an unchecked assertion that all elements are of that type,
     * and that no element is null, is missing, or is of some other type. If the
     * user cannot make such an assertion, some appropriate super-type should be
     * chosen, and if necessary elements should be individually type-checked.
     *
     * @param other the other value.
     * @return the resulting value.
     * @param <T> the type of the elements of the resulting array.
     */
    <T extends Expression> ArrayExpression<T> isArrayOr(ArrayExpression<? extends T> other);

    // TODO-END doc after Map merged, "record" and "schema objects" are decided
    <T extends DocumentExpression> T isDocumentOr(T other);

    <T extends Expression> MapExpression<T> isMapOr(MapExpression<? extends T> other);

    /**
     * The {@link StringExpression string} representation of {@code this} value.
     *
     * <p>This may "cause an error" if the type cannot be converted to string,
     * as is the case with {@link ArrayExpression arrays},
     * {@link DocumentExpression documents}, and {@link MapExpression maps}.
     * TODO-END what about null/missing?
     * TODO-END document vs record
     * TODO-END "cause an error" above
     *
     *
     * @see StringExpression#parseDate()
     * @see StringExpression#parseInteger()
     * TODO-END all the others? implement?
     * @return the resulting value.
     */
    StringExpression asString();

    /**
     * Applies the provided function to {@code this} value.
     *
     * <p>Equivalent to {@code f.apply(this)}, and allows lambdas and static,
     * user-defined functions to use the chaining syntax.
     *
     * @param f the function to apply.
     * @return the resulting value.
     * @param <R> the type of the resulting value.
     */
    <R extends Expression> R passTo(Function<? super Expression, ? extends R> f);
    /**
     * The value resulting from applying the provided switch mapping to
     * {@code this} value.
     *
     * <p>Can be used to perform pattern matching on the type of {@code this}
     * value, or to perform comparisons, or to perform any arbitrary check on
     * {@code this} value.
     *
     * <p>The suggested convention is to use "{@code on}" as the name of the
     * {@code mapping} parameter, for example:
     * {@code myValue.switchOn(on -> on.isInteger(...)...)}.
     *
     * @param mapping the switch mapping.
     * @return the resulting value.
     * @param <R> the type of the resulting value.
     */
    <R extends Expression> R switchOn(Function<Branches<Expression>, ? extends BranchesTerminal<Expression, ? extends R>> mapping);

}
