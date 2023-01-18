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
 * A value in the context of the MongoDB Query Language (MQL).
 *
 * <p>The API provided by this base type and its subtypes is the Java-native
 * variant of MQL. It is used to query the MongoDB server, to perform remote
 * computations, to store and retrieve data, or to otherwise work with data on
 * a MongoDB server or compatible execution context. Though the methods exposed
 * through this API generally correspond to MQL operations, this correspondence
 * is not exact.
 *
 * <p>The following is an example of usage within an aggregation pipeline. Here,
 * the current document value is obtained and its "numberArray" field is
 * filtered and summed, in a style similar to that of the Java Stream API:
 *
 * <pre>{@code
 * import static com.mongodb.client.model.expressions.Expressions.current;
 * MongoCollection<Document> col = ...;
 * AggregateIterable<Document> result = col.aggregate(Arrays.asList(
 *     addFields(new Field<>("result", current()
 *         .<MqlNumber>getArray("numberArray")
 *         .filter(v -> v.gt(of(0)))
 *         .sum(v -> v)))));
 * }</pre>
 *
 * <p>Values are typically initially obtained via the current document and its
 * fields, or specified via statically-imported methods on the
 * {@link Expressions} class.
 *
 * <p>As with the Java Stream API's terminal operations, corresponding Java
 * values are not directly available, but must be obtained indirectly via
 * {@code MongoCollection.aggregate} or {@code MongoCollection.find}.
 * Certain methods may cause an error, which will be produced
 * through these "terminal operations".
 *
 * <p>The null value is not part of, and cannot be used as if it were part
 * of, any explicit type (except the root type {@link Expression} itself).
 * See {@link Expressions#ofNull} for more details.
 *
 * <p>There is no explicit "missing" or "undefined" value. Users may use
 * {@link MapExpression#has}.
 *
 * <p>This type hierarchy differs from the {@linkplain org.bson} types in that
 * they provide computational operations, the numeric types are less granular,
 * and it offers multiple abstractions of certain types (document, map, entry).
 * It differs from the corresponding Java types (such as {@code int},
 * {@link String}, {@link java.util.Map}) in that the operations
 * available differ, and in that an implementation of this API may be used to
 * produce MQL in the form of BSON. (This API makes no guarantee regarding the
 * BSON output produced by its implementation, which in any case may vary due
 * to optimization or other factors.)
 *
 * <p>Some methods within the API constitute an assertion by the user that the
 * data is of a certain type. For example, {@link DocumentExpression#getArray}}
 * requires that the underlying field is both an array, and an array of some
 * certain type. If the field is not an array in the underlying data, behaviour
 * is undefined by this API (though behaviours may be defined by the execution
 * context, users are strongly discouraged from relying on behaviour that is not
 * part of this API).
 *
 *
 * <p>Users should treat these interfaces as sealed, and should not create
 * implementations.
 *
 * @see Expressions
 */
@Evolving
public interface Expression {

    /**
     * The method {@link Expression#eq} should be used to compare values for
     * equality. This method checks reference equality.
     */
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
     * <p>The result does not correlate with {@link Expression#equals(Object)}.
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
     * {@code this} value as a {@linkplain BooleanExpression boolean} if
     * {@code this} is a boolean, or the {@code other} boolean value if
     * {@code this} is null, or is missing, or is of any other non-boolean type.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    BooleanExpression isBooleanOr(BooleanExpression other);

    /**
     * {@code this} value as a {@linkplain NumberExpression number} if
     * {@code this} is a number, or the {@code other} number value if
     * {@code this} is null, or is missing, or is of any other non-number type.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    NumberExpression isNumberOr(NumberExpression other);

    /**
     * {@code this} value as an {@linkplain IntegerExpression integer} if
     * {@code this} is an integer, or the {@code other} integer value if
     * {@code this} is null, or is missing, or is of any other non-integer type.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    IntegerExpression isIntegerOr(IntegerExpression other);

    /**
     * {@code this} value as a {@linkplain StringExpression string} if
     * {@code this} is a string, or the {@code other} string value if
     * {@code this} is null, or is missing, or is of any other non-string type.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    StringExpression isStringOr(StringExpression other);

    /**
     * {@code this} value as a {@linkplain DateExpression boolean} if
     * {@code this} is a date, or the {@code other} date value if
     * {@code this} is null, or is missing, or is of any other non-date type.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    DateExpression isDateOr(DateExpression other);

    /**
     * {@code this} value as a {@linkplain ArrayExpression array} if
     * {@code this} is an array, or the {@code other} array value if
     * {@code this} is null, or is missing, or is of any other non-array type.
     *
     * <p>Warning: this operation does not guarantee type safety. While this
     * operation is guaranteed to produce an array, the type of the elements of
     * that array are not guaranteed by the API. The specification of a type by
     * the user is an unchecked assertion that all elements are of that type,
     * and that no element is null, is missing, or is of some other type. If the
     * user cannot make such an assertion, some appropriate super-type should be
     * chosen, and if necessary the elements should be individually type-checked.
     *
     * @param other the other value.
     * @return the resulting value.
     * @param <T> the type of the elements of the resulting array.
     */
    <T extends Expression> ArrayExpression<T> isArrayOr(ArrayExpression<? extends T> other);

    /**
     * {@code this} value as a {@linkplain DocumentExpression document} if
     * {@code this} is a document or document-like value (such as a
     * {@linkplain MapExpression map} or
     * {@linkplain EntryExpression entry})
     * or the {@code other} document value if
     * {@code this} is null, or is missing, or is of any other non-document type.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    <T extends DocumentExpression> T isDocumentOr(T other);

    /**
     * {@code this} value as a {@linkplain MapExpression map} if
     * {@code this} is a map or map-like value (such as a
     * {@linkplain DocumentExpression document} or
     * {@linkplain EntryExpression entry})
     * or the {@code other} map value if
     * {@code this} is null, or is missing, or is of any other non-map type.
     *
     * <p>Warning: this operation does not guarantee type safety. While this
     * operation is guaranteed to produce a map, the type of the values of
     * that array are not guaranteed by the API. The specification of a type by
     * the user is an unchecked assertion that all values are of that type,
     * and that no value is null of some other type. If the
     * user cannot make such an assertion, some appropriate super-type should be
     * chosen, and if necessary the values should be individually type-checked.
     *
     * @param other the other value.
     * @return the resulting value.
     * @param <T> the type of the values of the resulting map.
     */
    <T extends Expression> MapExpression<T> isMapOr(MapExpression<? extends T> other);

    /**
     * The {@linkplain StringExpression string} representation of {@code this} value.
     *
     * <p>This may cause an error to be produced if the type cannot be converted
     * to a {@linkplain StringExpression string}, as is the case with
     * {@linkplain ArrayExpression arrays},
     * {@linkplain DocumentExpression documents},
     * {@linkplain MapExpression maps},
     * {@linkplain EntryExpression entries}, and the
     * {@linkplain Expressions#ofNull() null value}.
     *
     * @see StringExpression#parseDate()
     * @see StringExpression#parseInteger()
     * @return the resulting value.
     */
    StringExpression asString();

    /**
     * The result of passing {@code this} value to the provided function.
     * Equivalent to {@code f.apply(this)}, and allows lambdas and static,
     * user-defined functions to use the chaining syntax.
     *
     * <p>The appropriate type-based variant should be used when the type
     * of {@code this} is known.
     *
     * @see BooleanExpression#passBooleanTo
     * @see IntegerExpression#passIntegerTo
     * @see NumberExpression#passNumberTo
     * @see StringExpression#passStringTo
     * @see DateExpression#passDateTo
     * @see ArrayExpression#passArrayTo
     * @see MapExpression#passMapTo
     * @see DocumentExpression#passDocumentTo
     *
     * @param f the function to apply.
     * @return the resulting value.
     * @param <R> the type of the resulting value.
     */
    <R extends Expression> R passTo(Function<? super Expression, ? extends R> f);

    /**
     * The result of applying the provided switch mapping to {@code this} value.
     *
     * <p>Can be used to perform pattern matching on the type of {@code this}
     * value, or to perform comparisons, or to perform any arbitrary check on
     * {@code this} value.
     *
     * <p>The suggested convention is to use "{@code on}" as the name of the
     * {@code mapping} parameter, for example:
     *
     * <pre>{@code
     * myValue.switchOn(on -> on
     *     .isInteger(...)
     *     ...
     *     .defaults(...))
     * }</pre>
     *
     * <p>The appropriate type-based variant should be used when the type
     * of {@code this} is known.
     *
     * @see BooleanExpression#switchBooleanOn
     * @see IntegerExpression#switchIntegerOn
     * @see NumberExpression#switchNumberOn
     * @see StringExpression#switchStringOn
     * @see DateExpression#switchDateOn
     * @see ArrayExpression#switchArrayOn
     * @see MapExpression#switchMapOn
     * @see DocumentExpression#switchDocumentOn
     *
     * @param mapping the switch mapping.
     * @return the resulting value.
     * @param <R> the type of the resulting value.
     */
    <R extends Expression> R switchOn(Function<Branches<Expression>, ? extends BranchesTerminal<Expression, ? extends R>> mapping);
}
