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

import static com.mongodb.client.model.mql.MqlUnchecked.Unchecked.TYPE_ARGUMENT;

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
 * import static com.mongodb.client.model.mql.MqlValues.current;
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
 * {@link MqlValues} class.
 *
 * <p>As with the Java Stream API's terminal operations, corresponding Java
 * values are not directly available, but must be obtained indirectly via
 * {@code MongoCollection.aggregate} or {@code MongoCollection.find}.
 * Certain methods may cause an error, which will be produced
 * through these "terminal operations".
 *
 * <p>The null value is not part of, and cannot be used as if it were part
 * of, any explicit type (except the root type {@link MqlValue} itself).
 * See {@link MqlValues#ofNull} for more details.
 *
 * <p>This API specifies no "missing" or "undefined" value. Users may use
 * {@link MqlMap#has} to check whether a value is present.
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
 * data is of a certain type. For example, {@link MqlDocument#getArray}}
 * requires that the underlying field is both an array, and an array of some
 * certain type. If the field is not an array in the underlying data, behaviour
 * is undefined by this API (though behaviours may be defined by the execution
 * context, users are strongly discouraged from relying on behaviour that is not
 * part of this API).
 *
 * <p>This API should be treated as sealed:
 * it must not be extended or implemented (unless explicitly allowed).
 *
 * @see MqlValues
 * @since 4.9.0
 */
@Sealed
@Beta(Beta.Reason.CLIENT)
public interface MqlValue {

    /**
     * The method {@link MqlValue#eq} should be used to compare values for
     * equality. This method checks reference equality.
     */
    @Override
    boolean equals(Object other);

    /**
     * Whether {@code this} value is equal to the {@code other} value.
     *
     * <p>The result does not correlate with {@link MqlValue#equals(Object)}.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    MqlBoolean eq(MqlValue other);

    /**
     * Whether {@code this} value is not equal to the {@code other} value.
     *
     * <p>The result does not correlate with {@link MqlValue#equals(Object)}.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    MqlBoolean ne(MqlValue other);

    /**
     * Whether {@code this} value is greater than the {@code other} value.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    MqlBoolean gt(MqlValue other);

    /**
     * Whether {@code this} value is greater than or equal to the {@code other}
     * value.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    MqlBoolean gte(MqlValue other);

    /**
     * Whether {@code this} value is less than the {@code other} value.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    MqlBoolean lt(MqlValue other);

    /**
     * Whether {@code this} value is less than or equal to the {@code other}
     * value.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    MqlBoolean lte(MqlValue other);

    /**
     * {@code this} value as a {@linkplain MqlBoolean boolean} if
     * {@code this} is a boolean, or the {@code other} boolean value if
     * {@code this} is null, or is missing, or is of any other non-boolean type.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    MqlBoolean isBooleanOr(MqlBoolean other);

    /**
     * {@code this} value as a {@linkplain MqlNumber number} if
     * {@code this} is a number, or the {@code other} number value if
     * {@code this} is null, or is missing, or is of any other non-number type.
     *
     * @mongodb.server.release 4.4
     * @param other the other value.
     * @return the resulting value.
     */
    MqlNumber isNumberOr(MqlNumber other);

    /**
     * {@code this} value as an {@linkplain MqlInteger integer} if
     * {@code this} is an integer, or the {@code other} integer value if
     * {@code this} is null, or is missing, or is of any other non-integer type.
     *
     * @mongodb.server.release 5.2
     * @param other the other value.
     * @return the resulting value.
     */
    MqlInteger isIntegerOr(MqlInteger other);

    /**
     * {@code this} value as a {@linkplain MqlString string} if
     * {@code this} is a string, or the {@code other} string value if
     * {@code this} is null, or is missing, or is of any other non-string type.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    MqlString isStringOr(MqlString other);

    /**
     * {@code this} value as a {@linkplain MqlDate boolean} if
     * {@code this} is a date, or the {@code other} date value if
     * {@code this} is null, or is missing, or is of any other non-date type.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    MqlDate isDateOr(MqlDate other);

    /**
     * {@code this} value as a {@linkplain MqlArray array} if
     * {@code this} is an array, or the {@code other} array value if
     * {@code this} is null, or is missing, or is of any other non-array type.
     *
     * <p>Warning: The type of the elements of the resulting array are not
     * enforced by the API. The specification of a type by the user is an
     * unchecked assertion that all elements are of that type.
     * If the array contains multiple types (such as both nulls and integers)
     * then a super-type encompassing all types must be chosen, and
     * if necessary the elements should be individually type-checked when used.
     *
     * @param other the other value.
     * @return the resulting value.
     * @param <T> the type of the elements of the resulting array.
     */
    <T extends MqlValue> MqlArray<@MqlUnchecked(TYPE_ARGUMENT) T> isArrayOr(MqlArray<? extends T> other);

    /**
     * {@code this} value as a {@linkplain MqlDocument document} if
     * {@code this} is a document (or document-like value, see
     * {@link MqlMap} and {@link MqlEntry})
     * or the {@code other} document value if {@code this} is null,
     * or is missing, or is of any other non-document type.
     *
     * @param other the other value.
     * @return the resulting value.
     * @param <T> the type.
     */
    <T extends MqlDocument> T isDocumentOr(T other);

    /**
     * {@code this} value as a {@linkplain MqlMap map} if
     * {@code this} is a map (or map-like value, see
     * {@link MqlDocument} and {@link MqlEntry})
     * or the {@code other} map value if {@code this} is null,
     * or is missing, or is of any other non-map type.
     *
     * <p>Warning: The type of the values of the resulting map are not
     * enforced by the API. The specification of a type by the user is an
     * unchecked assertion that all map values are of that type.
     * If the map contains multiple types (such as both nulls and integers)
     * then a super-type encompassing all types must be chosen, and
     * if necessary the elements should be individually type-checked when used.
     *
     * @param other the other value.
     * @return the resulting value.
     * @param <T> the type of the values of the resulting map.
     */
    <T extends MqlValue> MqlMap<@MqlUnchecked(TYPE_ARGUMENT) T> isMapOr(MqlMap<? extends T> other);

    /**
     * The {@linkplain MqlString string} representation of {@code this} value.
     *
     * <p>This will cause an error if the type cannot be converted
     * to a {@linkplain MqlString string}, as is the case with
     * {@linkplain MqlArray arrays},
     * {@linkplain MqlDocument documents},
     * {@linkplain MqlMap maps},
     * {@linkplain MqlEntry entries}, and the
     * {@linkplain MqlValues#ofNull() null value}.
     *
     * @mongodb.server.release 4.0
     * @see MqlString#parseDate()
     * @see MqlString#parseInteger()
     * @return the resulting value.
     */
    MqlString asString();

    /**
     * The result of passing {@code this} value to the provided function.
     * Equivalent to {@code f.apply(this)}, and allows lambdas and static,
     * user-defined functions to use the chaining syntax.
     *
     * <p>The appropriate type-based variant should be used when the type
     * of {@code this} is known.
     *
     * @see MqlBoolean#passBooleanTo
     * @see MqlInteger#passIntegerTo
     * @see MqlNumber#passNumberTo
     * @see MqlString#passStringTo
     * @see MqlDate#passDateTo
     * @see MqlArray#passArrayTo
     * @see MqlMap#passMapTo
     * @see MqlDocument#passDocumentTo
     *
     * @param f the function to apply.
     * @return the resulting value.
     * @param <R> the type of the resulting value.
     */
    <R extends MqlValue> R passTo(Function<? super MqlValue, ? extends R> f);

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
     * @see MqlBoolean#switchBooleanOn
     * @see MqlInteger#switchIntegerOn
     * @see MqlNumber#switchNumberOn
     * @see MqlString#switchStringOn
     * @see MqlDate#switchDateOn
     * @see MqlArray#switchArrayOn
     * @see MqlMap#switchMapOn
     * @see MqlDocument#switchDocumentOn
     *
     * @param mapping the switch mapping.
     * @return the resulting value.
     * @param <R> the type of the resulting value.
     */
    <R extends MqlValue> R switchOn(Function<Branches<MqlValue>, ? extends BranchesTerminal<MqlValue, ? extends R>> mapping);
}
