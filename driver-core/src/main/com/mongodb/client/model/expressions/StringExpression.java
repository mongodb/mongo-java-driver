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

import com.mongodb.annotations.Beta;
import com.mongodb.annotations.Sealed;

import java.util.function.Function;

import static com.mongodb.client.model.expressions.Expressions.of;

/**
 * A string {@linkplain Expression value} in the context of the MongoDB Query
 * Language (MQL).
 *
 * @since 4.9.0
 */
@Sealed
@Beta(Beta.Reason.CLIENT)
public interface StringExpression extends Expression {

    /**
     * Converts {@code this} string to lowercase.
     *
     * @return the resulting value.
     */
    StringExpression toLower();

    /**
     * Converts {@code this} string to uppercase.
     *
     * @return the resulting value.
     */
    StringExpression toUpper();

    /**
     * The concatenation of {@code this} string, followed by
     * the {@code other} string.
     *
     * @param other the other value.
     * @return the resulting value.
     */
    StringExpression concat(StringExpression other);

    /**
     * The number of Unicode code points in {@code this} string.
     *
     * @return the resulting value.
     */
    IntegerExpression strLen();

    /**
     * The number of UTF-8 encoded bytes in {@code this} string.
     *
     * @return the resulting value.
     */
    IntegerExpression strLenBytes();

    /**
     * The substring of {@code this} string, from the {@code start} index
     * inclusive, and including the specified {@code length}, up to
     * the end of the string.
     *
     * <p>Warning: the index position is in Unicode code points, not in
     * UTF-8 encoded bytes.
     *
     * @param start the start index in Unicode code points.
     * @param length the length in Unicode code points.
     * @return the resulting value.
     */
    StringExpression substr(IntegerExpression start, IntegerExpression length);

    /**
     * The substring of {@code this} string, from the {@code start} index
     * inclusive, and including the specified {@code length}, up to
     * the end of the string.
     *
     * <p>Warning: the index position is in Unicode code points, not in
     * UTF-8 encoded bytes.
     *
     * @param start the start index in Unicode code points.
     * @param length the length in Unicode code points.
     * @return the resulting value.
     */
    default StringExpression substr(final int start, final int length) {
        return this.substr(of(start), of(length));
    }

    /**
     * The substring of {@code this} string, from the {@code start} index
     * inclusive, and including the specified {@code length}, up to
     * the end of the string.
     *
     * <p>The index position is in UTF-8 encoded bytes, not in
     * Unicode code points.
     *
     * @param start the start index in UTF-8 encoded bytes.
     * @param length the length in UTF-8 encoded bytes.
     * @return the resulting value.
     */
    StringExpression substrBytes(IntegerExpression start, IntegerExpression length);

    /**
     * The substring of {@code this} string, from the {@code start} index
     * inclusive, and including the specified {@code length}, up to
     * the end of the string.
     *
     * <p>The index position is in UTF-8 encoded bytes, not in
     * Unicode code points.
     *
     * @param start the start index in UTF-8 encoded bytes.
     * @param length the length in UTF-8 encoded bytes.
     * @return the resulting value.
     */
    default StringExpression substrBytes(final int start, final int length) {
        return this.substrBytes(of(start), of(length));
    }

    /**
     * Converts {@code this} string to an {@linkplain IntegerExpression integer}.
     *
     * <p>This will cause an error if this string does not represent an integer.
     *
     * @mongodb.server.release 4.0
     * @return the resulting value.
     */
    IntegerExpression parseInteger();

    /**
     * Converts {@code this} string to a {@linkplain DateExpression date}.
     *
     * <p>This method behaves like {@link #parseDate(StringExpression)},
     * with the default format, which is {@code "%Y-%m-%dT%H:%M:%S.%LZ"}.
     *
     * <p>Will cause an error if this string does not represent a valid
     * date string (such as "2018-03-20", "2018-03-20T12:00:00Z", or
     * "2018-03-20T12:00:00+0500").
     *
     * @see DateExpression#asString()
     * @see DateExpression#asString(StringExpression, StringExpression)
     * @return the resulting value.
     */
    DateExpression parseDate();

    /**
     * Converts {@code this} string to a {@linkplain DateExpression date},
     * using the specified {@code format}. UTC is assumed if the timezone
     * offset element is not specified in the format.
     *
     * <p>Will cause an error if {@code this} string does not match the
     * specified {@code format}.
     * Will cause an error if an element is specified that is finer-grained
     * than an element that is not specified, with year being coarsest
     * (for example, minute is specified, but hour is not).
     * Omitted finer-grained elements will be parsed to 0.
     *
     * @see DateExpression#asString()
     * @see DateExpression#asString(StringExpression, StringExpression)
     * @mongodb.server.release 4.0
     * @mongodb.driver.manual reference/operator/aggregation/dateFromString/ Format Specifiers, UTC Offset, and Olson Timezone Identifier
     * @param format the format.
     * @return the resulting value.
     */
    DateExpression parseDate(StringExpression format);

    /**
     * Converts {@code this} string to a {@linkplain DateExpression date},
     * using the specified {@code timezone} and {@code format}.
     *

     * <p>Will cause an error if {@code this} string does not match the
     * specified {@code format}.
     * Will cause an error if an element is specified that is finer-grained
     * than an element that is not specified, with year being coarsest
     * (for example, minute is specified, but hour is not).
     * Omitted finer-grained elements will be parsed to 0.
     * Will cause an error if the format includes an offset or
     * timezone, even if it matches the supplied {@code timezone}.
     *
     * @see DateExpression#asString()
     * @see DateExpression#asString(StringExpression, StringExpression)
     * @mongodb.driver.manual reference/operator/aggregation/dateFromString/ Format Specifiers, UTC Offset, and Olson Timezone Identifier
     * @param format the format.
     * @param timezone the UTC Offset or Olson Timezone Identifier.
     * @return the resulting value.
     */
    DateExpression parseDate(StringExpression timezone, StringExpression format);

    /**
     * The result of passing {@code this} value to the provided function.
     * Equivalent to {@code f.apply(this)}, and allows lambdas and static,
     * user-defined functions to use the chaining syntax.
     *
     * @see Expression#passTo
     * @param f the function to apply.
     * @return the resulting value.
     * @param <R> the type of the resulting value.
     */
    <R extends Expression> R passStringTo(Function<? super StringExpression, ? extends R> f);

    /**
     * The result of applying the provided switch mapping to {@code this} value.
     *
     * @see Expression#switchOn
     * @param mapping the switch mapping.
     * @return the resulting value.
     * @param <R> the type of the resulting value.
     */
    <R extends Expression> R switchStringOn(Function<Branches<StringExpression>, ? extends BranchesTerminal<StringExpression, ? extends R>> mapping);
}
