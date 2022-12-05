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
 * A UTC date-time {@linkplain Expression value} in the context
 * of the MongoDB Query Language (MQL). Tracks the number of
 * milliseconds since the Unix epoch, and does not track the timezone.
 *
 * @mongodb.driver.manual reference/operator/aggregation/dateToString/ Format Specifiers, UTC Offset, and Olson Timezone Identifier
 */
public interface DateExpression extends Expression {

    /**
     * The year of {@code this} date as determined by the provided
     * {@code timezone}.
     *
     * @param timezone the UTC Offset or Olson Timezone Identifier.
     * @return the resulting value.
     */
    IntegerExpression year(StringExpression timezone);

    /**
     * The month of {@code this} date as determined by the provided
     * {@code timezone}, as an integer between 1 and 12.
     *
     * @param timezone the UTC Offset or Olson Timezone Identifier.
     * @return the resulting value.
     */
    IntegerExpression month(StringExpression timezone);

    /**
     * The day of the month of {@code this} date as determined by the provided
     * {@code timezone}, as an integer between 1 and 31.
     *
     * @param timezone the UTC Offset or Olson Timezone Identifier.
     * @return the resulting value.
     */
    IntegerExpression dayOfMonth(StringExpression timezone);

    /**
     * The day of the week of {@code this} date as determined by the provided
     * {@code timezone}, as an integer between 1 (Sunday) and 7 (Saturday).
     *
     * @param timezone the UTC Offset or Olson Timezone Identifier.
     * @return the resulting value.
     */
    IntegerExpression dayOfWeek(StringExpression timezone);

    /**
     * The day of the year of {@code this} date as determined by the provided
     * {@code timezone}, as an integer between 1 and 366.
     *
     * @param timezone the UTC Offset or Olson Timezone Identifier.
     * @return the resulting value.
     */
    IntegerExpression dayOfYear(StringExpression timezone);

    /**
     * The hour of {@code this} date as determined by the provided
     * {@code timezone}, as an integer between 0 and 23.
     *
     * @param timezone the UTC Offset or Olson Timezone Identifier.
     * @return the resulting value.
     */
    IntegerExpression hour(StringExpression timezone);

    /**
     * The minute of {@code this} date as determined by the provided
     * {@code timezone}, as an integer between 0 and 59.
     *
     * @param timezone the UTC Offset or Olson Timezone Identifier.
     * @return the resulting value.
     */
    IntegerExpression minute(StringExpression timezone);

    /**
     * The second of {@code this} date as determined by the provided
     * {@code timezone}, as an integer between 0 and 59, and 60 in the case
     * of a leap second.
     *
     * @param timezone the UTC Offset or Olson Timezone Identifier.
     * @return the resulting value.
     */
    IntegerExpression second(StringExpression timezone);

    /**
     * The week of the year of {@code this} date as determined by the provided
     * {@code timezone}, as an integer between 0 and 53.
     *
     * <p>Weeks begin on Sundays, and week 1 begins with the first Sunday of the
     * year. Days preceding the first Sunday of the year are in week 0.
     *
     * @param timezone the UTC Offset or Olson Timezone Identifier.
     * @return the resulting value.
     */
    IntegerExpression week(StringExpression timezone);

    /**
     * The millisecond part of {@code this} date as determined by the provided
     * {@code timezone}, as an integer between 0 and 999.
     *
     * @param timezone the UTC Offset or Olson Timezone Identifier.
     * @return the resulting value.
     */
    IntegerExpression millisecond(StringExpression timezone);

    /**
     * The string representation of {@code this} date as determined by the
     * provided {@code timezone}, and formatted according to the {@code format}.
     *
     * @param timezone the UTC Offset or Olson Timezone Identifier.
     * @param format the format specifier.
     * @return the resulting value.
     */
    StringExpression asString(StringExpression timezone, StringExpression format);

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
    <R extends Expression> R passDateTo(Function<? super DateExpression, ? extends R> f);

    /**
     * The result of applying the provided switch mapping to {@code this} value.
     *
     * @see Expression#switchOn
     * @param mapping the switch mapping.
     * @return the resulting value.
     * @param <R> the type of the resulting value.
     */
    <R extends Expression> R switchDateOn(Function<Branches<DateExpression>, ? extends BranchesTerminal<DateExpression, ? extends R>> mapping);
}
