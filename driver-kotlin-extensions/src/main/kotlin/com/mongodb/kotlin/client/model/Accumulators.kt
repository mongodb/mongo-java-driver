/*
 * Copyright 2008-present MongoDB, Inc.
 * Copyright (C) 2016/2022 Litote
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
 *
 * @custom-license-header
 */
package com.mongodb.kotlin.client.model

import com.mongodb.client.model.Accumulators
import com.mongodb.client.model.BsonField
import com.mongodb.client.model.QuantileMethod
import kotlin.reflect.KProperty
import org.bson.conversions.Bson

/**
 * Accumulators extension methods to improve Kotlin interop
 *
 * @since 5.3
 */
@Suppress("TooManyFunctions")
public object Accumulators {
    /**
     * Gets a field name for a $group operation representing the sum of the values of the given expression when applied
     * to all members of the group.
     *
     * @param property the data class property
     * @param expression the expression
     * @param <TExpression> the expression type
     * @return the field @mongodb.driver.manual reference/operator/aggregation/sum/ $sum
     */
    public fun <TExpression> sum(property: KProperty<*>, expression: TExpression): BsonField =
        Accumulators.sum(property.path(), expression)

    /**
     * Gets a field name for a $group operation representing the average of the values of the given expression when
     * applied to all members of the group.
     *
     * @param property the data class property
     * @param expression the expression
     * @param <TExpression> the expression type
     * @return the field @mongodb.driver.manual reference/operator/aggregation/avg/ $avg
     */
    public fun <TExpression> avg(property: KProperty<*>, expression: TExpression): BsonField =
        Accumulators.avg(property.path(), expression)

    /**
     * Returns a combination of a computed field and an accumulator that generates a BSON {@link org.bson.BsonType#ARRAY
     * Array} containing computed values from the given {@code inExpression} based on the provided {@code pExpression},
     * which represents an array of percentiles of interest within a group, where each element is a numeric value
     * between 0.0 and 1.0 (inclusive).
     *
     * @param property The data class property computed by the accumulator.
     * @param inExpression The input expression.
     * @param pExpression The expression representing a percentiles of interest.
     * @param method The method to be used for computing the percentiles.
     * @param <InExpression> The type of the input expression.
     * @param <PExpression> The type of the percentile expression.
     * @return The requested {@link BsonField}. @mongodb.driver.manual reference/operator/aggregation/percentile/
     *   $percentile @mongodb.server.release 7.0
     */
    public fun <InExpression, PExpression> percentile(
        property: KProperty<*>,
        inExpression: InExpression,
        pExpression: PExpression,
        method: QuantileMethod
    ): BsonField = Accumulators.percentile(property.path(), inExpression, pExpression, method)

    /**
     * Returns a combination of a computed field and an accumulator that generates a BSON {@link
     * org.bson.BsonType#DOUBLE Double } representing the median value computed from the given {@code inExpression}
     * within a group.
     *
     * @param property The data class property computed by the accumulator.
     * @param inExpression The input expression.
     * @param method The method to be used for computing the median.
     * @param <InExpression> The type of the input expression.
     * @return The requested {@link BsonField}. @mongodb.driver.manual reference/operator/aggregation/median/
     *   $median @mongodb.server.release 7.0
     */
    public fun <InExpression> median(
        property: KProperty<*>,
        inExpression: InExpression,
        method: QuantileMethod
    ): BsonField = Accumulators.median(property.path(), inExpression, method)

    /**
     * Gets a field name for a $group operation representing the value of the given expression when applied to the first
     * member of the group.
     *
     * @param property The data class property
     * @param expression the expression
     * @param <TExpression> the expression type
     * @return the field @mongodb.driver.manual reference/operator/aggregation/first/ $first
     */
    public fun <TExpression> first(property: KProperty<*>, expression: TExpression): BsonField =
        Accumulators.first(property.path(), expression)

    /**
     * Returns a combination of a computed field and an accumulator that produces a BSON {@link org.bson.BsonType#ARRAY
     * Array} of values of the given {@code inExpression} computed for the first {@code N} elements within a presorted
     * group, where {@code N} is the positive integral value of the {@code nExpression}.
     *
     * @param property The data class property computed by the accumulator.
     * @param inExpression The input expression.
     * @param nExpression The expression limiting the number of produced values.
     * @param <InExpression> The type of the input expression.
     * @param <NExpression> The type of the limiting expression.
     * @return The requested {@link BsonField}. @mongodb.driver.manual reference/operator/aggregation/firstN/
     *   $firstN @mongodb.server.release 5.2
     */
    public fun <InExpression, NExpression> firstN(
        property: KProperty<*>,
        inExpression: InExpression,
        nExpression: NExpression
    ): BsonField = Accumulators.firstN(property.path(), inExpression, nExpression)

    /**
     * Returns a combination of a computed field and an accumulator that produces a value of the given {@code
     * outExpression} computed for the top element within a group sorted according to the provided {@code sortBy}
     * specification.
     *
     * @param property The data class property computed by the accumulator.
     * @param sortBy The {@linkplain Sorts sort specification}. The syntax is identical to the one expected by {@link
     *   Aggregates#sort(Bson)}.
     * @param outExpression The output expression.
     * @param <OutExpression> The type of the output expression.
     * @return The requested {@link BsonField}. @mongodb.driver.manual reference/operator/aggregation/top/
     *   $top @mongodb.server.release 5.2
     */
    public fun <OutExpression> top(property: KProperty<*>, sortBy: Bson, outExpression: OutExpression): BsonField =
        Accumulators.top(property.path(), sortBy, outExpression)

    /**
     * Returns a combination of a computed field and an accumulator that produces a BSON {@link org.bson.BsonType#ARRAY
     * Array} of values of the given {@code outExpression} computed for the top {@code N} elements within a group sorted
     * according to the provided {@code sortBy} specification, where {@code N} is the positive integral value of the
     * {@code nExpression}.
     *
     * @param property The data class property computed by the accumulator.
     * @param sortBy The {@linkplain Sorts sort specification}. The syntax is identical to the one expected by {@link
     *   Aggregates#sort(Bson)}.
     * @param outExpression The output expression.
     * @param nExpression The expression limiting the number of produced values.
     * @param <OutExpression> The type of the output expression.
     * @param <NExpression> The type of the limiting expression.
     * @return The requested {@link BsonField}. @mongodb.driver.manual reference/operator/aggregation/topN/ $topN
     * @since 4.7 @mongodb.server.release 5.2
     */
    public fun <OutExpression, NExpression> topN(
        property: KProperty<*>,
        sortBy: Bson,
        outExpression: OutExpression,
        nExpression: NExpression
    ): BsonField = Accumulators.topN(property.path(), sortBy, outExpression, nExpression)

    /**
     * Gets a field name for a $group operation representing the value of the given expression when applied to the last
     * member of the group.
     *
     * @param property The data class property
     * @param expression the expression
     * @param <TExpression> the expression type
     * @return the field @mongodb.driver.manual reference/operator/aggregation/last/ $last
     */
    public fun <TExpression> last(property: KProperty<*>, expression: TExpression): BsonField =
        Accumulators.last(property.path(), expression)

    /**
     * Returns a combination of a computed field and an accumulator that produces a BSON {@link org.bson.BsonType#ARRAY
     * Array} of values of the given {@code inExpression} computed for the last {@code N} elements within a presorted
     * group, where {@code N} is the positive integral value of the {@code nExpression}.
     *
     * @param property The data class property computed by the accumulator.
     * @param inExpression The input expression.
     * @param nExpression The expression limiting the number of produced values.
     * @param <InExpression> The type of the input expression.
     * @param <NExpression> The type of the limiting expression.
     * @return The requested {@link BsonField}. @mongodb.driver.manual reference/operator/aggregation/lastN/
     *   $lastN @mongodb.server.release 5.2
     */
    public fun <InExpression, NExpression> lastN(
        property: KProperty<*>,
        inExpression: InExpression,
        nExpression: NExpression
    ): BsonField = Accumulators.lastN(property.path(), inExpression, nExpression)

    /**
     * Returns a combination of a computed field and an accumulator that produces a value of the given {@code
     * outExpression} computed for the bottom element within a group sorted according to the provided {@code sortBy}
     * specification.
     *
     * @param property The data class property computed by the accumulator.
     * @param sortBy The {@linkplain Sorts sort specification}. The syntax is identical to the one expected by {@link
     *   Aggregates#sort(Bson)}.
     * @param outExpression The output expression.
     * @param <OutExpression> The type of the output expression.
     * @return The requested {@link BsonField}. @mongodb.driver.manual reference/operator/aggregation/bottom/
     *   $bottom @mongodb.server.release 5.2
     */
    public fun <OutExpression> bottom(property: KProperty<*>, sortBy: Bson, outExpression: OutExpression): BsonField =
        Accumulators.bottom(property.path(), sortBy, outExpression)

    /**
     * Returns a combination of a computed field and an accumulator that produces a BSON {@link org.bson.BsonType#ARRAY
     * Array} of values of the given {@code outExpression} computed for the bottom {@code N} elements within a group
     * sorted according to the provided {@code sortBy} specification, where {@code N} is the positive integral value of
     * the {@code nExpression}.
     *
     * @param property The data class property computed by the accumulator.
     * @param sortBy The {@linkplain Sorts sort specification}. The syntax is identical to the one expected by {@link
     *   Aggregates#sort(Bson)}.
     * @param outExpression The output expression.
     * @param nExpression The expression limiting the number of produced values.
     * @param <OutExpression> The type of the output expression.
     * @param <NExpression> The type of the limiting expression.
     * @return The requested {@link BsonField}. @mongodb.driver.manual reference/operator/aggregation/bottomN/ $bottomN
     * @since 4.7 @mongodb.server.release 5.2
     */
    public fun <OutExpression, NExpression> bottomN(
        property: KProperty<*>,
        sortBy: Bson,
        outExpression: OutExpression,
        nExpression: NExpression
    ): BsonField = Accumulators.bottomN(property.path(), sortBy, outExpression, nExpression)

    /**
     * Gets a field name for a $group operation representing the maximum of the values of the given expression when
     * applied to all members of the group.
     *
     * @param property The data class property
     * @param expression the expression
     * @param <TExpression> the expression type
     * @return the field @mongodb.driver.manual reference/operator/aggregation/max/ $max
     */
    public fun <TExpression> max(property: KProperty<*>, expression: TExpression): BsonField =
        Accumulators.max(property.path(), expression)

    /**
     * Returns a combination of a computed field and an accumulator that produces a BSON {@link org.bson.BsonType#ARRAY
     * Array} of {@code N} largest values of the given {@code inExpression}, where {@code N} is the positive integral
     * value of the {@code nExpression}.
     *
     * @param property The data class property computed by the accumulator.
     * @param inExpression The input expression.
     * @param nExpression The expression limiting the number of produced values.
     * @param <InExpression> The type of the input expression.
     * @param <NExpression> The type of the limiting expression.
     * @return The requested {@link BsonField}. @mongodb.driver.manual reference/operator/aggregation/maxN/ $maxN
     * @since 4.7 @mongodb.server.release 5.2
     */
    public fun <InExpression, NExpression> maxN(
        property: KProperty<*>,
        inExpression: InExpression,
        nExpression: NExpression
    ): BsonField = Accumulators.maxN(property.path(), inExpression, nExpression)

    /**
     * Gets a field name for a $group operation representing the minimum of the values of the given expression when
     * applied to all members of the group.
     *
     * @param property The data class property
     * @param expression the expression
     * @param <TExpression> the expression type
     * @return the field @mongodb.driver.manual reference/operator/aggregation/min/ $min
     */
    public fun <TExpression> min(property: KProperty<*>, expression: TExpression): BsonField =
        Accumulators.min(property.path(), expression)

    /**
     * Returns a combination of a computed field and an accumulator that produces a BSON {@link org.bson.BsonType#ARRAY
     * Array} of {@code N} smallest values of the given {@code inExpression}, where {@code N} is the positive integral
     * value of the {@code nExpression}.
     *
     * @param property The data class property computed by the accumulator.
     * @param inExpression The input expression.
     * @param nExpression The expression limiting the number of produced values.
     * @param <InExpression> The type of the input expression.
     * @param <NExpression> The type of the limiting expression.
     * @return The requested {@link BsonField}. @mongodb.driver.manual reference/operator/aggregation/minN/
     *   $minN @mongodb.server.release 5.2
     */
    public fun <InExpression, NExpression> minN(
        property: KProperty<*>,
        inExpression: InExpression,
        nExpression: NExpression
    ): BsonField = Accumulators.minN(property.path(), inExpression, nExpression)

    /**
     * Gets a field name for a $group operation representing an array of all values that results from applying an
     * expression to each document in a group of documents that share the same group by key.
     *
     * @param property The data class property
     * @param expression the expression
     * @param <TExpression> the expression type
     * @return the field @mongodb.driver.manual reference/operator/aggregation/push/ $push
     */
    public fun <TExpression> push(property: KProperty<*>, expression: TExpression): BsonField =
        Accumulators.push(property.path(), expression)

    /**
     * Gets a field name for a $group operation representing all unique values that results from applying the given
     * expression to each document in a group of documents that share the same group by key.
     *
     * @param property The data class property
     * @param expression the expression
     * @param <TExpression> the expression type
     * @return the field @mongodb.driver.manual reference/operator/aggregation/addToSet/ $addToSet
     */
    public fun <TExpression> addToSet(property: KProperty<*>, expression: TExpression): BsonField =
        Accumulators.addToSet(property.path(), expression)

    /**
     * Gets a field name for a $group operation that concatenates arrays from the given expressions into a single array.
     *
     * @param property The data class property computed by the accumulator
     * @param expressions The list of array expressions to concatenate
     * @param <TExpression> The expression type
     * @return The field
     * @mongodb.driver.manual reference/operator/aggregation/concatArrays/ $concatArrays
     * @mongodb.server.release 8.1
     * @since 5.4
     */
    public fun <TExpression> concatArrays(property: KProperty<*>, expressions: List<TExpression>): BsonField =
        Accumulators.concatArrays(property.path(), expressions)

    /**
     * Gets a field name for a $group operation that computes the union of arrays from the given expressions, removing duplicates.
     *
     * @param property The data class property computed by the accumulator
     * @param expressions The list of array expressions to union
     * @param <TExpression> The expression type
     * @return The field
     * @mongodb.driver.manual reference/operator/aggregation/setUnion/ $setUnion
     * @mongodb.server.release 8.1
     * @since 5.4
     */
    public fun <TExpression> setUnion(property: KProperty<*>, expressions: List<TExpression>): BsonField =
        Accumulators.setUnion(property.path(), expressions)

    /**
     * Gets a field name for a $group operation representing the result of merging the fields of the documents. If
     * documents to merge include the same field name, the field, in the resulting document, has the value from the last
     * document merged for the field.
     *
     * @param property The data class property
     * @param expression the expression
     * @param <TExpression> the expression type
     * @return the field @mongodb.driver.manual reference/operator/aggregation/mergeObjects/ $mergeObjects
     */
    public fun <TExpression> mergeObjects(property: KProperty<*>, expression: TExpression): BsonField =
        Accumulators.mergeObjects(property.path(), expression)

    /**
     * Gets a field name for a $group operation representing the sample standard deviation of the values of the given
     * expression when applied to all members of the group.
     *
     * <p>Use if the values encompass the entire population of data you want to represent and do not wish to generalize
     * about a larger population.</p>
     *
     * @param property The data class property
     * @param expression the expression
     * @param <TExpression> the expression type
     * @return the field @mongodb.driver.manual reference/operator/aggregation/stdDevPop/
     *   $stdDevPop @mongodb.server.release 3.2
     */
    public fun <TExpression> stdDevPop(property: KProperty<*>, expression: TExpression): BsonField =
        Accumulators.stdDevPop(property.path(), expression)

    /**
     * Gets a field name for a $group operation representing the sample standard deviation of the values of the given
     * expression when applied to all members of the group.
     *
     * <p>Use if the values encompass a sample of a population of data from which to generalize about the
     * population.</p>
     *
     * @param property the data class property
     * @param expression the expression
     * @param <TExpression> the expression type
     * @return the field @mongodb.driver.manual reference/operator/aggregation/stdDevSamp/
     *   $stdDevSamp @mongodb.server.release 3.2
     */
    public fun <TExpression> stdDevSamp(property: KProperty<*>, expression: TExpression): BsonField =
        Accumulators.stdDevSamp(property.path(), expression)

    /**
     * Creates an $accumulator pipeline stage
     *
     * @param property the data class property
     * @param initFunction a function used to initialize the state
     * @param accumulateFunction a function used to accumulate documents
     * @param mergeFunction a function used to merge two internal states, e.g. accumulated on different shards or
     *   threads. It returns the resulting state of the accumulator.
     * @return the $accumulator pipeline stage @mongodb.driver.manual reference/operator/aggregation/accumulator/
     *   $accumulator @mongodb.server.release 4.4
     */
    public fun <T> accumulator(
        property: KProperty<T>,
        initFunction: String,
        accumulateFunction: String,
        mergeFunction: String
    ): BsonField = Accumulators.accumulator(property.path(), initFunction, accumulateFunction, mergeFunction)

    /**
     * Creates an $accumulator pipeline stage
     *
     * @param property the data class property
     * @param initFunction a function used to initialize the state
     * @param accumulateFunction a function used to accumulate documents
     * @param mergeFunction a function used to merge two internal states, e.g. accumulated on different shards or
     *   threads. It returns the resulting state of the accumulator.
     * @param finalizeFunction a function used to finalize the state and return the result (may be null)
     * @return the $accumulator pipeline stage @mongodb.driver.manual reference/operator/aggregation/accumulator/
     *   $accumulator @mongodb.server.release 4.4
     */
    public fun <T> accumulator(
        property: KProperty<T>,
        initFunction: String,
        accumulateFunction: String,
        mergeFunction: String,
        finalizeFunction: String?
    ): BsonField =
        Accumulators.accumulator(property.path(), initFunction, accumulateFunction, mergeFunction, finalizeFunction)

    /**
     * Creates an $accumulator pipeline stage
     *
     * @param property the data class property
     * @param initFunction a function used to initialize the state
     * @param initArgs init function’s arguments (may be null)
     * @param accumulateFunction a function used to accumulate documents
     * @param accumulateArgs additional accumulate function’s arguments (may be null). The first argument to the
     *   function is ‘state’.
     * @param mergeFunction a function used to merge two internal states, e.g. accumulated on different shards or
     *   threads. It returns the resulting state of the accumulator.
     * @param finalizeFunction a function used to finalize the state and return the result (may be null)
     * @return the $accumulator pipeline stage @mongodb.driver.manual reference/operator/aggregation/accumulator/
     *   $accumulator @mongodb.server.release 4.4
     */
    @Suppress("LongParameterList")
    public fun <T> accumulator(
        property: KProperty<T>,
        initFunction: String,
        initArgs: List<String>?,
        accumulateFunction: String,
        accumulateArgs: List<String>?,
        mergeFunction: String,
        finalizeFunction: String?
    ): BsonField =
        Accumulators.accumulator(
            property.path(),
            initFunction,
            initArgs,
            accumulateFunction,
            accumulateArgs,
            mergeFunction,
            finalizeFunction)

    /**
     * Creates an $accumulator pipeline stage
     *
     * @param property the data class property
     * @param initFunction a function used to initialize the state
     * @param accumulateFunction a function used to accumulate documents
     * @param mergeFunction a function used to merge two internal states, e.g. accumulated on different shards or
     *   threads. It returns the resulting state of the accumulator.
     * @param finalizeFunction a function used to finalize the state and return the result (may be null)
     * @param lang a language specifier
     * @return the $accumulator pipeline stage @mongodb.driver.manual reference/operator/aggregation/accumulator/
     *   $accumulator @mongodb.server.release 4.4
     */
    @Suppress("LongParameterList")
    public fun <T> accumulator(
        property: KProperty<T>,
        initFunction: String,
        accumulateFunction: String,
        mergeFunction: String,
        finalizeFunction: String?,
        lang: String
    ): BsonField =
        Accumulators.accumulator(
            property.path(), initFunction, accumulateFunction, mergeFunction, finalizeFunction, lang)

    /**
     * Creates an $accumulator pipeline stage
     *
     * @param property The data class property.
     * @param initFunction a function used to initialize the state
     * @param initArgs init function’s arguments (may be null)
     * @param accumulateFunction a function used to accumulate documents
     * @param accumulateArgs additional accumulate function’s arguments (may be null). The first argument to the
     *   function is ‘state’.
     * @param mergeFunction a function used to merge two internal states, e.g. accumulated on different shards or
     *   threads. It returns the resulting state of the accumulator.
     * @param finalizeFunction a function used to finalize the state and return the result (may be null)
     * @param lang a language specifier
     * @return the $accumulator pipeline stage @mongodb.driver.manual reference/operator/aggregation/accumulator/
     *   $accumulator @mongodb.server.release 4.4
     */
    @Suppress("LongParameterList")
    public fun <T> accumulator(
        property: KProperty<T>,
        initFunction: String,
        initArgs: List<String>?,
        accumulateFunction: String,
        accumulateArgs: List<String>?,
        mergeFunction: String,
        finalizeFunction: String?,
        lang: String
    ): BsonField =
        Accumulators.accumulator(
            property.path(),
            initFunction,
            initArgs,
            accumulateFunction,
            accumulateArgs,
            mergeFunction,
            finalizeFunction,
            lang)
}
