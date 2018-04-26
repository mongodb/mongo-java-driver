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

package com.mongodb.client.model;

/**
 * Builders for accumulators used in the group pipeline stage of an aggregation pipeline.
 *
 * @mongodb.driver.manual core/aggregation-pipeline/ Aggregation pipeline
 * @mongodb.driver.manual reference/operator/aggregation/group/#accumulator-operator Accumulators
 * @mongodb.driver.manual meta/aggregation-quick-reference/#aggregation-expressions Expressions
 * @mongodb.server.release 2.2
 * @since 3.1
 */
public final class Accumulators {

    /**
     * Gets a field name for a $group operation representing the sum of the values of the given expression when applied to all members of
     * the group.
     *
     * @param fieldName the field name
     * @param expression the expression
     * @param <TExpression> the expression type
     * @return the field
     * @mongodb.driver.manual reference/operator/aggregation/sum/ $sum
     */
    public static <TExpression> BsonField sum(final String fieldName, final TExpression expression) {
        return accumulator("$sum", fieldName, expression);
    }

    /**
     * Gets a field name for a $group operation representing the average of the values of the given expression when applied to all
     * members of the group.
     *
     * @param fieldName the field name
     * @param expression the expression
     * @param <TExpression> the expression type
     * @return the field
     * @mongodb.driver.manual reference/operator/aggregation/avg/ $avg
     */
    public static <TExpression> BsonField avg(final String fieldName, final TExpression expression) {
        return accumulator("$avg", fieldName, expression);
    }

    /**
     * Gets a field name for a $group operation representing the value of the given expression when applied to the first member of
     * the group.
     *
     * @param fieldName the field name
     * @param expression the expression
     * @param <TExpression> the expression type
     * @return the field
     * @mongodb.driver.manual reference/operator/aggregation/first/ $first
     */
    public static <TExpression> BsonField first(final String fieldName, final TExpression expression) {
        return accumulator("$first", fieldName, expression);
    }

    /**
     * Gets a field name for a $group operation representing the value of the given expression when applied to the last member of
     * the group.
     *
     * @param fieldName the field name
     * @param expression the expression
     * @param <TExpression> the expression type
     * @return the field
     * @mongodb.driver.manual reference/operator/aggregation/last/ $last
     */
    public static <TExpression> BsonField last(final String fieldName, final TExpression expression) {
        return accumulator("$last", fieldName, expression);
    }

    /**
     * Gets a field name for a $group operation representing the maximum of the values of the given expression when applied to all
     * members of the group.
     *
     * @param fieldName the field name
     * @param expression the expression
     * @param <TExpression> the expression type
     * @return the field
     * @mongodb.driver.manual reference/operator/aggregation/max/ $max
     */
    public static <TExpression> BsonField max(final String fieldName, final TExpression expression) {
        return accumulator("$max", fieldName, expression);
    }

    /**
     * Gets a field name for a $group operation representing the minimum of the values of the given expression when applied to all
     * members of the group.
     *
     * @param fieldName the field name
     * @param expression the expression
     * @param <TExpression> the expression type
     * @return the field
     * @mongodb.driver.manual reference/operator/aggregation/min/ $min
     */
    public static <TExpression> BsonField min(final String fieldName, final TExpression expression) {
        return accumulator("$min", fieldName, expression);
    }

    /**
     * Gets a field name for a $group operation representing an array of all values that results from applying an expression to each
     * document in a group of documents that share the same group by key.
     *
     * @param fieldName the field name
     * @param expression the expression
     * @param <TExpression> the expression type
     * @return the field
     * @mongodb.driver.manual reference/operator/aggregation/push/ $push
     */
    public static <TExpression> BsonField push(final String fieldName, final TExpression expression) {
        return accumulator("$push", fieldName, expression);
    }

    /**
     * Gets a field name for a $group operation representing all unique values that results from applying the given expression to each
     * document in a group of documents that share the same group by key.
     *
     * @param fieldName the field name
     * @param expression the expression
     * @param <TExpression> the expression type
     * @return the field
     * @mongodb.driver.manual reference/operator/aggregation/addToSet/ $addToSet
     */
    public static <TExpression> BsonField addToSet(final String fieldName, final TExpression expression) {
        return accumulator("$addToSet", fieldName, expression);
    }

    /**
     * Gets a field name for a $group operation representing the sample standard deviation of the values of the given expression
     * when applied to all members of the group.
     *
     * <p>Use if the values encompass the entire population of data you want to represent and do not wish to generalize about
     * a larger population.</p>
     *
     * @param fieldName the field name
     * @param expression the expression
     * @param <TExpression> the expression type
     * @return the field
     * @mongodb.driver.manual reference/operator/aggregation/stdDevPop/ $stdDevPop
     * @mongodb.server.release 3.2
     * @since 3.2
     */
    public static <TExpression> BsonField stdDevPop(final String fieldName, final TExpression expression) {
        return accumulator("$stdDevPop", fieldName, expression);
    }

    /**
     * Gets a field name for a $group operation representing the sample standard deviation of the values of the given expression
     * when applied to all members of the group.
     *
     * <p>Use if the values encompass a sample of a population of data from which to generalize about the population.</p>
     *
     * @param fieldName the field name
     * @param expression the expression
     * @param <TExpression> the expression type
     * @return the field
     * @mongodb.driver.manual reference/operator/aggregation/stdDevSamp/ $stdDevSamp
     * @mongodb.server.release 3.2
     * @since 3.2
     */
    public static <TExpression> BsonField stdDevSamp(final String fieldName, final TExpression expression) {
        return accumulator("$stdDevSamp", fieldName, expression);
    }

    private static <TExpression> BsonField accumulator(final String name, final String fieldName, final TExpression expression) {
        return new BsonField(fieldName, new SimpleExpression<TExpression>(name, expression));
    }

    private Accumulators() {
    }
}
