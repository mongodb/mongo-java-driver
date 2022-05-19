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

package org.mongodb.scala.model

import scala.collection.JavaConverters._
import com.mongodb.client.model.{ Accumulators => JAccumulators }
import org.mongodb.scala.bson.conversions.Bson

/**
 * Builders for accumulators used in the group pipeline stage of an aggregation pipeline.
 *
 * @see [[https://www.mongodb.com/docs/manual/core/aggregation-pipeline/ Aggregation pipeline]]
 * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/group/#accumulator-operator Accumulators]]
 * @see [[https://www.mongodb.com/docs/manual/meta/aggregation-quick-reference/#aggregation-expressions Expressions]]
 *
 * @since 1.0
 */
object Accumulators {

  /**
   * Gets a field name for a `\$group` operation representing the sum of the values of the given expression when applied to all members of
   * the group.
   *
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/sum/ \$sum]]
   * @param fieldName the field name
   * @param expression the expression
   * @tparam TExpression the expression type
   * @return the field
   */
  def sum[TExpression](fieldName: String, expression: TExpression): BsonField = JAccumulators.sum(fieldName, expression)

  /**
   * Gets a field name for a `\$group` operation representing the average of the values of the given expression when applied to all
   * members of the group.
   *
   * @param fieldName the field name
   * @param expression the expression
   * @tparam TExpression the expression type
   * @return the field
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/avg/ \$avg]]
   */
  def avg[TExpression](fieldName: String, expression: TExpression): BsonField = JAccumulators.avg(fieldName, expression)

  /**
   * Gets a field name for a `\$group` operation representing the value of the given expression when applied to the first member of
   * the group.
   *
   * @param fieldName the field name
   * @param expression the expression
   * @tparam TExpression the expression type
   * @return the field
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/first/ \$first]]
   */
  def first[TExpression](fieldName: String, expression: TExpression): BsonField =
    JAccumulators.first(fieldName, expression)

  /**
   * Returns a combination of a computed field and an accumulator that produces a BSON `Array`
   * of values of the given `inExpression` computed for the first `N` elements within a presorted group,
   * where `N` is the positive integral value of the `nExpression`.
   *
   * @param fieldName The field computed by the accumulator.
   * @param inExpression The input expression.
   * @param nExpression The expression limiting the number of produced values.
   * @tparam InExpression The type of the input expression.
   * @tparam NExpression The type of the limiting expression.
   * @return The requested [[BsonField]].
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/firstN/ \$firstN]]
   * @since 4.7
   * @note Requires MongoDB 5.2 or greater
   */
  def firstN[InExpression, NExpression](
      fieldName: String,
      inExpression: InExpression,
      nExpression: NExpression
  ): BsonField =
    JAccumulators.firstN(fieldName, inExpression, nExpression)

  /**
   * Returns a combination of a computed field and an accumulator that produces
   * a value of the given `outExpression` computed for the top element within a group
   * sorted according to the provided `sort` specification.
   *
   * @param fieldName The field computed by the accumulator.
   * @param sortBy The sort specification. The syntax is identical to the one expected by [[Aggregates.sort]].
   * @param outExpression The output expression.
   * @tparam OutExpression The type of the output expression.
   * @return The requested [[BsonField]].
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/topN/ \$topN]]
   * @since 4.7
   * @note Requires MongoDB 5.2 or greater
   */
  def top[OutExpression](
      fieldName: String,
      sortBy: Bson,
      outExpression: OutExpression
  ): BsonField =
    JAccumulators.top(fieldName, sortBy, outExpression)

  /**
   * Returns a combination of a computed field and an accumulator that produces a BSON `Array`
   * of values of the given `outExpression` computed for the top `N` elements within a group
   * sorted according to the provided `sort` specification,
   * where `N` is the positive integral value of the `nExpression`.
   *
   * @param fieldName The field computed by the accumulator.
   * @param sortBy The sort specification. The syntax is identical to the one expected by [[Aggregates.sort]].
   * @param outExpression The output expression.
   * @param nExpression The expression limiting the number of produced values.
   * @tparam OutExpression The type of the output expression.
   * @tparam NExpression The type of the limiting expression.
   * @return The requested [[BsonField]].
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/topN/ \$topN]]
   * @since 4.7
   * @note Requires MongoDB 5.2 or greater
   */
  def topN[OutExpression, NExpression](
      fieldName: String,
      sortBy: Bson,
      outExpression: OutExpression,
      nExpression: NExpression
  ): BsonField =
    JAccumulators.topN(fieldName, sortBy, outExpression, nExpression)

  /**
   * Gets a field name for a `\$group` operation representing the value of the given expression when applied to the last member of
   * the group.
   *
   * @param fieldName the field name
   * @param expression the expression
   * @tparam TExpression the expression type
   * @return the field
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/last/ \$last]]
   */
  def last[TExpression](fieldName: String, expression: TExpression): BsonField =
    JAccumulators.last(fieldName, expression)

  /**
   * Returns a combination of a computed field and an accumulator that produces a BSON `Array`
   * of values of the given `inExpression` computed for the last `N` elements within a presorted group
   * where `N` is the positive integral value of the `nExpression`.
   *
   * @param fieldName The field computed by the accumulator.
   * @param inExpression The input expression.
   * @param nExpression The expression limiting the number of produced values.
   * @tparam InExpression The type of the input expression.
   * @tparam NExpression The type of the limiting expression.
   * @return The requested [[BsonField]].
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/lastN/ \$lastN]]
   * @since 4.7
   * @note Requires MongoDB 5.2 or greater
   */
  def lastN[InExpression, NExpression](
      fieldName: String,
      inExpression: InExpression,
      nExpression: NExpression
  ): BsonField =
    JAccumulators.lastN(fieldName, inExpression, nExpression)

  /**
   * Returns a combination of a computed field and an accumulator that produces
   * a value of the given `outExpression` computed for the bottom element within a group
   * sorted according to the provided `sort` specification.
   *
   * @param fieldName The field computed by the accumulator.
   * @param sortBy The sort specification. The syntax is identical to the one expected by [[Aggregates.sort]].
   * @param outExpression The output expression.
   * @tparam OutExpression The type of the output expression.
   * @return The requested [[BsonField]].
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/bottom/ \$bottom]]
   * @since 4.7
   * @note Requires MongoDB 5.2 or greater
   */
  def bottom[OutExpression](
      fieldName: String,
      sortBy: Bson,
      outExpression: OutExpression
  ): BsonField =
    JAccumulators.bottom(fieldName, sortBy, outExpression)

  /**
   * Returns a combination of a computed field and an accumulator that produces a BSON `Array`
   * of values of the given `outExpression` computed for the bottom `N` elements within a group
   * sorted according to the provided `sort` specification,
   * where `N` is the positive integral value of the `nExpression`.
   *
   * @param fieldName The field computed by the accumulator.
   * @param sortBy The sort specification. The syntax is identical to the one expected by [[Aggregates.sort]].
   * @param outExpression The output expression.
   * @param nExpression The expression limiting the number of produced values.
   * @tparam OutExpression The type of the output expression.
   * @tparam NExpression The type of the limiting expression.
   * @return The requested [[BsonField]].
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/bottomN/ \$bottomN]]
   * @since 4.7
   * @note Requires MongoDB 5.2 or greater
   */
  def bottomN[OutExpression, NExpression](
      fieldName: String,
      sortBy: Bson,
      outExpression: OutExpression,
      nExpression: NExpression
  ): BsonField =
    JAccumulators.bottomN(fieldName, sortBy, outExpression, nExpression)

  /**
   * Gets a field name for a `\$group` operation representing the maximum of the values of the given expression when applied to all
   * members of the group.
   *
   * @param fieldName the field name
   * @param expression the expression
   * @tparam TExpression the expression type
   * @return the field
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/max/ \$max]]
   */
  def max[TExpression](fieldName: String, expression: TExpression): BsonField = JAccumulators.max(fieldName, expression)

  /**
   * Returns a combination of a computed field and an accumulator that produces a BSON `Array`
   * of `N` largest values of the given `inExpression`,
   * where `N` is the positive integral value of the `nExpression`.
   *
   * @param fieldName The field computed by the accumulator.
   * @param inExpression The input expression.
   * @param nExpression The expression limiting the number of produced values.
   * @tparam InExpression The type of the input expression.
   * @tparam NExpression The type of the limiting expression.
   * @return The requested [[BsonField]].
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/maxN/ \$maxN]]
   * @since 4.7
   * @note Requires MongoDB 5.2 or greater
   */
  def maxN[InExpression, NExpression](
      fieldName: String,
      inExpression: InExpression,
      nExpression: NExpression
  ): BsonField =
    JAccumulators.maxN(fieldName, inExpression, nExpression)

  /**
   * Gets a field name for a `\$group` operation representing the minimum of the values of the given expression when applied to all
   * members of the group.
   *
   * @param fieldName the field name
   * @param expression the expression
   * @tparam TExpression the expression type
   * @return the field
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/min/ \$min]]
   */
  def min[TExpression](fieldName: String, expression: TExpression): BsonField = JAccumulators.min(fieldName, expression)

  /**
   * Returns a combination of a computed field and an accumulator that produces a BSON `Array`
   * of `N` smallest values of the given `inExpression`,
   * where `N` is the positive integral value of the `nExpression`.
   *
   * @param fieldName The field computed by the accumulator.
   * @param inExpression The input expression.
   * @param nExpression The expression limiting the number of produced values.
   * @tparam InExpression The type of the input expression.
   * @tparam NExpression The type of the limiting expression.
   * @return The requested [[BsonField]].
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/minN/ \$minN]]
   * @since 4.7
   * @note Requires MongoDB 5.2 or greater
   */
  def minN[InExpression, NExpression](
      fieldName: String,
      inExpression: InExpression,
      nExpression: NExpression
  ): BsonField =
    JAccumulators.minN(fieldName, inExpression, nExpression)

  /**
   * Gets a field name for a `\$group` operation representing an array of all values that results from applying an expression to each
   * document in a group of documents that share the same group by key.
   *
   * @param fieldName the field name
   * @param expression the expression
   * @tparam TExpression the expression type
   * @return the field
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/push/ \$push]]
   */
  def push[TExpression](fieldName: String, expression: TExpression): BsonField =
    JAccumulators.push(fieldName, expression)

  /**
   * Gets a field name for a `\$group` operation representing all unique values that results from applying the given expression to each
   * document in a group of documents that share the same group by key.
   *
   * @param fieldName the field name
   * @param expression the expression
   * @tparam TExpression the expression type
   * @return the field
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/addToSet/ \$addToSet]]
   */
  def addToSet[TExpression](fieldName: String, expression: TExpression): BsonField =
    JAccumulators.addToSet(fieldName, expression)

  /**
   * Gets a field name for a `\$group` operation representing the result of merging the fields of the documents.
   * If documents to merge include the same field name, the field, in the resulting document, has the value from the last document
   * merged for the field.
   *
   * @param fieldName  the field name
   * @param expression the expression
   * @tparam TExpression the expression type
   * @return the field
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/mergeObjects/ \$mergeObjects]]
   * @since 4.4
   */
  def mergeObjects[TExpression](fieldName: String, expression: TExpression): BsonField =
    JAccumulators.mergeObjects(fieldName, expression)

  /**
   * Gets a field name for a `\$group` operation representing the sample standard deviation of the values of the given expression
   * when applied to all members of the group.
   *
   * Use if the values encompass the entire population of data you want to represent and do not wish to generalize about
   * a larger population.
   *
   * @note Requires MongoDB 3.2 or greater
   * @param fieldName the field name
   * @param expression the expression
   * @tparam TExpression the expression type
   * @return the field
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/stdDevPop/ \$stdDevPop]]
   * @since 1.1
   */
  def stdDevPop[TExpression](fieldName: String, expression: TExpression): BsonField =
    JAccumulators.stdDevPop(fieldName, expression)

  /**
   * Gets a field name for a `\$group` operation representing the sample standard deviation of the values of the given expression
   * when applied to all members of the group.
   *
   * Use if the values encompass a sample of a population of data from which to generalize about the population.
   *
   * @note Requires MongoDB 3.2 or greater
   * @param fieldName the field name
   * @param expression the expression
   * @tparam TExpression the expression type
   * @return the field
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/stdDevSamp/ \$stdDevSamp]]
   * @since 1.1
   */
  def stdDevSamp[TExpression](fieldName: String, expression: TExpression): BsonField =
    JAccumulators.stdDevSamp(fieldName, expression)

  /**
   * Creates an `\$accumulator` pipeline stage
   *
   * @param fieldName            the field name
   * @param initFunction         a function used to initialize the state
   * @param accumulateFunction   a function used to accumulate documents
   * @param mergeFunction        a function used to merge two internal states, e.g. accumulated on different shards or
   *                             threads. It returns the resulting state of the accumulator.
   * @return the `\$accumulator` pipeline stage
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/accumulator/ \$accumulator]]
   * @since 1.2
   * @note Requires MongoDB 4.4 or greater
   */
  def accumulator(
      fieldName: String,
      initFunction: String,
      accumulateFunction: String,
      mergeFunction: String
  ): BsonField =
    JAccumulators.accumulator(fieldName, initFunction, accumulateFunction, mergeFunction)

  /**
   * Creates an `\$accumulator` pipeline stage
   *
   * @param fieldName            the field name
   * @param initFunction         a function used to initialize the state
   * @param accumulateFunction   a function used to accumulate documents
   * @param mergeFunction        a function used to merge two internal states, e.g. accumulated on different shards or
   *                             threads. It returns the resulting state of the accumulator.
   * @param finalizeFunction     a function used to finalize the state and return the result (may be null)
   * @return the `\$accumulator` pipeline stage
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/accumulator/ \$accumulator]]
   * @since 1.2
   * @note Requires MongoDB 4.4 or greater
   */
  def accumulator(
      fieldName: String,
      initFunction: String,
      accumulateFunction: String,
      mergeFunction: String,
      finalizeFunction: String
  ): BsonField =
    JAccumulators.accumulator(fieldName, initFunction, accumulateFunction, mergeFunction, finalizeFunction)

  /**
   * Creates an `\$accumulator` pipeline stage
   *
   * @param fieldName            the field name
   * @param initFunction         a function used to initialize the state
   * @param initArgs             init function’s arguments (may be null)
   * @param accumulateFunction   a function used to accumulate documents
   * @param accumulateArgs       additional accumulate function’s arguments (may be null). The first argument to the
   *                             function is ‘state’.
   * @param mergeFunction        a function used to merge two internal states, e.g. accumulated on different shards or
   *                             threads. It returns the resulting state of the accumulator.
   * @param finalizeFunction     a function used to finalize the state and return the result (may be null)
   * @return the `\$accumulator` pipeline stage
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/accumulator/ \$accumulator]]
   * @since 1.2
   * @note Requires MongoDB 4.4 or greater
   */
  def accumulator(
      fieldName: String,
      initFunction: String,
      initArgs: Seq[String],
      accumulateFunction: String,
      accumulateArgs: Seq[String],
      mergeFunction: String,
      finalizeFunction: String
  ): BsonField =
    JAccumulators.accumulator(
      fieldName,
      initFunction,
      initArgs.asJava,
      accumulateFunction,
      accumulateArgs.asJava,
      mergeFunction,
      finalizeFunction
    )

  /**
   * Creates an `\$accumulator` pipeline stage
   *
   * @param fieldName            the field name
   * @param initFunction         a function used to initialize the state
   * @param initArgs             init function’s arguments (may be null)
   * @param accumulateFunction   a function used to accumulate documents
   * @param accumulateArgs       additional accumulate function’s arguments (may be null). The first argument to the
   *                             function is ‘state’.
   * @param mergeFunction        a function used to merge two internal states, e.g. accumulated on different shards or
   *                             threads. It returns the resulting state of the accumulator.
   * @param finalizeFunction     a function used to finalize the state and return the result (may be null)
   * @param lang                 a language specifier
   * @return the `\$accumulator` pipeline stage
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/accumulator/ \$accumulator]]
   * @since 1.2
   * @note Requires MongoDB 4.4 or greater
   */
  def accumulator(
      fieldName: String,
      initFunction: String,
      initArgs: Seq[String],
      accumulateFunction: String,
      accumulateArgs: Seq[String],
      mergeFunction: String,
      finalizeFunction: String,
      lang: String
  ): BsonField =
    JAccumulators.accumulator(
      fieldName,
      initFunction,
      initArgs.asJava,
      accumulateFunction,
      accumulateArgs.asJava,
      mergeFunction,
      finalizeFunction,
      lang
    )
}
