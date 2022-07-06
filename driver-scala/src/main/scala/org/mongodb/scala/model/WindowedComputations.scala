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

import com.mongodb.client.model.{ MongoTimeUnit => JMongoTimeUnit, WindowedComputations => JWindowedComputations }
import org.mongodb.scala.bson.conversions.Bson

/**
 * Builders for [[WindowedComputation windowed computations]] used in the
 * `Aggregates.setWindowFields` pipeline stage
 * of an aggregation pipeline. Each windowed computation is a triple:
 *  - A window function. Some functions require documents in a window to be sorted
 *  (see `sortBy` in `Aggregates.setWindowFields`).
 *  - An optional [[Window window]], a.k.a. frame.
 *  Specifying `None` window is equivalent to specifying an unbounded window,
 *  i.e., a window with both ends specified as [[Windows.Bound UNBOUNDED]].
 *  Some window functions, e.g., [[WindowedComputations.derivative]],
 *  require an explicit unbounded window instead of `None`.
 *  - A path to an output field to be computed by the window function over the window.
 *
 * A windowed computation is similar to an [[Accumulators accumulator]] but does not result in folding documents constituting
 * the window into a single document.
 *
 * @see [[https://www.mongodb.com/docs/manual/meta/aggregation-quick-reference/#field-paths Field paths]]
 * @since 4.3
 * @note Requires MongoDB 5.0 or greater.
 */
object WindowedComputations {

  /**
   * Creates a windowed computation from a document field in situations when there is no builder method that better satisfies your needs.
   * This method cannot be used to validate the document field syntax.
   *
   * {{{
   *  val pastWeek: Window = Windows.timeRange(-1, MongoTimeUnit.WEEK, Windows.Bound.CURRENT)
   *  val pastWeekExpenses1: WindowedComputation = WindowedComputations.sum("pastWeekExpenses", "\$expenses", pastWeek)
   *  val pastWeekExpenses2: WindowedComputation = WindowedComputations.of(
   *      BsonField("pastWeekExpenses", Document("\$sum" -> "\$expenses",
   *          "window" -> pastWeek.toBsonDocument)))
   * }}}
   *
   * @param windowedComputation A document field representing the required windowed computation.
   * @return The constructed windowed computation.
   */
  def of(windowedComputation: BsonField): WindowedComputation =
    JWindowedComputations.of(windowedComputation)

  /**
   * Builds a computation of the sum of the evaluation results of the `expression` over the `window`.
   *
   * @param path       The output field path.
   * @param expression The expression.
   * @param window     The window.
   * @tparam TExpression The expression type.
   * @return The constructed windowed computation.
   * @see [[https://dochub.mongodb.org/core/window-functions-sum \$sum]]
   */
  def sum[TExpression](path: String, expression: TExpression, window: Option[_ <: Window]): WindowedComputation =
    JWindowedComputations.sum(path, expression, window.orNull)

  /**
   * Builds a computation of the average of the evaluation results of the `expression` over the `window`.
   *
   * @param path       The output field path.
   * @param expression The expression.
   * @param window     The window.
   * @tparam TExpression The expression type.
   * @return The constructed windowed computation.
   * @see [[https://dochub.mongodb.org/core/window-functions-avg \$avg]]
   */
  def avg[TExpression](path: String, expression: TExpression, window: Option[_ <: Window]): WindowedComputation =
    JWindowedComputations.avg(path, expression, window.orNull)

  /**
   * Builds a computation of the sample standard deviation of the evaluation results of the `expression` over the `window`.
   *
   * @param path       The output field path.
   * @param expression The expression.
   * @param window     The window.
   * @tparam TExpression The expression type.
   * @return The constructed windowed computation.
   * @see [[https://dochub.mongodb.org/core/window-functions-std-dev-samp \$stdDevSamp]]
   */
  def stdDevSamp[TExpression](path: String, expression: TExpression, window: Option[_ <: Window]): WindowedComputation =
    JWindowedComputations.stdDevSamp(path, expression, window.orNull)

  /**
   * Builds a computation of the population standard deviation of the evaluation results of the `expression` over the `window`.
   *
   * @param path       The output field path.
   * @param expression The expression.
   * @param window     The window.
   * @tparam TExpression The expression type.
   * @return The constructed windowed computation.
   * @see [[https://dochub.mongodb.org/core/window-functions-std-dev-pop \$stdDevPop]]
   */
  def stdDevPop[TExpression](path: String, expression: TExpression, window: Option[_ <: Window]): WindowedComputation =
    JWindowedComputations.stdDevPop(path, expression, window.orNull)

  /**
   * Builds a computation of the lowest of the evaluation results of the `expression` over the `window`.
   *
   * @param path       The output field path.
   * @param expression The expression.
   * @param window     The window.
   * @tparam TExpression The expression type.
   * @return The constructed windowed computation.
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/min/ \$min]]
   */
  def min[TExpression](path: String, expression: TExpression, window: Option[_ <: Window]): WindowedComputation =
    JWindowedComputations.min(path, expression, window.orNull)

  /**
   * Builds a computation of a BSON `Array`
   * of `N` smallest evaluation results of the `inExpression` over the `window`,
   * where `N` is the positive integral value of the `nExpression`.
   *
   * @param path The output field path.
   * @param inExpression The input expression.
   * @param nExpression The expression limiting the number of produced values.
   * @tparam InExpression The type of the input expression.
   * @tparam NExpression The type of the limiting expression.
   * @return The constructed windowed computation.
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/minN/ \$minN]]
   * @since 4.7
   * @note Requires MongoDB 5.2 or greater
   */
  def minN[InExpression, NExpression](
      path: String,
      inExpression: InExpression,
      nExpression: NExpression,
      window: Option[_ <: Window]
  ): WindowedComputation = JWindowedComputations.minN(path, inExpression, nExpression, window.orNull)

  /**
   * Builds a computation of the highest of the evaluation results of the `expression` over the `window`.
   *
   * @param path       The output field path.
   * @param expression The expression.
   * @param window     The window.
   * @tparam TExpression The expression type.
   * @return The constructed windowed computation.
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/max/ \$max]]
   */
  def max[TExpression](path: String, expression: TExpression, window: Option[_ <: Window]): WindowedComputation =
    JWindowedComputations.max(path, expression, window.orNull)

  /**
   * Builds a computation of a BSON `Array`
   * of `N` largest evaluation results of the `inExpression` over the `window`,
   * where `N` is the positive integral value of the `nExpression`.
   *
   * @param path The output field path.
   * @param inExpression The input expression.
   * @param nExpression The expression limiting the number of produced values.
   * @tparam InExpression The type of the input expression.
   * @tparam NExpression The type of the limiting expression.
   * @return The constructed windowed computation.
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/maxN/ \$maxN]]
   * @since 4.7
   * @note Requires MongoDB 5.2 or greater
   */
  def maxN[InExpression, NExpression](
      path: String,
      inExpression: InExpression,
      nExpression: NExpression,
      window: Option[_ <: Window]
  ): WindowedComputation = JWindowedComputations.maxN(path, inExpression, nExpression, window.orNull)

  /**
   * Builds a computation of the number of documents in the `window`.
   *
   * @param path   The output field path.
   * @param window The window.
   * @return The constructed windowed computation.
   * @see [[https://dochub.mongodb.org/core/window-functions-count \$count]]
   */
  def count(path: String, window: Option[_ <: Window]): WindowedComputation =
    JWindowedComputations.count(path, window.orNull)

  /**
   * Builds a computation of the time derivative by subtracting the evaluation result of the `expression` against the last document
   * and the first document in the `window` and dividing it by the difference in the values of the
   * `sortBy` field of the respective documents.
   * Other documents in the `window` have no effect on the computation.
   *
   * Sorting is required.
   *
   * @param path       The output field path.
   * @param expression The expression.
   * @param window     The window.
   * @tparam TExpression The expression type.
   * @return The constructed windowed computation.
   * @see [[https://dochub.mongodb.org/core/window-functions-derivative \$derivative]]
   */
  def derivative[TExpression](path: String, expression: TExpression, window: Window): WindowedComputation =
    JWindowedComputations.derivative(path, expression, window)

  /**
   * Builds a computation of the time derivative by subtracting the evaluation result of the `expression` against the last document
   * and the first document in the `window` and dividing it by the difference in the BSON `Date`
   * values of the `sortBy` field of the respective documents.
   * Other documents in the `window` have no effect on the computation.
   *
   * Sorting is required.
   *
   * @param path       The output field path.
   * @param expression The expression.
   * @param window     The window.
   * @param unit       The desired time unit for the divisor. Allowed values are:
   *                   [[MongoTimeUnit WEEK]], [[MongoTimeUnit DAY]], [[MongoTimeUnit HOUR]], [[MongoTimeUnit MINUTE]],
   *                   [[MongoTimeUnit SECOND]], [[MongoTimeUnit MILLISECOND]].
   * @tparam TExpression The expression type.
   * @return The constructed windowed computation.
   * @see [[https://dochub.mongodb.org/core/window-functions-derivative \$derivative]]
   */
  def timeDerivative[TExpression](
      path: String,
      expression: TExpression,
      window: Window,
      unit: JMongoTimeUnit
  ): WindowedComputation =
    JWindowedComputations.timeDerivative(path, expression, window, unit)

  /**
   * Builds a computation of the approximate integral of a function that maps values of
   * the `sortBy` field to evaluation results of the `expression`
   * against the same document. The limits of integration match the `window` bounds.
   * The approximation is done by using the
   * <a href="https://www.khanacademy.org/math/ap-calculus-ab/ab-integration-new/ab-6-2/a/understanding-the-trapezoid-rule">
   * trapezoidal rule</a>.
   *
   * Sorting is required.
   *
   * @param path       The output field path.
   * @param expression The expression.
   * @param window     The window.
   * @tparam TExpression The expression type.
   * @return The constructed windowed computation.
   * @see [[https://dochub.mongodb.org/core/window-functions-integral \$integral]]
   */
  def integral[TExpression](path: String, expression: TExpression, window: Window): WindowedComputation =
    JWindowedComputations.integral(path, expression, window)

  /**
   * Builds a computation of the approximate integral of a function that maps BSON `Date` values of
   * the `sortBy` field to evaluation results of the `expression`
   * against the same document. The limits of integration match the `window` bounds.
   * The approximation is done by using the trapezoidal rule.
   *
   * Sorting is required.
   *
   * @param path       The output field path.
   * @param expression The expression.
   * @param window     The window.
   * @param unit       The desired time unit for the divisor. Allowed values are:
   *                   [[MongoTimeUnit WEEK]], [[MongoTimeUnit DAY]], [[MongoTimeUnit HOUR]], [[MongoTimeUnit MINUTE]],
   *                   [[MongoTimeUnit SECOND]], [[MongoTimeUnit MILLISECOND]].
   * @tparam TExpression The expression type.
   * @return The constructed windowed computation.
   * @see [[https://dochub.mongodb.org/core/window-functions-integral \$integral]]
   */
  def timeIntegral[TExpression](
      path: String,
      expression: TExpression,
      window: Window,
      unit: JMongoTimeUnit
  ): WindowedComputation =
    JWindowedComputations.timeIntegral(path, expression, window, unit)

  /**
   * Builds a computation of the sample covariance between the evaluation results of the two expressions over the `window`.
   *
   * @param path        The output field path.
   * @param expression1 The first expression.
   * @param expression2 The second expression.
   * @param window      The window.
   * @tparam TExpression The expression type.
   * @return The constructed windowed computation.
   * @see [[https://dochub.mongodb.org/core/window-functions-covariance-samp \$covarianceSamp]]
   */
  def covarianceSamp[TExpression](
      path: String,
      expression1: TExpression,
      expression2: TExpression,
      window: Option[_ <: Window]
  ): WindowedComputation =
    JWindowedComputations.covarianceSamp(path, expression1, expression2, window.orNull)

  /**
   * Builds a computation of the population covariance between the evaluation results of the two expressions over the `window`.
   *
   * @param path        The output field path.
   * @param expression1 The first expression.
   * @param expression2 The second expression.
   * @param window      The window.
   * @tparam TExpression The expression type.
   * @return The constructed windowed computation.
   * @see [[https://dochub.mongodb.org/core/window-functions-covariance-pop \$covariancePop]]
   */
  def covariancePop[TExpression](
      path: String,
      expression1: TExpression,
      expression2: TExpression,
      window: Option[_ <: Window]
  ): WindowedComputation =
    JWindowedComputations.covariancePop(path, expression1, expression2, window.orNull)

  /**
   * Builds a computation of the exponential moving average of the evaluation results of the `expression` over a window
   * that includes `n` - 1 documents preceding the current document and the current document, with more weight on documents
   * closer to the current one.
   *
   * Sorting is required.
   *
   * @param path       The output field path.
   * @param expression The expression.
   * @param n          Must be positive.
   * @tparam TExpression The expression type.
   * @return The constructed windowed computation.
   * @see [[https://dochub.mongodb.org/core/window-functions-exp-moving-avg \$expMovingAvg]]
   */
  def expMovingAvg[TExpression](path: String, expression: TExpression, n: Int): WindowedComputation =
    JWindowedComputations.expMovingAvg(path, expression, n)

  /**
   * Builds a computation of the exponential moving average of the evaluation results of the `expression` over the half-bounded
   * window `[`[[Windows.Bound UNBOUNDED]], [[Windows.Bound CURRENT]]`]`,
   * with `alpha` representing the degree of weighting decrease.
   *
   * Sorting is required.
   *
   * @param path       The output field path.
   * @param expression The expression.
   * @param alpha      A parameter specifying how fast weighting decrease happens. A higher `alpha` discounts older observations faster.
   *                   Must belong to the interval (0, 1).
   * @tparam TExpression The expression type.
   * @return The constructed windowed computation.
   * @see [[https://dochub.mongodb.org/core/window-functions-exp-moving-avg \$expMovingAvg]]
   */
  def expMovingAvg[TExpression](path: String, expression: TExpression, alpha: Double): WindowedComputation =
    JWindowedComputations.expMovingAvg(path, expression, alpha)

  /**
   * Builds a computation that adds the evaluation results of the `expression` over the `window`
   * to a BSON `Array`.
   * Order within the array is guaranteed if `sortBy` is specified.
   *
   * @param path       The output field path.
   * @param expression The expression.
   * @param window     The window.
   * @tparam TExpression The expression type.
   * @return The constructed windowed computation.
   * @see [[https://dochub.mongodb.org/core/window-functions-push \$push]]
   */
  def push[TExpression](path: String, expression: TExpression, window: Option[_ <: Window]): WindowedComputation =
    JWindowedComputations.push(path, expression, window.orNull)

  /**
   * Builds a computation that adds the evaluation results of the `expression` over the `window`
   * to a BSON `Array` and excludes duplicates.
   * Order within the array is not specified.
   *
   * @param path       The output field path.
   * @param expression The expression.
   * @param window     The window.
   * @tparam TExpression The expression type.
   * @return The constructed windowed computation.
   * @see [[https://dochub.mongodb.org/core/window-functions-add-to-set \$addToSet]]
   */
  def addToSet[TExpression](path: String, expression: TExpression, window: Option[_ <: Window]): WindowedComputation =
    JWindowedComputations.addToSet(path, expression, window.orNull)

  /**
   * Builds a computation of the evaluation result of the `expression` against the first document in the `window`.
   *
   * Sorting is required.
   *
   * @param path       The output field path.
   * @param expression The expression.
   * @param window     The window.
   * @tparam TExpression The expression type.
   * @return The constructed windowed computation.
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/first/ \$first]]
   */
  def first[TExpression](path: String, expression: TExpression, window: Option[_ <: Window]): WindowedComputation =
    JWindowedComputations.first(path, expression, window.orNull)

  /**
   * Builds a computation of a BSON `Array`
   * of evaluation results of the `inExpression` against the first `N`  documents in the `window`,
   * where `N` is the positive integral value of the `nExpression`.
   *
   * Sorting is required.
   *
   * @param path The output field path.
   * @param inExpression The input expression.
   * @param nExpression The expression limiting the number of produced values.
   * @tparam InExpression The type of the input expression.
   * @tparam NExpression The type of the limiting expression.
   * @return The constructed windowed computation.
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/firstN/ \$firstN]]
   * @since 4.7
   * @note Requires MongoDB 5.2 or greater
   */
  def firstN[InExpression, NExpression](
      path: String,
      inExpression: InExpression,
      nExpression: NExpression,
      window: Option[_ <: Window]
  ): WindowedComputation = JWindowedComputations.firstN(path, inExpression, nExpression, window.orNull)

  /**
   * Builds a computation of the evaluation result of the `outExpression` against the top document in the `window`
   * sorted according to the provided `sortBy` specification.
   *
   * @param path The output field path.
   * @param sortBy The sort specification. The syntax is identical to the one expected by [[Aggregates.sort]].
   * @param outExpression The output expression.
   * @tparam OutExpression The type of the input expression.
   * @return The constructed windowed computation.
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/top/ \$top]]
   * @since 4.7
   * @note Requires MongoDB 5.2 or greater
   */
  def top[OutExpression](
      path: String,
      sortBy: Bson,
      outExpression: OutExpression,
      window: Option[_ <: Window]
  ): WindowedComputation = JWindowedComputations.top(path, sortBy, outExpression, window.orNull)

  /**
   * Builds a computation of a BSON `Array`
   * of evaluation results of the `outExpression` against the top `N` documents in the `window`
   * sorted according to the provided `sortBy` specification,
   * where `N` is the positive integral value of the `nExpression`.
   *
   * @param path The output field path.
   * @param sortBy The sort specification. The syntax is identical to the one expected by [[Aggregates.sort]].
   * @param outExpression The output expression.
   * @param nExpression The expression limiting the number of produced values.
   * @tparam OutExpression The type of the input expression.
   * @tparam NExpression The type of the limiting expression.
   * @return The constructed windowed computation.
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/topN/ \$topN]]
   * @since 4.7
   * @note Requires MongoDB 5.2 or greater
   */
  def topN[OutExpression, NExpression](
      path: String,
      sortBy: Bson,
      outExpression: OutExpression,
      nExpression: NExpression,
      window: Option[_ <: Window]
  ): WindowedComputation = JWindowedComputations.topN(path, sortBy, outExpression, nExpression, window.orNull)

  /**
   * Builds a computation of the evaluation result of the `expression` against the last document in the `window`.
   *
   * Sorting is required.
   *
   * @param path       The output field path.
   * @param expression The expression.
   * @param window     The window.
   * @tparam TExpression The expression type.
   * @return The constructed windowed computation.
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/last/ \$last]]
   */
  def last[TExpression](path: String, expression: TExpression, window: Option[_ <: Window]): WindowedComputation =
    JWindowedComputations.last(path, expression, window.orNull)

  /**
   * Builds a computation of a BSON `Array`
   * of evaluation results of the `inExpression` against the last `N`  documents in the `window`,
   * where `N` is the positive integral value of the `nExpression`.
   *
   * Sorting is required.
   *
   * @param path The output field path.
   * @param inExpression The input expression.
   * @param nExpression The expression limiting the number of produced values.
   * @tparam InExpression The type of the input expression.
   * @tparam NExpression The type of the limiting expression.
   * @return The constructed windowed computation.
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/lastN/ \$lastN]]
   * @since 4.7
   * @note Requires MongoDB 5.2 or greater
   */
  def lastN[InExpression, NExpression](
      path: String,
      inExpression: InExpression,
      nExpression: NExpression,
      window: Option[_ <: Window]
  ): WindowedComputation = JWindowedComputations.lastN(path, inExpression, nExpression, window.orNull)

  /**
   * Builds a computation of the evaluation result of the `outExpression` against the bottom document in the `window`
   * sorted according to the provided `sortBy` specification.
   *
   * @param path The output field path.
   * @param sortBy The sort specification. The syntax is identical to the one expected by [[Aggregates.sort]].
   * @param outExpression The output expression.
   * @tparam OutExpression The type of the input expression.
   * @return The constructed windowed computation.
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/bottom/ \$bottom]]
   * @since 4.7
   * @note Requires MongoDB 5.2 or greater
   */
  def bottom[OutExpression](
      path: String,
      sortBy: Bson,
      outExpression: OutExpression,
      window: Option[_ <: Window]
  ): WindowedComputation = JWindowedComputations.bottom(path, sortBy, outExpression, window.orNull)

  /**
   * Builds a computation of a BSON `Array`
   * of evaluation results of the `outExpression` against the bottom `N` documents in the `window`
   * sorted according to the provided `sortBy` specification,
   * where `N` is the positive integral value of the `nExpression`.
   *
   * @param path The output field path.
   * @param sortBy The sort specification. The syntax is identical to the one expected by [[Aggregates.sort]].
   * @param outExpression The output expression.
   * @param nExpression The expression limiting the number of produced values.
   * @tparam OutExpression The type of the input expression.
   * @tparam NExpression The type of the limiting expression.
   * @return The constructed windowed computation.
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/bottomN/ \$bottomN]]
   * @since 4.7
   * @note Requires MongoDB 5.2 or greater
   */
  def bottomN[OutExpression, NExpression](
      path: String,
      sortBy: Bson,
      outExpression: OutExpression,
      nExpression: NExpression,
      window: Option[_ <: Window]
  ): WindowedComputation = JWindowedComputations.bottomN(path, sortBy, outExpression, nExpression, window.orNull)

  /**
   * Builds a computation of the evaluation result of the `expression` for the document whose position is shifted by the given
   * amount relative to the current document. If the shifted document is outside of the
   * partition containing the current document,
   * then the `defaultExpression` is used instead of the `expression`.
   *
   * Sorting is required.
   *
   * @param path              The output field path.
   * @param expression        The expression.
   * @param defaultExpression The default expression.
   *                          If `None`, then the default expression is evaluated to BSON `Null`.
   *                          Must evaluate to a constant value.
   * @param by                The shift specified similarly to [[Windows rules for window bounds]]:
   *                          - 0 means the current document;
   *                          - a negative value refers to the document preceding the current one;
   *                          - a positive value refers to the document following the current one.
   * @tparam TExpression The expression type.
   * @return The constructed windowed computation.
   * @see [[https://dochub.mongodb.org/core/window-functions-shift \$shift]]
   */
  def shift[TExpression >: Null](
      path: String,
      expression: TExpression,
      defaultExpression: Option[TExpression],
      by: Int
  ): WindowedComputation =
    JWindowedComputations.shift(path, expression, defaultExpression.orNull, by)

  /**
   * Builds a computation of the order number of each document in its
   * partition.
   *
   * Sorting is required.
   *
   * @param path The output field path.
   * @return The constructed windowed computation.
   * @see [[https://dochub.mongodb.org/core/window-functions-document-number \$documentNumber]]
   */
  def documentNumber(path: String): WindowedComputation =
    JWindowedComputations.documentNumber(path)

  /**
   * Builds a computation of the rank of each document in its
   * partition.
   * Documents with the same value(s) of the `sortBy` fields result in
   * the same ranking and result in gaps in the returned ranks.
   * For example, a partition with the sequence [1, 3, 3, 5] representing the values of the single `sortBy` field
   * produces the following sequence of rank values: [1, 2, 2, 4].
   *
   * Sorting is required.
   *
   * @param path The output field path.
   * @return The constructed windowed computation.
   * @see [[https://dochub.mongodb.org/core/window-functions-rank \$rank]]
   */
  def rank(path: String): WindowedComputation =
    JWindowedComputations.rank(path)

  /**
   * Builds a computation of the dense rank of each document in its
   * partition.
   * Documents with the same value(s) of the `sortBy` fields result in
   * the same ranking but do not result in gaps in the returned ranks.
   * For example, a partition with the sequence [1, 3, 3, 5] representing the values of the single `sortBy` field
   * produces the following sequence of rank values: [1, 2, 2, 3].
   *
   * Sorting is required.
   *
   * @param path The output field path.
   * @return The constructed windowed computation.
   * @see [[https://dochub.mongodb.org/core/window-functions-dense-rank \$denseRank]]
   */
  def denseRank(path: String): WindowedComputation =
    JWindowedComputations.denseRank(path)

  /**
   * Builds a computation of the last observed non-`Null` evaluation result of the `expression`.
   *
   * Sorting is required.
   *
   * @param path The output field path.
   * @param expression The expression.
   * @tparam TExpression The expression type.
   * @return The constructed windowed computation.
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/locf \$locf]]
   */
  def locf[TExpression](path: String, expression: TExpression): WindowedComputation =
    JWindowedComputations.locf(path, expression)

  /**
   * Builds a computation of a value that is equal to the evaluation result of the `expression` when it is non-`Null`,
   * or to the linear interpolation of surrounding evaluation results of the `expression` when the result is BSON `Null`.
   *
   * Sorting is required.
   *
   * @param path The output field path.
   * @param expression The expression.
   * @tparam TExpression The expression type.
   * @return The constructed windowed computation.
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/linearFill \$linearFill]]
   */
  def linearFill[TExpression](path: String, expression: TExpression): WindowedComputation =
    JWindowedComputations.linearFill(path, expression)
}
