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

import com.mongodb.annotations.Beta
import com.mongodb.client.model.{ MongoTimeUnit => JMongoTimeUnit, WindowedComputations => JWindowedComputations }

/**
 * Builders for [[WindowedComputation windowed computations]] used in the
 * [[Aggregates.setWindowFields \$setWindowFields]] pipeline stage
 * of an aggregation pipeline. Each windowed computation is a triple:
 *  - A window function. Some functions require documents in a window to be sorted
 *  (see `sortBy` in [[Aggregates.setWindowFields]]).
 *  - An optional [[Window window]], a.k.a. frame.
 *  Specifying `null` window is equivalent to specifying an unbounded window,
 *  i.e., a window with both ends specified as [[Windows.Bound UNBOUNDED]].
 *  Some window functions require to specify an explicit unbounded window instead of specifying `null`.
 *  - A path to an output field to be computed by the window function over the window.
 *
 * A windowed computation is similar to an [[Accumulators accumulator]] but does not result in folding documents constituting
 * the window into a single document.
 *
 * @see [[http://docs.mongodb.org/manual/meta/aggregation-quick-reference/#field-paths Field paths]]
 * @since 4.3
 * @note Requires MongoDB 5.0 or greater.
 */
@Beta
object WindowedComputations {

  /**
   * Builds a computation of the sum of the evaluation results of the `expression` over the `window`.
   *
   * @param path       The output field path.
   * @param expression The expression.
   * @param window     The window. May be `null`.
   * @tparam TExpression The expression type.
   * @return The constructed [[WindowedComputation]].
   * @see [[http://dochub.mongodb.org/core/window-functions-sum \$sum]]
   */
  def sum[TExpression](path: String, expression: TExpression, window: Window): WindowedComputation =
    JWindowedComputations.sum(path, expression, window)

  /**
   * Builds a computation of the average of the evaluation results of the `expression` over the `window`.
   *
   * @param path       The output field path.
   * @param expression The expression.
   * @param window     The window. May be `null`.
   * @tparam TExpression The expression type.
   * @return The constructed [[WindowedComputation]].
   * @see [[http://dochub.mongodb.org/core/window-functions-avg \$avg]]
   */
  def avg[TExpression](path: String, expression: TExpression, window: Window): WindowedComputation =
    JWindowedComputations.avg(path, expression, window)

  /**
   * Builds a computation of the sample standard deviation of the evaluation results of the `expression` over the `window`.
   *
   * @param path       The output field path.
   * @param expression The expression.
   * @param window     The window. May be `null`.
   * @tparam TExpression The expression type.
   * @return The constructed [[WindowedComputation]].
   * @see [[http://dochub.mongodb.org/core/window-functions-std-dev-samp \$stdDevSamp]]
   */
  def stdDevSamp[TExpression](path: String, expression: TExpression, window: Window): WindowedComputation =
    JWindowedComputations.stdDevSamp(path, expression, window)

  /**
   * Builds a computation of the population standard deviation of the evaluation results of the `expression` over the `window`.
   *
   * @param path       The output field path.
   * @param expression The expression.
   * @param window     The window. May be `null`.
   * @tparam TExpression The expression type.
   * @return The constructed [[WindowedComputation]].
   * @see [[http://dochub.mongodb.org/core/window-functions-std-dev-pop \$stdDevPop]]
   */
  def stdDevPop[TExpression](path: String, expression: TExpression, window: Window): WindowedComputation =
    JWindowedComputations.stdDevPop(path, expression, window)

  /**
   * Builds a computation of the lowest of the evaluation results of the `expression` over the `window`.
   *
   * @param path       The output field path.
   * @param expression The expression.
   * @param window     The window. May be `null`.
   * @tparam TExpression The expression type.
   * @return The constructed [[WindowedComputation]].
   * @see [[http://dochub.mongodb.org/core/window-functions-min \$min]]
   */
  def min[TExpression](path: String, expression: TExpression, window: Window): WindowedComputation =
    JWindowedComputations.min(path, expression, window)

  /**
   * Builds a computation of the highest of the evaluation results of the `expression` over the `window`.
   *
   * @param path       The output field path.
   * @param expression The expression.
   * @param window     The window. May be `null`.
   * @tparam TExpression The expression type.
   * @return The constructed [[WindowedComputation]].
   * @see [[http://dochub.mongodb.org/core/window-functions-max \$max]]
   */
  def max[TExpression](path: String, expression: TExpression, window: Window): WindowedComputation =
    JWindowedComputations.max(path, expression, window)

  /**
   * Builds a computation of the number of documents in the `window`.
   *
   * @param path   The output field path.
   * @param window The window. May be `null`.
   * @return The constructed [[WindowedComputation]].
   * @see [[http://dochub.mongodb.org/core/window-functions-count \$count]]
   */
  def count(path: String, window: Window): WindowedComputation =
    JWindowedComputations.count(path, window)

  /**
   * Builds a computation of the time derivative by subtracting the evaluation result of the `expression` against the last document
   * and the first document in the `window` and dividing it by the difference in the values of the
   * [[Aggregates.setWindowFields sortBy]] field of the respective documents.
   * Other documents in the `window` have no effect on the computation.
   *
   * [[Aggregates.setWindowFields Sorting]] is required.
   *
   * @param path       The output field path.
   * @param expression The expression.
   * @param window     The window.
   * @tparam TExpression The expression type.
   * @return The constructed [[WindowedComputation]].
   * @see [[http://dochub.mongodb.org/core/window-functions-derivative \$derivative]]
   */
  def derivative[TExpression](path: String, expression: TExpression, window: Window): WindowedComputation =
    JWindowedComputations.derivative(path, expression, window)

  /**
   * Builds a computation of the time derivative by subtracting the evaluation result of the `expression` against the last document
   * and the first document in the `window` and dividing it by the difference in the BSON `Date`
   * values of the [[Aggregates.setWindowFields sortBy]] field of the respective documents.
   * Other documents in the `window` have no effect on the computation.
   *
   * [[Aggregates.setWindowFields Sorting]] is required.
   *
   * @param path       The output field path.
   * @param expression The expression.
   * @param window     The window.
   * @param unit       The desired time unit for the divisor. Allowed values are:
   *                   [[MongoTimeUnit WEEK]], [[MongoTimeUnit DAY]], [[MongoTimeUnit HOUR]], [[MongoTimeUnit MINUTE]],
   *                   [[MongoTimeUnit SECOND]], [[MongoTimeUnit MILLISECOND]].
   * @tparam TExpression The expression type.
   * @return The constructed [[WindowedComputation]].
   * @see [[http://dochub.mongodb.org/core/window-functions-derivative \$derivative]]
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
   * the [[Aggregates.setWindowFields sortBy]] field to evaluation results of the `expression`
   * against the same document. The limits of integration match the `window` bounds.
   * The approximation is done by using the
   * <a href="https://www.khanacademy.org/math/ap-calculus-ab/ab-integration-new/ab-6-2/a/understanding-the-trapezoid-rule">
   * trapezoidal rule</a>.
   *
   * [[Aggregates.setWindowFields Sorting]] is required.
   *
   * @param path       The output field path.
   * @param expression The expression.
   * @param window     The window.
   * @tparam TExpression The expression type.
   * @return The constructed [[WindowedComputation]].
   * @see [[http://dochub.mongodb.org/core/window-functions-integral \$integral]]
   */
  def integral[TExpression](path: String, expression: TExpression, window: Window): WindowedComputation =
    JWindowedComputations.integral(path, expression, window)

  /**
   * Builds a computation of the approximate integral of a function that maps BSON `Date` values of
   * the [[Aggregates.setWindowFields sortBy]] field to evaluation results of the `expression`
   * against the same document. The limits of integration match the `window` bounds.
   * The approximation is done by using the trapezoidal rule.
   *
   * [[Aggregates.setWindowFields Sorting]] is required.
   *
   * @param path       The output field path.
   * @param expression The expression.
   * @param window     The window.
   * @param unit       The desired time unit for the divisor. Allowed values are:
   *                   [[MongoTimeUnit WEEK]], [[MongoTimeUnit DAY]], [[MongoTimeUnit HOUR]], [[MongoTimeUnit MINUTE]],
   *                   [[MongoTimeUnit SECOND]], [[MongoTimeUnit MILLISECOND]].
   * @tparam TExpression The expression type.
   * @return The constructed [[WindowedComputation]].
   * @see [[http://dochub.mongodb.org/core/window-functions-integral \$integral]]
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
   * @param window      The window. May be `null`.
   * @tparam TExpression The expression type.
   * @return The constructed [[WindowedComputation]].
   * @see [[http://dochub.mongodb.org/core/window-functions-covariance-samp \$covarianceSamp]]
   */
  def covarianceSamp[TExpression](
      path: String,
      expression1: TExpression,
      expression2: TExpression,
      window: Window
  ): WindowedComputation =
    JWindowedComputations.covarianceSamp(path, expression1, expression2, window)

  /**
   * Builds a computation of the population covariance between the evaluation results of the two expressions over the `window`.
   *
   * @param path        The output field path.
   * @param expression1 The first expression.
   * @param expression2 The second expression.
   * @param window      The window. May be `null`.
   * @tparam TExpression The expression type.
   * @return The constructed [[WindowedComputation]].
   * @see [[http://dochub.mongodb.org/core/window-functions-covariance-pop \$covariancePop]]
   */
  def covariancePop[TExpression](
      path: String,
      expression1: TExpression,
      expression2: TExpression,
      window: Window
  ): WindowedComputation =
    JWindowedComputations.covariancePop(path, expression1, expression2, window)

  /**
   * Builds a computation of the exponential moving average of the evaluation results of the `expression` over a window
   * that includes `n` - 1 documents preceding the current document and the current document, with more weight on documents
   * closer to the current one.
   *
   * [[Aggregates.setWindowFields Sorting]] is required.
   *
   * @param path       The output field path.
   * @param expression The expression.
   * @param n          Must be positive.
   * @tparam TExpression The expression type.
   * @return The constructed [[WindowedComputation]].
   * @see [[http://dochub.mongodb.org/core/window-functions-exp-moving-avg \$expMovingAvg]]
   */
  def expMovingAvg[TExpression](path: String, expression: TExpression, n: Int): WindowedComputation =
    JWindowedComputations.expMovingAvg(path, expression, n)

  /**
   * Builds a computation of the exponential moving average of the evaluation results of the `expression` over the half-bounded
   * window `[`[[Windows.Bound UNBOUNDED]], [[Windows.Bound CURRENT]]`]`,
   * with `alpha` representing the degree of weighting decrease.
   *
   * [[Aggregates.setWindowFields Sorting]] is required.
   *
   * @param path       The output field path.
   * @param expression The expression.
   * @param alpha      A parameter specifying how fast weighting decrease happens. A higher `alpha` discounts older observations faster.
   *                   Must belong to the interval (0, 1).
   * @tparam TExpression The expression type.
   * @return The constructed [[WindowedComputation]].
   * @see [[http://dochub.mongodb.org/core/window-functions-exp-moving-avg \$expMovingAvg]]
   */
  def expMovingAvg[TExpression](path: String, expression: TExpression, alpha: Double): WindowedComputation =
    JWindowedComputations.expMovingAvg(path, expression, alpha)

  /**
   * Builds a computation that adds the evaluation results of the `expression` over the `window`
   * to a BSON `Array`.
   * Order within the array is guaranteed if [[Aggregates.setWindowFields sortBy]] is specified.
   *
   * @param path       The output field path.
   * @param expression The expression.
   * @param window     The window. May be `null`.
   * @tparam TExpression The expression type.
   * @return The constructed [[WindowedComputation]].
   * @see [[http://dochub.mongodb.org/core/window-functions-push \$push]]
   */
  def push[TExpression](path: String, expression: TExpression, window: Window): WindowedComputation =
    JWindowedComputations.push(path, expression, window)

  /**
   * Builds a computation that adds the evaluation results of the `expression` over the `window`
   * to a BSON `Array` and excludes duplicates.
   * Order within the array is not specified.
   *
   * @param path       The output field path.
   * @param expression The expression.
   * @param window     The window. May be `null`.
   * @tparam TExpression The expression type.
   * @return The constructed [[WindowedComputation]].
   * @see [[http://dochub.mongodb.org/core/window-functions-add-to-set \$addToSet]]
   */
  def addToSet[TExpression](path: String, expression: TExpression, window: Window): WindowedComputation =
    JWindowedComputations.addToSet(path, expression, window)

  /**
   * Builds a computation of the evaluation result of the `expression` against the first document in the `window`.
   *
   * [[Aggregates.setWindowFields Sorting]] is required.
   *
   * @param path       The output field path.
   * @param expression The expression.
   * @param window     The window. May be `null`.
   * @tparam TExpression The expression type.
   * @return The constructed [[WindowedComputation]].
   * @see [[http://dochub.mongodb.org/core/window-functions-first \$first]]
   */
  def first[TExpression](path: String, expression: TExpression, window: Window): WindowedComputation =
    JWindowedComputations.first(path, expression, window)

  /**
   * Builds a computation of the evaluation result of the `expression` against the last document in the `window`.
   *
   * [[Aggregates.setWindowFields Sorting]] is required.
   *
   * @param path       The output field path.
   * @param expression The expression.
   * @param window     The window. May be `null`.
   * @tparam TExpression The expression type.
   * @return The constructed [[WindowedComputation]].
   * @see [[http://dochub.mongodb.org/core/window-functions-last \$last]]
   */
  def last[TExpression](path: String, expression: TExpression, window: Window): WindowedComputation =
    JWindowedComputations.last(path, expression, window)

  /**
   * Builds a computation of the evaluation result of the `expression` for the document whose position is shifted by the given
   * amount relative to the current document. If the shifted document is outside of the
   * [[Aggregates.setWindowFields partition]] containing the current document,
   * then the `defaultExpression` is used instead of the `expression`.
   *
   * [[Aggregates.setWindowFields Sorting]] is required.
   *
   * @param path              The output field path.
   * @param expression        The expression.
   * @param defaultExpression The default expression.
   *                          If `null`, then the default expression is evaluated to BSON `Null`.
   *                          Must evaluate to a constant value.
   * @param by                The shift specified similarly to [[Windows rules for window bounds]]:
   *                          - 0 means the current document;
   *                          - a negative value refers to the document preceding the current one;
   *                          - a positive value refers to the document following the current one.
   * @tparam TExpression The expression type.
   * @return The constructed [[WindowedComputation]].
   * @see [[http://dochub.mongodb.org/core/window-functions-shift \$shift]]
   */
  def shift[TExpression](
      path: String,
      expression: TExpression,
      defaultExpression: TExpression,
      by: Int
  ): WindowedComputation =
    JWindowedComputations.shift(path, expression, defaultExpression, by)

  /**
   * Builds a computation of the order number of each document in its
   * [[Aggregates.setWindowFields partition]].
   *
   * [[Aggregates.setWindowFields Sorting]] is required.
   *
   * @param path The output field path.
   * @return The constructed [[WindowedComputation]].
   * @see [[http://dochub.mongodb.org/core/window-functions-document-number \$documentNumber]]
   */
  def documentNumber(path: String): WindowedComputation =
    JWindowedComputations.documentNumber(path)

  /**
   * Builds a computation of the rank of each document in its
   * [[Aggregates.setWindowFields partition]].
   * Documents with the same value(s) of the [[Aggregates.setWindowFields sortBy]] fields result in
   * the same ranking and result in gaps in the returned ranks.
   * For example, a partition with the sequence [1, 3, 3, 5] representing the values of the single `sortBy` field
   * produces the following sequence of rank values: [1, 2, 2, 4].
   *
   * [[Aggregates.setWindowFields Sorting]] is required.
   *
   * @param path The output field path.
   * @return The constructed [[WindowedComputation]].
   * @see [[http://dochub.mongodb.org/core/window-functions-rank \$rank]]
   */
  def rank(path: String): WindowedComputation =
    JWindowedComputations.rank(path)

  /**
   * Builds a computation of the dense rank of each document in its
   * [[Aggregates.setWindowFields partition]].
   * Documents with the same value(s) of the [[Aggregates.setWindowFields sortBy]] fields result in
   * the same ranking but do not result in gaps in the returned ranks.
   * For example, a partition with the sequence [1, 3, 3, 5] representing the values of the single `sortBy` field
   * produces the following sequence of rank values: [1, 2, 2, 3].
   *
   * [[Aggregates.setWindowFields Sorting]] is required.
   *
   * @param path The output field path.
   * @return The constructed [[WindowedComputation]].
   * @see [[http://dochub.mongodb.org/core/window-functions-dense-rank \$denseRank]]
   */
  def denseRank(path: String): WindowedComputation =
    JWindowedComputations.denseRank(path)
}
