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
import com.mongodb.client.model.{ MongoTimeUnit => JMongoTimeUnit, Windows => JWindows }
import org.bson.types.Decimal128
import org.mongodb.scala.bson.conversions.Bson

/**
 * Builders for [[Window windows]] used when expressing [[WindowedComputation windowed computations]].
 * There are two types of windows: documents and range.
 *
 * Bounded and half-bounded windows require sorting.
 * Window bounds are inclusive and the lower bound must always be less than or equal to the upper bound.
 * The following type-specific rules are applied to windows:
 *  - documents
 *    - bounds
 *      - 0 refers to the current document and is functionally equivalent to [[Windows.Bound CURRENT]];
 *      - a negative value refers to documents preceding the current one;
 *      - a positive value refers to documents following the current one;
 *  - range
 *    - [[Aggregates.setWindowFields sortBy]]
 *      - must contain exactly one field;
 *      - must specify the ascending sort order;
 *      - the `sortBy` field must be of either a numeric BSON type
 *      (see the `\$isNumber` aggregation pipeline stage)
 *      or the BSON `Date` type if time
 *      bounds are used;
 *    - bounds
 *      - if numeric, i.e., not `com.mongodb.client.model.Windows.Bound`, then the bound is calculated by adding
 *      the value to the value of the `sortBy` field in the current document;
 *      - if [[Windows.Bound CURRENT]], then the bound is determined by the current document
 *      and not the current value of the `sortBy` field;
 *      - time bounds require specifying a [[MongoTimeUnit time unit]] and are added as per the
 *      `\$dateAdd`/`\$dateSubtract` aggregation pipeline stage specification.
 *
 * @see WindowedComputation
 * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/isNumber/ \$isNumber aggregation pipeline stage]]
 * @see [[https://www.mongodb.com/docs/manual/reference/bson-types/#date BSON Date type]]
 * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateAdd/ \$dateAdd aggregation pipeline stage]]
 * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateSubtract/ \$dateSubtract aggregation pipeline stage]]
 * @since 4.3
 * @note Requires MongoDB 5.0 or greater.
 */
@Beta
object Windows {

  /**
   * Creates a window from `Bson` in situations when there is no builder method that better satisfies your needs.
   * This method cannot be used to validate the syntax.
   *
   * <i>Example</i><br>
   * The following code creates two functionally identical windows, though they may not be equal.
   * {{{
   *  val pastWeek1: Window = Windows.timeRange(-1, MongoTimeUnit.WEEK, Windows.Bound.CURRENT)
   *  val pastWeek2: Window = Windows.of(
   *      Document("range" -> BsonArray(-1, "current"),
   *          "unit" -> BsonString("week")))
   * }}}
   *
   * @param window A `Bson` representing the required window.
   * @return The constructed window.
   */
  def of(window: Bson): Window = JWindows.of(window)

  /**
   * Creates a documents window whose bounds are determined by a number of documents before and after the current document.
   *
   * @param lower A value based on which the lower bound of the window is calculated.
   * @param upper A value based on which the upper bound of the window is calculated.
   * @return The constructed documents window.
   */
  def documents(lower: Int, upper: Int): Window = JWindows.documents(lower, upper)

  /**
   * Creates a documents window whose bounds are determined by a number of documents before and after the current document.
   *
   * @param lower A value based on which the lower bound of the window is calculated.
   * @param upper A value based on which the upper bound of the window is calculated.
   * @return The constructed documents window.
   * @note Requires MongoDB 5.0 or greater.
   */
  def documents(lower: JWindows.Bound, upper: Int): Window = JWindows.documents(lower, upper)

  /**
   * Creates a documents window whose bounds are determined by a number of documents before and after the current document.
   *
   * @param lower A value based on which the lower bound of the window is calculated.
   * @param upper A value based on which the upper bound of the window is calculated.
   * @return The constructed documents window.
   */
  def documents(lower: Int, upper: JWindows.Bound): Window = JWindows.documents(lower, upper)

  /**
   * Creates a documents window whose bounds are determined by a number of documents before and after the current document.
   *
   * @param lower A value based on which the lower bound of the window is calculated.
   * @param upper A value based on which the upper bound of the window is calculated.
   * @return The constructed documents window.
   */
  def documents(lower: JWindows.Bound, upper: JWindows.Bound): Window = JWindows.documents(lower, upper)

  /**
   * Creates a dynamically-sized range window whose bounds are determined by a range of possible values around
   * the value of the [[Aggregates.setWindowFields sortBy]] field in the current document.
   *
   * @param lower A value based on which the lower bound of the window is calculated.
   * @param upper A value based on which the upper bound of the window is calculated.
   * @return The constructed range window.
   */
  def range(lower: Long, upper: Long): Window = JWindows.range(lower, upper)

  /**
   * Creates a dynamically-sized range window whose bounds are determined by a range of possible values around
   * the value of the [[Aggregates.setWindowFields sortBy]] field in the current document.
   *
   * @param lower A value based on which the lower bound of the window is calculated.
   * @param upper A value based on which the upper bound of the window is calculated.
   * @return The constructed range window.
   */
  def range(lower: Double, upper: Double): Window = JWindows.range(lower, upper)

  /**
   * Creates a dynamically-sized range window whose bounds are determined by a range of possible values around
   * the value of the [[Aggregates.setWindowFields sortBy]] field in the current document.
   *
   * @param lower A value based on which the lower bound of the window is calculated.
   * @param upper A value based on which the upper bound of the window is calculated.
   * @return The constructed range window.
   */
  def range(lower: Decimal128, upper: Decimal128): Window = JWindows.range(lower, upper)

  /**
   * Creates a dynamically-sized range window whose bounds are determined by a range of possible values around
   * the value of the [[Aggregates.setWindowFields sortBy]] field in the current document.
   *
   * @param lower A value based on which the lower bound of the window is calculated.
   * @param upper A value based on which the upper bound of the window is calculated.
   * @return The constructed range window.
   */
  def range(lower: JWindows.Bound, upper: Long): Window = JWindows.range(lower, upper)

  /**
   * Creates a dynamically-sized range window whose bounds are determined by a range of possible values around
   * the value of the [[Aggregates.setWindowFields sortBy]] field in the current document.
   *
   * @param lower A value based on which the lower bound of the window is calculated.
   * @param upper A value based on which the upper bound of the window is calculated.
   * @return The constructed range window.
   */
  def range(lower: JWindows.Bound, upper: Double): Window = JWindows.range(lower, upper)

  /**
   * Creates a dynamically-sized range window whose bounds are determined by a range of possible values around
   * the value of the [[Aggregates.setWindowFields sortBy]] field in the current document.
   *
   * @param lower A value based on which the lower bound of the window is calculated.
   * @param upper A value based on which the upper bound of the window is calculated.
   * @return The constructed range window.
   */
  def range(lower: JWindows.Bound, upper: Decimal128): Window = JWindows.range(lower, upper)

  /**
   * Creates a dynamically-sized range window whose bounds are determined by a range of possible values around
   * the value of the [[Aggregates.setWindowFields sortBy]] field in the current document.
   *
   * @param lower A value based on which the lower bound of the window is calculated.
   * @param upper A value based on which the upper bound of the window is calculated.
   * @return The constructed range window.
   */
  def range(lower: Long, upper: JWindows.Bound): Window = JWindows.range(lower, upper)

  /**
   * Creates a dynamically-sized range window whose bounds are determined by a range of possible values around
   * the value of the [[Aggregates.setWindowFields sortBy]] field in the current document.
   *
   * @param lower A value based on which the lower bound of the window is calculated.
   * @param upper A value based on which the upper bound of the window is calculated.
   * @return The constructed range window.
   */
  def range(lower: Double, upper: JWindows.Bound): Window = JWindows.range(lower, upper)

  /**
   * Creates a dynamically-sized range window whose bounds are determined by a range of possible values around
   * the value of the [[Aggregates.setWindowFields sortBy]] field in the current document.
   *
   * @param lower A value based on which the lower bound of the window is calculated.
   * @param upper A value based on which the upper bound of the window is calculated.
   * @return The constructed range window.
   */
  def range(lower: Decimal128, upper: JWindows.Bound): Window = JWindows.range(lower, upper)

  /**
   * Creates a dynamically-sized range window whose bounds are determined by a range of possible values around
   * the BSON `Date` value of the [[Aggregates.setWindowFields sortBy]]
   * field in the current document.
   *
   * @param lower A value based on which the lower bound of the window is calculated.
   * @param upper A value based on which the upper bound of the window is calculated.
   * @param unit  A time unit in which `lower` and `upper` are specified.
   * @return The constructed range window.
   */
  def timeRange(lower: Long, upper: Long, unit: JMongoTimeUnit): Window = JWindows.timeRange(lower, upper, unit)

  /**
   * Creates a dynamically-sized range window whose bounds are determined by a range of possible values around
   * the BSON `Date` value of the [[Aggregates.setWindowFields sortBy]]
   * field in the current document.
   *
   * @param lower A value based on which the lower bound of the window is calculated.
   * @param upper A value based on which the upper bound of the window is calculated.
   * @param unit  A time unit in which `upper` is specified.
   * @return The constructed range window.
   */
  def timeRange(lower: JWindows.Bound, upper: Long, unit: JMongoTimeUnit): Window =
    JWindows.timeRange(lower, upper, unit)

  /**
   * Creates a dynamically-sized range window whose bounds are determined by a range of possible values around
   * the BSON `Date` value of the [[Aggregates.setWindowFields sortBy]]
   * field in the current document.
   *
   * @param lower A value based on which the lower bound of the window is calculated.
   * @param unit  A time unit in which `lower` is specified.
   * @param upper A value based on which the upper bound of the window is calculated.
   * @return The constructed range window.
   */
  def timeRange(lower: Long, unit: JMongoTimeUnit, upper: JWindows.Bound): Window =
    JWindows.timeRange(lower, unit, upper)

  /**
   * Special values that may be used when specifying the bounds of a [[Window window]].
   *
   * @since 4.3
   * @note Requires MongoDB 5.0 or greater.
   */
  @Beta
  object Bound {

    /**
     * The [[Window window]] bound is determined by the current document and is inclusive.
     */
    val UNBOUNDED = JWindows.Bound.UNBOUNDED

    /**
     * The [[Window window]] bound is the same as the corresponding bound of the partition encompassing it.
     */
    val CURRENT = JWindows.Bound.CURRENT
  }
}
