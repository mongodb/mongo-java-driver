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
package org.mongodb.scala.model.densify

import com.mongodb.client.model.{ MongoTimeUnit => JMongoTimeUnit }
import com.mongodb.client.model.densify.{ DensifyRange => JDensifyRange }
import org.bson.conversions.Bson

import java.time.Instant

/**
 * A specification of how to compute the missing field values
 * for which new documents must be added. It specifies a half-closed interval of values with the lower bound being inclusive, and a step.
 * The first potentially missing value within each interval is its lower bound, other values are computed by adding the step
 * multiple times, until the result is out of the interval. Each time the step is added, the result is a potentially missing value for
 * which a new document must be added if the sequence of documents that is being densified does not have a document
 * with equal value of the field.
 *
 * @see `Aggregates.densify`
 * @note Requires MongoDB 5.1 or greater.
 * @since 4.7
 */
object DensifyRange {

  /**
   * Returns a `DensifyRange` that represents an interval with the smallest
   * BSON `32-bit integer` / `64-bit integer` / `Double` / `Decimal128` value of the field
   * in the sequence of documents being its lower bound, and the largest value being the upper bound.
   *
   * @param step The positive step.
   * @return The requested `DensifyRange`.
   */
  def fullRangeWithStep(step: Number): NumberDensifyRange = JDensifyRange.fullRangeWithStep(step)

  /**
   * Returns a `DensifyRange` that represents an interval with the smallest
   * BSON `32-bit integer` / `64-bit integer` / `Double` / `Decimal128` value of the field
   * in the partition of documents being its lower bound, and the largest value being the upper bound.
   *
   * @param step The positive step.
   * @return The requested `DensifyRange`.
   */
  def partitionRangeWithStep(step: Number): NumberDensifyRange =
    JDensifyRange.partitionRangeWithStep(step)

  /**
   * Returns a `DensifyRange` that represents a single interval [l; u).
   *
   * @param l The lower bound.
   * @param u The upper bound.
   * @param step The positive step.
   * @return The requested `DensifyRange`.
   */
  def rangeWithStep(l: Number, u: Number, step: Number): NumberDensifyRange =
    JDensifyRange.rangeWithStep(l, u, step)

  /**
   * Returns a `DensifyRange` that represents an interval with the smallest BSON `Date` value of the field
   * in the sequence of documents being its lower bound, and the largest value being the upper bound.
   *
   * @param step The positive step.
   * @param unit The unit in which the `step` is specified.
   * @return The requested `DensifyRange`.
   */
  def fullRangeWithStep(step: Long, unit: JMongoTimeUnit): DateDensifyRange =
    JDensifyRange.fullRangeWithStep(step, unit)

  /**
   * Returns a `DensifyRange` that represents an interval with the smallest BSON `Date` value of the field
   * in the partition of documents being its lower bound, and the largest value being the upper bound.
   *
   * @param step The positive step.
   * @param unit The unit in which the `step` is specified.
   * @return The requested `DensifyRange`.
   */
  def partitionRangeWithStep(step: Long, unit: JMongoTimeUnit): DateDensifyRange =
    JDensifyRange.partitionRangeWithStep(step, unit)

  /**
   * Returns a `DensifyRange` that represents a single interval [l; u).
   *
   * @param l The lower bound.
   * @param u The upper bound.
   * @param step The positive step.
   * @param unit The unit in which the `step` is specified.
   * @return The requested `DensifyRange`.
   */
  def rangeWithStep(l: Instant, u: Instant, step: Long, unit: JMongoTimeUnit): DateDensifyRange =
    JDensifyRange.rangeWithStep(l, u, step, unit)

  /**
   * Creates a `DensifyRange` from a `Bson` in situations when there is no builder method that better satisfies your needs.
   * This method cannot be used to validate the syntax.
   *
   * <i>Example</i><br>
   * The following code creates two functionally equivalent `DensifyRange`s,
   * though they may not be equal.
   * {{{
   *  val range1 = DensifyRange.partitionRangeWithStep(
   *    1, MongoTimeUnit.MINUTE)
   *  val range2 = DensifyRange.of(Document("bounds" -> "partition",
   *    "step" -> 1, "unit" -> MongoTimeUnit.MINUTE.value()))
   * }}}
   *
   * @param range A `Bson` representing the required `DensifyRange`.
   *
   * @return The requested `DensifyRange`.
   */
  def of(range: Bson): DensifyRange = JDensifyRange.of(range)
}
