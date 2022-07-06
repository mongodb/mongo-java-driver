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
package org.mongodb.scala.model.fill

import com.mongodb.client.model.fill.{ FillComputation => JFillComputation }
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.WindowedComputations

/**
 * The core part of the `\$fill` pipeline stage of an aggregation pipeline.
 * A pair of an expression/method and a path to a field to be filled with evaluation results of the expression/method.
 *
 * @see `Aggregates.fill`
 * @note Requires MongoDB 5.3 or greater.
 * @since 4.7
 */
object FillComputation {

  /**
   * Returns a `FillComputation` that uses the specified `expression`.
   *
   * @param field The field to fill.
   * @param expression The expression.
   * @tparam TExpression The `expression` type.
   * @return The requested `FillComputation`.
   * @see [[https://www.mongodb.com/docs/manual/core/document/#dot-notation Dot notation]]
   */
  def value[TExpression](field: String, expression: TExpression): ValueFillComputation =
    JFillComputation.value(field, expression)

  /**
   * Returns a `FillComputation` that uses the [[WindowedComputations.locf]] method.
   *
   * @param field The field to fill.
   * @return The requested `FillComputation`.
   * @see [[https://www.mongodb.com/docs/manual/core/document/#dot-notation Dot notation]]
   */
  def locf(field: String): LocfFillComputation = JFillComputation.locf(field)

  /**
   * Returns a `FillComputation` that uses the [[WindowedComputations.linearFill]] method.
   *
   * Sorting (`FillOptions.sortBy`) is required.
   *
   * @param field The field to fill.
   * @return The requested `FillComputation`.
   * @see [[https://www.mongodb.com/docs/manual/core/document/#dot-notation Dot notation]]
   */
  def linear(field: String): LinearFillComputation = JFillComputation.linear(field)

  /**
   * Creates a `FillComputation` from a `Bson` in situations when there is no builder method that better satisfies your needs.
   * This method cannot be used to validate the syntax.
   *
   * <i>Example</i><br>
   * The following code creates two functionally equivalent `FillComputation`s,
   * though they may not be equal.
   * {{{
   *  val field1 = FillComputation.locf("fieldName")
   *  val field2 = FillComputation.of(Document("fieldName" -> Document("method" -> "locf")))
   * }}}
   *
   * @param fill A `Bson` representing the required `FillComputation`.
   *
   * @return The requested `FillComputation`.
   */
  def of(fill: Bson): FillComputation = JFillComputation.of(fill)
}
