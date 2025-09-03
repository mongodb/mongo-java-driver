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

import com.mongodb.client.model.fill.{ FillOutputField => JFillOutputField }
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.WindowOutputFields

/**
 * The core part of the `\$fill` pipeline stage of an aggregation pipeline.
 * A pair of an expression/method and a path to a field to be filled with evaluation results of the expression/method.
 *
 * @see `Aggregates.fill`
 * @note Requires MongoDB 5.3 or greater.
 * @since 4.7
 */
object FillOutputField {

  /**
   * Returns a `FillOutputField` that uses the specified `expression`.
   *
   * @param field The field to fill.
   * @param expression The expression.
   * @tparam TExpression The `expression` type.
   * @return The requested `FillOutputField`.
   * @see [[https://www.mongodb.com/docs/manual/core/document/#dot-notation Dot notation]]
   */
  def value[TExpression](field: String, expression: TExpression): ValueFillOutputField =
    JFillOutputField.value(field, expression)

  /**
   * Returns a `FillOutputField` that uses the [[WindowOutputFields.locf]] method.
   *
   * @param field The field to fill.
   * @return The requested `FillOutputField`.
   * @see [[https://www.mongodb.com/docs/manual/core/document/#dot-notation Dot notation]]
   */
  def locf(field: String): LocfFillOutputField = JFillOutputField.locf(field)

  /**
   * Returns a `FillOutputField` that uses the [[WindowOutputFields.linearFill]] method.
   *
   * Sorting (`FillOptions.sortBy`) is required.
   *
   * @param field The field to fill.
   * @return The requested `FillOutputField`.
   * @see [[https://www.mongodb.com/docs/manual/core/document/#dot-notation Dot notation]]
   */
  def linear(field: String): LinearFillOutputField = JFillOutputField.linear(field)

  /**
   * Creates a `FillOutputField` from a `Bson` in situations when there is no builder method that better satisfies your needs.
   * This method cannot be used to validate the syntax.
   *
   * <i>Example</i><br>
   * The following code creates two functionally equivalent `FillOutputField`s,
   * though they may not be equal.
   * {{{
   *  val field1 = FillOutputField.locf("fieldName")
   *  val field2 = FillOutputField.of(Document("fieldName" -> Document("method" -> "locf")))
   * }}}
   *
   * @param fill A `Bson` representing the required `FillOutputField`.
   *
   * @return The requested `FillOutputField`.
   */
  def of(fill: Bson): FillOutputField = JFillOutputField.of(fill)
}
