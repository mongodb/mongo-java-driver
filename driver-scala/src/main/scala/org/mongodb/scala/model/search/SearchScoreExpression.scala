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
package org.mongodb.scala.model.search

import com.mongodb.client.model.search.{ SearchScoreExpression => JSearchScoreExpression }
import org.mongodb.scala.bson.conversions.Bson

/**
 * @see [[SearchScore.function]]
 * @see [[https://www.mongodb.com/docs/atlas/atlas-search/scoring/#expressions Expressions for the function score modifier]]
 * @since 4.7
 */
object SearchScoreExpression {

  /**
   * Returns a `SearchScoreExpression` that evaluates into the relevance score of a document.
   *
   * @return The requested `SearchScoreExpression`.
   */
  def relevanceExpression(): RelevanceSearchScoreExpression = JSearchScoreExpression.relevanceExpression()

  /**
   * Returns a `SearchScoreExpression` that evaluates into the value of the specified field.
   *
   * @param path The numeric field whose value to use as the result of the expression.
   * @return The requested `SearchScoreExpression`.
   * @see `SearchScore.boost(FieldSearchPath)`
   */
  def pathExpression(path: FieldSearchPath): PathSearchScoreExpression = JSearchScoreExpression.pathExpression(path)

  /**
   * Returns a `SearchScoreExpression` that evaluates into the specified `value`.
   *
   * @param value The value to use as the result of the expression. Unlike [[SearchScore.constant]], does not have constraints.
   * @return The requested `SearchScoreExpression`.
   * @see [[SearchScore.constant]]
   */
  def constantExpression(value: Float): ConstantSearchScoreExpression = JSearchScoreExpression.constantExpression(value)

  /**
   * Creates a `SearchScoreExpression` from a `Bson` in situations when there is no builder method
   * that better satisfies your needs.
   * This method cannot be used to validate the syntax.
   *
   * <i>Example</i><br>
   * The following code creates two functionally equivalent `SearchScoreExpression`s,
   * though they may not be equal.
   * {{{
   *  val expression1: SearchScoreExpression = SearchScoreExpression.pathExpression(
   *    SearchPath.fieldPath("fieldName"))
   *    .undefined(-1.5f)
   *  val expression2: SearchScoreExpression = SearchScoreExpression.of(Document("path" ->
   *    Document("value" -> SearchPath.fieldPath("fieldName").toValue,
   *      "undefined" -> -1.5)))
   * }}}
   *
   * @param expression A `Bson` representing the required `SearchScoreExpression`.
   *
   * @return The requested `SearchScoreExpression`.
   */
  def of(expression: Bson): SearchScoreExpression = JSearchScoreExpression.of(expression)
}
