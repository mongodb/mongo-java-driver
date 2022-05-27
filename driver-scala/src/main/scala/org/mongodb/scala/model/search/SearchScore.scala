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

import com.mongodb.annotations.Beta
import com.mongodb.client.model.search.{ SearchScore => JSearchScore }
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.Projections

/**
 * A modifier of the relevance score.
 * You may use the `\$meta: "searchScore"` expression, e.g., via [[Projections.metaSearchScore]],
 * to extract the relevance score assigned to each found document.
 *
 * @see [[https://www.mongodb.com/docs/atlas/atlas-search/scoring/ Scoring]]
 * @since 4.7
 */
@Beta(Array(Beta.Reason.CLIENT))
object SearchScore {

  /**
   * Returns a `SearchScore` that instructs to multiply the score by the specified `value`.
   *
   * @param value The positive value to multiply the score by.
   * @return The requested `SearchScore`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/scoring/#boost boost score modifier]]
   */
  def boost(value: Float): ValueBoostSearchScore = JSearchScore.boost(value)

  /**
   * Returns a `SearchScore` that instructs to multiply the score by the value of the specified field.
   *
   * @param path The numeric field whose value to multiply the score by.
   * @return The requested `SearchScore`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/scoring/#boost boost score modifier]]
   * @see [[SearchScoreExpression.pathExpression]]
   */
  def boost(path: FieldSearchPath): PathBoostSearchScore = JSearchScore.boost(path)

  /**
   * Returns a `SearchScore` that instructs to replace the score with the specified `value`.
   *
   * @param value The positive value to replace the score with.
   * @return The requested `SearchScore`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/scoring/#constant constant score modifier]]
   * @see [[SearchScoreExpression.constantExpression]]
   */
  def constant(value: Float): ConstantSearchScore = JSearchScore.constant(value)

  /**
   * Returns a `SearchScore` that instructs to compute the score using the specified `expression`.
   *
   * @param expression The expression to use when calculating the score.
   * @return The requested `SearchScore`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/scoring/#function function score modifier]]
   */
  def function(expression: SearchScoreExpression): FunctionSearchScore = JSearchScore.function(expression)

  /**
   * Creates a `SearchScore` from a `Bson` in situations when there is no builder method that better satisfies your needs.
   * This method cannot be used to validate the syntax.
   *
   * <i>Example</i><br>
   * The following code creates two functionally equivalent `SearchScore`s,
   * though they may not be equal.
   * {{{
   *  val score1: SearchScore = SearchScore.boost(
   *    SearchPath.fieldPath("fieldName"))
   *  val score2: SearchScore = SearchScore.of(Document("boost" ->
   *    Document("path" -> SearchPath.fieldPath("fieldName").toValue)))
   * }}}
   *
   * @param score A `Bson` representing the required `SearchScore`.
   *
   * @return The requested `SearchScore`.
   */
  def of(score: Bson): SearchScore = JSearchScore.of(score)
}
