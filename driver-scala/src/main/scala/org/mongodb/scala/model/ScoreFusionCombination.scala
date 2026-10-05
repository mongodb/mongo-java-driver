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

import com.mongodb.client.model.{ ScoreFusionCombination => JScoreFusionCombination }
import org.mongodb.scala.bson.conversions.Bson

/**
 * The way in which the normalized scores produced by the `\$scoreFusion` input pipelines are combined.
 *
 * @note Requires MongoDB 8.2 or greater
 * @since 5.10
 */
object ScoreFusionCombination {

  /**
   * Returns a `WeightedScoreFusionCombination` that combines the normalized scores using the given weights.
   *
   * @param weights the weights
   * @return The requested `WeightedScoreFusionCombination`.
   */
  def weighted(weights: Bson): WeightedScoreFusionCombination = JScoreFusionCombination.weighted(weights)

  /**
   * Returns a `ScoreFusionCombination` that combines the normalized scores using the given expression.
   *
   * @param expression the expression
   * @return The requested `ScoreFusionCombination`.
   */
  def expression(expression: Bson): ScoreFusionCombination = JScoreFusionCombination.expression(expression)
}
