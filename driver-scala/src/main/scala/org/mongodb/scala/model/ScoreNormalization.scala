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

import com.mongodb.client.model.{ ScoreNormalization => JScoreNormalization }

/**
 * Normalization methods for the `\$score` and `\$scoreFusion` pipeline stages.
 *
 * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/score/ \$score]]
 * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/scoreFusion/ \$scoreFusion]]
 * @note Requires MongoDB 8.2 or greater
 * @since 5.10
 */
object ScoreNormalization {

  /**
   * No normalization is applied.
   */
  val NONE: ScoreNormalization = JScoreNormalization.NONE

  /**
   * Normalizes the score to the range (0, 1) by applying the sigmoid function.
   */
  val SIGMOID: ScoreNormalization = JScoreNormalization.SIGMOID

  /**
   * Normalizes the score to the range [0, 1] by applying min-max scaling.
   */
  val MIN_MAX_SCALER: ScoreNormalization = JScoreNormalization.MIN_MAX_SCALER
}
