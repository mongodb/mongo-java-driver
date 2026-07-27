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

import com.mongodb.annotations.Sealed
import com.mongodb.client.model.{ ScoreNormalization => JScoreNormalization }
import org.bson.BsonValue

/**
 * The way in which the scores produced by the `\$scoreFusion` input pipelines are normalized before being combined.
 *
 * @note Requires MongoDB 8.2 or greater
 * @since 5.10
 */
@Sealed object ScoreNormalization {

  /**
   * Returns a `ScoreNormalization` instance representing no normalization.
   *
   * @return The requested `ScoreNormalization`.
   */
  def none: ScoreNormalization = JScoreNormalization.none()

  /**
   * Returns a `ScoreNormalization` instance representing normalization via the sigmoid function.
   *
   * @return The requested `ScoreNormalization`.
   */
  def sigmoid: ScoreNormalization = JScoreNormalization.sigmoid()

  /**
   * Returns a `ScoreNormalization` instance representing min-max scaling of the scores to the range [0, 1].
   *
   * @return The requested `ScoreNormalization`.
   */
  def minMaxScaler: ScoreNormalization = JScoreNormalization.minMaxScaler()

  /**
   * Returns a `ScoreNormalization` instance representing the given normalization.
   *
   * @param normalization the normalization
   * @return The requested `ScoreNormalization`.
   */
  def of(normalization: BsonValue): ScoreNormalization = JScoreNormalization.of(normalization)
}
