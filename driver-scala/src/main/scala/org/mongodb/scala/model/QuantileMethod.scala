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
import com.mongodb.client.model.{ QuantileMethod => JQuantileMethod }

/**
 * This interface represents a quantile method used in quantile accumulators of the `$group` and
 * `$setWindowFields` stages.
 * <p>
 * It provides methods for creating and converting quantile methods to `BsonValue`.
 * </p>
 *
 * @see [[org.mongodb.scala.model.Accumulators.percentile]]
 * @see [[org.mongodb.scala.model.Accumulators.median]]
 * @see [[org.mongodb.scala.model.WindowOutputFields.percentile]]
 * @see [[org.mongodb.scala.model.WindowOutputFields.median]]
 * @since 4.10
 * @note Requires MongoDB 7.0 or greater
 */
@Sealed object QuantileMethod {

  /**
   * Returns a `QuantileMethod` instance representing the "approximate" quantile method.
   *
   * @return The requested `QuantileMethod`.
   */
  def approximate: ApproximateQuantileMethod = JQuantileMethod.approximate()
}
