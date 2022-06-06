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

import com.mongodb.annotations.Evolving

/**
 * @see `Aggregates.densify`
 * @note Requires MongoDB 5.1 or greater.
 * @since 4.7
 */
package object densify {

  /**
   * A specification of how to compute the missing field values
   * for which new documents must be added. It specifies a half-closed interval of values with the lower bound being inclusive, and a step.
   * The first potentially missing value within each interval is its lower bound, other values are computed by adding the step
   * multiple times, until the result is out of the interval. Each time the step is added, the result is a potentially missing value for
   * which a new document must be added if the sequence of documents that is being densified does not have a document
   * with equal value of the field.
   *
   * @see `Aggregates.densify`
   */
  @Evolving
  type DensifyRange = com.mongodb.client.model.densify.DensifyRange

  /**
   * @see `DensifyRange.fullRangeWithStep`
   * @see `DensifyRange.partitionRangeWithStep`
   * @see `DensifyRange.rangeWithStep`
   */
  @Evolving
  type NumberDensifyRange = com.mongodb.client.model.densify.NumberDensifyRange

  /**
   * @see `DensifyRange.fullRangeWithStep`
   * @see `DensifyRange.partitionRangeWithStep`
   * @see `DensifyRange.rangeWithStep`
   */
  @Evolving
  type DateDensifyRange = com.mongodb.client.model.densify.DateDensifyRange

  /**
   * Represents optional fields of the `\$densify` pipeline stage of an aggregation pipeline.
   *
   * @see `Aggregates.densify`
   */
  @Evolving
  type DensifyOptions = com.mongodb.client.model.densify.DensifyOptions
}
