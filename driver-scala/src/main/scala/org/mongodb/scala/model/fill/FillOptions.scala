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

import com.mongodb.client.model.fill.{ FillOptions => JFillOptions }

/**
 * Represents optional fields of the `\$fill` pipeline stage of an aggregation pipeline.
 *
 * @see `Aggregates.fill`
 * @note Requires MongoDB 5.3 or greater.
 * @since 4.7
 */
object FillOptions {

  /**
   * Returns `FillOptions` that represents server defaults.
   *
   * @return `FillOptions` that represents server defaults.
   */
  def fillOptions(): FillOptions = JFillOptions.fillOptions()
}
