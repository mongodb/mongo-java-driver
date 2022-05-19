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
import com.mongodb.client.model.search.{ SearchOptions => JSearchOptions }

/**
 * Represents optional fields of the `\$search` pipeline stage of an aggregation pipeline.
 *
 * @see [[https://www.mongodb.com/docs/atlas/atlas-search/query-syntax/#-search \$search syntax]]
 * @since 4.7
 */
@Beta(Array(Beta.Reason.CLIENT))
object SearchOptions {

  /**
   * Returns `SearchOptions` that represents server defaults.
   *
   * @return `SearchOptions` that represents server defaults.
   */
  def defaultSearchOptions(): SearchOptions = JSearchOptions.defaultSearchOptions()
}
