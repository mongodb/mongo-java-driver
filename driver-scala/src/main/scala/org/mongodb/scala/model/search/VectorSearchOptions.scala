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
import com.mongodb.client.model.search.{ VectorSearchOptions => JVectorSearchOptions }

/**
 * Represents optional fields of the `\$vectorSearch` pipeline stage of an aggregation pipeline.
 *
 * @see [[https://www.mongodb.com/docs/atlas/atlas-vector-search/vector-search-stage/ \$vectorSearch]]
 * @note Requires MongoDB 6.0.10 or greater
 * @since 4.11
 */
@Beta(Array(Beta.Reason.SERVER))
object VectorSearchOptions {

  /**
   * Returns `VectorSearchOptions` that represents server defaults.
   *
   * @return `VectorSearchOptions` that represents server defaults.
   */
  def vectorSearchOptions(): VectorSearchOptions = JVectorSearchOptions.vectorSearchOptions()
}
