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

import com.mongodb.client.model.search.{ VectorSearchNestedOptions => JVectorSearchNestedOptions }

/**
 * Represents the optional `\$vectorSearch` `nestedOptions` sub-document,
 * used when searching against arrays of embeddings within nested (embedded) documents.
 *
 * @see [[https://www.mongodb.com/docs/atlas/atlas-vector-search/vector-search-stage/ \$vectorSearch]]
 * @since 5.10
 */
object VectorSearchNestedOptions {

  /**
   * Returns `VectorSearchNestedOptions` that represents server defaults.
   *
   * @return `VectorSearchNestedOptions` that represents server defaults.
   */
  def vectorSearchNestedOptions(): VectorSearchNestedOptions = JVectorSearchNestedOptions.vectorSearchNestedOptions()
}
