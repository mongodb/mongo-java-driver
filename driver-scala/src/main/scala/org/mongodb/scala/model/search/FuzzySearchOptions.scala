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
import com.mongodb.client.model.search.{ FuzzySearchOptions => JFuzzySearchOptions }

/**
 * Fuzzy search options that may be used with some `SearchOperator`s.
 *
 * @see [[https://www.mongodb.com/docs/atlas/atlas-search/autocomplete/ autocomplete operator]]
 * @see [[https://www.mongodb.com/docs/atlas/atlas-search/text/ text operator]]
 * @since 4.7
 */
@Beta(Array(Beta.Reason.CLIENT))
object FuzzySearchOptions {

  /**
   * Returns `FuzzySearchOptions` that represents server defaults.
   *
   * @return `FuzzySearchOptions` that represents server defaults.
   */
  def fuzzySearchOptions(): FuzzySearchOptions = JFuzzySearchOptions.fuzzySearchOptions()
}
