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

import com.mongodb.client.model.search.{ SearchFuzzy => JSearchFuzzy }
import org.mongodb.scala.bson.conversions.Bson

/**
 * Fuzzy search options that may be used with some `SearchOperator`s.
 *
 * @see [[https://www.mongodb.com/docs/atlas/atlas-search/autocomplete/ autocomplete operator]]
 * @see [[https://www.mongodb.com/docs/atlas/atlas-search/text/ text operator]]
 * @since 4.7
 */
object SearchFuzzy {

  /**
   * Creates a `SearchFuzzy` from a `Bson` in situations when there is no builder method that better satisfies your needs.
   * This method cannot be used to validate the syntax.
   *
   * <i>Example</i><br>
   * The following code creates two functionally equivalent `SearchFuzzy`s,
   * though they may not be equal.
   * {{{
   *  val fuzzy1: SearchFuzzy = SearchFuzzy.defaultSearchFuzzy().maxEdits(1)
   *  val fuzzy2: SearchFuzzy = SearchFuzzy.of(Document("maxEdits" -> 1))
   * }}}
   *
   * @param fuzzy A `Bson` representing the required `SearchFuzzy`.
   *
   * @return The requested `SearchFuzzy`.
   */
  def of(fuzzy: Bson): SearchFuzzy = JSearchFuzzy.of(fuzzy)

  /**
   * Returns `SearchFuzzy` that represents server defaults.
   *
   * @return `SearchFuzzy` that represents server defaults.
   */
  def defaultSearchFuzzy(): SearchFuzzy = JSearchFuzzy.defaultSearchFuzzy()
}
