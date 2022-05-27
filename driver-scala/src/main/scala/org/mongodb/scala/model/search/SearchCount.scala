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
import com.mongodb.client.model.search.{ SearchCount => JSearchCount }
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.Projections

/**
 * Counting options.
 * You may use the `$$SEARCH_META` variable, e.g., via [[Projections.computedSearchMeta]],
 * to extract the results of counting.
 *
 * @see [[https://www.mongodb.com/docs/atlas/atlas-search/counting/ Counting]]
 * @since 4.7
 */
@Beta(Array(Beta.Reason.CLIENT, Beta.Reason.SERVER))
object SearchCount {

  /**
   * Returns a `SearchCount` that instructs to count documents exactly.
   *
   * @return The requested `SearchCount`.
   */
  def total(): TotalSearchCount = JSearchCount.total()

  /**
   * Returns a `SearchCount` that instructs to count documents exactly only up to
   * `LowerBoundSearchCount.threshold`.
   *
   * @return The requested `SearchCount`.
   */
  def lowerBound(): LowerBoundSearchCount = JSearchCount.lowerBound()

  /**
   * Creates a `SearchCount` from a `Bson` in situations when there is no builder method that better satisfies your needs.
   * This method cannot be used to validate the syntax.
   *
   * <i>Example</i><br>
   * The following code creates two functionally equivalent `SearchCount`s,
   * though they may not be equal.
   * {{{
   *  val count1: SearchCount = SearchCount.lowerBound()
   *  val count2: SearchCount = SearchCount.of(Document("type" -> "lowerBound"))
   * }}}
   *
   * @param count A `Bson` representing the required `SearchCount`.
   *
   * @return The requested `SearchCount`.
   */
  def of(count: Bson): SearchCount = JSearchCount.of(count)
}
