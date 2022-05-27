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
import com.mongodb.client.model.search.{ SearchHighlight => JSearchHighlight }
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.Projections

import collection.JavaConverters._

/**
 * Highlighting options.
 * You may use the `\$meta: "searchHighlights"` expression, e.g., via [[Projections.metaSearchHighlights]],
 * to extract the results of highlighting.
 *
 * @see [[https://www.mongodb.com/docs/atlas/atlas-search/highlighting/ Highlighting]]
 * @since 4.7
 */
@Beta(Array(Beta.Reason.CLIENT))
object SearchHighlight {

  /**
   * Returns a `SearchHighlight` for the given `path`.
   *
   * @param path The field to be searched.
   * @return The requested `SearchHighlight`.
   */
  def path(path: SearchPath): SearchHighlight = JSearchHighlight.path(path)

  /**
   * Returns a `SearchHighlight` for the given `paths`.
   *
   * @param paths The non-empty fields to be searched.
   * @return The requested `SearchHighlight`.
   */
  def paths(paths: Iterable[_ <: SearchPath]): SearchHighlight = JSearchHighlight.paths(paths.asJava)

  /**
   * Creates a `SearchHighlight` from a `Bson` in situations when there is no builder method that better satisfies your needs.
   * This method cannot be used to validate the syntax.
   *
   * <i>Example</i><br>
   * The following code creates two functionally equivalent `SearchHighlight`s,
   * though they may not be equal.
   * {{{
   *  val highlight1: SearchHighlight = SearchHighlight.paths(Seq(
   *    SearchPath.fieldPath("fieldName"),
   *    SearchPath.wildcardPath("wildc*rd")))
   *  val highlight2: SearchHighlight = SearchHighlight.of(Document("path" -> Seq(
   *    SearchPath.fieldPath("fieldName").toBsonValue,
   *    SearchPath.wildcardPath("wildc*rd").toBsonValue)))
   * }}}
   *
   * @param highlight A `Bson` representing the required `SearchHighlight`.
   *
   * @return The requested `SearchHighlight`.
   */
  def of(highlight: Bson): SearchHighlight = JSearchHighlight.of(highlight)
}
