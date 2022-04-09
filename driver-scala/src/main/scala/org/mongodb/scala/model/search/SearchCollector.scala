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
import com.mongodb.client.model.search.{ SearchCollector => JSearchCollector }
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.Projections

import scala.collection.JavaConverters._

/**
 * The core part of the `\$search` pipeline stage of an aggregation pipeline.
 * `SearchCollector`s allow returning metadata together with the matching search results.
 * You may use the `$$SEARCH_META` variable, e.g., via [[Projections.computedSearchMeta]], to extract this metadata.
 *
 * @see [[https://www.mongodb.com/docs/atlas/atlas-search/operators-and-collectors/#collectors Search collectors]]
 * @since 4.6
 */
object SearchCollector {

  /**
   * Returns a `SearchCollector` that groups results by values or ranges in the specified faceted fields and returns the count
   * for each of those groups.
   *
   * @param operator A search operator to use.
   * @param facets Non-empty facet definitions.
   * @return The requested `SearchCollector`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/facet/ facet collector]]
   */
  @Beta
  def facet(operator: SearchOperator, facets: Iterable[_ <: SearchFacet]): FacetSearchCollector =
    JSearchCollector.facet(operator, facets.asJava)

  /**
   * Creates a `SearchCollector` from a `Bson` in situations when there is no builder method that better satisfies your needs.
   * This method cannot be used to validate the syntax.
   *
   * <i>Example</i><br>
   * The following code creates two functionally equivalent `SearchCollector`s,
   * though they may not be equal.
   * {{{
   *  val collector1: SearchCollector = SearchCollector.facet(
   *    SearchOperator.exists(
   *      SearchPath.fieldPath("fieldName")),
   *    Seq(
   *      SearchFacet.stringFacet(
   *        "stringFacetName",
   *        SearchPath.fieldPath("stringFieldName")),
   *      SearchFacet.numberFacet(
   *        "numberFacetName",
   *        SearchPath.fieldPath("numberFieldName"),
   *        Seq(10, 20, 30))))
   *  val collector2: SearchCollector = SearchCollector.of(Document("facet" ->
   *    Document("operator" -> SearchOperator.exists(
   *      SearchPath.fieldPath("fieldName")).toBsonDocument,
   *      "facets" -> SearchFacet.combineToBson(Seq(
   *        SearchFacet.stringFacet(
   *          "stringFacetName",
   *          SearchPath.fieldPath("stringFieldName")),
   *        SearchFacet.numberFacet(
   *          "numberFacetName",
   *          SearchPath.fieldPath("numberFieldName"),
   *          Seq(10, 20, 30)))).toBsonDocument)))
   * }}}
   *
   * @param collector A `Bson` representing the required `SearchCollector`.
   *
   * @return The requested `SearchCollector`.
   */
  def of(collector: Bson): SearchCollector = JSearchCollector.of(collector)
}
