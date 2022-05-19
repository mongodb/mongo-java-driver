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
import com.mongodb.client.model.search.{ NumberSearchFacet, SearchFacet => JSearchFacet }
import org.mongodb.scala.bson.conversions.Bson

import java.time.Instant
import collection.JavaConverters._

/**
 * A facet definition for [[FacetSearchCollector]].
 *
 * @see [[https://www.mongodb.com/docs/atlas/atlas-search/facet/#facet-definition Facet definition]]
 * @since 4.7
 */
@Beta(Array(Beta.Reason.CLIENT, Beta.Reason.SERVER))
object SearchFacet {

  /**
   * Returns a `SearchFacet` that allows narrowing down search results based on the most frequent
   * BSON `String` values of the specified field.
   *
   * @param name The facet name.
   * @param path The field to facet on.
   * @return The requested `SearchFacet`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/facet/#string-facets String facet definition]]
   */
  def stringFacet(name: String, path: FieldSearchPath): StringSearchFacet = JSearchFacet.stringFacet(name, path)

  /**
   * Returns a `SearchFacet` that allows determining the frequency of
   * BSON `32-bit integer` / `64-bit integer` / `Double` values
   * in the search results by breaking the results into separate ranges.
   *
   * @param name The facet name.
   * @param path The path to facet on.
   * @param boundaries Bucket boundaries in ascending order. Must contain at least two boundaries.
   * @return The requested `SearchFacet`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/facet/#numeric-facets Numeric facet definition]]
   */
  def numberFacet(name: String, path: FieldSearchPath, boundaries: Iterable[Number]): NumberSearchFacet =
    JSearchFacet.numberFacet(name, path, boundaries.asJava)

  /**
   * Returns a `SearchFacet` that allows determining the frequency of BSON `Date` values
   * in the search results by breaking the results into separate ranges.
   *
   * @param name The facet name.
   * @param path The path to facet on.
   * @param boundaries Bucket boundaries in ascending order. Must contain at least two boundaries.
   * @return The requested `SearchFacet`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/facet/#date-facets Date facet definition]]
   * @see `org.bson.codecs.jsr310.InstantCodec`
   */
  def dateFacet(name: String, path: FieldSearchPath, boundaries: Iterable[Instant]): DateSearchFacet =
    JSearchFacet.dateFacet(name, path, boundaries.asJava)

  /**
   * Creates a `SearchFacet` from a `Bson` in situations when there is no builder method that better satisfies your needs.
   * This method cannot be used to validate the syntax.
   *
   * <i>Example</i><br>
   * The following code creates two functionally equivalent `SearchFacet`s,
   * though they may not be equal.
   * {{{
   *  val facet1: SearchFacet = SearchFacet.stringFacet("facetName",
   *    SearchPath.fieldPath("fieldName"))
   *  val facet2: SearchFacet = SearchFacet.of(Document("facetName" -> Document("type" -> "string",
   *    "path" -> SearchPath.fieldPath("fieldName").toValue)))
   * }}}
   *
   * @param facet A `Bson` representing the required `SearchFacet`.
   *
   * @return The requested `SearchFacet`.
   */
  def of(facet: Bson): SearchFacet = JSearchFacet.of(facet)

  /**
   * Combines `SearchFacet`s into a `Bson`.
   *
   * This method may be useful when using [[SearchCollector.of]].
   *
   * @param facets The non-empty facet definitions to combine.
   * @return A `Bson` representing combined `facets`.
   */
  def combineToBson(facets: Iterable[_ <: SearchFacet]): Bson =
    JSearchFacet.combineToBson(facets.asJava)
}
