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
package org.mongodb.scala.model

import com.mongodb.annotations.{ Beta, Evolving }

/**
 * Query building API for MongoDB Atlas full-text search.
 *
 * @see [[https://www.mongodb.com/docs/atlas/atlas-search/ Atlas search]]
 * @see [[https://www.mongodb.com/docs/atlas/atlas-search/query-syntax/ Atlas search aggregation pipeline stages]]
 * @since 4.6
 */
package object search {

  /**
   * The core part of the `\$search` pipeline stage of an aggregation pipeline.
   *
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/operators-and-collectors/#operators Search operators]]
   */
  @Evolving
  type SearchOperator = com.mongodb.client.model.search.SearchOperator

  /**
   * @see `SearchOperator.exists(FieldSearchPath)`
   */
  @Evolving
  type ExistsSearchOperator = com.mongodb.client.model.search.ExistsSearchOperator

  /**
   * The core part of the `\$search` pipeline stage of an aggregation pipeline.
   * [[SearchCollector]]s allow returning metadata together with the matching search results.
   * You may use the `$$SEARCH_META` variable, e.g., via [[Projections.computedSearchMeta]], to extract this metadata.
   *
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/operators-and-collectors/#collectors Search collectors]]
   */
  @Evolving
  type SearchCollector = com.mongodb.client.model.search.SearchCollector

  /**
   * @see `SearchCollector.facet(SearchOperator, Iterable)`
   */
  @Beta
  @Evolving
  type FacetSearchCollector = com.mongodb.client.model.search.FacetSearchCollector

  /**
   * This interface represents optional fields of the `\$search` pipeline stage of an aggregation pipeline.
   *
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/query-syntax/#-search \$search syntax]]
   */
  @Evolving
  type SearchOptions = com.mongodb.client.model.search.SearchOptions

  /**
   * Highlighting options.
   * You may use the `\$meta: "searchHighlights"` expression, e.g., via [[Projections.metaSearchHighlights]],
   * to extract the results of highlighting.
   *
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/highlighting/ Highlighting]]
   */
  @Evolving
  type SearchHighlight = com.mongodb.client.model.search.SearchHighlight

  /**
   * Counting options.
   * You may use the `$$SEARCH_META` variable, e.g., via [[Projections.computedSearchMeta]],
   * to extract the results of counting.
   * You may use [[Projections.computedSearchMeta]] to extract the count results.
   *
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/counting/ Counting]]
   */
  @Beta
  @Evolving
  type SearchCount = com.mongodb.client.model.search.SearchCount

  /**
   * @see `SearchCount.total()`
   */
  @Beta
  @Evolving
  type TotalSearchCount = com.mongodb.client.model.search.TotalSearchCount

  /**
   * @see `SearchCount.lowerBound()`
   */
  @Beta
  @Evolving
  type LowerBoundSearchCount = com.mongodb.client.model.search.LowerBoundSearchCount

  /**
   * A facet definition for [[FacetSearchCollector]].
   *
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/facet/#facet-definition Facet definition]]
   */
  @Beta
  @Evolving
  type SearchFacet = com.mongodb.client.model.search.SearchFacet

  /**
   * @see `SearchFacet.stringFacet(String, FieldSearchPath)`
   */
  @Beta
  @Evolving
  type StringSearchFacet = com.mongodb.client.model.search.StringSearchFacet

  /**
   * @see `SearchFacet.numberFacet(String, FieldSearchPath, Iterable)`
   */
  @Beta
  @Evolving
  type NumericSearchFacet = com.mongodb.client.model.search.NumericSearchFacet

  /**
   * @see `SearchFacet.dateFacet(String, FieldSearchPath, Iterable)`
   */
  @Beta
  @Evolving
  type DateSearchFacet = com.mongodb.client.model.search.DateSearchFacet

  /**
   * A specification of document fields to be searched.
   *
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/path-construction/ Path]]
   */
  @Evolving
  type SearchPath = com.mongodb.client.model.search.SearchPath

  /**
   * @see `SearchPath.fieldPath(String)`
   */
  @Evolving
  type FieldSearchPath = com.mongodb.client.model.search.FieldSearchPath

  /**
   * @see `SearchPath.wildcardPath(String)`
   */
  @Evolving
  type WildcardSearchPath = com.mongodb.client.model.search.WildcardSearchPath
}
