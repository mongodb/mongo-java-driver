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
 * While all the building blocks of this API, such as
 * `SearchOptions`, `SearchHighlight`, etc.,
 * are not necessary immutable, they are unmodifiable due to methods like
 * `SearchHighlight.maxCharsToExamine` returning new instances instead of modifying the instance
 * on which they are called. This allows storing and using such instances as templates.
 *
 * @see `Aggregates.search`
 * @see [[https://www.mongodb.com/docs/atlas/atlas-search/ Atlas Search]]
 * @see [[https://www.mongodb.com/docs/atlas/atlas-search/query-syntax/ Atlas Search aggregation pipeline stages]]
 * @since 4.7
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
   * A base for a [[CompoundSearchOperator]] which allows creating instances of this operator.
   * This interface is a technicality and does not represent a meaningful element of the full-text search query syntax.
   *
   * @see `SearchOperator.compound()`
   */
  @Evolving
  type CompoundSearchOperatorBase = com.mongodb.client.model.search.CompoundSearchOperatorBase

  /**
   * @see `SearchOperator.compound()`
   */
  @Evolving
  type CompoundSearchOperator = com.mongodb.client.model.search.CompoundSearchOperator

  /**
   * A representation of a [[CompoundSearchOperator]] that allows changing
   * `must`-specific options, if any.
   * This interface is a technicality and does not represent a meaningful element of the full-text search query syntax.
   *
   * @see `CompoundSearchOperatorBase.must(Iterable)`
   */
  @Evolving
  type MustCompoundSearchOperator = com.mongodb.client.model.search.MustCompoundSearchOperator

  /**
   * A representation of a [[CompoundSearchOperator]] that allows changing
   * `mustNot`-specific options, if any.
   * This interface is a technicality and does not represent a meaningful element of the full-text search query syntax.
   *
   * @see `CompoundSearchOperatorBase.mustNot(Iterable)`
   */
  @Evolving
  type MustNotCompoundSearchOperator = com.mongodb.client.model.search.MustNotCompoundSearchOperator

  /**
   * A representation of a [[CompoundSearchOperator]] that allows changing
   * `should`-specific options, if any.
   * This interface is a technicality and does not represent a meaningful element of the full-text search query syntax.
   *
   * @see `CompoundSearchOperatorBase.should(Iterable)`
   */
  @Evolving
  type ShouldCompoundSearchOperator = com.mongodb.client.model.search.ShouldCompoundSearchOperator

  /**
   * A representation of a [[CompoundSearchOperator]] that allows changing
   * `filter`-specific options, if any.
   * This interface is a technicality and does not represent a meaningful element of the full-text search query syntax.
   *
   * @see `CompoundSearchOperatorBase.filter(Iterable)`
   */
  @Evolving
  type FilterCompoundSearchOperator = com.mongodb.client.model.search.FilterCompoundSearchOperator

  /**
   * @see `SearchOperator.exists(FieldSearchPath)`
   */
  @Evolving
  type ExistsSearchOperator = com.mongodb.client.model.search.ExistsSearchOperator

  /**
   * @see `SearchOperator.text(String, SearchPath)`
   * @see `SearchOperator.text(Iterable, Iterable)`
   */
  @Evolving
  type TextSearchOperator = com.mongodb.client.model.search.TextSearchOperator

  /**
   * @see `SearchOperator.autocomplete(String, FieldSearchPath)`
   * @see `SearchOperator.autocomplete(Iterable, FieldSearchPath)`
   */
  @Evolving
  type AutocompleteSearchOperator = com.mongodb.client.model.search.AutocompleteSearchOperator

  /**
   * A base for a [[NumberRangeSearchOperatorBase]] which allows creating instances of this operator.
   * This interface is a technicality and does not represent a meaningful element of the full-text search query syntax.
   *
   * @see `SearchOperator.numberRange`
   */
  @Evolving
  type NumberRangeSearchOperatorBase = com.mongodb.client.model.search.NumberRangeSearchOperatorBase

  /**
   * A base for a [[DateRangeSearchOperatorBase]] which allows creating instances of this operator.
   * This interface is a technicality and does not represent a meaningful element of the full-text search query syntax.
   *
   * @see `SearchOperator.dateRange`
   */
  @Evolving
  type DateRangeSearchOperatorBase = com.mongodb.client.model.search.DateRangeSearchOperatorBase

  /**
   * @see `SearchOperator.numberRange`
   */
  @Evolving
  type NumberRangeSearchOperator = com.mongodb.client.model.search.NumberRangeSearchOperator

  /**
   * @see `SearchOperator.dateRange`
   */
  @Evolving
  type DateRangeSearchOperator = com.mongodb.client.model.search.DateRangeSearchOperator

  /**
   * @see `SearchOperator.near`
   */
  @Evolving
  type NumberNearSearchOperator = com.mongodb.client.model.search.NumberNearSearchOperator

  /**
   * @see `SearchOperator.near`
   */
  @Evolving
  type DateNearSearchOperator = com.mongodb.client.model.search.DateNearSearchOperator

  /**
   * @see `SearchOperator.near`
   */
  @Evolving
  type GeoNearSearchOperator = com.mongodb.client.model.search.GeoNearSearchOperator

  /**
   * Fuzzy search options that may be used with some [[SearchOperator]]s.
   *
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/autocomplete/ autocomplete operator]]
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/text/ text operator]]
   */
  @Evolving
  type SearchFuzzy = com.mongodb.client.model.search.SearchFuzzy

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
  @Evolving
  @Beta(Array(Beta.Reason.CLIENT, Beta.Reason.SERVER))
  type FacetSearchCollector = com.mongodb.client.model.search.FacetSearchCollector

  /**
   * Represents optional fields of the `\$search` pipeline stage of an aggregation pipeline.
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
  @Evolving
  @Beta(Array(Beta.Reason.CLIENT, Beta.Reason.SERVER))
  type SearchCount = com.mongodb.client.model.search.SearchCount

  /**
   * @see `SearchCount.total()`
   */
  @Evolving
  @Beta(Array(Beta.Reason.CLIENT, Beta.Reason.SERVER))
  type TotalSearchCount = com.mongodb.client.model.search.TotalSearchCount

  /**
   * @see `SearchCount.lowerBound()`
   */
  @Evolving
  @Beta(Array(Beta.Reason.CLIENT, Beta.Reason.SERVER))
  type LowerBoundSearchCount = com.mongodb.client.model.search.LowerBoundSearchCount

  /**
   * A facet definition for [[FacetSearchCollector]].
   *
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/facet/#facet-definition Facet definition]]
   */
  @Evolving
  @Beta(Array(Beta.Reason.CLIENT, Beta.Reason.SERVER))
  type SearchFacet = com.mongodb.client.model.search.SearchFacet

  /**
   * @see `SearchFacet.stringFacet(String, FieldSearchPath)`
   */
  @Evolving
  @Beta(Array(Beta.Reason.CLIENT, Beta.Reason.SERVER))
  type StringSearchFacet = com.mongodb.client.model.search.StringSearchFacet

  /**
   * @see `SearchFacet.numberFacet(String, FieldSearchPath, Iterable)`
   */
  @Evolving
  @Beta(Array(Beta.Reason.CLIENT, Beta.Reason.SERVER))
  type NumberSearchFacet = com.mongodb.client.model.search.NumberSearchFacet

  /**
   * @see `SearchFacet.dateFacet(String, FieldSearchPath, Iterable)`
   */
  @Evolving
  @Beta(Array(Beta.Reason.CLIENT, Beta.Reason.SERVER))
  type DateSearchFacet = com.mongodb.client.model.search.DateSearchFacet

  /**
   * A specification of fields to be searched.
   *
   * Despite `SearchPath` being `Bson`,
   * its value conforming to the correct syntax must be obtained via either `SearchPath.toBsonValue` or `FieldSearchPath.toValue`.
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

  /**
   * A modifier of the relevance score.
   * You may use the `\$meta: "searchScore"` expression, e.g., via [[Projections.metaSearchScore]],
   * to extract the relevance score assigned to each found document.
   *
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/scoring/ Scoring]]
   */
  @Evolving
  type SearchScore = com.mongodb.client.model.search.SearchScore

  /**
   * @see `SearchScore.boost(float)`
   */
  @Evolving
  type ValueBoostSearchScore = com.mongodb.client.model.search.ValueBoostSearchScore

  /**
   * @see `SearchScore.boost(FieldSearchPath)`
   */
  @Evolving
  type PathBoostSearchScore = com.mongodb.client.model.search.PathBoostSearchScore

  /**
   * @see `SearchScore.constant`
   */
  @Evolving
  type ConstantSearchScore = com.mongodb.client.model.search.ConstantSearchScore

  /**
   * @see `SearchScore.function`
   */
  @Evolving
  type FunctionSearchScore = com.mongodb.client.model.search.FunctionSearchScore

  /**
   * @see `SearchScore.function`
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/scoring/#expressions Expressions for the function score modifier]]
   */
  @Evolving
  type SearchScoreExpression = com.mongodb.client.model.search.SearchScoreExpression

  /**
   * @see `SearchScoreExpression.relevanceExpression`
   */
  @Evolving
  type RelevanceSearchScoreExpression = com.mongodb.client.model.search.RelevanceSearchScoreExpression

  /**
   * @see `SearchScoreExpression.pathExpression`
   */
  @Evolving
  type PathSearchScoreExpression = com.mongodb.client.model.search.PathSearchScoreExpression

  /**
   * @see `SearchScoreExpression.constantExpression`
   */
  @Evolving
  type ConstantSearchScoreExpression = com.mongodb.client.model.search.ConstantSearchScoreExpression

  /**
   * @see `SearchScoreExpression.gaussExpression`
   */
  @Evolving
  type GaussSearchScoreExpression = com.mongodb.client.model.search.GaussSearchScoreExpression

  /**
   * @see `SearchScoreExpression.log`
   */
  @Evolving
  type LogSearchScoreExpression = com.mongodb.client.model.search.LogSearchScoreExpression

  /**
   * @see `SearchScoreExpression.log1p`
   */
  @Evolving
  type Log1pSearchScoreExpression = com.mongodb.client.model.search.Log1pSearchScoreExpression

  /**
   * @see `SearchScoreExpression.addExpression`
   */
  @Evolving
  type AddSearchScoreExpression = com.mongodb.client.model.search.AddSearchScoreExpression

  /**
   * @see `SearchScoreExpression.multiplyExpression`
   */
  @Evolving
  type MultiplySearchScoreExpression = com.mongodb.client.model.search.MultiplySearchScoreExpression
}
