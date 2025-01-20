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

import com.mongodb.annotations.{ Beta, Reason, Sealed }

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
 * @see `Aggregates.vectorSearch`
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
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type SearchOperator = com.mongodb.client.model.search.SearchOperator

  /**
   * A base for a [[CompoundSearchOperator]] which allows creating instances of this operator.
   * This interface is a technicality and does not represent a meaningful element of the full-text search query syntax.
   *
   * @see `SearchOperator.compound()`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type CompoundSearchOperatorBase = com.mongodb.client.model.search.CompoundSearchOperatorBase

  /**
   * @see `SearchOperator.compound()`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type CompoundSearchOperator = com.mongodb.client.model.search.CompoundSearchOperator

  /**
   * A representation of a [[CompoundSearchOperator]] that allows changing
   * `must`-specific options, if any.
   * This interface is a technicality and does not represent a meaningful element of the full-text search query syntax.
   *
   * @see `CompoundSearchOperatorBase.must(Iterable)`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type MustCompoundSearchOperator = com.mongodb.client.model.search.MustCompoundSearchOperator

  /**
   * A representation of a [[CompoundSearchOperator]] that allows changing
   * `mustNot`-specific options, if any.
   * This interface is a technicality and does not represent a meaningful element of the full-text search query syntax.
   *
   * @see `CompoundSearchOperatorBase.mustNot(Iterable)`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type MustNotCompoundSearchOperator = com.mongodb.client.model.search.MustNotCompoundSearchOperator

  /**
   * A representation of a [[CompoundSearchOperator]] that allows changing
   * `should`-specific options, if any.
   * This interface is a technicality and does not represent a meaningful element of the full-text search query syntax.
   *
   * @see `CompoundSearchOperatorBase.should(Iterable)`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type ShouldCompoundSearchOperator = com.mongodb.client.model.search.ShouldCompoundSearchOperator

  /**
   * A representation of a [[CompoundSearchOperator]] that allows changing
   * `filter`-specific options, if any.
   * This interface is a technicality and does not represent a meaningful element of the full-text search query syntax.
   *
   * @see `CompoundSearchOperatorBase.filter(Iterable)`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type FilterCompoundSearchOperator = com.mongodb.client.model.search.FilterCompoundSearchOperator

  /**
   * @see `SearchOperator.exists(FieldSearchPath)`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type ExistsSearchOperator = com.mongodb.client.model.search.ExistsSearchOperator

  /**
   * @see `SearchOperator.text(String, SearchPath)`
   * @see `SearchOperator.text(Iterable, Iterable)`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type TextSearchOperator = com.mongodb.client.model.search.TextSearchOperator

  /**
   * @see `SearchOperator.phrase(String, SearchPath)`
   * @see `SearchOperator.phrase(Iterable, Iterable)`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type PhraseSearchOperator = com.mongodb.client.model.search.PhraseSearchOperator

  /**
   * @see `SearchOperator.autocomplete(String, FieldSearchPath)`
   * @see `SearchOperator.autocomplete(Iterable, FieldSearchPath)`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type AutocompleteSearchOperator = com.mongodb.client.model.search.AutocompleteSearchOperator

  /**
   * @see `SearchOperator.regex(String, SearchPath)`
   * @see `SearchOperator.regex(Iterable, Iterable)`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type RegexSearchOperator = com.mongodb.client.model.search.RegexSearchOperator

  /**
   * A base for a [[NumberRangeSearchOperatorBase]] which allows creating instances of this operator.
   * This interface is a technicality and does not represent a meaningful element of the full-text search query syntax.
   *
   * @see `SearchOperator.numberRange`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type NumberRangeSearchOperatorBase = com.mongodb.client.model.search.NumberRangeSearchOperatorBase

  /**
   * A base for a [[DateRangeSearchOperatorBase]] which allows creating instances of this operator.
   * This interface is a technicality and does not represent a meaningful element of the full-text search query syntax.
   *
   * @see `SearchOperator.dateRange`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type DateRangeSearchOperatorBase = com.mongodb.client.model.search.DateRangeSearchOperatorBase

  /**
   * @see `SearchOperator.numberRange`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type NumberRangeSearchOperator = com.mongodb.client.model.search.NumberRangeSearchOperator

  /**
   * @see `SearchOperator.dateRange`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type DateRangeSearchOperator = com.mongodb.client.model.search.DateRangeSearchOperator

  /**
   * @see `SearchOperator.near`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type NumberNearSearchOperator = com.mongodb.client.model.search.NumberNearSearchOperator

  /**
   * @see `SearchOperator.near`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type DateNearSearchOperator = com.mongodb.client.model.search.DateNearSearchOperator

  /**
   * @see `SearchOperator.near`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type GeoNearSearchOperator = com.mongodb.client.model.search.GeoNearSearchOperator

  /**
   * @see `SearchOperator.equals`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type EqualsSearchOperator = com.mongodb.client.model.search.EqualsSearchOperator

  /**
   * @see `SearchOperator.moreLikeThis`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type MoreLikeThisSearchOperator = com.mongodb.client.model.search.MoreLikeThisSearchOperator

  /**
   * @see `SearchOperator.wildcard(String, SearchPath)`
   * @see `SearchOperator.wildcard(Iterable, Iterable)`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type WildcardSearchOperator = com.mongodb.client.model.search.WildcardSearchOperator

  /**
   * @see `SearchOperator.queryString`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type QueryStringSearchOperator = com.mongodb.client.model.search.QueryStringSearchOperator

  /**
   * Fuzzy search options that may be used with some [[SearchOperator]]s.
   *
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/autocomplete/ autocomplete operator]]
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/text/ text operator]]
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type FuzzySearchOptions = com.mongodb.client.model.search.FuzzySearchOptions

  /**
   * The core part of the `\$search` pipeline stage of an aggregation pipeline.
   * [[SearchCollector]]s allow returning metadata together with the matching search results.
   * You may use the `$$SEARCH_META` variable, e.g., via [[Projections.computedSearchMeta]], to extract this metadata.
   *
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/operators-and-collectors/#collectors Search collectors]]
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type SearchCollector = com.mongodb.client.model.search.SearchCollector

  /**
   * @see `SearchCollector.facet(SearchOperator, Iterable)`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT, Reason.SERVER))
  type FacetSearchCollector = com.mongodb.client.model.search.FacetSearchCollector

  /**
   * Represents optional fields of the `\$search` pipeline stage of an aggregation pipeline.
   *
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/query-syntax/#-search \$search syntax]]
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type SearchOptions = com.mongodb.client.model.search.SearchOptions

  /**
   * Represents optional fields of the `\$vectorSearch` pipeline stage of an aggregation pipeline.
   *
   * @see [[https://www.mongodb.com/docs/atlas/atlas-vector-search/vector-search-stage/ \$vectorSearch]]
   * @note Requires MongoDB 6.0.11 or greater
   * @since 4.11
   */
  @Sealed
  @Beta(Array(Reason.SERVER))
  type VectorSearchOptions = com.mongodb.client.model.search.VectorSearchOptions

  /**
   * Represents optional fields of the `\$vectorSearch` pipeline stage of an aggregation pipeline.
   * <p>
   * Configures approximate vector search for Atlas Vector Search to enable searches that may not return the exact closest vectors.
   *
   * @see [[https://www.mongodb.com/docs/atlas/atlas-vector-search/vector-search-stage/ \$vectorSearch]]
   * @note Requires MongoDB 6.0.11, 7.0.2 or greater
   * @since 5.2
   */
  @Sealed
  @Beta(Array(Reason.SERVER))
  type ApproximateVectorSearchOptions = com.mongodb.client.model.search.ApproximateVectorSearchOptions

  /**
   * Represents optional fields of the `\$vectorSearch` pipeline stage of an aggregation pipeline.
   * <p>
   * Configures exact vector search for Atlas Vector Search to enable precise matching, ensuring that
   * results are the closest vectors to a given query vector.
   *
   * @see [[https://www.mongodb.com/docs/atlas/atlas-vector-search/vector-search-stage/ \$vectorSearch]]
   * @note Requires MongoDB 6.0.16, 7.0.10, 7.3.2 or greater
   * @since 5.2
   */
  @Sealed
  @Beta(Array(Reason.SERVER))
  type ExactVectorSearchOptions = com.mongodb.client.model.search.ExactVectorSearchOptions

  /**
   * Highlighting options.
   * You may use the `\$meta: "searchHighlights"` expression, e.g., via [[Projections.metaSearchHighlights]],
   * to extract the results of highlighting.
   *
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/highlighting/ Highlighting]]
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type SearchHighlight = com.mongodb.client.model.search.SearchHighlight

  /**
   * Counting options.
   * You may use the `$$SEARCH_META` variable, e.g., via [[Projections.computedSearchMeta]],
   * to extract the results of counting.
   * You may use [[Projections.computedSearchMeta]] to extract the count results.
   *
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/counting/ Counting]]
   */
  @Sealed
  @Beta(Array(Reason.CLIENT, Reason.SERVER))
  type SearchCount = com.mongodb.client.model.search.SearchCount

  /**
   * @see `SearchCount.total()`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT, Reason.SERVER))
  type TotalSearchCount = com.mongodb.client.model.search.TotalSearchCount

  /**
   * @see `SearchCount.lowerBound()`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT, Reason.SERVER))
  type LowerBoundSearchCount = com.mongodb.client.model.search.LowerBoundSearchCount

  /**
   * A facet definition for [[FacetSearchCollector]].
   *
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/facet/#facet-definition Facet definition]]
   */
  @Sealed
  @Beta(Array(Reason.CLIENT, Reason.SERVER))
  type SearchFacet = com.mongodb.client.model.search.SearchFacet

  /**
   * @see `SearchFacet.stringFacet(String, FieldSearchPath)`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT, Reason.SERVER))
  type StringSearchFacet = com.mongodb.client.model.search.StringSearchFacet

  /**
   * @see `SearchFacet.numberFacet(String, FieldSearchPath, Iterable)`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT, Reason.SERVER))
  type NumberSearchFacet = com.mongodb.client.model.search.NumberSearchFacet

  /**
   * @see `SearchFacet.dateFacet(String, FieldSearchPath, Iterable)`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT, Reason.SERVER))
  type DateSearchFacet = com.mongodb.client.model.search.DateSearchFacet

  /**
   * A specification of fields to be searched.
   *
   * Despite `SearchPath` being `Bson`,
   * its value conforming to the correct syntax must be obtained via either `SearchPath.toBsonValue` or `FieldSearchPath.toValue`.
   *
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/path-construction/ Path]]
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type SearchPath = com.mongodb.client.model.search.SearchPath

  /**
   * @see `SearchPath.fieldPath(String)`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type FieldSearchPath = com.mongodb.client.model.search.FieldSearchPath

  /**
   * @see `SearchPath.wildcardPath(String)`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type WildcardSearchPath = com.mongodb.client.model.search.WildcardSearchPath

  /**
   * A modifier of the relevance score.
   * You may use the `\$meta: "searchScore"` expression, e.g., via [[Projections.metaSearchScore]],
   * to extract the relevance score assigned to each found document.
   *
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/scoring/ Scoring]]
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type SearchScore = com.mongodb.client.model.search.SearchScore

  /**
   * @see `SearchScore.boost(float)`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type ValueBoostSearchScore = com.mongodb.client.model.search.ValueBoostSearchScore

  /**
   * @see `SearchScore.boost(FieldSearchPath)`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type PathBoostSearchScore = com.mongodb.client.model.search.PathBoostSearchScore

  /**
   * @see `SearchScore.constant`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type ConstantSearchScore = com.mongodb.client.model.search.ConstantSearchScore

  /**
   * @see `SearchScore.function`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type FunctionSearchScore = com.mongodb.client.model.search.FunctionSearchScore

  /**
   * @see `SearchScore.function`
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/scoring/#expressions Expressions for the function score modifier]]
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type SearchScoreExpression = com.mongodb.client.model.search.SearchScoreExpression

  /**
   * @see `SearchScoreExpression.relevanceExpression`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type RelevanceSearchScoreExpression = com.mongodb.client.model.search.RelevanceSearchScoreExpression

  /**
   * @see `SearchScoreExpression.pathExpression`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type PathSearchScoreExpression = com.mongodb.client.model.search.PathSearchScoreExpression

  /**
   * @see `SearchScoreExpression.constantExpression`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type ConstantSearchScoreExpression = com.mongodb.client.model.search.ConstantSearchScoreExpression

  /**
   * @see `SearchScoreExpression.gaussExpression`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type GaussSearchScoreExpression = com.mongodb.client.model.search.GaussSearchScoreExpression

  /**
   * @see `SearchScoreExpression.log`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type LogSearchScoreExpression = com.mongodb.client.model.search.LogSearchScoreExpression

  /**
   * @see `SearchScoreExpression.log1p`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type Log1pSearchScoreExpression = com.mongodb.client.model.search.Log1pSearchScoreExpression

  /**
   * @see `SearchScoreExpression.addExpression`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type AddSearchScoreExpression = com.mongodb.client.model.search.AddSearchScoreExpression

  /**
   * @see `SearchScoreExpression.multiplyExpression`
   */
  @Sealed
  @Beta(Array(Reason.CLIENT))
  type MultiplySearchScoreExpression = com.mongodb.client.model.search.MultiplySearchScoreExpression
}
