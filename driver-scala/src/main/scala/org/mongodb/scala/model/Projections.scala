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

import scala.collection.JavaConverters._

import com.mongodb.client.model.{ Projections => JProjections }

import org.mongodb.scala.bson.conversions.Bson

/**
 * A factory for projections. A convenient way to use this class is to statically import all of its methods, which allows usage like:
 *
 * `collection.find().projection(fields(include("x", "y"), excludeId()))`
 *
 * @since 1.0
 */
object Projections {

  /**
   * Creates a projection of a field whose value is computed from the given expression. Projection with an expression can be used in the
   * following contexts:
   * <ul>
   *    <li>\$project aggregation pipeline stage.</li>
   *    <li>Starting from MongoDB 4.4, it's also accepted in various find-related methods within the
   * `MongoCollection`-based API where projection is supported, for example:
   *        <ul>
   *          <li>`find()`</li>
   *          <li>`findOneAndReplace()`</li>
   *          <li>`findOneAndUpdate()`</li>
   *          <li>`findOneAndDelete()`</li>
   *        </ul>
   *    </li>
   * </ul>
   *
   * @param fieldName     the field name
   * @param  expression   the expression
   * @tparam TExpression  the expression type
   * @return the projection
   * @see [[Projections.computedSearchMeta]]
   * @see Aggregates#project(Bson)
   */
  def computed[TExpression](fieldName: String, expression: TExpression): Bson =
    JProjections.computed(fieldName, expression)

  /**
   * Creates a projection of a field whose value is equal to the `$$SEARCH_META` variable,
   * for use with `Aggregates.search(SearchOperator, SearchOptions)` / `Aggregates.search(SearchCollector, SearchOptions)`.
   * Calling this method is equivalent to calling [[Projections.computed]] with `"$$SEARCH_META"` as the second argument.
   *
   * @param fieldName the field name
   * @return the projection
   * @see [[org.mongodb.scala.model.search.SearchCount]]
   * @see [[org.mongodb.scala.model.search.SearchCollector]]
   */
  def computedSearchMeta(fieldName: String): Bson =
    JProjections.computedSearchMeta(fieldName)

  /**
   * Creates a projection that includes all of the given fields.
   *
   * @param fieldNames the field names
   * @return the projection
   */
  def include(fieldNames: String*): Bson = JProjections.include(fieldNames.asJava)

  /**
   * Creates a projection that excludes all of the given fields.
   *
   * @param fieldNames the field names
   * @return the projection
   */
  def exclude(fieldNames: String*): Bson = JProjections.exclude(fieldNames.asJava)

  /**
   * Creates a projection that excludes the _id field.  This suppresses the automatic inclusion of _id that is the default, even when
   * other fields are explicitly included.
   *
   * @return the projection
   */
  def excludeId(): Bson = JProjections.excludeId

  /**
   * Creates a projection that includes for the given field only the first element of an array that matches the query filter.  This is
   * referred to as the positional `\$` operator.
   *
   * @param fieldName the field name whose value is the array
   * @return the projection
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/projection/positional/#projection Project the first matching element (\$ operator)]]
   */
  def elemMatch(fieldName: String): Bson = JProjections.elemMatch(fieldName)

  /**
   * Creates a projection that includes for the given field only the first element of the array value of that field that matches the given
   * query filter.
   *
   * @param fieldName the field name
   * @param filter    the filter to apply
   * @return the projection
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/projection/elemMatch elemMatch]]
   */
  def elemMatch(fieldName: String, filter: Bson): Bson = JProjections.elemMatch(fieldName, filter)

  /**
   * Creates a `\$meta` projection to the given field name for the given meta field name.
   *
   * @param fieldName the field name
   * @param metaFieldName the meta field name
   * @return the projection
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/meta/ meta]]
   * @see [[Projections.metaTextScore]]
   * @see [[Projections.metaSearchScore]]
   * @see [[Projections.metaVectorSearchScore]]
   * @see [[Projections.metaSearchHighlights]]
   * @since 4.1
   */
  def meta(fieldName: String, metaFieldName: String): Bson = JProjections.meta(fieldName, metaFieldName)

  /**
   * Creates a projection to the given field name of the textScore, for use with text queries.
   * Calling this method is equivalent to calling [[Projections.meta]] with `"textScore"` as the second argument.
   *
   * @param fieldName the field name
   * @return the projection
   * @see `Filters.text(String, TextSearchOptions)`
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/meta/#text-score-metadata--meta---textscore- textScore]]
   */
  def metaTextScore(fieldName: String): Bson = JProjections.metaTextScore(fieldName)

  /**
   * Creates a projection to the given field name of the searchScore,
   * for use with `Aggregates.search(SearchOperator, SearchOptions)` / `Aggregates.search(SearchCollector, SearchOptions)`.
   * Calling this method is equivalent to calling [[Projections.meta]] with `"searchScore"` as the second argument.
   *
   * @param fieldName the field name
   * @return the projection
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/scoring/ Scoring]]
   */
  def metaSearchScore(fieldName: String): Bson = JProjections.metaSearchScore(fieldName)

  /**
   * Creates a projection to the given field name of the vectorSearchScore,
   * for use with `Aggregates.vectorSearch(FieldSearchPath, Iterable, String, Long, Long, VectorSearchOptions)`.
   * Calling this method is equivalent to calling [[Projections.meta]] with `"vectorSearchScore"` as the second argument.
   *
   * @param fieldName the field name
   * @return the projection
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/scoring/ Scoring]]
   * @note Requires MongoDB 6.0.10 or greater
   * @since 4.11
   */
  def metaVectorSearchScore(fieldName: String): Bson = JProjections.metaVectorSearchScore(fieldName)

  /**
   * Creates a projection to the given field name of the searchHighlights,
   * for use with `Aggregates.search(SearchOperator, SearchOptions)` / `Aggregates.search(SearchCollector, SearchOptions)`.
   * Calling this method is equivalent to calling [[Projections.meta]] with `"searchHighlights"` as the second argument.
   *
   * @param fieldName the field name
   * @return the projection
   * @see [[org.mongodb.scala.model.search.SearchHighlight]]
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/highlighting/ Highlighting]]
   */
  def metaSearchHighlights(fieldName: String): Bson = JProjections.metaSearchHighlights(fieldName)

  /**
   * Creates a projection to the given field name of a slice of the array value of that field.
   *
   * @param fieldName the field name
   * @param limit the number of elements to project.
   * @return the projection
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/projection/slice Slice]]
   */
  def slice(fieldName: String, limit: Int): Bson = JProjections.slice(fieldName, limit)

  /**
   * Creates a projection to the given field name of a slice of the array value of that field.
   *
   * @param fieldName the field name
   * @param skip the number of elements to skip before applying the limit
   * @param limit the number of elements to project
   * @return the projection
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/projection/slice Slice]]
   */
  def slice(fieldName: String, skip: Int, limit: Int): Bson = JProjections.slice(fieldName, skip, limit)

  /**
   * Creates a projection that combines the list of projections into a single one.  If there are duplicate keys, the last one takes
   * precedence.
   *
   * @param projections the list of projections to combine
   * @return the combined projection
   */
  def fields(projections: Bson*): Bson = JProjections.fields(projections.asJava)
}
