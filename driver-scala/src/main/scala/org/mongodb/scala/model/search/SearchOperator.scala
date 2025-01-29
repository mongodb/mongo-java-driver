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

import com.mongodb.annotations.{ Beta, Reason }
import com.mongodb.client.model.search.{ SearchOperator => JSearchOperator }

import java.util.UUID

import org.mongodb.scala.bson.BsonDocument
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.geojson.Point

import org.bson.types.ObjectId;

import java.time.{ Duration, Instant }

import collection.JavaConverters._

/**
 * The core part of the `\$search` pipeline stage of an aggregation pipeline.
 *
 * @see [[https://www.mongodb.com/docs/atlas/atlas-search/operators-and-collectors/#operators Search operators]]
 * @since 4.7
 */
@Beta(Array(Reason.CLIENT))
object SearchOperator {

  /**
   * Returns a base for a `SearchOperator` that may combine multiple `SearchOperator`s.
   * Combining `SearchOperator`s affects calculation of the relevance score.
   *
   * @return A base for a `CompoundSearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/compound/ compound operator]]
   */
  def compound(): CompoundSearchOperatorBase = JSearchOperator.compound()

  /**
   * Returns a `SearchOperator` that tests if the `path` exists in a document.
   *
   * @param path The path to test.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/exists/ exists operator]]
   */
  def exists(path: FieldSearchPath): ExistsSearchOperator = JSearchOperator.exists(path)

  /**
   * Returns a `SearchOperator` that performs a full-text search.
   *
   * @param path The field to be searched.
   * @param query The string to search for.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/text/ text operator]]
   */
  def text(path: SearchPath, query: String): TextSearchOperator = JSearchOperator.text(path, query)

  /**
   * Returns a `SearchOperator` that performs a full-text search.
   *
   * @param paths The non-empty fields to be searched.
   * @param queries The non-empty strings to search for.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/text/ text operator]]
   */
  def text(paths: Iterable[_ <: SearchPath], queries: Iterable[String]): TextSearchOperator =
    JSearchOperator.text(paths.asJava, queries.asJava)

  /**
   * Returns a `SearchOperator` that may be used to implement search-as-you-type functionality.
   *
   * @param path The field to be searched.
   * @param query The string to search for.
   * @param queries More strings to search for.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/autocomplete/ autocomplete operator]]
   */
  def autocomplete(path: FieldSearchPath, query: String, queries: String*): AutocompleteSearchOperator =
    JSearchOperator.autocomplete(path, query, queries: _*)

  /**
   * Returns a `SearchOperator` that may be used to implement search-as-you-type functionality.
   *
   * @param path The field to be searched.
   * @param queries The non-empty strings to search for.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/autocomplete/ autocomplete operator]]
   */
  def autocomplete(path: FieldSearchPath, queries: Iterable[String]): AutocompleteSearchOperator =
    JSearchOperator.autocomplete(path, queries.asJava)

  /**
   * Returns a base for a `SearchOperator` that tests if the
   * BSON `32-bit integer` / `64-bit integer` / `Double` values
   * of the specified fields are within an interval.
   *
   * @param path The field to be searched.
   * @param paths More fields to be searched.
   * @return A base for a `NumberRangeSearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/range/ range operator]]
   */
  def numberRange(path: FieldSearchPath, paths: FieldSearchPath*): NumberRangeSearchOperatorBase =
    JSearchOperator.numberRange(path, paths: _*)

  /**
   * Returns a base for a `SearchOperator` that tests if the
   * BSON `32-bit integer` / `64-bit integer` / `Double` values
   * of the specified fields are within an interval.
   *
   * @param paths The non-empty fields to be searched.
   * @return A base for a `NumberRangeSearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/range/ range operator]]
   */
  def numberRange(paths: Iterable[_ <: FieldSearchPath]): NumberRangeSearchOperatorBase =
    JSearchOperator.numberRange(paths.asJava)

  /**
   * Returns a base for a `SearchOperator` that tests if the
   * BSON `Date` values of the specified fields are within an interval.
   *
   * @param path The field to be searched.
   * @param paths More fields to be searched.
   * @return A base for a `DateRangeSearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/range/ range operator]]
   */
  def dateRange(path: FieldSearchPath, paths: FieldSearchPath*): DateRangeSearchOperatorBase =
    JSearchOperator.dateRange(path, paths: _*)

  /**
   * Returns a base for a `SearchOperator` that tests if the
   * BSON `Date` values of the specified fields are within an interval.
   *
   * @param paths The non-empty fields to be searched.
   * @return A base for a `DateRangeSearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/range/ range operator]]
   */
  def dateRange(paths: Iterable[_ <: FieldSearchPath]): DateRangeSearchOperatorBase =
    JSearchOperator.dateRange(paths.asJava)

  /**
   * Returns a `SearchOperator` that allows finding results that are near the specified `origin`.
   *
   * @param origin The origin from which the proximity of the results is measured.
   * The relevance score is 1 if the values of the fields are `origin`.
   * @param pivot The positive distance from the `origin` at which the relevance score drops in half.
   * @param path The field to be searched.
   * @param paths More fields to be searched.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/near/ near operator]]
   */
  def near(origin: Number, pivot: Number, path: FieldSearchPath, paths: FieldSearchPath*): NumberNearSearchOperator =
    JSearchOperator.near(origin, pivot, path, paths: _*)

  /**
   * Returns a `SearchOperator` that allows finding results that are near the specified `origin`.
   *
   * @param origin The origin from which the proximity of the results is measured.
   * The relevance score is 1 if the values of the fields are `origin`.
   * @param pivot The positive distance from the `origin` at which the relevance score drops in half.
   * @param paths The non-empty fields to be searched.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/near/ near operator]]
   */
  def near(origin: Number, pivot: Number, paths: Iterable[_ <: FieldSearchPath]): NumberNearSearchOperator =
    JSearchOperator.near(origin, pivot, paths.asJava)

  /**
   * Returns a `SearchOperator` that allows finding results that are near the specified `origin`.
   *
   * @param origin The origin from which the proximity of the results is measured.
   * The relevance score is 1 if the values of the fields are `origin`.
   * @param pivot The positive distance from the `origin` at which the relevance score drops in half.
   * Data is extracted via `Duration.toMillis`.
   * @param path The field to be searched.
   * @param paths More fields to be searched.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/near/ near operator]]
   * @see `org.bson.codecs.jsr310.InstantCodec`
   */
  def near(origin: Instant, pivot: Duration, path: FieldSearchPath, paths: FieldSearchPath*): DateNearSearchOperator =
    JSearchOperator.near(origin, pivot, path, paths: _*)

  /**
   * Returns a `SearchOperator` that allows finding results that are near the specified `origin`.
   *
   * @param origin The origin from which the proximity of the results is measured.
   * The relevance score is 1 if the values of the fields are `origin`.
   * @param pivot The positive distance from the `origin` at which the relevance score drops in half.
   * Data is extracted via `Duration.toMillis`.
   * @param paths The non-empty fields to be searched.
   * It is converted to `long` via `Duration.toMillis`.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/near/ near operator]]
   * @see `org.bson.codecs.jsr310.InstantCodec`
   */
  def near(origin: Instant, pivot: Duration, paths: Iterable[_ <: FieldSearchPath]): DateNearSearchOperator =
    JSearchOperator.near(origin, pivot, paths.asJava)

  /**
   * Returns a `SearchOperator` that allows finding results that are near the specified `origin`.
   *
   * @param origin The origin from which the proximity of the results is measured.
   * The relevance score is 1 if the values of the fields are `origin`.
   * @param pivot The positive distance in meters from the `origin` at which the relevance score drops in half.
   * @param path The field to be searched.
   * @param paths More fields to be searched.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/near/ near operator]]
   */
  def near(origin: Point, pivot: Number, path: FieldSearchPath, paths: FieldSearchPath*): GeoNearSearchOperator =
    JSearchOperator.near(origin, pivot, path, paths: _*)

  /**
   * Returns a `SearchOperator` that allows finding results that are near the specified `origin`.
   *
   * @param origin The origin from which the proximity of the results is measured.
   * The relevance score is 1 if the values of the fields are `origin`.
   * @param pivot The positive distance in meters from the `origin` at which the relevance score drops in half.
   * @param paths The non-empty fields to be searched.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/near/ near operator]]
   */
  def near(origin: Point, pivot: Number, paths: Iterable[_ <: FieldSearchPath]): GeoNearSearchOperator =
    JSearchOperator.near(origin, pivot, paths.asJava)

  /**
   * Returns a `SearchOperator` that searches for documents where the value
   * or array of values at a given path contains any of the specified values
   *
   * @param path The indexed field to be searched.
   * @param value The boolean value to search for.
   * @param values More fields to be searched.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/in/ in operator]]
   */
  def in(path: FieldSearchPath, value: Boolean, values: Boolean*): InSearchOperator =
    JSearchOperator.in(path, value, values: _*)

  /**
   * Returns a `SearchOperator` that searches for documents where the value
   * or array of values at a given path contains any of the specified values
   *
   * @param path The indexed field to be searched.
   * @param value The objectId value to search for.
   * @param values More fields to be searched.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/in/ in operator]]
   */
  def in(path: FieldSearchPath, value: ObjectId, values: ObjectId*): InSearchOperator =
    JSearchOperator.in(path, value, values: _*)

  /**
   * Returns a `SearchOperator` that searches for documents where the value
   * or array of values at a given path contains any of the specified values
   *
   * @param path The indexed field to be searched.
   * @param value The number value to search for.
   * @param values More fields to be searched.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/in/ in operator]]
   */
  def in(path: FieldSearchPath, value: Number, values: Number*): InSearchOperator =
    JSearchOperator.in(path, value, values: _*)

  /**
   * Returns a `SearchOperator` that searches for documents where the value
   * or array of values at a given path contains any of the specified values
   *
   * @param path The indexed field to be searched.
   * @param value The instant date value to search for.
   * @param values More fields to be searched.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/in/ in operator]]
   */
  def in(path: FieldSearchPath, value: Instant, values: Instant*): InSearchOperator =
    JSearchOperator.in(path, value, values: _*)

  /**
   * Returns a `SearchOperator` that searches for documents where the value
   * or array of values at a given path contains any of the specified values
   *
   * @param path The indexed field to be searched.
   * @param value The uuid value to search for.
   * @param values More fields to be searched.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/in/ in operator]]
   */
  def in(path: FieldSearchPath, value: UUID, values: UUID*): InSearchOperator =
    JSearchOperator.in(path, value, values: _*)

  /**
   * Returns a `SearchOperator` that searches for documents where the value
   * or array of values at a given path contains any of the specified values
   *
   * @param path The indexed field to be searched.
   * @param value The string value to search for.
   * @param values More fields to be searched.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/in/ in operator]]
   */
  def in(path: FieldSearchPath, value: String, values: String*): InSearchOperator =
    JSearchOperator.in(path, value, values: _*)

  /**
   * Returns a `SearchOperator` that searches for documents where the value
   * or array of values at a given path contains any of the specified values
   *
   * @param path The indexed field to be searched.
   * @param values The non-empty values to search for. Value can be either a single value or an array of values of only one of the supported BSON types and can't be a mix of different types.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/in/ in operator]]
   */
  def in[T](path: FieldSearchPath, values: Iterable[_ <: T]): InSearchOperator =
    JSearchOperator.in(path, values.asJava)

  /**
   * Returns a `SearchOperator` that searches for documents where a field matches the specified value.
   *
   * @param path The indexed field to be searched.
   * @param value The boolean value to query for.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/equals/ equals operator]]
   */
  def equals(path: FieldSearchPath, value: Boolean): EqualsSearchOperator =
    JSearchOperator.equals(path, value)

  /**
   * Returns a `SearchOperator` that searches for documents where a field matches the specified value.
   *
   * @param path The indexed field to be searched.
   * @param value The object id value to query for.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/equals/ equals operator]]
   */
  def equals(path: FieldSearchPath, value: ObjectId): EqualsSearchOperator =
    JSearchOperator.equals(path, value)

  /**
   * Returns a `SearchOperator` that searches for documents where a field matches the specified value.
   *
   * @param path The indexed field to be searched.
   * @param value The number value to query for.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/equals/ equals operator]]
   */
  def equals(path: FieldSearchPath, value: Number): EqualsSearchOperator =
    JSearchOperator.equals(path, value)

  /**
   * Returns a `SearchOperator` that searches for documents where a field matches the specified value.
   *
   * @param path The indexed field to be searched.
   * @param value The instant date value to query for.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/equals/ equals operator]]
   */
  def equals(path: FieldSearchPath, value: Instant): EqualsSearchOperator =
    JSearchOperator.equals(path, value)

  /**
   * Returns a `SearchOperator` that searches for documents where a field matches the specified value.
   *
   * @param path The indexed field to be searched.
   * @param value The string value to query for.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/equals/ equals operator]]
   */
  def equals(path: FieldSearchPath, value: String): EqualsSearchOperator =
    JSearchOperator.equals(path, value)

  /**
   * Returns a `SearchOperator` that searches for documents where a field matches the specified value.
   *
   * @param path The indexed field to be searched.
   * @param value The uuid value to query for.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/equals/ equals operator]]
   */
  def equals(path: FieldSearchPath, value: UUID): EqualsSearchOperator =
    JSearchOperator.equals(path, value)

  /**
   * Returns a `SearchOperator` that searches for documents where a field matches null.
   *
   * @param path The indexed field to be searched.
   * @param value The uuid value to query for.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/equals/ equals operator]]
   */
  def equalsNull(path: FieldSearchPath): EqualsSearchOperator =
    JSearchOperator.equalsNull(path)

  /**
   * Returns a `SearchOperator` that returns documents similar to input document.
   *
   * @param like The BSON document that is used to extract representative terms to query for.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/morelikethis/ moreLikeThis operator]]
   */
  def moreLikeThis(like: BsonDocument): MoreLikeThisSearchOperator = JSearchOperator.moreLikeThis(like)

  /**
   * Returns a `SearchOperator` that returns documents similar to input documents.
   *
   * @param likes The BSON documents that are used to extract representative terms to query for.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/morelikethis/ moreLikeThis operator]]
   */
  def moreLikeThis(likes: Iterable[BsonDocument]): MoreLikeThisSearchOperator =
    JSearchOperator.moreLikeThis(likes.asJava)

  /**
   * Returns a `SearchOperator` that enables queries which use special characters in the search string that can match any character.
   *
   * @param query The string to search for.
   * @param path The indexed field to be searched.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/wildcard/ wildcard operator]]
   */
  def wildcard(query: String, path: SearchPath): WildcardSearchOperator = JSearchOperator.wildcard(query, path)

  /**
   * Returns a `SearchOperator` that enables queries which use special characters in the search string that can match any character.
   *
   * @param queries The non-empty strings to search for.
   * @param paths The non-empty indexed fields to be searched.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/wildcard/ wildcard operator]]
   */
  def wildcard(queries: Iterable[String], paths: Iterable[_ <: SearchPath]): WildcardSearchOperator =
    JSearchOperator.wildcard(queries.asJava, paths.asJava)

  /**
   * Returns a `SearchOperator` that supports querying a combination of indexed fields and values.
   *
   * @param defaultPath The field to be searched by default.
   * @param query One or more indexed fields and values to search.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/queryString/ queryString operator]]
   */
  def queryString(defaultPath: FieldSearchPath, query: String): QueryStringSearchOperator =
    JSearchOperator.queryString(defaultPath, query)

  /**
   * Returns a `SearchOperator` that performs a search for documents containing an ordered sequence of terms.
   *
   * @param path The field to be searched.
   * @param query The string to search for.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/phrase/ phrase operator]]
   */
  def phrase(path: SearchPath, query: String): PhraseSearchOperator = JSearchOperator.phrase(path, query)

  /**
   * Returns a `SearchOperator` that performs a search for documents containing an ordered sequence of terms.
   *
   * @param paths The non-empty fields to be searched.
   * @param queries The non-empty strings to search for.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/phrase/ phrase operator]]
   */
  def phrase(paths: Iterable[_ <: SearchPath], queries: Iterable[String]): PhraseSearchOperator =
    JSearchOperator.phrase(paths.asJava, queries.asJava)

  /**
   * Returns a `SearchOperator` that performs a search using a regular expression.
   *
   * @param path The field to be searched.
   * @param query The string to search for.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/regex/ regex operator]]
   */
  def regex(path: SearchPath, query: String): RegexSearchOperator = JSearchOperator.regex(path, query)

  /**
   * Returns a `SearchOperator` that performs a search using a regular expression.
   *
   * @param paths The non-empty fields to be searched.
   * @param queries The non-empty strings to search for.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/regex/ regex operator]]
   */
  def regex(paths: Iterable[_ <: SearchPath], queries: Iterable[String]): RegexSearchOperator =
    JSearchOperator.regex(paths.asJava, queries.asJava)

  /**
   * Creates a `SearchOperator` from a `Bson` in situations when there is no builder method that better satisfies your needs.
   * This method cannot be used to validate the syntax.
   *
   * <i>Example</i><br>
   * The following code creates two functionally equivalent `SearchOperator`s,
   * though they may not be equal.
   * {{{
   *  val operator1: SearchOperator = SearchOperator.exists(
   *    SearchPath.fieldPath("fieldName"))
   *  val operator2: SearchOperator = SearchOperator.of(Document("exists" ->
   *    Document("path" -> SearchPath.fieldPath("fieldName").toValue)))
   * }}}
   *
   * @param operator A `Bson` representing the required `SearchOperator`.
   *
   * @return The requested `SearchOperator`.
   */
  def of(operator: Bson): SearchOperator = JSearchOperator.of(operator)
}
