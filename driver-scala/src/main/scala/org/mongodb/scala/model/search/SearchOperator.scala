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

import com.mongodb.client.model.search.{ SearchOperator => JSearchOperator }
import org.mongodb.scala.bson.conversions.Bson

import collection.JavaConverters._

/**
 * The core part of the `\$search` pipeline stage of an aggregation pipeline.
 *
 * @see [[https://www.mongodb.com/docs/atlas/atlas-search/operators-and-collectors/#operators Search operators]]
 * @since 4.7
 */
object SearchOperator {

  /**
   * Returns a base for a `SearchOperator` that may combine multiple `SearchOperator`s.
   * Combining `SearchOperator`s affects calculation of the relevance score.
   *
   * @return A base for a `SearchOperator`.
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
   * @param query A string to search for.
   * @param path A document field to be searched.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/text/ text operator]]
   */
  def text(query: String, path: SearchPath): TextSearchOperator = JSearchOperator.text(query, path)

  /**
   * Returns a `SearchOperator` that performs a full-text search.
   *
   * @param queries Non-empty terms to search for.
   * @param paths Non-empty document fields to be searched.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/text/ text operator]]
   */
  def text(queries: Iterable[String], paths: Iterable[_ <: SearchPath]): TextSearchOperator =
    JSearchOperator.text(queries.asJava, paths.asJava)

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
