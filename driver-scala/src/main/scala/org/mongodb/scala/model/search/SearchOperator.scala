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

/**
 * The core part of the `\$search` pipeline stage of an aggregation pipeline.
 *
 * @see [[https://www.mongodb.com/docs/atlas/atlas-search/operators-and-collectors/#operators Search operators]]
 * @since 4.6
 */
object SearchOperator {

  /**
   * Returns a `SearchOperator` that tests if the `path` exists in a document.
   *
   * @param path The path to test.
   * @return The requested `SearchOperator`.
   * @see [[https://www.mongodb.com/docs/atlas/atlas-search/exists/ exists operator]]
   */
  def exists(path: FieldSearchPath): ExistsSearchOperator = JSearchOperator.exists(path)

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
   *  val operator2: SearchOperator = SearchOperator.of(BsonDocument("exists" ->
   *    BsonDocument("path" -> SearchPath.fieldPath("fieldName").toBsonValue)))
   * }}}
   *
   * @param operator A `Bson` representing the required `SearchOperator`.
   *
   * @return The requested `SearchOperator`.
   */
  def of(operator: Bson): SearchOperator = {
    JSearchOperator.of(operator)
  }
}
