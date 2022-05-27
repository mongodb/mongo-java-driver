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
import com.mongodb.client.model.search.{ SearchPath => JSearchPath }

/**
 * A specification of fields to be searched.
 *
 * Depending on the context, one of the following methods may be used to get a representation of a `SearchPath`
 * with the correct syntax: `SearchPath.toBsonDocument`, `SearchPath.toBsonValue`, `FieldSearchPath.toValue`.
 *
 * @see [[https://www.mongodb.com/docs/atlas/atlas-search/path-construction/ Path]]
 * @since 4.7
 */
@Beta(Array(Beta.Reason.CLIENT))
object SearchPath {

  /**
   * Returns a `SearchPath` for the given `path`.
   *
   * @param path The name of the field. Must not contain [[SearchPath.wildcardPath wildcard]] characters.
   * @return The requested `SearchPath`.
   * @see [[https://www.mongodb.com/docs/manual/core/document/#dot-notation Dot notation]]
   */
  def fieldPath(path: String): FieldSearchPath = JSearchPath.fieldPath(path)

  /**
   * Returns a `SearchPath` for the given `wildcardPath`.
   *
   * @param wildcardPath The specification of the fields that contains wildcard (`'*'`) characters.
   * Must not contain `'**'`.
   * @return The requested `SearchPath`.
   * @see [[https://www.mongodb.com/docs/manual/core/document/#dot-notation Dot notation]]
   */
  def wildcardPath(wildcardPath: String): WildcardSearchPath = JSearchPath.wildcardPath(wildcardPath)
}
