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

import com.mongodb.client.model.search.{ SearchPath => JSearchPath }

/**
 * @since 4.6
 */
object SearchPath {

  /**
   * Returns a `SearchPath` for the given `path`.
   *
   * @param path The name of the field to search. Must not contain [[SearchPath.wildcardPath wildcard]] characters.
   * @return The requested `SearchPath`.
   * @see [[https://www.mongodb.com/docs/manual/core/document/#dot-notation Dot notation]]
   */
  def fieldPath(path: String): FieldSearchPath = JSearchPath.fieldPath(path)

  /**
   * Returns a `SearchPath` for the given `wildcardPath`.
   *
   * @param wildcardPath The specification of the fields to search that contains wildcard (`'*'`) characters.
   * Must not contain `'**'`.
   * @return The requested `SearchPath`.
   * @see [[https://www.mongodb.com/docs/manual/core/document/#dot-notation Dot notation]]
   */
  def wildcardPath(wildcardPath: String): WildcardSearchPath = JSearchPath.wildcardPath(wildcardPath)
}
