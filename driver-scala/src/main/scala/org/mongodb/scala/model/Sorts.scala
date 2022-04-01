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

import com.mongodb.client.model.{ Sorts => JSorts }

import org.mongodb.scala.bson.conversions.Bson

/**
 * A factory for sort specifications.   A convenient way to use this class is to statically import all of its methods, which allows
 * usage like:
 *
 * `collection.find().sort(orderBy(ascending("x", "y"), descending("z")))`
 *
 * @since 1.0
 */
object Sorts {

  /**
   * Create a sort specification for an ascending sort on the given fields.
   *
   * @param fieldNames the field names, which must contain at least one
   * @return the sort specification
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/meta/orderby Sort]]
   */
  def ascending(fieldNames: String*): Bson = JSorts.ascending(fieldNames.asJava)

  /**
   * Create a sort specification for an ascending sort on the given fields.
   *
   * @param fieldNames the field names, which must contain at least one
   * @return the sort specification
   * @see [[https://www.mongodb.com/docs/manual/reference/operator/meta/orderby Sort]]
   */
  def descending(fieldNames: String*): Bson = JSorts.descending(fieldNames.asJava)

  /**
   * Create a sort specification for the text score meta projection on the given field.
   *
   * @param fieldName the field name
   * @return the sort specification
   * @see Filters.text(String, TextSearchOptions)
   * @see [[https://docs.mongodb.com/manual/reference/operator/aggregation/meta/#text-score-metadata--meta---textscore- textScore]]
   */
  def metaTextScore(fieldName: String): Bson = JSorts.metaTextScore(fieldName)

  /**
   * Combine multiple sort specifications.  If any field names are repeated, the last one takes precendence.
   *
   * @param sorts the sort specifications
   * @return the combined sort specification
   */
  def orderBy(sorts: Bson*): Bson = JSorts.orderBy(sorts.asJava)
}
