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

import com.mongodb.client.model.{Projections => JProjections}

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
   * Creates a projection of a field whose value is computed from the given expression.  Projection with an expression is only supported
   * using the \$project aggregation pipeline stage.
   *
   * @param fieldName     the field name
   * @param  expression   the expression
   * @tparam TExpression  the expression type
   * @return the projection
   * @see Aggregates#project(Bson)
   */
  def computed[TExpression](fieldName: String, expression: TExpression): Bson = JProjections.computed(fieldName, expression)

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
   * referred to as the positional \$ operator.
   *
   * @param fieldName the field name whose value is the array
   * @return the projection
   * @see [[http://http://docs.mongodb.org/manual/reference/operator/projection/positional/#projection Project the first matching element (\$ operator)]]
   */
  def elemMatch(fieldName: String): Bson = JProjections.elemMatch(fieldName)

  /**
   * Creates a projection that includes for the given field only the first element of the array value of that field that matches the given
   * query filter.
   *
   * @param fieldName the field name
   * @param filter    the filter to apply
   * @return the projection
   * @see [[http://http://docs.mongodb.org/manual/reference/operator/projection/elemMatch elemMatch]]
   */
  def elemMatch(fieldName: String, filter: Bson): Bson = JProjections.elemMatch(fieldName, filter)

  /**
   * Creates a projection to the given field name of the textScore, for use with text queries.
   *
   * @param fieldName the field name
   * @return the projection
   * @see [[http://http://docs.mongodb.org/manual/reference/operator/projection/meta/#projection textScore]]
   */
  def metaTextScore(fieldName: String): Bson = JProjections.metaTextScore(fieldName)

  /**
   * Creates a projection to the given field name of a slice of the array value of that field.
   *
   * @param fieldName the field name
   * @param limit the number of elements to project.
   * @return the projection
   * @see [[http://http://docs.mongodb.org/manual/reference/operator/projection/slice Slice]]
   */
  def slice(fieldName: String, limit: Int): Bson = JProjections.slice(fieldName, limit)

  /**
   * Creates a projection to the given field name of a slice of the array value of that field.
   *
   * @param fieldName the field name
   * @param skip the number of elements to skip before applying the limit
   * @param limit the number of elements to project
   * @return the projection
   * @see [[http://http://docs.mongodb.org/manual/reference/operator/projection/slice Slice]]
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
