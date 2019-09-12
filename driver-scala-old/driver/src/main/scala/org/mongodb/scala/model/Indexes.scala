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

import com.mongodb.client.model.{Indexes => JIndexes}

import org.mongodb.scala.bson.conversions.Bson

/**
 * A factory for defining index keys. A convenient way to use this class is to statically import all of its methods, which allows usage
 * like:
 *
 * {{{
 * collection.createIndex(compoundIndex(ascending("x"), descending("y")))
 * }}}
 * @since 1.0
 */
object Indexes {
  /**
   * Create an index key for an ascending index on the given fields.
   *
   * @param fieldNames the field names, which must contain at least one
   * @return the index specification
   * @see [[http://docs.mongodb.org/manual/core/indexes indexes]]
   */
  def ascending(fieldNames: String*): Bson = JIndexes.ascending(fieldNames.asJava)

  /**
   * Create an index key for an ascending index on the given fields.
   *
   * @param fieldNames the field names, which must contain at least one
   * @return the index specification
   * @see [[http://docs.mongodb.org/manual/core/indexes indexes]]
   */
  def descending(fieldNames: String*): Bson = JIndexes.descending(fieldNames.asJava)

  /**
   * Create an index key for an 2dsphere index on the given fields.
   *
   * @param fieldNames the field names, which must contain at least one
   * @return the index specification
   * @see [[http://docs.mongodb.org/manual/core/2dsphere 2dsphere Index]]
   */
  def geo2dsphere(fieldNames: String*): Bson = JIndexes.geo2dsphere(fieldNames.asJava)

  /**
   * Create an index key for a 2d index on the given field.
   *
   * <p>
   * <strong>Note: </strong>A 2d index is for data stored as points on a two-dimensional plane.
   * The 2d index is intended for legacy coordinate pairs used in MongoDB 2.2 and earlier.
   * </p>
   *
   * @param fieldName the field to create a 2d index on
   * @return the index specification
   * @see [[http://docs.mongodb.org/manual/core/2d 2d index]]
   */
  def geo2d(fieldName: String): Bson = JIndexes.geo2d(fieldName)

  /**
   * Create an index key for a geohaystack index on the given field.
   *
   * <p>
   * <strong>Note: </strong>For queries that use spherical geometry, a 2dsphere index is a better option than a haystack index.
   * 2dsphere indexes allow field reordering; geoHaystack indexes require the first field to be the location field. Also, geoHaystack
   * indexes are only usable via commands and so always return all results at once..
   * </p>
   *
   * @param fieldName the field to create a geoHaystack index on
   * @param additional the additional field that forms the geoHaystack index key
   * @return the index specification
   * @see [[http://docs.mongodb.org/manual/core/geohaystack geoHaystack index]]
   */
  def geoHaystack(fieldName: String, additional: Bson): Bson = JIndexes.geoHaystack(fieldName, additional)

  /**
   * Create an index key for a text index on the given field.
   *
   * @param fieldName the field to create a text index on
   * @return the index specification
   * @see [[http://docs.mongodb.org/manual/core/text text index]]
   */
  def text(fieldName: String): Bson = JIndexes.text(fieldName)

  /**
   * Create an index key for a hashed index on the given field.
   *
   * @param fieldName the field to create a hashed index on
   * @return the index specification
   * @see [[http://docs.mongodb.org/manual/core/hashed hashed index]]
   */
  def hashed(fieldName: String): Bson = JIndexes.hashed(fieldName)

  /**
   * create a compound index specifications.  If any field names are repeated, the last one takes precedence.
   *
   * @param indexes the index specifications
   * @return the compound index specification
   * @see [[http://docs.mongodb.org/manual/core/index-compound compoundIndex]]
   */
  def compoundIndex(indexes: Bson*): Bson = JIndexes.compoundIndex(indexes.asJava)

}
