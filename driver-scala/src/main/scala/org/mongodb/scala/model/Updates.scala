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

import com.mongodb.client.model.{ PushOptions => JPushOptions, Updates => JUpdates }

import org.mongodb.scala.bson.conversions.Bson

/**
 * A factory for document updates. A convenient way to use this class is to statically import all of its methods, which allows usage like:
 *
 * `collection.updateOne(eq("x", 1), set("x", 2))`
 *
 * @since 1.0
 */
object Updates {

  /**
   * Combine a list of updates into a single update.
   *
   * @param updates the list of updates
   * @return a combined update
   */
  def combine(updates: Bson*): Bson = JUpdates.combine(updates.asJava)

  /**
   * Creates an update that sets the value of the field with the given name to the given value.
   *
   * @param fieldName the non-null field name
   * @param value     the value
   * @tparam TItem   the value type
   * @return the update
   * @see [[http://docs.mongodb.com/manual/reference/operator/update/set/ \$set]]
   */
  def set[TItem](fieldName: String, value: TItem): Bson = JUpdates.set(fieldName, value)

  /**
   * Creates an update that sets the value for the document.
   *
   * @param value     the value
   * @return the update
   * @see [[http://docs.mongodb.com/manual/reference/operator/update/set/ \$set]]
   */
  def set(value: Bson): Bson = JUpdates.set(value)

  /**
   * Creates an update that deletes the field with the given name.
   *
   * @param fieldName the non-null field name
   * @return the update
   * @see [[http://docs.mongodb.com/manual/reference/operator/update/unset/ \$unset]]
   */
  def unset(fieldName: String): Bson = JUpdates.unset(fieldName)

  /**
   * Creates an update that sets the value of the field with the given name to the given value, but only if the update is an upsert that
   * results in an insert of a document.
   *
   * @param fieldName the non-null field name
   * @param value     the value
   * @tparam TItem   the value type
   * @return the update
   * @see [[http://docs.mongodb.com/manual/reference/operator/update/setOnInsert/ \$setOnInsert]]
   * @see UpdateOptions#upsert(boolean)
   */
  def setOnInsert[TItem](fieldName: String, value: TItem): Bson = JUpdates.setOnInsert(fieldName, value)

  /**
   * Creates an update that sets the values for the document, but only if the update is an upsert that results in an insert of a document.
   *
   * @param value     the value
   * @return the update
   * @see [[http://docs.mongodb.com/manual/reference/operator/update/setOnInsert/ \$setOnInsert]]
   * @see UpdateOptions#upsert(boolean)
   */
  def setOnInsert(value: Bson): Bson = JUpdates.setOnInsert(value)

  /**
   * Creates an update that renames a field.
   *
   * @param fieldName    the non-null field name
   * @param newFieldName the non-null new field name
   * @return the update
   * @see [[http://docs.mongodb.com/manual/reference/operator/update/rename/ \$rename]]
   */
  def rename(fieldName: String, newFieldName: String): Bson = JUpdates.rename(fieldName, newFieldName)

  /**
   * Creates an update that increments the value of the field with the given name by the given value.
   *
   * @param fieldName the non-null field name
   * @param number     the value
   * @return the update
   * @see [[http://docs.mongodb.com/manual/reference/operator/update/inc/ \$inc]]
   */
  def inc(fieldName: String, number: Number): Bson = JUpdates.inc(fieldName, number)

  /**
   * Creates an update that multiplies the value of the field with the given name by the given number.
   *
   * @param fieldName the non-null field name
   * @param number    the non-null number
   * @return the update
   * @see [[http://docs.mongodb.com/manual/reference/operator/update/mul/ \$mul]]
   */
  def mul(fieldName: String, number: Number): Bson = JUpdates.mul(fieldName, number)

  /**
   * Creates an update that sets the value of the field to the given value if the given value is less than the current value of the
   * field.
   *
   * @param fieldName the non-null field name
   * @param value     the value
   * @tparam TItem   the value type
   * @return the update
   * @see [[http://docs.mongodb.com/manual/reference/operator/update/min/ \$min]]
   */
  def min[TItem](fieldName: String, value: TItem): Bson = JUpdates.min(fieldName, value)

  /**
   * Creates an update that sets the value of the field to the given value if the given value is greater than the current value of the
   * field.
   *
   * @param fieldName the non-null field name
   * @param value     the value
   * @tparam TItem   the value type
   * @return the update
   * @see [[http://docs.mongodb.com/manual/reference/operator/update/min/ \$min]]
   */
  def max[TItem](fieldName: String, value: TItem): Bson = JUpdates.max(fieldName, value)

  /**
   * Creates an update that sets the value of the field to the current date as a BSON date.
   *
   * @param fieldName the non-null field name
   * @return the update
   * @see [[http://docs.mongodb.com/manual/reference/operator/update/currentDate/ \$currentDate]]
   * @see [[http://docs.mongodb.com/manual/reference/bson-types/#date Date]]
   */
  def currentDate(fieldName: String): Bson = JUpdates.currentDate(fieldName)

  /**
   * Creates an update that sets the value of the field to the current date as a BSON timestamp.
   *
   * @param fieldName the non-null field name
   * @return the update
   * @see [[http://docs.mongodb.com/manual/reference/operator/update/currentDate/ \$currentDate]]
   * @see [[http://docs.mongodb.com/manual/reference/bson-types/#document-bson-type-timestamp Timestamp]]
   */
  def currentTimestamp(fieldName: String): Bson = JUpdates.currentTimestamp(fieldName)

  /**
   * Creates an update that adds the given value to the array value of the field with the given name, unless the value is
   * already present, in which case it does nothing
   *
   * @param fieldName the non-null field name
   * @param value     the value
   * @tparam TItem   the value type
   * @return the update
   * @see [[http://docs.mongodb.com/manual/reference/operator/update/addToSet/ \$addToSet]]
   */
  def addToSet[TItem](fieldName: String, value: TItem): Bson = JUpdates.addToSet(fieldName, value)

  /**
   * Creates an update that adds each of the given values to the array value of the field with the given name, unless the value is
   * already present, in which case it does nothing
   *
   * @param fieldName the non-null field name
   * @param values     the values
   * @tparam TItem   the value type
   * @return the update
   * @see [[http://docs.mongodb.com/manual/reference/operator/update/addToSet/ \$addToSet]]
   */
  def addEachToSet[TItem](fieldName: String, values: TItem*): Bson = JUpdates.addEachToSet(fieldName, values.asJava)

  /**
   * Creates an update that adds the given value to the array value of the field with the given name.
   *
   * @param fieldName the non-null field name
   * @param value     the value
   * @tparam TItem   the value type
   * @return the update
   * @see [[http://docs.mongodb.com/manual/reference/operator/update/push/ \$push]]
   */
  def push[TItem](fieldName: String, value: TItem): Bson = JUpdates.push(fieldName, value)

  /**
   * Creates an update that adds each of the given values to the array value of the field with the given name.
   *
   * @param fieldName the non-null field name
   * @param values    the values
   * @tparam TItem   the value type
   * @return the update
   * @see [[http://docs.mongodb.com/manual/reference/operator/update/push/ \$push]]
   */
  def pushEach[TItem](fieldName: String, values: TItem*): Bson = JUpdates.pushEach(fieldName, values.asJava)

  /**
   * Creates an update that adds each of the given values to the array value of the field with the given name, applying the given
   * options for positioning the pushed values, and then slicing and/or sorting the array.
   *
   * @param fieldName the non-null field name
   * @param values    the values
   * @param options   the non-null push options
   * @tparam TItem   the value type
   * @return the update
   * @see [[http://docs.mongodb.com/manual/reference/operator/update/push/ \$push]]
   */
  def pushEach[TItem](fieldName: String, options: JPushOptions, values: TItem*): Bson =
    JUpdates.pushEach(fieldName, values.asJava, options)

  /**
   * Creates an update that removes all instances of the given value from the array value of the field with the given name.
   *
   * @param fieldName the non-null field name
   * @param value     the value
   * @tparam TItem   the value type
   * @return the update
   * @see [[http://docs.mongodb.com/manual/reference/operator/update/pull/ \$pull]]
   */
  def pull[TItem](fieldName: String, value: TItem): Bson = JUpdates.pull(fieldName, value)

  /**
   * Creates an update that removes from an array all elements that match the given filter.
   *
   * @param filter the query filter
   * @return the update
   * @see [[http://docs.mongodb.com/manual/reference/operator/update/pull/ \$pull]]
   */
  def pullByFilter(filter: Bson): Bson = JUpdates.pullByFilter(filter)

  /**
   * Creates an update that removes all instances of the given values from the array value of the field with the given name.
   *
   * @param fieldName the non-null field name
   * @param values    the values
   * @tparam TItem   the value type
   * @return the update
   * @see [[http://docs.mongodb.com/manual/reference/operator/update/pull/ \$pull]]
   */
  def pullAll[TItem](fieldName: String, values: TItem*): Bson = JUpdates.pullAll(fieldName, values.asJava)

  /**
   * Creates an update that pops the first element of an array that is the value of the field with the given name.
   *
   * @param fieldName the non-null field name
   * @return the update
   * @see [[http://docs.mongodb.com/manual/reference/operator/update/pop/ \$pop]]
   */
  def popFirst(fieldName: String): Bson = JUpdates.popFirst(fieldName)

  /**
   * Creates an update that pops the last element of an array that is the value of the field with the given name.
   *
   * @param fieldName the non-null field name
   * @return the update
   * @see [[http://docs.mongodb.com/manual/reference/operator/update/pop/ \$pop]]
   */
  def popLast(fieldName: String): Bson = JUpdates.popLast(fieldName)

  /**
   * Creates an update that performs a bitwise and between the given integer value and the integral value of the field with the given
   * name.
   *
   * @param fieldName the field name
   * @param value     the value
   * @return the update
   */
  def bitwiseAnd(fieldName: String, value: Int): Bson = JUpdates.bitwiseAnd(fieldName, value)

  /**
   * Creates an update that performs a bitwise and between the given long value and the integral value of the field with the given name.
   *
   * @param fieldName the field name
   * @param value     the value
   * @return the update
   * @see [[http://docs.mongodb.com/manual/reference/operator/update/bit/ \$bit]]
   */
  def bitwiseAnd(fieldName: String, value: Long): Bson = JUpdates.bitwiseAnd(fieldName, value)

  /**
   * Creates an update that performs a bitwise or between the given integer value and the integral value of the field with the given
   * name.
   *
   * @param fieldName the field name
   * @param value     the value
   * @return the update
   * @see [[http://docs.mongodb.com/manual/reference/operator/update/bit/ \$bit]]
   */
  def bitwiseOr(fieldName: String, value: Int): Bson = JUpdates.bitwiseOr(fieldName, value)

  /**
   * Creates an update that performs a bitwise or between the given long value and the integral value of the field with the given name.
   *
   * @param fieldName the field name
   * @param value     the value
   * @return the update
   * @see [[http://docs.mongodb.com/manual/reference/operator/update/bit/ \$bit]]
   */
  def bitwiseOr(fieldName: String, value: Long): Bson = JUpdates.bitwiseOr(fieldName, value)

  /**
   * Creates an update that performs a bitwise xor between the given integer value and the integral value of the field with the given
   * name.
   *
   * @param fieldName the field name
   * @param value     the value
   * @return the update
   */
  def bitwiseXor(fieldName: String, value: Int): Bson = JUpdates.bitwiseXor(fieldName, value)

  /**
   * Creates an update that performs a bitwise xor between the given long value and the integral value of the field with the given name.
   *
   * @param fieldName the field name
   * @param value     the value
   * @return the update
   */
  def bitwiseXor(fieldName: String, value: Long): Bson = JUpdates.bitwiseXor(fieldName, value)
}
