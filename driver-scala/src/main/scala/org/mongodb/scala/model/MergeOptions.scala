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

import scala.jdk.CollectionConverters._
import org.mongodb.scala.bson.conversions.Bson

import com.mongodb.client.model.{ MergeOptions => JMergeOptions }

/**
 * Options to control the behavior of the `\$merge` aggregation stage
 *
 * @since 2.7
 */
object MergeOptions {

  /**
   * The behavior of `\$merge` if a result document and an existing document in the collection have the same value for the specified on
   * field(s).
   */
  object WhenMatched {

    /**
     * Replace the existing document in the output collection with the matching results document.
     */
    val REPLACE = JMergeOptions.WhenMatched.REPLACE

    /**
     * Keep the existing document in the output collection.
     */
    val KEEP_EXISTING = JMergeOptions.WhenMatched.KEEP_EXISTING

    /**
     * Merge the matching documents
     */
    val MERGE = JMergeOptions.WhenMatched.MERGE

    /**
     * An aggregation pipeline to update the document in the collection.
     *
     * @see #whenMatchedPipeline(List)
     */
    val PIPELINE = JMergeOptions.WhenMatched.PIPELINE

    /**
     * Stop and fail the aggregation operation. Any changes to the output collection from previous documents are not reverted.
     */
    val FAIL = JMergeOptions.WhenMatched.FAIL

  }

  /**
   * The behavior of `\$merge` if a result document does not match an existing document in the out collection.
   */
  object WhenNotMatched {

    /**
     * Insert the document into the output collection.
     */
    val INSERT = JMergeOptions.WhenNotMatched.INSERT

    /**
     * Discard the document; i.e. `\$merge` does not insert the document into the output collection.
     */
    val DISCARD = JMergeOptions.WhenNotMatched.DISCARD

    /**
     * Stop and fail the aggregation operation. Any changes to the output collection from previous documents are not reverted.
     */
    val FAIL = JMergeOptions.WhenNotMatched.FAIL
  }
}

/**
 * Options to control the behavior of the `\$merge` aggregation stage
 *
 * @since 2.7
 */
case class MergeOptions(wrapped: JMergeOptions = new JMergeOptions()) {

  /**
   * Sets the field that act as a unique identifier for a document. The identifier determine if a results document matches an
   * already existing document in the output collection.
   *
   * @param uniqueIdentifiers the unique identifier(s)
   * @return this
   */
  def uniqueIdentifier(uniqueIdentifiers: String*): MergeOptions = {
    wrapped.uniqueIdentifier(uniqueIdentifiers.asJava)
    this
  }

  /**
   * Sets the behavior of `\$merge` if a result document and an existing document in the collection have the same value for the specified
   * on field(s).
   *
   * @param whenMatched when matched
   * @return this
   */
  def whenMatched(whenMatched: JMergeOptions.WhenMatched): MergeOptions = {
    wrapped.whenMatched(whenMatched)
    this
  }

  /**
   * Sets the variables accessible for use in the whenMatched pipeline.
   *
   * @param variables the variables
   * @return this
   */
  def variables(variables: Variable[_]*): MergeOptions = {
    wrapped.variables(variables.asJava)
    this
  }

  /**
   * Sets aggregation pipeline to update the document in the collection.
   *
   * @param whenMatchedPipeline when matched pipeline
   * @return this
   * @see WhenMatched#PIPELINE
   */
  def whenMatchedPipeline(whenMatchedPipeline: Bson*): MergeOptions = {
    wrapped.whenMatchedPipeline(whenMatchedPipeline.asJava)
    this
  }

  /**
   * Sets the behavior of `\$merge` if a result document does not match an existing document in the out collection.
   *
   * @param whenNotMatched when not matched
   * @return this
   */
  def whenNotMatched(whenNotMatched: JMergeOptions.WhenNotMatched): MergeOptions = {
    wrapped.whenNotMatched(whenNotMatched)
    this
  }

}
