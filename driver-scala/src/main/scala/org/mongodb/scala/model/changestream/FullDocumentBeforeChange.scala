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

package org.mongodb.scala.model.changestream

import com.mongodb.client.model.changestream.{ FullDocumentBeforeChange => JFullDocumentBeforeChange }

import scala.util.Try

/**
 * Change Stream fullDocumentBeforeChange configuration.
 *
 * Determines what to return for update operations when using a Change Stream. Defaults to [[FullDocumentBeforeChange#DEFAULT]].
 *
 * @note Requires MongoDB 6.0 or greater
 * @since 4.7
 */
object FullDocumentBeforeChange {

  /**
   * Default
   *
   * Returns the servers default value in the `fullDocument` field.
   */
  val DEFAULT = JFullDocumentBeforeChange.DEFAULT

  /**
   * Lookup
   *
   * The change stream for partial updates will include both a delta describing the changes to the document as well as a copy of the
   * entire document that was changed from *some time* after the change occurred.
   */
  val OFF = JFullDocumentBeforeChange.OFF

  /**
   * Configures the change stream to return the post-image of the modified document for replace and update change events, if it
   * is available.
   */
  val WHEN_AVAILABLE = JFullDocumentBeforeChange.WHEN_AVAILABLE

  /**
   * The same behavior as WHEN_AVAILABLE except that an error is raised if the post-image is not available.
   */
  val REQUIRED = JFullDocumentBeforeChange.REQUIRED

  /**
   * Returns the FullDocumentBeforeChange from the string value.
   *
   * @param fullDocumentBeforeChange the string value.
   * @return the FullDocumentBeforeChange value
   */
  def fromString(fullDocumentBeforeChange: String): Try[JFullDocumentBeforeChange] =
    Try(JFullDocumentBeforeChange.fromString(fullDocumentBeforeChange))

}
