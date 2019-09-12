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

import com.mongodb.client.model.changestream.{FullDocument => JFullDocument}

import scala.util.Try

/**
 * Change Stream fullDocument configuration.
 *
 * Determines what to return for update operations when using a Change Stream. Defaults to [[FullDocument.DEFAULT]].
 * When set to [[FullDocument.UPDATE_LOOKUP]], the change stream for partial updates will include both a delta describing the
 * changes to the document as well as a copy of the entire document that was changed from *some time<*> after the change occurred.
 * @note Requires MongoDB 3.6 or greater
 * @since 2.4
 */
object FullDocument {

  /**
   * Default
   *
   * Returns the servers default value in the `fullDocument` field.
   */
  val DEFAULT = JFullDocument.DEFAULT

  /**
   * Lookup
   *
   * The change stream for partial updates will include both a delta describing the changes to the document as well as a copy of the
   * entire document that was changed from *some time* after the change occurred.
   */
  val UPDATE_LOOKUP = JFullDocument.UPDATE_LOOKUP

  /**
   * Returns the FullDocument from the string value.
   *
   * @param fullDocument the string value.
   * @return the read concern
   */
  def fromString(fullDocument: String): Try[FullDocument] = Try(JFullDocument.fromString(fullDocument))

}
