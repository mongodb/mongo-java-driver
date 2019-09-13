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

import scala.util.Try

import com.mongodb.client.model.{ CollationAlternate => JCollationAlternate }

/**
 * Collation support allows the specific configuration of whether or not spaces and punctuation are considered base characters.
 *
 * @note Requires MongoDB 3.4 or greater
 * @since 1.2
 */
object CollationAlternate {

  /**
   * Non-ignorable
   *
   * Spaces and punctuation are considered base characters
   */
  val NON_IGNORABLE: CollationAlternate = JCollationAlternate.NON_IGNORABLE

  /**
   * Shifted
   *
   * Spaces and punctuation are not considered base characters, and are only distinguished when the collation strength is > 3
   *
   * @see CollationMaxVariable
   */
  val SHIFTED: CollationAlternate = JCollationAlternate.SHIFTED

  /**
   * Returns the CollationAlternate from the string value.
   *
   * @param collationAlternate the string value.
   * @return the read concern
   */
  def fromString(collationAlternate: String): Try[CollationAlternate] =
    Try(JCollationAlternate.fromString(collationAlternate))

}
