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

import com.mongodb.client.model.{ CollationMaxVariable => JCollationMaxVariable }

/**
 * Collation support allows the specific configuration of whether or not spaces and punctuation are considered base characters.
 *
 * `CollationMaxVariable` controls which characters are affected by [[CollationAlternate$.SHIFTED]].
 *
 * @note Requires MongoDB 3.4 or greater
 * @since 1.2
 */
object CollationMaxVariable {

  /**
   * Punct
   *
   * Both punctuation and spaces are affected.
   */
  val PUNCT: CollationMaxVariable = JCollationMaxVariable.PUNCT

  /**
   * Shifted
   *
   * Only spaces are affected.
   */
  val SPACE: CollationMaxVariable = JCollationMaxVariable.SPACE

  /**
   * Returns the CollationMaxVariable from the string value.
   *
   * @param collationMaxVariable the string value.
   * @return the read concern
   */
  def fromString(collationMaxVariable: String): Try[CollationMaxVariable] =
    Try(JCollationMaxVariable.fromString(collationMaxVariable))

}
