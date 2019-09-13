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

import com.mongodb.client.model.{ CollationStrength => JCollationStrength }

/**
 * Collation support allows the specific configuration of how character cases are handled.
 *
 * @note Requires MongoDB 3.4 or greater
 * @since 1.2
 */
object CollationStrength {

  /**
   * Strongest level, denote difference between base characters
   */
  val PRIMARY: CollationStrength = JCollationStrength.PRIMARY

  /**
   * Accents in characters are considered secondary differences
   */
  val SECONDARY: CollationStrength = JCollationStrength.SECONDARY

  /**
   * Upper and lower case differences in characters are distinguished at the tertiary level. The server default.
   */
  val TERTIARY: CollationStrength = JCollationStrength.TERTIARY

  /**
   * When punctuation is ignored at level 1-3, an additional level can be used to distinguish words with and without punctuation.
   */
  val QUATERNARY: CollationStrength = JCollationStrength.QUATERNARY

  /**
   * When all other levels are equal, the identical level is used as a tiebreaker.
   * The Unicode code point values of the NFD form of each string are compared at this level, just in case there is no difference at
   * levels 1-4
   */
  val IDENTICAL: CollationStrength = JCollationStrength.IDENTICAL

  /**
   * Returns the CollationStrength from the string value.
   *
   * @param collationStrength the int value.
   * @return the read concern
   */
  def fromInt(collationStrength: Int): Try[CollationStrength] = Try(JCollationStrength.fromInt(collationStrength))

}
