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

import com.mongodb.client.model.{ CollationCaseFirst => JCollationCaseFirst }

/**
 * Collation support allows the specific configuration of how character cases are handled.
 *
 * @note Requires MongoDB 3.4 or greater
 * @since 1.2
 */
object CollationCaseFirst {

  /**
   * Uppercase first
   */
  val UPPER: CollationCaseFirst = JCollationCaseFirst.UPPER

  /**
   * Lowercase first
   */
  val LOWER: CollationCaseFirst = JCollationCaseFirst.LOWER

  /**
   * Off
   */
  val OFF: CollationCaseFirst = JCollationCaseFirst.OFF

  /**
   * Returns the CollationCaseFirst from the string value.
   *
   * @param collationCaseFirst the string value.
   * @return the read concern
   */
  def fromString(collationCaseFirst: String): Try[CollationCaseFirst] =
    Try(JCollationCaseFirst.fromString(collationCaseFirst))

}
