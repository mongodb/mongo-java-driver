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

package org.mongodb.scala

import scala.util.Try

import com.mongodb.{ ReadConcernLevel => JReadConcernLevel }

/**
 * The readConcern level of isolation for reads.
 *
 * @note Requires MongoDB 3.2 or greater
 * @since 1.1
 */
object ReadConcernLevel {

  /**
   * Return the node's most recent copy of data. Provides no guarantee that the data has been written to a majority of the nodes.
   */
  val LOCAL: ReadConcernLevel = JReadConcernLevel.LOCAL

  /**
   * Return the node's most recent copy of the data confirmed as having been written to a majority of the nodes.
   */
  val MAJORITY: ReadConcernLevel = JReadConcernLevel.MAJORITY

  /**
   * The linearizable read concern.
   *
   * This read concern is only compatible with [[org.mongodb.scala.ReadPreference$.primary]]
   *
   * @note Requires MongoDB 3.4 or greater
   * @since 2.2
   */
  val LINEARIZABLE: ReadConcernLevel = JReadConcernLevel.LINEARIZABLE

  /**
   * The snapshot read concern level.
   *
   * @note Requires MongoDB 4.0 or greater
   * @since 2.4
   */
  val SNAPSHOT: ReadConcernLevel = JReadConcernLevel.SNAPSHOT

  /**
   * The available read concern level.
   *
   * @note Requires MongoDB 4.0 or greater
   * @since 2.5
   */
  val AVAILABLE = JReadConcernLevel.AVAILABLE

  /**
   * Returns the ReadConcern from the string read concern level.
   *
   * @param readConcernLevel the read concern level string.
   * @return the read concern
   */
  def fromString(readConcernLevel: String): Try[ReadConcernLevel] = Try(JReadConcernLevel.fromString(readConcernLevel))
}
