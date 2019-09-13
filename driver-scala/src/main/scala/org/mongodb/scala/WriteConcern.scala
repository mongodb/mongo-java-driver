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

import com.mongodb.{ WriteConcern => JWriteConcern }

/**
 * Controls the acknowledgment of write operations with various options.
 *
 * ==`w`==
 * - 0: Don't wait for acknowledgement from the server
 * - 1: Wait for acknowledgement, but don't wait for secondaries to replicate
 * - >=2: Wait for one or more secondaries to also acknowledge
 * - "majority": Wait for a majority of secondaries to also acknowledge
 * - "<tag set name>": Wait for one or more secondaries to also acknowledge based on a tag set name
 *
 * ==`wTimeout` - how long to wait for slaves before failing ==
 * - 0: indefinite
 * - >0: time to wait in milliseconds
 *
 * ==Other options:==
 *
 * - `journal`: If true block until write operations have been committed to the journal. Cannot be used in combination with `fsync`.
 * Prior to MongoDB 2.6 this option was ignored if the server was running without journaling.  Starting with MongoDB 2.6
 * write operations will fail with an exception if this option is used when the server is running without journaling.
 *
 * == Implicit helper ==
 *
 * The [[ScalaWriteConcern]] implicit allows for chainable building of the WriteConcern eg:
 *
 * {{{
 *  val myWriteConcern = WriteConcern.ACKNOWLEDGED.withJournal(true)).withWTimeout(Duration(10, TimeUnit.MILLISECONDS))
 * }}}
 *
 * @since 1.0
 */
object WriteConcern {

  /**
   * Write operations that use this write concern will wait for acknowledgement from the primary server before returning. Exceptions are
   * raised for network issues, and server errors.
   */
  val ACKNOWLEDGED: JWriteConcern = JWriteConcern.ACKNOWLEDGED

  /**
   * Write operations that use this write concern will wait for acknowledgement from a single member.
   */
  val W1: JWriteConcern = apply(1)

  /**
   * Write operations that use this write concern will wait for acknowledgement from two members.
   */
  val W2: JWriteConcern = apply(2)

  /**
   * Write operations that use this write concern will wait for acknowledgement from three members.
   */
  val W3: JWriteConcern = apply(3)

  /**
   * Write operations that use this write concern will return as soon as the message is written to the socket. Exceptions are raised for
   * network issues, but not server errors.
   */
  val UNACKNOWLEDGED: JWriteConcern = JWriteConcern.UNACKNOWLEDGED

  /**
   * Exceptions are raised for network issues, and server errors; the write operation waits for the server to group commit to the journal
   * file on disk.
   */
  val JOURNALED: JWriteConcern = JWriteConcern.JOURNALED

  /**
   * Exceptions are raised for network issues, and server errors; waits on a majority of servers for the write operation.
   */
  val MAJORITY: JWriteConcern = JWriteConcern.MAJORITY

  /**
   * Create a WriteConcern with the set number of acknowledged writes before returning
   *
   * @param w number of writes
   */
  def apply(w: Int): JWriteConcern = new JWriteConcern(w)

  /**
   * Tag set named write concern or a "majority" write concern.
   *
   * @param w Write Concern tag set name or "majority", representing the servers to ensure write propagation to before acknowledgment.
   *          Do not use string representation of integer values for w.
   */
  def apply(w: String): JWriteConcern = new JWriteConcern(w)

}
