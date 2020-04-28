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

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import com.mongodb.{ CreateIndexCommitQuorum => JCreateIndexCommitQuorum }

/**
 * The commit quorum specifies how many data-bearing members of a replica set, including the primary, must
 * complete the index builds successfully before the primary marks the indexes as ready.
 *
 * @since 4.1
 */
object CreateIndexCommitQuorum {

  /**
   * A create index commit quorum of majority.
   */
  val MAJORITY: JCreateIndexCommitQuorum = JCreateIndexCommitQuorum.MAJORITY

  /**
   * A create index commit quorum of voting members.
   */
  val VOTING_MEMBERS: JCreateIndexCommitQuorum = JCreateIndexCommitQuorum.VOTING_MEMBERS

  /**
   * Create a create index commit quorum with a mode value.
   *
   * @param mode the mode value
   */
  def create(mode: String): JCreateIndexCommitQuorum = JCreateIndexCommitQuorum.create(mode)

  /**
   * Create a create index commit quorum with a w value.
   *
   * @param w the w value
   */
  def create(w: Int): JCreateIndexCommitQuorum = JCreateIndexCommitQuorum.create(w)
}
