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

import java.lang.reflect.Modifier.isStatic

import scala.collection.JavaConverters._

import org.scalatest.{ FlatSpec, Matchers }

class CreateIndexCommitQuorumSpec extends BaseSpec {

  "CreateIndexCommitQuorum" should "have the same methods as the wrapped CreateIndexCommitQuorum" in {
    val wrapped =
      classOf[com.mongodb.CreateIndexCommitQuorum].getDeclaredMethods
        .filter(f => isStatic(f.getModifiers))
        .map(_.getName)
        .toSet ++
        classOf[com.mongodb.CreateIndexCommitQuorum].getDeclaredFields
          .filter(f => isStatic(f.getModifiers))
          .map(_.getName)
          .toSet
    val local = CreateIndexCommitQuorum.getClass.getDeclaredMethods.map(_.getName).toSet

    local should equal(wrapped)
  }

  it should "return the correct create index commit quorum for majority" in {
    val commitQuorumMajority = CreateIndexCommitQuorum.MAJORITY
    commitQuorumMajority shouldBe com.mongodb.CreateIndexCommitQuorum.MAJORITY
  }

  it should "return the correct create index commit quorum for voting members" in {
    val commitQuorumVotingMembers = CreateIndexCommitQuorum.VOTING_MEMBERS
    commitQuorumVotingMembers shouldBe com.mongodb.CreateIndexCommitQuorum.VOTING_MEMBERS
  }

  it should "return the correct create index commit quorum with a mode" in {
    val commitQuorumMode = CreateIndexCommitQuorum.create("majority")
    commitQuorumMode shouldBe com.mongodb.CreateIndexCommitQuorum.create("majority")
  }

  it should "return the correct create index commit quorum with a w value" in {
    val commitQuorumW = CreateIndexCommitQuorum.create(2)
    commitQuorumW shouldBe com.mongodb.CreateIndexCommitQuorum.create(2)
  }
}
