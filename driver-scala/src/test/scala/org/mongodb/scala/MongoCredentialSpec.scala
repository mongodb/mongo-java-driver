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

import com.mongodb.{MongoCredential => JMongoCredential}
import org.scalatest.{FlatSpec, Matchers}

class MongoCredentialSpec extends FlatSpec with Matchers {

  "MongoCredential" should "have the same methods as the wrapped MongoClient" in {
    val wrapped = classOf[JMongoCredential].getMethods.map(_.getName)
    val local = classOf[MongoCredential].getMethods.map(_.getName)

    wrapped.foreach((name: String) => {
      val cleanedName = name.stripPrefix("get")
      assert(local.contains(name) | local.contains(cleanedName.head.toLower + cleanedName.tail), s"Missing: $name")
    })
  }

  it should "create the expected credential" in {
    MongoCredential.createCredential("user", "source", "pass".toCharArray) should
      equal(JMongoCredential.createCredential("user", "source", "pass".toCharArray))
  }

  it should "create the expected createScramSha1Credential" in {
    MongoCredential.createScramSha1Credential("user", "source", "pass".toCharArray) should
      equal(JMongoCredential.createScramSha1Credential("user", "source", "pass".toCharArray))
  }

  it should "create the expected createScramSha256Credential" in {
    MongoCredential.createScramSha256Credential("user", "source", "pass".toCharArray) should
      equal(JMongoCredential.createScramSha256Credential("user", "source", "pass".toCharArray))
  }

  it should "create the expected createMongoX509Credential" in {
    MongoCredential.createMongoX509Credential() should equal(JMongoCredential.createMongoX509Credential())
    MongoCredential.createMongoX509Credential("user") should equal(JMongoCredential.createMongoX509Credential("user"))
  }

  it should "create the expected createPlainCredential" in {
    MongoCredential.createPlainCredential("user", "source", "pass".toCharArray) should
      equal(JMongoCredential.createPlainCredential("user", "source", "pass".toCharArray))
  }

  it should "create the expected createGSSAPICredential" in {
    MongoCredential.createGSSAPICredential("user") should equal(JMongoCredential.createGSSAPICredential("user"))
  }
}
