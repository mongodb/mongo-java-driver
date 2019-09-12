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

package org.mongodb.scala.model.vault

import java.lang.reflect.Modifier.{isPublic, isStatic}

import com.mongodb.reactivestreams.client.vault.{ClientEncryption => JClientEncryption}
import org.mongodb.scala.bson.{BsonBinary, BsonString}
import org.mongodb.scala.vault.ClientEncryption
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{FlatSpec, Matchers}

class ClientEncryptionSpec extends FlatSpec with Matchers with MockFactory {

  val wrapped = mock[JClientEncryption]
  val clientEncryption = ClientEncryption(wrapped)

  "ClientEncryption" should "have the same methods as the wrapped Filters" in {
    val wrapped = classOf[JClientEncryption].getDeclaredMethods
      .filter(f => isStatic(f.getModifiers) && isPublic(f.getModifiers)).map(_.getName).toSet
    val ignore = Set("toString", "apply", "unapply")
    val local = ClientEncryption.getClass.getDeclaredMethods.filter(f => isPublic(f.getModifiers)).map(_.getName).toSet -- ignore

    local should equal(wrapped)
  }

  it should "call createDataKey" in {
    val kmsProvider = "kmsProvider"
    val options = DataKeyOptions()

    wrapped.expects(Symbol("createDataKey"))(kmsProvider, *).once()
    clientEncryption.createDataKey(kmsProvider)

    wrapped.expects(Symbol("createDataKey"))(kmsProvider, options).once()
    clientEncryption.createDataKey(kmsProvider, options)
  }

  it should "call encrypt" in {
    val bsonValue = BsonString("")
    val options = EncryptOptions("algorithm")
    wrapped.expects(Symbol("encrypt"))(bsonValue, options).once()

    clientEncryption.encrypt(bsonValue, options)
  }

  it should "call decrypt" in {
    val bsonBinary = BsonBinary(Array[Byte](1, 2, 3))
    wrapped.expects(Symbol("decrypt"))(bsonBinary).once()

    clientEncryption.decrypt(bsonBinary)
  }

}
