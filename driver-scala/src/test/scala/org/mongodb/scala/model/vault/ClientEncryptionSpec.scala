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

import com.mongodb.client.model.CreateEncryptedCollectionParams

import com.mongodb.reactivestreams.client.vault.{ ClientEncryption => JClientEncryption }
import org.mockito.ArgumentMatchers.{ any, same }
import org.mockito.Mockito.verify
import org.mongodb.scala.{ BaseSpec, MongoDatabase }
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.bson.{ BsonBinary, BsonString }
import org.mongodb.scala.model.CreateCollectionOptions
import org.mongodb.scala.vault.ClientEncryption
import org.scalatestplus.mockito.MockitoSugar

import java.lang.reflect.Modifier.{ isPublic, isStatic }

class ClientEncryptionSpec extends BaseSpec with MockitoSugar {

  val wrapped = mock[JClientEncryption]
  val clientEncryption = ClientEncryption(wrapped)

  "ClientEncryption" should "have the same methods as the wrapped Filters" in {
    val wrapped = classOf[JClientEncryption].getDeclaredMethods.map(_.getName).toSet
    val local = classOf[ClientEncryption].getDeclaredMethods.map(_.getName).toSet

    wrapped.foreach((name: String) => {
      val cleanedName = name.stripPrefix("get")
      assert(local.contains(name) || local.contains(cleanedName.head.toLower + cleanedName.tail), s"Missing: $name")
    })
  }

  it should "call createDataKey" in {
    val kmsProvider = "kmsProvider"
    val options = DataKeyOptions()

    clientEncryption.createDataKey(kmsProvider)
    verify(wrapped).createDataKey(same(kmsProvider), any())

    clientEncryption.createDataKey(kmsProvider, options)
    verify(wrapped).createDataKey(kmsProvider, options)
  }

  it should "call getKey" in {
    val bsonBinary = BsonBinary(Array[Byte](1, 2, 3))

    clientEncryption.getKey(bsonBinary)
    verify(wrapped).getKey(same(bsonBinary))
  }

  it should "call getKeyByAltName" in {
    val altKeyName = "altKeyName"

    clientEncryption.getKeyByAltName(altKeyName)
    verify(wrapped).getKeyByAltName(same(altKeyName))
  }

  it should "call getKeys" in {
    clientEncryption.keys
    verify(wrapped).getKeys
  }

  it should "call addKeyAltName" in {
    val bsonBinary = BsonBinary(Array[Byte](1, 2, 3))
    val altKeyName = "altKeyName"

    clientEncryption.addKeyAltName(bsonBinary, altKeyName)
    verify(wrapped).addKeyAltName(same(bsonBinary), same(altKeyName))
  }

  it should "call deleteKey" in {
    val bsonBinary = BsonBinary(Array[Byte](1, 2, 3))

    clientEncryption.deleteKey(bsonBinary)
    verify(wrapped).deleteKey(same(bsonBinary))
  }

  it should "call removeKeyAltName" in {
    val bsonBinary = BsonBinary(Array[Byte](1, 2, 3))
    val altKeyName = "altKeyName"

    clientEncryption.removeKeyAltName(bsonBinary, altKeyName)
    verify(wrapped).removeKeyAltName(same(bsonBinary), same(altKeyName))
  }

  it should "call rewrapManyDataKey" in {
    val bsonDocument = Document()
    val options = RewrapManyDataKeyOptions()

    clientEncryption.rewrapManyDataKey(bsonDocument)
    verify(wrapped).rewrapManyDataKey(same(bsonDocument))

    clientEncryption.rewrapManyDataKey(bsonDocument, options)
    verify(wrapped).rewrapManyDataKey(same(bsonDocument), same(options))
  }

  it should "call encrypt" in {
    val bsonValue = BsonString("")
    val options = EncryptOptions("algorithm")
    clientEncryption.encrypt(bsonValue, options)

    verify(wrapped).encrypt(bsonValue, options)
  }

  it should "call encrypt Expression" in {
    val bsonDocument = Document()
    val options = EncryptOptions("algorithm").rangeOptions(RangeOptions())
    clientEncryption.encryptExpression(bsonDocument, options)

    verify(wrapped).encryptExpression(bsonDocument.toBsonDocument, options)
  }

  it should "call decrypt" in {
    val bsonBinary = BsonBinary(Array[Byte](1, 2, 3))
    clientEncryption.decrypt(bsonBinary)

    verify(wrapped).decrypt(bsonBinary)
  }

  it should "call createEncryptedCollection" in {
    val database = mock[MongoDatabase]
    val collectionName = "collectionName"
    val createCollectionOptions = new CreateCollectionOptions()
    val createEncryptedCollectionParams = new CreateEncryptedCollectionParams("kmsProvider")
    clientEncryption.createEncryptedCollection(
      database,
      collectionName,
      createCollectionOptions,
      createEncryptedCollectionParams
    )
    verify(wrapped).createEncryptedCollection(
      same(database.wrapped),
      same(collectionName),
      same(createCollectionOptions),
      same(createEncryptedCollectionParams)
    )
  }

  it should "call close" in {
    clientEncryption.close()

    verify(wrapped).close()
  }
}
