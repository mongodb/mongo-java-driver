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

import java.security.SecureRandom

import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.BsonString
import org.mongodb.scala.model.vault.{ DataKeyOptions, EncryptOptions }
import org.mongodb.scala.vault.ClientEncryptions

import scala.collection.JavaConverters._

class ClientSideEncryptionBypassAutoEncryptionSpec extends RequiresMongoDBISpec with FuturesSpec {

  "ClientSideEncryption" should "be able to bypass auto encryption" in withDatabase { db =>
    assume(serverVersionAtLeast(List(4, 1, 0)))

    val localMasterKey = new Array[Byte](96)
    new SecureRandom().nextBytes(localMasterKey)

    val kmsProviders = Map("local" -> Map[String, AnyRef]("key" -> localMasterKey).asJava).asJava

    val keyVaultNamespace: MongoNamespace = new MongoNamespace(databaseName, "testKeyVault")

    db.drop().futureValue

    val clientEncryptionSettings: ClientEncryptionSettings = ClientEncryptionSettings
      .builder()
      .keyVaultMongoClientSettings(mongoClientSettings)
      .keyVaultNamespace(keyVaultNamespace.getFullName)
      .kmsProviders(kmsProviders)
      .build()

    val clientEncryption = ClientEncryptions.create(clientEncryptionSettings)

    val autoEncryptionSettings: AutoEncryptionSettings = AutoEncryptionSettings
      .builder()
      .keyVaultNamespace(keyVaultNamespace.getFullName)
      .kmsProviders(kmsProviders)
      .bypassAutoEncryption(true)
      .build()

    val clientSettings: MongoClientSettings = mongoClientSettingsBuilder
      .autoEncryptionSettings(autoEncryptionSettings)
      .codecRegistry(DEFAULT_CODEC_REGISTRY)
      .build

    withTempClient(
      clientSettings,
      clientEncrypted => {

        val fieldValue = BsonString("123456789")

        val dataKeyId = clientEncryption.createDataKey("local", DataKeyOptions()).head().futureValue

        val encryptedFieldValue = clientEncryption
          .encrypt(fieldValue, EncryptOptions("AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic").keyId(dataKeyId))
          .head()
          .futureValue

        val collection: MongoCollection[Document] =
          clientEncrypted.getDatabase(databaseName).getCollection[Document]("test")

        collection.insertOne(Document("encryptedField" -> encryptedFieldValue)).futureValue

        val result = collection.find().first().head().futureValue

        result.get[BsonString]("encryptedField") should equal(Some(fieldValue))

      }
    )
  }

}
