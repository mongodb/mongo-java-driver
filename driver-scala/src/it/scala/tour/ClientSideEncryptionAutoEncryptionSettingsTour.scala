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

package tour

import java.security.SecureRandom
import java.util.Base64

import scala.collection.JavaConverters._
import org.mongodb.scala._
import org.mongodb.scala.bson.BsonDocument
import org.mongodb.scala.model.vault.DataKeyOptions
import org.mongodb.scala.vault.ClientEncryptions
import tour.Helpers._

/**
 * ClientSideEncryption AutoEncryptionSettings tour
 */
object ClientSideEncryptionAutoEncryptionSettingsTour {

  /**
   * Run this main method to see the output of this quick example.
   *
   * Requires the mongodb-crypt library in the class path and mongocryptd on the system path.
   *
   * @param args ignored args
   */
  def main(args: Array[String]): Unit = { // This would have to be the same master key as was used to create the encryption key
    val localMasterKey = new Array[Byte](96)
    new SecureRandom().nextBytes(localMasterKey)

    val kmsProviders = Map("local" -> Map[String, AnyRef]("key" -> localMasterKey).asJava).asJava

    val keyVaultNamespace = "admin.datakeys"

    val clientEncryptionSettings = ClientEncryptionSettings
      .builder()
      .keyVaultMongoClientSettings(
        MongoClientSettings.builder().applyConnectionString(ConnectionString("mongodb://localhost")).build()
      )
      .keyVaultNamespace(keyVaultNamespace)
      .kmsProviders(kmsProviders)
      .build()

    val clientEncryption = ClientEncryptions.create(clientEncryptionSettings)

    val dataKey = clientEncryption.createDataKey("local", DataKeyOptions()).headResult()

    val base64DataKeyId = Base64.getEncoder.encodeToString(dataKey.getData)
    val dbName = "test"
    val collName = "coll"
    val autoEncryptionSettings = AutoEncryptionSettings
      .builder()
      .keyVaultNamespace(keyVaultNamespace)
      .kmsProviders(kmsProviders)
      .schemaMap(Map(s"$dbName.$collName" -> BsonDocument(s"""{
            properties: {
              encryptedField: {
                encrypt: {
                  keyId: [{
                    "$$binary": {
                      "base64": "$base64DataKeyId",
                      "subType": "04"
                    }
                  }],
                  bsonType: "string",
                  algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                }
              }
            },
            bsonType: "object"
          }""")).asJava)
      .build()

    val clientSettings = MongoClientSettings.builder().autoEncryptionSettings(autoEncryptionSettings).build()
    val mongoClient = MongoClient(clientSettings)
    val collection = mongoClient.getDatabase("test").getCollection("coll")

    collection.drop().headResult()

    collection.insertOne(Document("encryptedField" -> "123456789")).headResult()

    collection.find().first().printHeadResult()

    // release resources
    mongoClient.close()
  }
}
