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

import com.mongodb.MongoNamespace
import org.mongodb.scala._
import org.mongodb.scala.bson.{ BsonBinary, BsonString }
import org.mongodb.scala.model.vault.{ DataKeyOptions, EncryptOptions }
import org.mongodb.scala.model.{ Filters, IndexOptions, Indexes }
import org.mongodb.scala.vault.ClientEncryptions
import tour.Helpers._

import scala.collection.JavaConverters._

/**
 * ClientSideEncryption explicit encryption and decryption tour
 */
object ClientSideEncryptionExplicitEncryptionAndDecryptionTour {

  /**
   * Run this main method to see the output of this quick example.
   *
   * @param args ignored args
   */
  def main(args: Array[String]): Unit = {

    // This would have to be the same master key as was used to create the encryption key
    val localMasterKey = new Array[Byte](96)
    new SecureRandom().nextBytes(localMasterKey)

    val kmsProviders = Map("local" -> Map[String, AnyRef]("key" -> localMasterKey).asJava).asJava

    val keyVaultNamespace = new MongoNamespace("encryption.testKeyVault")

    val clientSettings = MongoClientSettings.builder().build()
    val mongoClient = MongoClient(clientSettings)

    // Set up the key vault for this example
    val keyVaultCollection =
      mongoClient.getDatabase(keyVaultNamespace.getDatabaseName).getCollection(keyVaultNamespace.getCollectionName)
    keyVaultCollection.drop().headResult()

    // Ensure that two data keys cannot share the same keyAltName.
    keyVaultCollection.createIndex(
      Indexes.ascending("keyAltNames"),
      new IndexOptions()
        .unique(true)
        .partialFilterExpression(Filters.exists("keyAltNames"))
    )

    val collection = mongoClient.getDatabase("test").getCollection("coll")
    collection.drop().headResult()

    // Create the ClientEncryption instance
    val clientEncryptionSettings = ClientEncryptionSettings
      .builder()
      .keyVaultMongoClientSettings(
        MongoClientSettings.builder().applyConnectionString(ConnectionString("mongodb://localhost")).build()
      )
      .keyVaultNamespace(keyVaultNamespace.getFullName)
      .kmsProviders(kmsProviders)
      .build()

    val clientEncryption = ClientEncryptions.create(clientEncryptionSettings)

    val dataKeyId = clientEncryption.createDataKey("local", DataKeyOptions()).headResult()

    // Explicitly encrypt a field
    val encryptedFieldValue = clientEncryption
      .encrypt(BsonString("123456789"), EncryptOptions("AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic").keyId(dataKeyId))
      .headResult()

    collection.insertOne(Document("encryptedField" -> encryptedFieldValue)).headResult()

    val doc = collection.find.first().headResult()
    println(doc.toJson())

    // Explicitly decrypt the field
    println(clientEncryption.decrypt(doc.get[BsonBinary]("encryptedField").get).headResult())

    // release resources
    clientEncryption.close()
    mongoClient.close()
  }
}
