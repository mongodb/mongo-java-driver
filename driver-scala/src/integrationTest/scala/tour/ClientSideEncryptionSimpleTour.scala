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

import org.mongodb.scala.{ AutoEncryptionSettings, Document, MongoClient, MongoClientSettings }
import tour.Helpers._

import scala.collection.JavaConverters._

/**
 * ClientSideEncryption Simple tour
 */
object ClientSideEncryptionSimpleTour {

  /**
   * Run this main method to see the output of this quick example.
   *
   * Requires the mongodb-crypt library in the class path and mongocryptd on the system path.
   *
   * @param args ignored args
   */
  def main(args: Array[String]): Unit = {
    val localMasterKey = new Array[Byte](96)
    new SecureRandom().nextBytes(localMasterKey)

    val kmsProviders = Map("local" -> Map[String, AnyRef]("key" -> localMasterKey).asJava).asJava

    val keyVaultNamespace = "admin.datakeys"

    val autoEncryptionSettings = AutoEncryptionSettings
      .builder()
      .keyVaultNamespace(keyVaultNamespace)
      .kmsProviders(kmsProviders)
      .build()

    val clientSettings = MongoClientSettings
      .builder()
      .autoEncryptionSettings(autoEncryptionSettings)
      .build()

    val mongoClient = MongoClient(clientSettings)
    val collection = mongoClient.getDatabase("test").getCollection("coll")

    collection.drop().headResult()

    collection.insertOne(Document("encryptedField" -> "123456789")).headResult()

    collection.find().first().printHeadResult()

    // release resources
    mongoClient.close()
  }
}
