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

package org.mongodb.scala.unified

import com.mongodb.client.gridfs.{ GridFSBucket => JGridFSBucket }
import com.mongodb.client.unified.UnifiedTest.Language
import com.mongodb.client.unified.{ UnifiedTest, UnifiedTest => JUnifiedTest }
import com.mongodb.client.vault.{ ClientEncryption => JClientEncryption }
import com.mongodb.client.{ MongoClient => JMongoClient, MongoDatabase => JMongoDatabase }
import com.mongodb.reactivestreams.client.internal.vault.ClientEncryptionImpl
import com.mongodb.{ ClientEncryptionSettings => JClientEncryptionSettings, MongoClientSettings }
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.params.provider.Arguments
import org.mongodb.scala.MongoClient
import org.mongodb.scala.MongoClient.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.syncadapter.{ SyncClientEncryption, SyncMongoClient }
import org.mongodb.scala.vault.ClientEncryption

import java.util

@TestInstance(Lifecycle.PER_CLASS)
abstract class UnifiedTest extends JUnifiedTest {

  val directory: String

  def data(): util.Collection[Arguments] = JUnifiedTest.getTestData(directory, true, Language.SCALA)

  override def createMongoClient(settings: MongoClientSettings): JMongoClient =
    SyncMongoClient(MongoClient(MongoClientSettings.builder(settings).codecRegistry(DEFAULT_CODEC_REGISTRY).build()))

  override def createGridFSBucket(database: JMongoDatabase): JGridFSBucket =
    throw new NotImplementedError("Not implemented")

  override def createClientEncryption(
      keyVaultClient: JMongoClient,
      clientEncryptionSettings: JClientEncryptionSettings
  ): JClientEncryption = {
    keyVaultClient match {
      case client: SyncMongoClient =>
        SyncClientEncryption(ClientEncryption(new ClientEncryptionImpl(
          client.wrapped.wrapped,
          clientEncryptionSettings
        )))
      case _ => throw new IllegalArgumentException(s"Invalid keyVaultClient type: ${keyVaultClient.getClass}")
    }
  }

  override protected def isReactive: Boolean = true

  override protected def getLanguage: Language = Language.SCALA
}
