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

package org.mongodb.scala.syncadapter

import com.mongodb.ClusterFixture.TIMEOUT_DURATION
import com.mongodb.client.model.{ CreateCollectionOptions, CreateEncryptedCollectionParams }
import com.mongodb.client.model.vault.{
  DataKeyOptions,
  EncryptOptions,
  RewrapManyDataKeyOptions,
  RewrapManyDataKeyResult
}
import com.mongodb.client.result.DeleteResult
import com.mongodb.client.vault.{ ClientEncryption => JClientEncryption }
import com.mongodb.client.{ MongoDatabase => JMongoDatabase }
import org.bson.{ BsonBinary, BsonDocument, BsonValue }
import org.bson.conversions.Bson
import org.mongodb.scala.vault.ClientEncryption
import reactor.core.publisher.Mono

import java.util.Objects.requireNonNull

case class SyncClientEncryption(wrapped: ClientEncryption) extends JClientEncryption {

  override def createDataKey(kmsProvider: String): BsonBinary =
    requireNonNull(Mono.from(wrapped.createDataKey(kmsProvider, new DataKeyOptions)).block(TIMEOUT_DURATION))

  override def createDataKey(kmsProvider: String, dataKeyOptions: DataKeyOptions): BsonBinary =
    requireNonNull(Mono.from(wrapped.createDataKey(kmsProvider, dataKeyOptions)).block(TIMEOUT_DURATION))

  override def encrypt(value: BsonValue, options: EncryptOptions): BsonBinary =
    requireNonNull(Mono.from(wrapped.encrypt(value, options)).block(TIMEOUT_DURATION))

  override def encryptExpression(expression: Bson, options: EncryptOptions): BsonDocument =
    requireNonNull(Mono.from(wrapped
      .encryptExpression(expression.toBsonDocument, options)).block(TIMEOUT_DURATION).toBsonDocument)

  override def decrypt(value: BsonBinary): BsonValue =
    requireNonNull(Mono.from(wrapped.decrypt(value)).block(TIMEOUT_DURATION))

  override def deleteKey(id: BsonBinary): DeleteResult =
    requireNonNull(Mono.from(wrapped.deleteKey(id)).block(TIMEOUT_DURATION))

  override def getKey(id: BsonBinary): BsonDocument = Mono.from(wrapped.getKey(id)).block(TIMEOUT_DURATION)

  override def getKeys = new SyncFindIterable[BsonDocument](wrapped.keys)

  override def addKeyAltName(id: BsonBinary, keyAltName: String): BsonDocument =
    Mono.from(wrapped.addKeyAltName(id, keyAltName)).block(TIMEOUT_DURATION)

  override def removeKeyAltName(id: BsonBinary, keyAltName: String): BsonDocument =
    Mono.from(wrapped.removeKeyAltName(id, keyAltName)).block(TIMEOUT_DURATION)

  override def getKeyByAltName(keyAltName: String): BsonDocument =
    Mono.from(wrapped.getKeyByAltName(keyAltName)).block(TIMEOUT_DURATION)

  override def rewrapManyDataKey(filter: Bson): RewrapManyDataKeyResult =
    requireNonNull(Mono.from(wrapped.rewrapManyDataKey(filter)).block(TIMEOUT_DURATION))

  override def rewrapManyDataKey(filter: Bson, options: RewrapManyDataKeyOptions): RewrapManyDataKeyResult =
    requireNonNull(Mono.from(wrapped.rewrapManyDataKey(filter, options)).block(TIMEOUT_DURATION))

  override def createEncryptedCollection(
      database: JMongoDatabase,
      collectionName: String,
      createCollectionOptions: CreateCollectionOptions,
      createEncryptedCollectionParams: CreateEncryptedCollectionParams
  ): BsonDocument = {
    database match {
      case syncMongoDatabase: SyncMongoDatabase =>
        requireNonNull(Mono.from(wrapped.createEncryptedCollection(
          syncMongoDatabase.wrapped,
          collectionName,
          createCollectionOptions,
          createEncryptedCollectionParams
        )).block(TIMEOUT_DURATION))
      case _ => throw new AssertionError(s"Unexpected database type: ${database.getClass}")
    }
  }

  override def close(): Unit = {
    wrapped.close()
  }
}
