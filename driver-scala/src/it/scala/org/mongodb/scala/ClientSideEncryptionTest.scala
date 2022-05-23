/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mongodb.scala

import com.mongodb.client.AbstractClientSideEncryptionTest
import org.bson.{ BsonArray, BsonDocument }
import org.junit.After
import org.mongodb.scala.syncadapter.SyncMongoClient

class ClientSideEncryptionTest(
    val filename: String,
    val description: String,
    val specDocument: BsonDocument,
    val data: BsonArray,
    val definition: BsonDocument,
    val skipTest: Boolean
) extends AbstractClientSideEncryptionTest(filename, description, specDocument, data, definition, skipTest) {
  private var mongoClient: SyncMongoClient = _

  override protected def createMongoClient(mongoClientSettings: MongoClientSettings): Unit = {
    mongoClient = SyncMongoClient(MongoClient(mongoClientSettings))
  }

  override protected def getDatabase(databaseName: String): com.mongodb.client.MongoDatabase =
    mongoClient.getDatabase(databaseName)

  @After
  def cleanUp(): Unit = {
    if (mongoClient != null) mongoClient.close()
  }
}
