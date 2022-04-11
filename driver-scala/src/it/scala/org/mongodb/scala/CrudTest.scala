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

import com.mongodb.client.Fixture.getMongoClientSettingsBuilder
import com.mongodb.client.AbstractCrudTest
import com.mongodb.event.CommandListener
import org.bson.{ BsonArray, BsonDocument }
import org.mongodb.scala.syncadapter.SyncMongoClient

class CrudTest(
    val filename: String,
    val description: String,
    val databaseName: String,
    val collectionName: String,
    val data: BsonArray,
    val definition: BsonDocument,
    val skipTest: Boolean
) extends AbstractCrudTest(filename, description, databaseName, collectionName, data, definition, skipTest) {
  private var mongoClient: SyncMongoClient = null

  override protected def createMongoClient(commandListener: CommandListener): Unit = {
    mongoClient = SyncMongoClient(MongoClient(getMongoClientSettingsBuilder.addCommandListener(commandListener).build))
  }

  override protected def getDatabase(databaseName: String): com.mongodb.client.MongoDatabase =
    mongoClient.getDatabase(databaseName)

  override def cleanUp(): Unit = Option(mongoClient).foreach(_.close())
}
