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
package com.mongodb.kotlin.client.coroutine

import com.mongodb.client.AbstractCrudTest
import com.mongodb.client.Fixture
import com.mongodb.client.MongoDatabase
import com.mongodb.event.CommandListener
import com.mongodb.kotlin.client.coroutine.syncadapter.SyncMongoClient
import org.bson.BsonArray
import org.bson.BsonDocument

data class CrudTest(
    val filename: String,
    val description: String,
    val databaseName: String,
    val collectionName: String,
    val data: BsonArray,
    val definition: BsonDocument,
    val skipTest: Boolean
) : AbstractCrudTest(filename, description, databaseName, collectionName, data, definition, skipTest) {

    private var mongoClient: SyncMongoClient? = null

    override fun createMongoClient(commandListener: CommandListener) {
        mongoClient =
            SyncMongoClient(
                MongoClient.create(Fixture.getMongoClientSettingsBuilder().addCommandListener(commandListener).build()))
    }

    override fun getDatabase(databaseName: String): MongoDatabase = mongoClient!!.getDatabase(databaseName)

    override fun cleanUp() {
        mongoClient?.close()
    }
}
