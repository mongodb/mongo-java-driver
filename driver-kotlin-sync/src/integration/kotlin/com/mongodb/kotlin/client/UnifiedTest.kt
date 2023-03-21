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
package com.mongodb.kotlin.client

import com.mongodb.ClientEncryptionSettings
import com.mongodb.MongoClientSettings
import com.mongodb.client.MongoClient as JMongoClient
import com.mongodb.client.MongoDatabase as JMongoDatabase
import com.mongodb.client.gridfs.GridFSBucket
import com.mongodb.client.unified.UnifiedTest as JUnifiedTest
import com.mongodb.client.vault.ClientEncryption
import com.mongodb.kotlin.client.syncadapter.SyncMongoClient
import org.bson.BsonArray
import org.bson.BsonDocument

internal abstract class UnifiedTest(
    fileDescription: String?,
    schemaVersion: String,
    runOnRequirements: BsonArray?,
    entitiesArray: BsonArray,
    initialData: BsonArray,
    definition: BsonDocument
) : JUnifiedTest(fileDescription, schemaVersion, runOnRequirements, entitiesArray, initialData, definition) {

    override fun createMongoClient(settings: MongoClientSettings): JMongoClient =
        SyncMongoClient(MongoClient.create(settings))

    override fun createGridFSBucket(database: JMongoDatabase?): GridFSBucket {
        TODO("Not yet implemented - JAVA-4893")
    }

    override fun createClientEncryption(
        keyVaultClient: JMongoClient?,
        clientEncryptionSettings: ClientEncryptionSettings?
    ): ClientEncryption {
        TODO("Not yet implemented - JAVA-4896")
    }
}
