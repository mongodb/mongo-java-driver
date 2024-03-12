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
package com.mongodb.kotlin.client.syncadapter

import com.mongodb.ClientSessionOptions
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.WriteConcern
import com.mongodb.client.ChangeStreamIterable
import com.mongodb.client.ClientSession
import com.mongodb.client.ListDatabasesIterable
import com.mongodb.client.MongoClientOperations as JMongoClientOperations
import com.mongodb.client.MongoDatabase
import com.mongodb.client.MongoIterable
import com.mongodb.kotlin.client.MongoClientOperations
import java.util.concurrent.TimeUnit
import org.bson.Document
import org.bson.codecs.configuration.CodecRegistry
import org.bson.conversions.Bson

internal open class SyncMongoClientOperations(open val wrapped: MongoClientOperations) : JMongoClientOperations {
    override fun getCodecRegistry(): CodecRegistry = wrapped.codecRegistry

    override fun getReadPreference(): ReadPreference = wrapped.readPreference

    override fun getWriteConcern(): WriteConcern = wrapped.writeConcern

    override fun getReadConcern(): ReadConcern = wrapped.readConcern

    override fun getTimeout(timeUnit: TimeUnit): Long? = wrapped.timeout(timeUnit)

    override fun withCodecRegistry(codecRegistry: CodecRegistry): SyncMongoClientOperations =
        SyncMongoClientOperations(wrapped.withCodecRegistry(codecRegistry))

    override fun withReadPreference(readPreference: ReadPreference): SyncMongoClientOperations =
        SyncMongoClientOperations(wrapped.withReadPreference(readPreference))

    override fun withReadConcern(readConcern: ReadConcern): SyncMongoClientOperations =
        SyncMongoClientOperations(wrapped.withReadConcern(readConcern))

    override fun withWriteConcern(writeConcern: WriteConcern): SyncMongoClientOperations =
        SyncMongoClientOperations(wrapped.withWriteConcern(writeConcern))

    override fun withTimeout(timeout: Long, timeUnit: TimeUnit): SyncMongoClientOperations =
        SyncMongoClientOperations(wrapped.withTimeout(timeout, timeUnit))

    override fun getDatabase(databaseName: String): MongoDatabase = SyncMongoDatabase(wrapped.getDatabase(databaseName))

    override fun startSession(): ClientSession = SyncClientSession(wrapped.startSession(), this)

    override fun startSession(options: ClientSessionOptions): ClientSession =
        SyncClientSession(wrapped.startSession(options), this)

    override fun listDatabaseNames(): MongoIterable<String> = SyncMongoIterable(wrapped.listDatabaseNames())

    override fun listDatabaseNames(clientSession: ClientSession): MongoIterable<String> =
        SyncMongoIterable(wrapped.listDatabaseNames(clientSession.unwrapped()))

    override fun listDatabases(): ListDatabasesIterable<Document> = SyncListDatabasesIterable(wrapped.listDatabases())

    override fun listDatabases(clientSession: ClientSession): ListDatabasesIterable<Document> =
        SyncListDatabasesIterable(wrapped.listDatabases(clientSession.unwrapped()))

    override fun <T : Any> listDatabases(resultClass: Class<T>): ListDatabasesIterable<T> =
        SyncListDatabasesIterable(wrapped.listDatabases(resultClass))

    override fun <T : Any> listDatabases(
        clientSession: ClientSession,
        resultClass: Class<T>
    ): ListDatabasesIterable<T> =
        SyncListDatabasesIterable(wrapped.listDatabases(clientSession.unwrapped(), resultClass))

    override fun watch(): ChangeStreamIterable<Document> = SyncChangeStreamIterable(wrapped.watch())

    override fun <T : Any> watch(resultClass: Class<T>): ChangeStreamIterable<T> =
        SyncChangeStreamIterable(wrapped.watch(resultClass = resultClass))

    override fun watch(pipeline: MutableList<out Bson>): ChangeStreamIterable<Document> =
        SyncChangeStreamIterable(wrapped.watch(pipeline))

    override fun <T : Any> watch(pipeline: MutableList<out Bson>, resultClass: Class<T>): ChangeStreamIterable<T> =
        SyncChangeStreamIterable(wrapped.watch(pipeline, resultClass))

    override fun watch(clientSession: ClientSession): ChangeStreamIterable<Document> =
        SyncChangeStreamIterable(wrapped.watch(clientSession.unwrapped()))

    override fun <T : Any> watch(clientSession: ClientSession, resultClass: Class<T>): ChangeStreamIterable<T> =
        SyncChangeStreamIterable(wrapped.watch(clientSession.unwrapped(), resultClass = resultClass))

    override fun watch(clientSession: ClientSession, pipeline: MutableList<out Bson>): ChangeStreamIterable<Document> =
        SyncChangeStreamIterable(wrapped.watch(clientSession.unwrapped(), pipeline))

    override fun <T : Any> watch(
        clientSession: ClientSession,
        pipeline: MutableList<out Bson>,
        resultClass: Class<T>
    ): ChangeStreamIterable<T> =
        SyncChangeStreamIterable(wrapped.watch(clientSession.unwrapped(), pipeline, resultClass))

    private fun ClientSession.unwrapped() = (this as SyncClientSession).wrapped
}
