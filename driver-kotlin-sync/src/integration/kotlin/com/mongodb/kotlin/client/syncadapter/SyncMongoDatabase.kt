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

import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.WriteConcern
import com.mongodb.client.AggregateIterable
import com.mongodb.client.ChangeStreamIterable
import com.mongodb.client.ClientSession
import com.mongodb.client.ListCollectionsIterable
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase as JMongoDatabase
import com.mongodb.client.MongoIterable
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.client.model.CreateViewOptions
import com.mongodb.kotlin.client.MongoDatabase
import org.bson.Document
import org.bson.codecs.configuration.CodecRegistry
import org.bson.conversions.Bson

data class SyncMongoDatabase(val wrapped: MongoDatabase) : JMongoDatabase {
    override fun getName(): String = wrapped.name

    override fun getCodecRegistry(): CodecRegistry = wrapped.codecRegistry

    override fun getReadPreference(): ReadPreference = wrapped.readPreference

    override fun getWriteConcern(): WriteConcern = wrapped.writeConcern

    override fun getReadConcern(): ReadConcern = wrapped.readConcern

    override fun withCodecRegistry(codecRegistry: CodecRegistry): SyncMongoDatabase =
        SyncMongoDatabase(wrapped.withCodecRegistry(codecRegistry))

    override fun withReadPreference(readPreference: ReadPreference): SyncMongoDatabase =
        SyncMongoDatabase(wrapped.withReadPreference(readPreference))

    override fun withWriteConcern(writeConcern: WriteConcern): SyncMongoDatabase =
        SyncMongoDatabase(wrapped.withWriteConcern(writeConcern))

    override fun withReadConcern(readConcern: ReadConcern): SyncMongoDatabase =
        SyncMongoDatabase(wrapped.withReadConcern(readConcern))

    override fun getCollection(collectionName: String): MongoCollection<Document> =
        SyncMongoCollection(wrapped.getCollection(collectionName, Document::class.java))

    override fun <T : Any> getCollection(collectionName: String, documentClass: Class<T>): MongoCollection<T> =
        SyncMongoCollection(wrapped.getCollection(collectionName, documentClass))

    override fun runCommand(command: Bson): Document = wrapped.runCommand(command)

    override fun runCommand(command: Bson, readPreference: ReadPreference): Document =
        wrapped.runCommand(command, readPreference)

    override fun <T : Any> runCommand(command: Bson, resultClass: Class<T>): T =
        wrapped.runCommand(command, resultClass = resultClass)

    override fun <T : Any> runCommand(command: Bson, readPreference: ReadPreference, resultClass: Class<T>): T =
        wrapped.runCommand(command, readPreference, resultClass)

    override fun runCommand(clientSession: ClientSession, command: Bson): Document =
        wrapped.runCommand(clientSession.unwrapped(), command)

    override fun runCommand(clientSession: ClientSession, command: Bson, readPreference: ReadPreference): Document =
        wrapped.runCommand(clientSession.unwrapped(), command, readPreference)

    override fun <T : Any> runCommand(clientSession: ClientSession, command: Bson, resultClass: Class<T>): T =
        wrapped.runCommand(clientSession.unwrapped(), command, resultClass = resultClass)

    override fun <T : Any> runCommand(
        clientSession: ClientSession,
        command: Bson,
        readPreference: ReadPreference,
        resultClass: Class<T>
    ): T = wrapped.runCommand(clientSession.unwrapped(), command, readPreference, resultClass)

    override fun drop() = wrapped.drop()

    override fun drop(clientSession: ClientSession) = wrapped.drop(clientSession.unwrapped())

    override fun listCollectionNames(): MongoIterable<String> = SyncMongoIterable(wrapped.listCollectionNames())

    override fun listCollectionNames(clientSession: ClientSession): MongoIterable<String> =
        SyncMongoIterable(wrapped.listCollectionNames(clientSession.unwrapped()))

    override fun listCollections(): ListCollectionsIterable<Document> =
        SyncListCollectionsIterable(wrapped.listCollections())

    override fun <T : Any> listCollections(resultClass: Class<T>): ListCollectionsIterable<T> =
        SyncListCollectionsIterable(wrapped.listCollections(resultClass))

    override fun listCollections(clientSession: ClientSession): ListCollectionsIterable<Document> =
        SyncListCollectionsIterable(wrapped.listCollections(clientSession.unwrapped()))

    override fun <T : Any> listCollections(
        clientSession: ClientSession,
        resultClass: Class<T>
    ): ListCollectionsIterable<T> =
        SyncListCollectionsIterable(wrapped.listCollections(clientSession.unwrapped(), resultClass))

    override fun createCollection(collectionName: String) = wrapped.createCollection(collectionName)

    override fun createCollection(collectionName: String, createCollectionOptions: CreateCollectionOptions) =
        wrapped.createCollection(collectionName, createCollectionOptions)

    override fun createCollection(clientSession: ClientSession, collectionName: String) =
        wrapped.createCollection(clientSession.unwrapped(), collectionName)

    override fun createCollection(
        clientSession: ClientSession,
        collectionName: String,
        createCollectionOptions: CreateCollectionOptions
    ) = wrapped.createCollection(clientSession.unwrapped(), collectionName, createCollectionOptions)

    override fun createView(viewName: String, viewOn: String, pipeline: MutableList<out Bson>) =
        wrapped.createView(viewName, viewOn, pipeline)

    override fun createView(
        viewName: String,
        viewOn: String,
        pipeline: MutableList<out Bson>,
        createViewOptions: CreateViewOptions
    ) = wrapped.createView(viewName, viewOn, pipeline, createViewOptions)

    override fun createView(
        clientSession: ClientSession,
        viewName: String,
        viewOn: String,
        pipeline: MutableList<out Bson>
    ) = wrapped.createView(clientSession.unwrapped(), viewName, viewOn, pipeline)

    override fun createView(
        clientSession: ClientSession,
        viewName: String,
        viewOn: String,
        pipeline: MutableList<out Bson>,
        createViewOptions: CreateViewOptions
    ) = wrapped.createView(clientSession.unwrapped(), viewName, viewOn, pipeline, createViewOptions)

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

    override fun aggregate(pipeline: MutableList<out Bson>): AggregateIterable<Document> =
        SyncAggregateIterable(wrapped.aggregate(pipeline))

    override fun <T : Any> aggregate(pipeline: MutableList<out Bson>, resultClass: Class<T>): AggregateIterable<T> =
        SyncAggregateIterable(wrapped.aggregate(pipeline, resultClass))

    override fun aggregate(clientSession: ClientSession, pipeline: MutableList<out Bson>): AggregateIterable<Document> =
        SyncAggregateIterable(wrapped.aggregate(clientSession.unwrapped(), pipeline))

    override fun <T : Any> aggregate(
        clientSession: ClientSession,
        pipeline: MutableList<out Bson>,
        resultClass: Class<T>
    ): AggregateIterable<T> = SyncAggregateIterable(wrapped.aggregate(clientSession.unwrapped(), pipeline, resultClass))

    private fun ClientSession.unwrapped() = (this as SyncClientSession).wrapped
}
