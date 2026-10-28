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

import com.mongodb.ClientBulkWriteException
import com.mongodb.ClientSessionOptions
import com.mongodb.MongoClientSettings
import com.mongodb.MongoException
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.WriteConcern
import com.mongodb.client.MongoCluster as JMongoCluster
import com.mongodb.client.model.bulk.ClientBulkWriteOptions
import com.mongodb.client.model.bulk.ClientBulkWriteResult
import com.mongodb.client.model.bulk.ClientNamespacedDeleteManyModel
import com.mongodb.client.model.bulk.ClientNamespacedUpdateManyModel
import com.mongodb.client.model.bulk.ClientNamespacedWriteModel
import org.bson.Document
import org.bson.codecs.configuration.CodecRegistry
import org.bson.conversions.Bson

/**
 * The client-side representation of a MongoDB cluster operations.
 *
 * The originating [MongoClient] is responsible for the closing of resources. If the originator [MongoClient] is closed,
 * then any operations will fail.
 *
 * @see MongoClient
 * @since 5.2
 */
public open class MongoCluster protected constructor(private val wrapped: JMongoCluster) {

    /** The codec registry. */
    public val codecRegistry: CodecRegistry
        get() = wrapped.codecRegistry

    /** The read concern. */
    public val readConcern: ReadConcern
        get() = wrapped.readConcern

    /** The read preference. */
    public val readPreference: ReadPreference
        get() = wrapped.readPreference

    /** The write concern. */
    public val writeConcern: WriteConcern
        get() = wrapped.writeConcern

    /**
     * Create a new MongoCluster instance with a different codec registry.
     *
     * The [CodecRegistry] configured by this method is effectively treated by the driver as an instance of
     * [org.bson.codecs.configuration.CodecProvider], which [CodecRegistry] extends. So there is no benefit to defining
     * a class that implements [CodecRegistry]. Rather, an application should always create [CodecRegistry] instances
     * using the factory methods in [org.bson.codecs.configuration.CodecRegistries].
     *
     * @param newCodecRegistry the new [org.bson.codecs.configuration.CodecRegistry] for the database
     * @return a new MongoCluster instance with the different codec registry
     * @see org.bson.codecs.configuration.CodecRegistries
     */
    public fun withCodecRegistry(newCodecRegistry: CodecRegistry): MongoCluster =
        MongoCluster(wrapped.withCodecRegistry(newCodecRegistry))

    /**
     * Create a new MongoCluster instance with a different read preference.
     *
     * @param newReadPreference the new [ReadPreference] for the database
     * @return a new MongoCluster instance with the different readPreference
     */
    public fun withReadPreference(newReadPreference: ReadPreference): MongoCluster =
        MongoCluster(wrapped.withReadPreference(newReadPreference))

    /**
     * Create a new MongoCluster instance with a different read concern.
     *
     * @param newReadConcern the new [ReadConcern] for the database
     * @return a new MongoCluster instance with the different ReadConcern
     * @see [Read Concern](https://www.mongodb.com/docs/manual/reference/readConcern/)
     */
    public fun withReadConcern(newReadConcern: ReadConcern): MongoCluster =
        MongoCluster(wrapped.withReadConcern(newReadConcern))

    /**
     * Create a new MongoCluster instance with a different write concern.
     *
     * @param newWriteConcern the new [WriteConcern] for the database
     * @return a new MongoCluster instance with the different writeConcern
     */
    public fun withWriteConcern(newWriteConcern: WriteConcern): MongoCluster =
        MongoCluster(wrapped.withWriteConcern(newWriteConcern))

    /**
     * Gets a [MongoDatabase] instance for the given database name.
     *
     * @param databaseName the name of the database to retrieve
     * @return a `MongoDatabase` representing the specified database
     * @throws IllegalArgumentException if databaseName is invalid
     * @see com.mongodb.MongoNamespace.checkDatabaseNameValidity
     */
    public fun getDatabase(databaseName: String): MongoDatabase = MongoDatabase(wrapped.getDatabase(databaseName))

    /**
     * Creates a client session.
     *
     * Note: A ClientSession instance can not be used concurrently in multiple operations.
     *
     * @param options the options for the client session
     * @return the client session
     */
    public fun startSession(options: ClientSessionOptions = ClientSessionOptions.builder().build()): ClientSession =
        ClientSession(wrapped.startSession(options))

    /**
     * Get a list of the database names
     *
     * @return an iterable containing all the names of all the databases
     * @see [List Databases](https://www.mongodb.com/docs/manual/reference/command/listDatabases)
     */
    public fun listDatabaseNames(): MongoIterable<String> = MongoIterable(wrapped.listDatabaseNames())

    /**
     * Gets the list of databases
     *
     * @param clientSession the client session with which to associate this operation
     * @return the list databases iterable interface
     * @see [List Databases](https://www.mongodb.com/docs/manual/reference/command/listDatabases)
     */
    public fun listDatabaseNames(clientSession: ClientSession): MongoIterable<String> =
        MongoIterable(wrapped.listDatabaseNames(clientSession.wrapped))

    /**
     * Gets the list of databases
     *
     * @return the list databases iterable interface
     */
    @JvmName("listDatabasesAsDocument")
    public fun listDatabases(): ListDatabasesIterable<Document> = listDatabases<Document>()

    /**
     * Gets the list of databases
     *
     * @param clientSession the client session with which to associate this operation
     * @return the list databases iterable interface
     */
    @JvmName("listDatabasesAsDocumentWithSession")
    public fun listDatabases(clientSession: ClientSession): ListDatabasesIterable<Document> =
        listDatabases<Document>(clientSession)

    /**
     * Gets the list of databases
     *
     * @param T the type of the class to use
     * @param resultClass the target document type of the iterable.
     * @return the list databases iterable interface
     */
    public fun <T : Any> listDatabases(resultClass: Class<T>): ListDatabasesIterable<T> =
        ListDatabasesIterable(wrapped.listDatabases(resultClass))

    /**
     * Gets the list of databases
     *
     * @param T the type of the class to use
     * @param clientSession the client session with which to associate this operation
     * @param resultClass the target document type of the iterable.
     * @return the list databases iterable interface
     */
    public fun <T : Any> listDatabases(clientSession: ClientSession, resultClass: Class<T>): ListDatabasesIterable<T> =
        ListDatabasesIterable(wrapped.listDatabases(clientSession.wrapped, resultClass))

    /**
     * Gets the list of databases
     *
     * @param T the type of the class to use
     * @return the list databases iterable interface
     */
    public inline fun <reified T : Any> listDatabases(): ListDatabasesIterable<T> = listDatabases(T::class.java)

    /**
     * Gets the list of databases
     *
     * @param clientSession the client session with which to associate this operation
     * @param T the type of the class to use
     * @return the list databases iterable interface
     */
    public inline fun <reified T : Any> listDatabases(clientSession: ClientSession): ListDatabasesIterable<T> =
        listDatabases(clientSession, T::class.java)

    /**
     * Creates a change stream for this client.
     *
     * @param pipeline the aggregation pipeline to apply to the change stream, defaults to an empty pipeline.
     * @return the change stream iterable
     * @see [Change Streams](https://dochub.mongodb.org/changestreams]
     */
    @JvmName("watchAsDocument")
    public fun watch(pipeline: List<Bson> = emptyList()): ChangeStreamIterable<Document> = watch<Document>(pipeline)

    /**
     * Creates a change stream for this client.
     *
     * @param clientSession the client session with which to associate this operation
     * @param pipeline the aggregation pipeline to apply to the change stream, defaults to an empty pipeline.
     * @return the change stream iterable
     * @see [Change Streams](https://dochub.mongodb.org/changestreams]
     */
    @JvmName("watchAsDocumentWithSession")
    public fun watch(clientSession: ClientSession, pipeline: List<Bson> = emptyList()): ChangeStreamIterable<Document> =
        watch<Document>(clientSession, pipeline)

    /**
     * Creates a change stream for this client.
     *
     * @param T the target document type of the iterable.
     * @param pipeline the aggregation pipeline to apply to the change stream, defaults to an empty pipeline.
     * @param resultClass the target document type of the iterable.
     * @return the change stream iterable
     * @see [Change Streams](https://dochub.mongodb.org/changestreams]
     */
    public fun <T : Any> watch(pipeline: List<Bson> = emptyList(), resultClass: Class<T>): ChangeStreamIterable<T> =
        ChangeStreamIterable(wrapped.watch(pipeline, resultClass))

    /**
     * Creates a change stream for this client.
     *
     * @param T the target document type of the iterable.
     * @param clientSession the client session with which to associate this operation
     * @param pipeline the aggregation pipeline to apply to the change stream, defaults to an empty pipeline.
     * @param resultClass the target document type of the iterable.
     * @return the change stream iterable
     * @see [Change Streams](https://dochub.mongodb.org/changestreams]
     */
    public fun <T : Any> watch(
        clientSession: ClientSession,
        pipeline: List<Bson> = emptyList(),
        resultClass: Class<T>
    ): ChangeStreamIterable<T> = ChangeStreamIterable(wrapped.watch(clientSession.wrapped, pipeline, resultClass))

    /**
     * Creates a change stream for this client.
     *
     * @param T the target document type of the iterable.
     * @param pipeline the aggregation pipeline to apply to the change stream, defaults to an empty pipeline.
     * @return the change stream iterable
     * @see [Change Streams](https://dochub.mongodb.org/changestreams]
     */
    public inline fun <reified T : Any> watch(pipeline: List<Bson> = emptyList()): ChangeStreamIterable<T> =
        watch(pipeline, T::class.java)

    /**
     * Creates a change stream for this client.
     *
     * @param T the target document type of the iterable.
     * @param clientSession the client session with which to associate this operation
     * @param pipeline the aggregation pipeline to apply to the change stream, defaults to an empty pipeline.
     * @return the change stream iterable
     * @see [Change Streams](https://dochub.mongodb.org/changestreams]
     */
    public inline fun <reified T : Any> watch(
        clientSession: ClientSession,
        pipeline: List<Bson> = emptyList()
    ): ChangeStreamIterable<T> = watch(clientSession, pipeline, T::class.java)

    /**
     * Executes a client-level bulk write operation. This method is functionally equivalent to
     * [bulkWrite(models, options)][bulkWrite] with the
     * [default options][ClientBulkWriteOptions.clientBulkWriteOptions].
     *
     * This operation supports [retryable writes][MongoClientSettings.getRetryWrites]. Depending on the number of
     * `models`, encoded size of `models`, and the size limits in effect, executing this operation may require multiple
     * `bulkWrite` commands. The eligibility for retries is determined per each `bulkWrite` command:
     * [ClientNamespacedUpdateManyModel], [ClientNamespacedDeleteManyModel] in a command render it non-retryable.
     *
     * @param models The [individual write operations][ClientNamespacedWriteModel].
     * @return The [ClientBulkWriteResult] if the operation is successful.
     * @throws ClientBulkWriteException If and only if the operation is unsuccessful or partially unsuccessful, and
     *   there is at least one of the following pieces of information to report:
     *   [ClientBulkWriteException.getWriteConcernErrors], [ClientBulkWriteException.getWriteErrors],
     *   [ClientBulkWriteException.getPartialResult].
     * @throws MongoException Only if the operation is unsuccessful.
     * @see [BulkWrite command](https://www.mongodb.com/docs/manual/reference/command/bulkWrite/)
     * @since 5.3
     */
    public fun bulkWrite(models: List<ClientNamespacedWriteModel>): ClientBulkWriteResult = wrapped.bulkWrite(models)

    /**
     * Executes a client-level bulk write operation.
     *
     * This operation supports [retryable writes][MongoClientSettings.getRetryWrites]. Depending on the number of
     * `models`, encoded size of `models`, and the size limits in effect, executing this operation may require multiple
     * `bulkWrite` commands. The eligibility for retries is determined per each `bulkWrite` command:
     * [ClientNamespacedUpdateManyModel], [ClientNamespacedDeleteManyModel] in a command render it non-retryable.
     *
     * @param models The [individual write operations][ClientNamespacedWriteModel].
     * @param options The [options][ClientBulkWriteOptions].
     * @return The [ClientBulkWriteResult] if the operation is successful.
     * @throws ClientBulkWriteException If and only if the operation is unsuccessful or partially unsuccessful, and
     *   there is at least one of the following pieces of information to report:
     *   [ClientBulkWriteException.getWriteConcernErrors], [ClientBulkWriteException.getWriteErrors],
     *   [ClientBulkWriteException.getPartialResult].
     * @throws MongoException Only if the operation is unsuccessful.
     * @see [BulkWrite command](https://www.mongodb.com/docs/manual/reference/command/bulkWrite/)
     * @since 5.3
     */
    public fun bulkWrite(
        models: List<ClientNamespacedWriteModel>,
        options: ClientBulkWriteOptions
    ): ClientBulkWriteResult = wrapped.bulkWrite(models, options)

    /**
     * Executes a client-level bulk write operation. This method is functionally equivalent to
     * [bulkWrite(clientSession, models, options)][bulkWrite] with the
     * [default options][ClientBulkWriteOptions.clientBulkWriteOptions].
     *
     * This operation supports [retryable writes][MongoClientSettings.getRetryWrites]. Depending on the number of
     * `models`, encoded size of `models`, and the size limits in effect, executing this operation may require multiple
     * `bulkWrite` commands. The eligibility for retries is determined per each `bulkWrite` command:
     * [ClientNamespacedUpdateManyModel], [ClientNamespacedDeleteManyModel] in a command render it non-retryable.
     *
     * @param clientSession The [client session][ClientSession] with which to associate this operation.
     * @param models The [individual write operations][ClientNamespacedWriteModel].
     * @return The [ClientBulkWriteResult] if the operation is successful.
     * @throws ClientBulkWriteException If and only if the operation is unsuccessful or partially unsuccessful, and
     *   there is at least one of the following pieces of information to report:
     *   [ClientBulkWriteException.getWriteConcernErrors], [ClientBulkWriteException.getWriteErrors],
     *   [ClientBulkWriteException.getPartialResult].
     * @throws MongoException Only if the operation is unsuccessful.
     * @see [BulkWrite command](https://www.mongodb.com/docs/manual/reference/command/bulkWrite/)
     * @since 5.3
     */
    public fun bulkWrite(
        clientSession: ClientSession,
        models: List<ClientNamespacedWriteModel>
    ): ClientBulkWriteResult = wrapped.bulkWrite(clientSession.wrapped, models)

    /**
     * Executes a client-level bulk write operation.
     *
     * This operation supports [retryable writes][com.mongodb.MongoClientSettings.getRetryWrites]. Depending on the
     * number of `models`, encoded size of `models`, and the size limits in effect, executing this operation may require
     * multiple `bulkWrite` commands. The eligibility for retries is determined per each `bulkWrite` command:
     * [ClientNamespacedUpdateManyModel], [ClientNamespacedDeleteManyModel] in a command render it non-retryable.
     *
     * @param clientSession The [client session][ClientSession] with which to associate this operation.
     * @param models The [individual write operations][ClientNamespacedWriteModel].
     * @param options The [options][ClientBulkWriteOptions].
     * @return The [ClientBulkWriteResult] if the operation is successful.
     * @throws ClientBulkWriteException If and only if the operation is unsuccessful or partially unsuccessful, and
     *   there is at least one of the following pieces of information to report:
     *   [ClientBulkWriteException.getWriteConcernErrors], [ClientBulkWriteException.getWriteErrors],
     *   [ClientBulkWriteException.getPartialResult].
     * @throws MongoException Only if the operation is unsuccessful.
     * @see [BulkWrite command](https://www.mongodb.com/docs/manual/reference/command/bulkWrite/)
     * @since 5.3
     */
    public fun bulkWrite(
        clientSession: ClientSession,
        models: List<ClientNamespacedWriteModel>,
        options: ClientBulkWriteOptions
    ): ClientBulkWriteResult = wrapped.bulkWrite(clientSession.wrapped, models, options)
}
