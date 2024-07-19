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

import com.mongodb.ClientSessionOptions
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.WriteConcern
import com.mongodb.annotations.Alpha
import com.mongodb.annotations.Reason
import com.mongodb.reactivestreams.client.MongoCluster as JMongoCluster
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitSingle
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
     * The time limit for the full execution of an operation.
     *
     * If not null the following deprecated options will be ignored: `waitQueueTimeoutMS`, `socketTimeoutMS`,
     * `wTimeoutMS`, `maxTimeMS` and `maxCommitTimeMS`.
     * - `null` means that the timeout mechanism for operations will defer to using:
     *     - `waitQueueTimeoutMS`: The maximum wait time in milliseconds that a thread may wait for a connection to
     *       become available
     *     - `socketTimeoutMS`: How long a send or receive on a socket can take before timing out.
     *     - `wTimeoutMS`: How long the server will wait for the write concern to be fulfilled before timing out.
     *     - `maxTimeMS`: The time limit for processing operations on a cursor. See:
     *       [cursor.maxTimeMS](https://docs.mongodb.com/manual/reference/method/cursor.maxTimeMS").
     *     - `maxCommitTimeMS`: The maximum amount of time to allow a single `commitTransaction` command to execute.
     * - `0` means infinite timeout.
     * - `> 0` The time limit to use for the full execution of an operation.
     *
     * @return the optional timeout duration
     */
    @Alpha(Reason.CLIENT)
    public fun timeout(timeUnit: TimeUnit = TimeUnit.MILLISECONDS): Long? = wrapped.getTimeout(timeUnit)

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
     * Create a new MongoCluster instance with the set time limit for the full execution of an operation.
     * - `0` means an infinite timeout
     * - `> 0` The time limit to use for the full execution of an operation.
     *
     * @param timeout the timeout, which must be greater than or equal to 0
     * @param timeUnit the time unit, defaults to Milliseconds
     * @return a new MongoCluster instance with the set time limit for operations
     * @see [MongoDatabase.timeout]
     * @since 5.2
     */
    @Alpha(Reason.CLIENT)
    public fun withTimeout(timeout: Long, timeUnit: TimeUnit = TimeUnit.MILLISECONDS): MongoCluster =
        MongoCluster(wrapped.withTimeout(timeout, timeUnit))

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
    public suspend fun startSession(
        options: ClientSessionOptions = ClientSessionOptions.builder().build()
    ): ClientSession = ClientSession(wrapped.startSession(options).awaitSingle())

    /**
     * Get a list of the database names
     *
     * @return an iterable containing all the names of all the databases
     * @see [List Databases](https://www.mongodb.com/docs/manual/reference/command/listDatabases)
     */
    public fun listDatabaseNames(): Flow<String> = wrapped.listDatabaseNames().asFlow()

    /**
     * Gets the list of databases
     *
     * @param clientSession the client session with which to associate this operation
     * @return the list databases iterable interface
     * @see [List Databases](https://www.mongodb.com/docs/manual/reference/command/listDatabases)
     */
    public fun listDatabaseNames(clientSession: ClientSession): Flow<String> =
        wrapped.listDatabaseNames(clientSession.wrapped).asFlow()

    /**
     * Gets the list of databases
     *
     * @return the list databases iterable interface
     */
    @JvmName("listDatabasesAsDocument")
    public fun listDatabases(): ListDatabasesFlow<Document> = listDatabases<Document>()

    /**
     * Gets the list of databases
     *
     * @param clientSession the client session with which to associate this operation
     * @return the list databases iterable interface
     */
    @JvmName("listDatabasesAsDocumentWithSession")
    public fun listDatabases(clientSession: ClientSession): ListDatabasesFlow<Document> =
        listDatabases<Document>(clientSession)

    /**
     * Gets the list of databases
     *
     * @param T the type of the class to use
     * @param resultClass the target document type of the iterable.
     * @return the list databases iterable interface
     */
    public fun <T : Any> listDatabases(resultClass: Class<T>): ListDatabasesFlow<T> =
        ListDatabasesFlow(wrapped.listDatabases(resultClass))

    /**
     * Gets the list of databases
     *
     * @param T the type of the class to use
     * @param clientSession the client session with which to associate this operation
     * @param resultClass the target document type of the iterable.
     * @return the list databases iterable interface
     */
    public fun <T : Any> listDatabases(clientSession: ClientSession, resultClass: Class<T>): ListDatabasesFlow<T> =
        ListDatabasesFlow(wrapped.listDatabases(clientSession.wrapped, resultClass))

    /**
     * Gets the list of databases
     *
     * @param T the type of the class to use
     * @return the list databases iterable interface
     */
    public inline fun <reified T : Any> listDatabases(): ListDatabasesFlow<T> = listDatabases(T::class.java)

    /**
     * Gets the list of databases
     *
     * @param clientSession the client session with which to associate this operation
     * @param T the type of the class to use
     * @return the list databases iterable interface
     */
    public inline fun <reified T : Any> listDatabases(clientSession: ClientSession): ListDatabasesFlow<T> =
        listDatabases(clientSession, T::class.java)

    /**
     * Creates a change stream for this client.
     *
     * @param pipeline the aggregation pipeline to apply to the change stream, defaults to an empty pipeline.
     * @return the change stream iterable
     * @see [Change Streams](https://dochub.mongodb.org/changestreams]
     */
    @JvmName("watchAsDocument")
    public fun watch(pipeline: List<Bson> = emptyList()): ChangeStreamFlow<Document> = watch<Document>(pipeline)

    /**
     * Creates a change stream for this client.
     *
     * @param clientSession the client session with which to associate this operation
     * @param pipeline the aggregation pipeline to apply to the change stream, defaults to an empty pipeline.
     * @return the change stream iterable
     * @see [Change Streams](https://dochub.mongodb.org/changestreams]
     */
    @JvmName("watchAsDocumentWithSession")
    public fun watch(clientSession: ClientSession, pipeline: List<Bson> = emptyList()): ChangeStreamFlow<Document> =
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
    public fun <T : Any> watch(pipeline: List<Bson> = emptyList(), resultClass: Class<T>): ChangeStreamFlow<T> =
        ChangeStreamFlow(wrapped.watch(pipeline, resultClass))

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
    ): ChangeStreamFlow<T> = ChangeStreamFlow(wrapped.watch(clientSession.wrapped, pipeline, resultClass))

    /**
     * Creates a change stream for this client.
     *
     * @param T the target document type of the iterable.
     * @param pipeline the aggregation pipeline to apply to the change stream, defaults to an empty pipeline.
     * @return the change stream iterable
     * @see [Change Streams](https://dochub.mongodb.org/changestreams]
     */
    public inline fun <reified T : Any> watch(pipeline: List<Bson> = emptyList()): ChangeStreamFlow<T> =
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
    ): ChangeStreamFlow<T> = watch(clientSession, pipeline, T::class.java)
}
