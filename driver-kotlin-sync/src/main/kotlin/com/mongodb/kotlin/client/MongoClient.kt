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

import com.mongodb.ClientSessionOptions
import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.MongoDriverInformation
import com.mongodb.client.MongoClient as JMongoClient
import com.mongodb.client.MongoClients as JMongoClients
import com.mongodb.connection.ClusterDescription
import com.mongodb.lang.Nullable
import java.io.Closeable
import org.bson.Document
import org.bson.conversions.Bson

/**
 * A client-side representation of a MongoDB cluster.
 *
 * Instances can represent either a standalone MongoDB instance, a replica set, or a sharded cluster. Instance of this
 * class are responsible for maintaining an up-to-date state of the cluster, and possibly cache resources related to
 * this, including background threads for monitoring, and connection pools.
 *
 * Instances of this class serve as factories for [MongoDatabase] instances. Instances of this class can be created via
 * the [MongoClient.create] helpers
 *
 * @see MongoClient.create
 */
public class MongoClient(private val wrapped: JMongoClient) : Closeable {

    /**
     * A factory for [MongoClient] instances.
     *
     * @see MongoClient
     * @since 4.10
     */
    public companion object Factory {
        /**
         * Create a new client with the given connection string as if by a call to [create].
         *
         * @param connectionString the connection
         * @return the client
         */
        public fun create(connectionString: String): MongoClient = create(ConnectionString(connectionString))

        /**
         * Create a new client with the given connection string.
         *
         * @param connectionString the connection string, defaults to `mongodb://localhost`.
         * @param mongoDriverInformation any driver information to associate with the MongoClient
         * @return the client
         */
        public fun create(
            connectionString: ConnectionString = ConnectionString("mongodb://localhost"),
            @Nullable mongoDriverInformation: MongoDriverInformation? = null
        ): MongoClient {
            return create(
                MongoClientSettings.builder().applyConnectionString(connectionString).build(), mongoDriverInformation)
        }

        /**
         * Create a new client with the given connection string.
         *
         * For each of the settings classed configurable via [MongoClientSettings], the connection string is applied by
         * calling the `applyConnectionString` method on an instance of setting's builder class, building the setting,
         * and adding it to an instance of [com.mongodb.MongoClientSettings.Builder].
         *
         * @param settings the client settings
         * @param mongoDriverInformation any driver information to associate with the MongoClient
         * @return
         */
        public fun create(
            settings: MongoClientSettings,
            @Nullable mongoDriverInformation: MongoDriverInformation? = null
        ): MongoClient {
            val builder =
                if (mongoDriverInformation == null) MongoDriverInformation.builder()
                else MongoDriverInformation.builder(mongoDriverInformation)
            return MongoClient(JMongoClients.create(settings, builder.driverName("kotlin").build()))
        }
    }

    public override fun close(): Unit = wrapped.close()

    /**
     * Gets the current cluster description.
     *
     * This method will not block, meaning that it may return a [ClusterDescription] whose `clusterType` is unknown and
     * whose [com.mongodb.connection.ServerDescription]s are all in the connecting state. If the application requires
     * notifications after the driver has connected to a member of the cluster, it should register a
     * [com.mongodb.event.ClusterListener] via the [com.mongodb.connection.ClusterSettings] in
     * [com.mongodb.MongoClientSettings].
     *
     * @return the current cluster description
     * @see com.mongodb.connection.ClusterSettings.Builder.addClusterListener
     * @see com.mongodb.MongoClientSettings.Builder.applyToClusterSettings
     */
    public fun getClusterDescription(): ClusterDescription = wrapped.clusterDescription

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
}
