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

import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.WriteConcern
import com.mongodb.client.ClientSession
import com.mongodb.client.MongoDatabase as JMongoDatabase
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.client.model.CreateViewOptions
import java.util.concurrent.TimeUnit
import org.bson.Document
import org.bson.codecs.configuration.CodecRegistry
import org.bson.conversions.Bson

/** The MongoDatabase representation. */
public class MongoDatabase(@PublishedApi internal val wrapped: JMongoDatabase) {

    /** The name of the database. */
    public val name: String
        get() = wrapped.name

    /** The codec registry for the database. */
    public val codecRegistry: CodecRegistry
        get() = wrapped.codecRegistry

    /** The read preference for the database. */
    public val readPreference: ReadPreference
        get() = wrapped.readPreference

    /**
     * The read concern for the database.
     *
     * @see [Read Concern](https://www.mongodb.com/docs/manual/reference/readConcern/)
     */
    public val readConcern: ReadConcern
        get() = wrapped.readConcern

    /** The write concern for the database. */
    public val writeConcern: WriteConcern
        get() = wrapped.writeConcern

    /**
     * Create a new MongoDatabase instance with a different codec registry.
     *
     * The [CodecRegistry] configured by this method is effectively treated by the driver as an instance of
     * [org.bson.codecs.configuration.CodecProvider], which [CodecRegistry] extends. So there is no benefit to defining
     * a class that implements [CodecRegistry]. Rather, an application should always create [CodecRegistry] instances
     * using the factory methods in [org.bson.codecs.configuration.CodecRegistries].
     *
     * @param newCodecRegistry the new [org.bson.codecs.configuration.CodecRegistry] for the database
     * @return a new MongoDatabase instance with the different codec registry
     * @see org.bson.codecs.configuration.CodecRegistries
     */
    public fun withCodecRegistry(newCodecRegistry: CodecRegistry): MongoDatabase =
        MongoDatabase(wrapped.withCodecRegistry(newCodecRegistry))

    /**
     * Create a new MongoDatabase instance with a different read preference.
     *
     * @param readPreference the new [ReadPreference] for the database
     * @return a new MongoDatabase instance with the different readPreference
     */
    public fun withReadPreference(newReadPreference: ReadPreference): MongoDatabase =
        MongoDatabase(wrapped.withReadPreference(newReadPreference))

    /**
     * Create a new MongoDatabase instance with a different read concern.
     *
     * @param readConcern the new [ReadConcern] for the database
     * @return a new MongoDatabase instance with the different ReadConcern
     * @see [Read Concern](https://www.mongodb.com/docs/manual/reference/readConcern/)
     */
    public fun withReadConcern(newReadConcern: ReadConcern): MongoDatabase =
        MongoDatabase(wrapped.withReadConcern(newReadConcern))

    /**
     * Create a new MongoDatabase instance with a different write concern.
     *
     * @param writeConcern the new [WriteConcern] for the database
     * @return a new MongoDatabase instance with the different writeConcern
     */
    public fun withWriteConcern(newWriteConcern: WriteConcern): MongoDatabase =
        MongoDatabase(wrapped.withWriteConcern(newWriteConcern))

    /**
     * Gets a collection.
     *
     * @param T the default class to covert documents returned from the collection into.
     * @param collectionName the name of the collection to return
     * @param resultClass the target document type for the collection
     * @return the collection
     */
    public fun <T : Any> getCollection(collectionName: String, resultClass: Class<T>): MongoCollection<T> =
        MongoCollection(wrapped.getCollection(collectionName, resultClass))

    /**
     * Gets a collection.
     *
     * @param T the default class to covert documents returned from the collection into.
     * @param collectionName the name of the collection to return
     * @return the collection
     */
    public inline fun <reified T : Any> getCollection(collectionName: String): MongoCollection<T> =
        getCollection(collectionName, T::class.java)

    /**
     * Executes the given command in the context of the current database with the given read preference.
     *
     * @param command the command to be run
     * @param readPreference the [ReadPreference] to be used when executing the command, defaults to
     *   [MongoDatabase.readPreference]
     * @return the command result
     */
    public fun runCommand(command: Bson, readPreference: ReadPreference = this.readPreference): Document =
        runCommand<Document>(command, readPreference)

    /**
     * Executes the given command in the context of the current database with the given read preference.
     *
     * @param clientSession the client session with which to associate this operation
     * @param command the command to be run
     * @param readPreference the [ReadPreference] to be used when executing the command, defaults to
     *   [MongoDatabase.readPreference]
     * @return the command result
     */
    public fun runCommand(
        clientSession: ClientSession,
        command: Bson,
        readPreference: ReadPreference = this.readPreference
    ): Document = runCommand<Document>(clientSession, command, readPreference)

    /**
     * Executes the given command in the context of the current database with the given read preference.
     *
     * @param T the class to decode each document into
     * @param command the command to be run
     * @param readPreference the [ReadPreference] to be used when executing the command, defaults to
     *   [MongoDatabase.readPreference]
     * @param resultClass the target document class
     * @return the command result
     */
    public fun <T : Any> runCommand(
        command: Bson,
        readPreference: ReadPreference = this.readPreference,
        resultClass: Class<T>
    ): T = wrapped.runCommand(command, readPreference, resultClass)

    /**
     * Executes the given command in the context of the current database with the given read preference.
     *
     * @param T the class to decode each document into
     * @param clientSession the client session with which to associate this operation
     * @param command the command to be run
     * @param readPreference the [ReadPreference] to be used when executing the command, defaults to
     *   [MongoDatabase.readPreference]
     * @param resultClass the target document class
     * @return the command result
     */
    public fun <T : Any> runCommand(
        clientSession: ClientSession,
        command: Bson,
        readPreference: ReadPreference = this.readPreference,
        resultClass: Class<T>
    ): T = wrapped.runCommand(clientSession, command, readPreference, resultClass)

    /**
     * Executes the given command in the context of the current database with the given read preference.
     *
     * @param T the class to decode each document into
     * @param command the command to be run
     * @param readPreference the [ReadPreference] to be used when executing the command, defaults to
     *   [MongoDatabase.readPreference]
     * @return the command result
     */
    public inline fun <reified T : Any> runCommand(
        command: Bson,
        readPreference: ReadPreference = this.readPreference
    ): T = runCommand(command, readPreference, T::class.java)

    /**
     * Executes the given command in the context of the current database with the given read preference.
     *
     * @param T the class to decode each document into
     * @param clientSession the client session with which to associate this operation
     * @param command the command to be run
     * @param readPreference the [ReadPreference] to be used when executing the command, defaults to
     *   [MongoDatabase.readPreference]
     * @return the command result
     */
    public inline fun <reified T : Any> runCommand(
        clientSession: ClientSession,
        command: Bson,
        readPreference: ReadPreference = this.readPreference
    ): T = runCommand(clientSession, command, readPreference, T::class.java)

    /**
     * Drops this database.
     *
     * @see [Drop database](https://www.mongodb.com/docs/manual/reference/command/dropDatabase/#dbcmd.dropDatabase)
     */
    public fun drop(): Unit = wrapped.drop()

    /**
     * Drops this database.
     *
     * @param clientSession the client session with which to associate this operation
     * @see [Drop database](https://www.mongodb.com/docs/manual/reference/command/dropDatabase/#dbcmd.dropDatabase)
     */
    public fun drop(clientSession: ClientSession): Unit = wrapped.drop(clientSession)

    /**
     * Gets the names of all the collections in this database.
     *
     * @return an iterable containing all the names of all the collections in this database
     */
    public fun listCollectionNames(): MongoIterable<String> = MongoIterable(wrapped.listCollectionNames())

    /**
     * Gets the names of all the collections in this database.
     *
     * @param clientSession the client session with which to associate this operation
     * @return an iterable containing all the names of all the collections in this database
     */
    public fun listCollectionNames(clientSession: ClientSession): MongoIterable<String> =
        MongoIterable(wrapped.listCollectionNames(clientSession))

    /**
     * Gets all the collections in this database.
     *
     * @return the list collections iterable interface
     * @see [listCollections](https://www.mongodb.com/docs/manual/reference/command/listCollections)
     */
    @JvmName("listCollectionsAsDocument")
    public fun listCollections(): ListCollectionsIterable<Document> = listCollections<Document>()

    /**
     * Gets all the collections in this database.
     *
     * @param clientSession the client session with which to associate this operation
     * @return the list collections iterable interface
     * @see [listCollections](https://www.mongodb.com/docs/manual/reference/command/listCollections)
     */
    @JvmName("listCollectionsAsDocumentWithSession")
    public fun listCollections(clientSession: ClientSession): ListCollectionsIterable<Document> =
        listCollections<Document>(clientSession)

    /**
     * Gets all the collections in this database.
     *
     * @param T the type of the class to use
     * @param resultClass the target document type of the iterable.
     * @return the list collections iterable interface
     * @see [listCollections](https://www.mongodb.com/docs/manual/reference/command/listCollections)
     */
    public fun <T : Any> listCollections(resultClass: Class<T>): ListCollectionsIterable<T> =
        ListCollectionsIterable(wrapped.listCollections(resultClass))

    /**
     * Gets all the collections in this database.
     *
     * @param T the type of the class to use
     * @param clientSession the client session with which to associate this operation
     * @param resultClass the target document type of the iterable.
     * @return the list collections iterable interface
     * @see [listCollections](https://www.mongodb.com/docs/manual/reference/command/listCollections)
     */
    public fun <T : Any> listCollections(
        clientSession: ClientSession,
        resultClass: Class<T>
    ): ListCollectionsIterable<T> = ListCollectionsIterable(wrapped.listCollections(clientSession, resultClass))

    /**
     * Gets all the collections in this database.
     *
     * @param T the type of the class to use
     * @return the list collections iterable interface
     * @see [listCollections](https://www.mongodb.com/docs/manual/reference/command/listCollections)
     */
    public inline fun <reified T : Any> listCollections(): ListCollectionsIterable<T> = listCollections(T::class.java)

    /**
     * Gets all the collections in this database.
     *
     * @param clientSession the client session with which to associate this operation
     * @param T the type of the class to use
     * @return the list collections iterable interface
     * @see [listCollections](https://www.mongodb.com/docs/manual/reference/command/listCollections)
     */
    public inline fun <reified T : Any> listCollections(clientSession: ClientSession): ListCollectionsIterable<T> =
        listCollections(clientSession, T::class.java)

    /**
     * Create a new collection with the selected options
     *
     * @param collectionName the name for the new collection to create
     * @param createCollectionOptions various options for creating the collection
     * @see [Create Command](https://www.mongodb.com/docs/manual/reference/command/create)
     */
    public fun createCollection(
        collectionName: String,
        createCollectionOptions: CreateCollectionOptions = CreateCollectionOptions()
    ): Unit = wrapped.createCollection(collectionName, createCollectionOptions)

    /**
     * Create a new collection with the selected options
     *
     * @param clientSession the client session with which to associate this operation
     * @param collectionName the name for the new collection to create
     * @param createCollectionOptions various options for creating the collection
     * @see [Create Command](https://www.mongodb.com/docs/manual/reference/command/create)
     */
    public fun createCollection(
        clientSession: ClientSession,
        collectionName: String,
        createCollectionOptions: CreateCollectionOptions = CreateCollectionOptions()
    ): Unit = wrapped.createCollection(clientSession, collectionName, createCollectionOptions)

    /**
     * Creates a view with the given name, backing collection/view name, aggregation pipeline, and options that defines
     * the view.
     *
     * @param viewName the name of the view to create
     * @param viewOn the backing collection/view for the view
     * @param pipeline the pipeline that defines the view
     * @param createViewOptions various options for creating the view
     * @see [Create Command](https://www.mongodb.com/docs/manual/reference/command/create)
     */
    public fun createView(
        viewName: String,
        viewOn: String,
        pipeline: List<Bson>,
        createViewOptions: CreateViewOptions = CreateViewOptions()
    ): Unit = wrapped.createView(viewName, viewOn, pipeline, createViewOptions)

    /**
     * Creates a view with the given name, backing collection/view name, aggregation pipeline, and options that defines
     * the view.
     *
     * @param clientSession the client session with which to associate this operation
     * @param viewName the name of the view to create
     * @param viewOn the backing collection/view for the view
     * @param pipeline the pipeline that defines the view
     * @param createViewOptions various options for creating the view
     * @see [Create Command](https://www.mongodb.com/docs/manual/reference/command/create)
     */
    public fun createView(
        clientSession: ClientSession,
        viewName: String,
        viewOn: String,
        pipeline: List<Bson>,
        createViewOptions: CreateViewOptions = CreateViewOptions()
    ): Unit = wrapped.createView(clientSession, viewName, viewOn, pipeline, createViewOptions)

    /**
     * Runs an aggregation framework pipeline on the database for pipeline stages that do not require an underlying
     * collection, such as `$currentOp` and `$listLocalSessions`.
     *
     * @param pipeline the aggregation pipeline
     * @return an iterable containing the result of the aggregation operation
     * @see [Aggregate Command](https://www.mongodb.com/docs/manual/reference/command/aggregate/#dbcmd.aggregate)
     */
    @JvmName("aggregateAsDocument")
    public fun aggregate(pipeline: List<Bson>): AggregateIterable<Document> = aggregate<Document>(pipeline)

    /**
     * Runs an aggregation framework pipeline on the database for pipeline stages that do not require an underlying
     * collection, such as `$currentOp` and `$listLocalSessions`.
     *
     * @param clientSession the client session with which to associate this operation
     * @param pipeline the aggregation pipeline
     * @return an iterable containing the result of the aggregation operation
     * @see [Aggregate Command](https://www.mongodb.com/docs/manual/reference/command/aggregate/#dbcmd.aggregate)
     */
    @JvmName("aggregateAsDocumentWithSession")
    public fun aggregate(clientSession: ClientSession, pipeline: List<Bson>): AggregateIterable<Document> =
        aggregate<Document>(clientSession, pipeline)

    /**
     * Runs an aggregation framework pipeline on the database for pipeline stages that do not require an underlying
     * collection, such as `$currentOp` and `$listLocalSessions`.
     *
     * @param T the class to decode each document into
     * @param pipeline the aggregation pipeline
     * @param resultClass the target document type of the iterable.
     * @return an iterable containing the result of the aggregation operation
     * @see [Aggregate Command](https://www.mongodb.com/docs/manual/reference/command/aggregate/#dbcmd.aggregate)
     */
    public fun <T : Any> aggregate(pipeline: List<Bson>, resultClass: Class<T>): AggregateIterable<T> =
        AggregateIterable(wrapped.aggregate(pipeline, resultClass))

    /**
     * Runs an aggregation framework pipeline on the database for pipeline stages that do not require an underlying
     * collection, such as `$currentOp` and `$listLocalSessions`.
     *
     * @param T the class to decode each document into
     * @param clientSession the client session with which to associate this operation
     * @param pipeline the aggregation pipeline
     * @param resultClass the target document type of the iterable.
     * @return an iterable containing the result of the aggregation operation
     * @see [Aggregate Command](https://www.mongodb.com/docs/manual/reference/command/aggregate/#dbcmd.aggregate)
     */
    public fun <T : Any> aggregate(
        clientSession: ClientSession,
        pipeline: List<Bson>,
        resultClass: Class<T>
    ): AggregateIterable<T> = AggregateIterable(wrapped.aggregate(clientSession, pipeline, resultClass))

    /**
     * Runs an aggregation framework pipeline on the database for pipeline stages that do not require an underlying
     * collection, such as `$currentOp` and `$listLocalSessions`.
     *
     * @param T the class to decode each document into
     * @param pipeline the aggregation pipeline
     * @return an iterable containing the result of the aggregation operation
     * @see [Aggregate Command](https://www.mongodb.com/docs/manual/reference/command/aggregate/#dbcmd.aggregate)
     */
    public inline fun <reified T : Any> aggregate(pipeline: List<Bson>): AggregateIterable<T> =
        aggregate(pipeline, T::class.java)

    /**
     * Runs an aggregation framework pipeline on the database for pipeline stages that do not require an underlying
     * collection, such as `$currentOp` and `$listLocalSessions`.
     *
     * @param T the class to decode each document into
     * @param clientSession the client session with which to associate this operation
     * @param pipeline the aggregation pipeline
     * @return an iterable containing the result of the aggregation operation
     * @see [Aggregate Command](https://www.mongodb.com/docs/manual/reference/command/aggregate/#dbcmd.aggregate)
     */
    public inline fun <reified T : Any> aggregate(
        clientSession: ClientSession,
        pipeline: List<Bson>
    ): AggregateIterable<T> = aggregate(clientSession, pipeline, T::class.java)

    /**
     * Creates a change stream for this database.
     *
     * @param pipeline the aggregation pipeline to apply to the change stream, defaults to an empty pipeline.
     * @return the change stream iterable
     * @see [Change Streams](https://dochub.mongodb.org/changestreams]
     */
    @JvmName("watchAsDocument")
    public fun watch(pipeline: List<Bson> = emptyList()): ChangeStreamIterable<Document> = watch<Document>(pipeline)

    /**
     * Creates a change stream for this database.
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
     * Creates a change stream for this database.
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
     * Creates a change stream for this database.
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
    ): ChangeStreamIterable<T> = ChangeStreamIterable(wrapped.watch(clientSession, pipeline, resultClass))

    /**
     * Creates a change stream for this database.
     *
     * @param T the target document type of the iterable.
     * @param pipeline the aggregation pipeline to apply to the change stream, defaults to an empty pipeline.
     * @return the change stream iterable
     * @see [Change Streams](https://dochub.mongodb.org/changestreams]
     */
    public inline fun <reified T : Any> watch(pipeline: List<Bson> = emptyList()): ChangeStreamIterable<T> =
        watch(pipeline, T::class.java)

    /**
     * Creates a change stream for this database.
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

/**
 * expireAfter extension function
 *
 * @param maxTime time in seconds
 * @return the options
 */
public fun CreateCollectionOptions.expireAfter(maxTime: Long): CreateCollectionOptions =
    this.apply { expireAfter(maxTime, TimeUnit.SECONDS) }
