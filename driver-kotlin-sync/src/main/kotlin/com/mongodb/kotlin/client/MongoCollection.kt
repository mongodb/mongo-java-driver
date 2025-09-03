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

import com.mongodb.MongoNamespace
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.WriteConcern
import com.mongodb.annotations.Alpha
import com.mongodb.annotations.Reason
import com.mongodb.bulk.BulkWriteResult
import com.mongodb.client.MongoCollection as JMongoCollection
import com.mongodb.client.model.BulkWriteOptions
import com.mongodb.client.model.CountOptions
import com.mongodb.client.model.CreateIndexOptions
import com.mongodb.client.model.DeleteOptions
import com.mongodb.client.model.DropCollectionOptions
import com.mongodb.client.model.DropIndexOptions
import com.mongodb.client.model.EstimatedDocumentCountOptions
import com.mongodb.client.model.FindOneAndDeleteOptions
import com.mongodb.client.model.FindOneAndReplaceOptions
import com.mongodb.client.model.FindOneAndUpdateOptions
import com.mongodb.client.model.IndexModel
import com.mongodb.client.model.IndexOptions
import com.mongodb.client.model.InsertManyOptions
import com.mongodb.client.model.InsertOneOptions
import com.mongodb.client.model.RenameCollectionOptions
import com.mongodb.client.model.ReplaceOptions
import com.mongodb.client.model.SearchIndexModel
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.model.WriteModel
import com.mongodb.client.result.DeleteResult
import com.mongodb.client.result.InsertManyResult
import com.mongodb.client.result.InsertOneResult
import com.mongodb.client.result.UpdateResult
import java.util.concurrent.TimeUnit
import org.bson.BsonDocument
import org.bson.Document
import org.bson.codecs.configuration.CodecRegistry
import org.bson.conversions.Bson

/**
 * The MongoCollection representation.
 *
 * Note: Additions to this interface will not be considered to break binary compatibility.
 *
 * @param T The type of documents the collection will encode documents from and decode documents to.
 * @property wrapped the underlying sync MongoCollection
 */
public class MongoCollection<T : Any>(private val wrapped: JMongoCollection<T>) {

    /** The class of documents stored in this collection. */
    public val documentClass: Class<T>
        get() = wrapped.documentClass

    /** The namespace of this collection. */
    public val namespace: MongoNamespace
        get() = wrapped.namespace

    /** The codec registry for the collection. */
    public val codecRegistry: CodecRegistry
        get() = wrapped.codecRegistry

    /** the read preference for the collection. */
    public val readPreference: ReadPreference
        get() = wrapped.readPreference

    /** The read concern for the collection. */
    public val readConcern: ReadConcern
        get() = wrapped.readConcern

    /** The write concern for the collection. */
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
     * @since 5.2
     */
    @Alpha(Reason.CLIENT)
    public fun timeout(timeUnit: TimeUnit = TimeUnit.MILLISECONDS): Long? = wrapped.getTimeout(timeUnit)

    /**
     * Create a new collection instance with a different default class to cast any documents returned from the database
     * into.
     *
     * @param R the default class to cast any documents returned from the database into.
     * @param resultClass the target document type for the collection.
     * @return a new MongoCollection instance with the different default class
     */
    public fun <R : Any> withDocumentClass(resultClass: Class<R>): MongoCollection<R> =
        MongoCollection(wrapped.withDocumentClass(resultClass))

    /**
     * Create a new collection instance with a different default class to cast any documents returned from the database
     * into.
     *
     * @param R the default class to cast any documents returned from the database into.
     * @return a new MongoCollection instance with the different default class
     */
    public inline fun <reified R : Any> withDocumentClass(): MongoCollection<R> = withDocumentClass(R::class.java)

    /**
     * Create a new collection instance with a different codec registry.
     *
     * The [CodecRegistry] configured by this method is effectively treated by the driver as an instance of
     * [org.bson.codecs.configuration.CodecProvider], which [CodecRegistry] extends. So there is no benefit to defining
     * a class that implements [CodecRegistry]. Rather, an application should always create [CodecRegistry] instances
     * using the factory methods in [org.bson.codecs.configuration.CodecRegistries].
     *
     * @param newCodecRegistry the new [org.bson.codecs.configuration.CodecRegistry] for the collection
     * @return a new MongoCollection instance with the different codec registry
     * @see org.bson.codecs.configuration.CodecRegistries
     */
    public fun withCodecRegistry(newCodecRegistry: CodecRegistry): MongoCollection<T> =
        MongoCollection(wrapped.withCodecRegistry(newCodecRegistry))

    /**
     * Create a new collection instance with a different read preference.
     *
     * @param newReadPreference the new [com.mongodb.ReadPreference] for the collection
     * @return a new MongoCollection instance with the different readPreference
     */
    public fun withReadPreference(newReadPreference: ReadPreference): MongoCollection<T> =
        MongoCollection(wrapped.withReadPreference(newReadPreference))

    /**
     * Create a new collection instance with a different read concern.
     *
     * @param newReadConcern the new [ReadConcern] for the collection
     * @return a new MongoCollection instance with the different ReadConcern
     * @see [Read Concern](https://www.mongodb.com/docs/manual/reference/readConcern/)
     */
    public fun withReadConcern(newReadConcern: ReadConcern): MongoCollection<T> =
        MongoCollection(wrapped.withReadConcern(newReadConcern))

    /**
     * Create a new collection instance with a different write concern.
     *
     * @param newWriteConcern the new [com.mongodb.WriteConcern] for the collection
     * @return a new MongoCollection instance with the different writeConcern
     */
    public fun withWriteConcern(newWriteConcern: WriteConcern): MongoCollection<T> =
        MongoCollection(wrapped.withWriteConcern(newWriteConcern))

    /**
     * Create a new MongoCollection instance with the set time limit for the full execution of an operation.
     * - `0` means an infinite timeout
     * - `> 0` The time limit to use for the full execution of an operation.
     *
     * @param timeout the timeout, which must be greater than or equal to 0
     * @param timeUnit the time unit, defaults to Milliseconds
     * @return a new MongoCollection instance with the set time limit for operations
     * @see [MongoCollection.timeout]
     * @since 5.2
     */
    @Alpha(Reason.CLIENT)
    public fun withTimeout(timeout: Long, timeUnit: TimeUnit = TimeUnit.MILLISECONDS): MongoCollection<T> =
        MongoCollection(wrapped.withTimeout(timeout, timeUnit))

    /**
     * Counts the number of documents in the collection.
     *
     * Note: For a fast count of the total documents in a collection see [estimatedDocumentCount]. When migrating from
     * `count()` to `countDocuments()` the following query operators must be replaced:
     * ```
     * +-------------+--------------------------------+
     * | Operator    | Replacement                    |
     * +=============+================================+
     * | $where      |  $expr                         |
     * +-------------+--------------------------------+
     * | $near       |  $geoWithin with $center       |
     * +-------------+--------------------------------+
     * | $nearSphere |  $geoWithin with $centerSphere |
     * +-------------+--------------------------------+
     * ```
     *
     * @return the number of documents in the collection
     */
    public fun countDocuments(filter: Bson = BsonDocument(), options: CountOptions = CountOptions()): Long =
        wrapped.countDocuments(filter, options)

    /**
     * Counts the number of documents in the collection according to the given options.
     *
     * Note: For a fast count of the total documents in a collection see [estimatedDocumentCount]. When migrating from
     * `count()` to `countDocuments()` the following query operators must be replaced:
     * ```
     * +-------------+--------------------------------+
     * | Operator    | Replacement                    |
     * +=============+================================+
     * | $where      |  $expr                         |
     * +-------------+--------------------------------+
     * | $near       |  $geoWithin with $center       |
     * +-------------+--------------------------------+
     * | $nearSphere |  $geoWithin with $centerSphere |
     * +-------------+--------------------------------+
     * ```
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter the query filter
     * @param options the options describing the count
     * @return the number of documents in the collection
     */
    public fun countDocuments(
        clientSession: ClientSession,
        filter: Bson = BsonDocument(),
        options: CountOptions = CountOptions()
    ): Long = wrapped.countDocuments(clientSession.wrapped, filter, options)

    /**
     * Gets an estimate of the count of documents in a collection using collection metadata.
     *
     * Implementation note: this method is implemented using the MongoDB server's count command
     *
     * @param options the options describing the count
     * @return the number of documents in the collection
     * @see [Count behaviour](https://www.mongodb.com/docs/manual/reference/command/count/#behavior)
     */
    public fun estimatedDocumentCount(options: EstimatedDocumentCountOptions = EstimatedDocumentCountOptions()): Long =
        wrapped.estimatedDocumentCount(options)

    /**
     * Gets the distinct values of the specified field name.
     *
     * @param R the target type of the iterable.
     * @param fieldName the field name
     * @param filter the query filter
     * @param resultClass the target document type of the iterable.
     * @return an iterable of distinct values
     * @see [Distinct command](https://www.mongodb.com/docs/manual/reference/command/distinct/)
     */
    public fun <R : Any?> distinct(
        fieldName: String,
        filter: Bson = BsonDocument(),
        resultClass: Class<R>
    ): DistinctIterable<R> = DistinctIterable(wrapped.distinct(fieldName, filter, resultClass))

    /**
     * Gets the distinct values of the specified field name.
     *
     * @param R the target type of the iterable.
     * @param clientSession the client session with which to associate this operation
     * @param fieldName the field name
     * @param filter the query filter
     * @param resultClass the target document type of the iterable.
     * @return an iterable of distinct values
     * @see [Distinct command](https://www.mongodb.com/docs/manual/reference/command/distinct/)
     */
    public fun <R : Any?> distinct(
        clientSession: ClientSession,
        fieldName: String,
        filter: Bson = BsonDocument(),
        resultClass: Class<R>
    ): DistinctIterable<R> = DistinctIterable(wrapped.distinct(clientSession.wrapped, fieldName, filter, resultClass))

    /**
     * Gets the distinct values of the specified field name.
     *
     * @param R the target type of the iterable.
     * @param fieldName the field name
     * @param filter the query filter
     * @return an iterable of distinct values
     * @see [Distinct command](https://www.mongodb.com/docs/manual/reference/command/distinct/)
     */
    public inline fun <reified R : Any?> distinct(
        fieldName: String,
        filter: Bson = BsonDocument()
    ): DistinctIterable<R> = distinct(fieldName, filter, R::class.java)

    /**
     * Gets the distinct values of the specified field name.
     *
     * @param R the target type of the iterable.
     * @param clientSession the client session with which to associate this operation
     * @param fieldName the field name
     * @param filter the query filter
     * @return an iterable of distinct values
     * @see [Distinct command](https://www.mongodb.com/docs/manual/reference/command/distinct/)
     */
    public inline fun <reified R : Any?> distinct(
        clientSession: ClientSession,
        fieldName: String,
        filter: Bson = BsonDocument()
    ): DistinctIterable<R> = distinct(clientSession, fieldName, filter, R::class.java)

    /**
     * Finds all documents in the collection.
     *
     * @param filter the query filter
     * @return the find iterable interface
     * @see [Query Documents](https://www.mongodb.com/docs/manual/tutorial/query-documents/)
     */
    @JvmName("findAsT") public fun find(filter: Bson = BsonDocument()): FindIterable<T> = find(filter, documentClass)

    /**
     * Finds all documents in the collection.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter the query filter
     * @return the find iterable interface
     * @see [Query Documents](https://www.mongodb.com/docs/manual/tutorial/query-documents/)
     */
    @JvmName("findAsTWithSession")
    public fun find(clientSession: ClientSession, filter: Bson = BsonDocument()): FindIterable<T> =
        find(clientSession, filter, documentClass)

    /**
     * Finds all documents in the collection.
     *
     * @param R the class to decode each document into
     * @param filter the query filter
     * @param resultClass the target document type of the iterable.
     * @return the find iterable interface
     * @see [Query Documents](https://www.mongodb.com/docs/manual/tutorial/query-documents/)
     */
    public fun <R : Any> find(filter: Bson = BsonDocument(), resultClass: Class<R>): FindIterable<R> =
        FindIterable(wrapped.find(filter, resultClass))

    /**
     * Finds all documents in the collection.
     *
     * @param R the class to decode each document into
     * @param clientSession the client session with which to associate this operation
     * @param filter the query filter
     * @param resultClass the target document type of the iterable.
     * @return the find iterable interface
     * @see [Query Documents](https://www.mongodb.com/docs/manual/tutorial/query-documents/)
     */
    public fun <R : Any> find(
        clientSession: ClientSession,
        filter: Bson = BsonDocument(),
        resultClass: Class<R>
    ): FindIterable<R> = FindIterable(wrapped.find(clientSession.wrapped, filter, resultClass))

    /**
     * Finds all documents in the collection.
     *
     * @param R the class to decode each document into
     * @param filter the query filter
     * @return the find iterable interface
     * @see [Query Documents](https://www.mongodb.com/docs/manual/tutorial/query-documents/)
     */
    public inline fun <reified R : Any> find(filter: Bson = BsonDocument()): FindIterable<R> =
        find(filter, R::class.java)

    /**
     * Finds all documents in the collection.
     *
     * @param R the class to decode each document into
     * @param clientSession the client session with which to associate this operation
     * @param filter the query filter
     * @return the find iterable interface
     * @see [Query Documents](https://www.mongodb.com/docs/manual/tutorial/query-documents/)
     */
    public inline fun <reified R : Any> find(
        clientSession: ClientSession,
        filter: Bson = BsonDocument()
    ): FindIterable<R> = find(clientSession, filter, R::class.java)

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     *
     * @param pipeline the aggregation pipeline
     * @return an iterable containing the result of the aggregation operation
     * @see [Aggregate Command](https://www.mongodb.com/docs/manual/reference/command/aggregate/#dbcmd.aggregate/)
     */
    @JvmName("aggregateAsT")
    public fun aggregate(pipeline: List<Bson>): AggregateIterable<T> =
        AggregateIterable(wrapped.aggregate(pipeline, documentClass))

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     *
     * @param clientSession the client session with which to associate this operation
     * @param pipeline the aggregation pipeline
     * @return an iterable containing the result of the aggregation operation
     * @see [Aggregate Command](https://www.mongodb.com/docs/manual/reference/command/aggregate/#dbcmd.aggregate/)
     */
    @JvmName("aggregateAsTWithSession")
    public fun aggregate(clientSession: ClientSession, pipeline: List<Bson>): AggregateIterable<T> =
        AggregateIterable(wrapped.aggregate(clientSession.wrapped, pipeline, documentClass))

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     *
     * @param R the class to decode each document into
     * @param pipeline the aggregation pipeline
     * @param resultClass the target document type of the iterable.
     * @return an iterable containing the result of the aggregation operation
     * @see [Aggregate Command](https://www.mongodb.com/docs/manual/reference/command/aggregate/#dbcmd.aggregate/)
     */
    public fun <R : Any> aggregate(pipeline: List<Bson>, resultClass: Class<R>): AggregateIterable<R> =
        AggregateIterable(wrapped.aggregate(pipeline, resultClass))

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     *
     * @param R the class to decode each document into
     * @param clientSession the client session with which to associate this operation
     * @param pipeline the aggregation pipeline
     * @param resultClass the target document type of the iterable.
     * @return an iterable containing the result of the aggregation operation
     * @see [Aggregate Command](https://www.mongodb.com/docs/manual/reference/command/aggregate/#dbcmd.aggregate/)
     */
    public fun <R : Any> aggregate(
        clientSession: ClientSession,
        pipeline: List<Bson>,
        resultClass: Class<R>
    ): AggregateIterable<R> = AggregateIterable(wrapped.aggregate(clientSession.wrapped, pipeline, resultClass))

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     *
     * @param R the class to decode each document into
     * @param pipeline the aggregation pipeline
     * @return an iterable containing the result of the aggregation operation
     * @see [Aggregate Command](https://www.mongodb.com/docs/manual/reference/command/aggregate/#dbcmd.aggregate/)
     */
    public inline fun <reified R : Any> aggregate(pipeline: List<Bson>): AggregateIterable<R> =
        aggregate(pipeline, R::class.java)

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     *
     * @param R the class to decode each document into
     * @param clientSession the client session with which to associate this operation
     * @param pipeline the aggregation pipeline
     * @return an iterable containing the result of the aggregation operation
     * @see [Aggregate Command](https://www.mongodb.com/docs/manual/reference/command/aggregate/#dbcmd.aggregate/)
     */
    public inline fun <reified R : Any> aggregate(
        clientSession: ClientSession,
        pipeline: List<Bson>
    ): AggregateIterable<R> = aggregate(clientSession, pipeline, R::class.java)

    /**
     * Creates a change stream for this collection.
     *
     * @param pipeline the aggregation pipeline to apply to the change stream, defaults to an empty pipeline.
     * @return the change stream iterable
     * @see [Change Streams](https://dochub.mongodb.org/changestreams]
     */
    @JvmName("watchAsDocument")
    public fun watch(pipeline: List<Bson> = emptyList()): ChangeStreamIterable<T> = watch(pipeline, documentClass)

    /**
     * Creates a change stream for this collection.
     *
     * @param clientSession the client session with which to associate this operation
     * @param pipeline the aggregation pipeline to apply to the change stream, defaults to an empty pipeline.
     * @return the change stream iterable
     * @see [Change Streams](https://dochub.mongodb.org/changestreams]
     */
    @JvmName("watchAsDocumentWithSession")
    public fun watch(clientSession: ClientSession, pipeline: List<Bson> = emptyList()): ChangeStreamIterable<T> =
        watch(clientSession, pipeline, documentClass)

    /**
     * Creates a change stream for this collection.
     *
     * @param R the target document type of the iterable.
     * @param pipeline the aggregation pipeline to apply to the change stream, defaults to an empty pipeline.
     * @param resultClass the target document type of the iterable.
     * @return the change stream iterable
     * @see [Change Streams](https://dochub.mongodb.org/changestreams]
     */
    public fun <R : Any> watch(pipeline: List<Bson> = emptyList(), resultClass: Class<R>): ChangeStreamIterable<R> =
        ChangeStreamIterable(wrapped.watch(pipeline, resultClass))

    /**
     * Creates a change stream for this collection.
     *
     * @param R the target document type of the iterable.
     * @param clientSession the client session with which to associate this operation
     * @param pipeline the aggregation pipeline to apply to the change stream, defaults to an empty pipeline.
     * @param resultClass the target document type of the iterable.
     * @return the change stream iterable
     * @see [Change Streams](https://dochub.mongodb.org/changestreams]
     */
    public fun <R : Any> watch(
        clientSession: ClientSession,
        pipeline: List<Bson> = emptyList(),
        resultClass: Class<R>
    ): ChangeStreamIterable<R> = ChangeStreamIterable(wrapped.watch(clientSession.wrapped, pipeline, resultClass))

    /**
     * Creates a change stream for this collection.
     *
     * @param R the target document type of the iterable.
     * @param pipeline the aggregation pipeline to apply to the change stream, defaults to an empty pipeline.
     * @return the change stream iterable
     * @see [Change Streams](https://dochub.mongodb.org/changestreams]
     */
    public inline fun <reified R : Any> watch(pipeline: List<Bson> = emptyList()): ChangeStreamIterable<R> =
        watch(pipeline, R::class.java)

    /**
     * Creates a change stream for this collection.
     *
     * @param R the target document type of the iterable.
     * @param clientSession the client session with which to associate this operation
     * @param pipeline the aggregation pipeline to apply to the change stream, defaults to an empty pipeline.
     * @return the change stream iterable
     * @see [Change Streams](https://dochub.mongodb.org/changestreams]
     */
    public inline fun <reified R : Any> watch(
        clientSession: ClientSession,
        pipeline: List<Bson> = emptyList()
    ): ChangeStreamIterable<R> = watch(clientSession, pipeline, R::class.java)

    /**
     * Inserts the provided document. If the document is missing an identifier, the driver should generate one.
     *
     * Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.
     *
     * @param document the document to insert
     * @param options the options to apply to the operation
     * @return the insert one result
     * @throws com.mongodb.MongoWriteException if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write
     *   concern
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException if the write failed due some other failure
     */
    public fun insertOne(document: T, options: InsertOneOptions = InsertOneOptions()): InsertOneResult =
        wrapped.insertOne(document, options)

    /**
     * Inserts the provided document. If the document is missing an identifier, the driver should generate one.
     *
     * Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.
     *
     * @param clientSession the client session with which to associate this operation
     * @param document the document to insert
     * @param options the options to apply to the operation
     * @return the insert one result
     * @throws com.mongodb.MongoWriteException if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write
     *   concern
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException if the write failed due some other failure
     */
    public fun insertOne(
        clientSession: ClientSession,
        document: T,
        options: InsertOneOptions = InsertOneOptions()
    ): InsertOneResult = wrapped.insertOne(clientSession.wrapped, document, options)

    /**
     * Inserts one or more documents. A call to this method is equivalent to a call to the `bulkWrite` method
     *
     * Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.
     *
     * @param documents the documents to insert
     * @param options the options to apply to the operation
     * @return the insert many result
     * @throws com.mongodb.MongoBulkWriteException if there's an exception in the bulk write operation
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException if the write failed due some other failure
     * @throws IllegalArgumentException if the documents list is null or empty, or any of the documents in the list are
     *   null
     */
    public fun insertMany(documents: List<T>, options: InsertManyOptions = InsertManyOptions()): InsertManyResult =
        wrapped.insertMany(documents, options)

    /**
     * Inserts one or more documents. A call to this method is equivalent to a call to the `bulkWrite` method
     *
     * Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.
     *
     * @param clientSession the client session with which to associate this operation
     * @param documents the documents to insert
     * @param options the options to apply to the operation
     * @return the insert many result
     * @throws com.mongodb.MongoBulkWriteException if there's an exception in the bulk write operation
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException if the write failed due some other failure
     * @throws IllegalArgumentException if the documents list is null or empty, or any of the documents in the list are
     *   null
     */
    public fun insertMany(
        clientSession: ClientSession,
        documents: List<T>,
        options: InsertManyOptions = InsertManyOptions()
    ): InsertManyResult = wrapped.insertMany(clientSession.wrapped, documents, options)

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * Use this method to only update the corresponding fields in the document according to the update operators used in
     * the update document. To replace the entire document with a new document, use the corresponding [replaceOne]
     * method.
     *
     * Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.
     *
     * @param filter a document describing the query filter, which may not be null.
     * @param update a document describing the update, which may not be null. The update to apply must include at least
     *   one update operator.
     * @param options the options to apply to the update operation
     * @return the result of the update one operation
     * @throws com.mongodb.MongoWriteException if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write
     *   concern
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException if the write failed due some other failure
     * @see [Modify Documents](https://www.mongodb.com/docs/manual/tutorial/modify-documents/)
     * @see [Update Operators](https://www.mongodb.com/docs/manual/reference/operator/update/)
     * @see [Update Command Behaviors](https://www.mongodb.com/docs/manual/reference/command/update/)
     * @see [replaceOne]
     */
    public fun updateOne(filter: Bson, update: Bson, options: UpdateOptions = UpdateOptions()): UpdateResult =
        wrapped.updateOne(filter, update, options)

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * Use this method to only update the corresponding fields in the document according to the update operators used in
     * the update document. To replace the entire document with a new document, use the corresponding [replaceOne]
     * method.
     *
     * Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter a document describing the query filter, which may not be null.
     * @param update a document describing the update, which may not be null. The update to apply must include at least
     *   one update operator.
     * @param options the options to apply to the update operation
     * @return the result of the update one operation
     * @throws com.mongodb.MongoWriteException if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write
     *   concern
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException if the write failed due some other failure
     * @see [Modify Documents](https://www.mongodb.com/docs/manual/tutorial/modify-documents/)
     * @see [Update Operators](https://www.mongodb.com/docs/manual/reference/operator/update/)
     * @see [Update Command](https://www.mongodb.com/docs/manual/reference/command/update/)
     * @see com.mongodb.client.MongoCollection.replaceOne
     */
    public fun updateOne(
        clientSession: ClientSession,
        filter: Bson,
        update: Bson,
        options: UpdateOptions = UpdateOptions()
    ): UpdateResult = wrapped.updateOne(clientSession.wrapped, filter, update, options)

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.
     *
     * @param filter a document describing the query filter, which may not be null.
     * @param update a pipeline describing the update, which may not be null.
     * @param options the options to apply to the update operation
     * @return the result of the update one operation
     * @throws com.mongodb.MongoWriteException if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoWriteConcernException if the write failed due being unable to fulfil the write concern
     * @throws com.mongodb.MongoException if the write failed due some other failure
     * @see [Modify Documents](https://www.mongodb.com/docs/manual/tutorial/modify-documents/)
     * @see [Update Operators](https://www.mongodb.com/docs/manual/reference/operator/update/)
     */
    public fun updateOne(filter: Bson, update: List<Bson>, options: UpdateOptions = UpdateOptions()): UpdateResult =
        wrapped.updateOne(filter, update, options)

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter a document describing the query filter, which may not be null.
     * @param update a pipeline describing the update, which may not be null.
     * @param options the options to apply to the update operation
     * @return the result of the update one operation
     * @throws com.mongodb.MongoWriteException if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoWriteConcernException if the write failed due being unable to fulfil the write concern
     * @throws com.mongodb.MongoException if the write failed due some other failure
     * @see [Modify Documents](https://www.mongodb.com/docs/manual/tutorial/modify-documents/)
     * @see [Update Operators](https://www.mongodb.com/docs/manual/reference/operator/update/)
     */
    public fun updateOne(
        clientSession: ClientSession,
        filter: Bson,
        update: List<Bson>,
        options: UpdateOptions = UpdateOptions()
    ): UpdateResult = wrapped.updateOne(clientSession.wrapped, filter, update, options)

    /**
     * Update all documents in the collection according to the specified arguments.
     *
     * @param filter a document describing the query filter, which may not be null.
     * @param update a document describing the update, which may not be null. The update to apply must include only
     *   update operators.
     * @param options the options to apply to the update operation
     * @return the result of the update many operation
     * @throws com.mongodb.MongoWriteException if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write
     *   concern
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException if the write failed due some other failure
     * @see [Modify Documents](https://www.mongodb.com/docs/manual/tutorial/modify-documents/)
     * @see [Update Operators](https://www.mongodb.com/docs/manual/reference/operator/update/)
     */
    public fun updateMany(filter: Bson, update: Bson, options: UpdateOptions = UpdateOptions()): UpdateResult =
        wrapped.updateMany(filter, update, options)

    /**
     * Update all documents in the collection according to the specified arguments.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter a document describing the query filter, which may not be null.
     * @param update a document describing the update, which may not be null. The update to apply must include only
     *   update operators.
     * @param options the options to apply to the update operation
     * @return the result of the update many operation
     * @throws com.mongodb.MongoWriteException if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write
     *   concern
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException if the write failed due some other failure
     * @see [Modify Documents](https://www.mongodb.com/docs/manual/tutorial/modify-documents/)
     * @see [Update Operators](https://www.mongodb.com/docs/manual/reference/operator/update/)
     */
    public fun updateMany(
        clientSession: ClientSession,
        filter: Bson,
        update: Bson,
        options: UpdateOptions = UpdateOptions()
    ): UpdateResult = wrapped.updateMany(clientSession.wrapped, filter, update, options)

    /**
     * Update all documents in the collection according to the specified arguments.
     *
     * @param filter a document describing the query filter, which may not be null.
     * @param update a pipeline describing the update, which may not be null.
     * @param options the options to apply to the update operation
     * @return the result of the update many operation
     * @throws com.mongodb.MongoWriteException if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoWriteConcernException if the write failed due being unable to fulfil the write concern
     * @throws com.mongodb.MongoException if the write failed due some other failure
     * @see [Modify Documents](https://www.mongodb.com/docs/manual/tutorial/modify-documents/)
     * @see [Update Operators](https://www.mongodb.com/docs/manual/reference/operator/update/)
     */
    public fun updateMany(filter: Bson, update: List<Bson>, options: UpdateOptions = UpdateOptions()): UpdateResult =
        wrapped.updateMany(filter, update, options)

    /**
     * Update all documents in the collection according to the specified arguments.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter a document describing the query filter, which may not be null.
     * @param update a pipeline describing the update, which may not be null.
     * @param options the options to apply to the update operation
     * @return the result of the update many operation
     * @throws com.mongodb.MongoWriteException if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoWriteConcernException if the write failed due being unable to fulfil the write concern
     * @throws com.mongodb.MongoException if the write failed due some other failure
     * @see [Modify Documents](https://www.mongodb.com/docs/manual/tutorial/modify-documents/)
     * @see [Update Operators](https://www.mongodb.com/docs/manual/reference/operator/update/)
     */
    public fun updateMany(
        clientSession: ClientSession,
        filter: Bson,
        update: List<Bson>,
        options: UpdateOptions = UpdateOptions()
    ): UpdateResult = wrapped.updateMany(clientSession.wrapped, filter, update, options)

    /**
     * Replace a document in the collection according to the specified arguments.
     *
     * Use this method to replace a document using the specified replacement argument. To update the document with
     * update operators, use the corresponding [updateOne] method.
     *
     * Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.
     *
     * @param filter the query filter to apply the replace operation
     * @param replacement the replacement document
     * @param options the options to apply to the replace operation
     * @return the result of the replace one operation
     * @throws com.mongodb.MongoWriteException if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write
     *   concern
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException if the write failed due some other failure
     * @see [Modify Documents](https://www.mongodb.com/docs/manual/tutorial/modify-documents/#replace-the-document/)
     * @see [Update Command Behaviors](https://www.mongodb.com/docs/manual/reference/command/update/)
     */
    public fun replaceOne(filter: Bson, replacement: T, options: ReplaceOptions = ReplaceOptions()): UpdateResult =
        wrapped.replaceOne(filter, replacement, options)

    /**
     * Replace a document in the collection according to the specified arguments.
     *
     * Use this method to replace a document using the specified replacement argument. To update the document with
     * update operators, use the corresponding [updateOne] method.
     *
     * Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter the query filter to apply the replace operation
     * @param replacement the replacement document
     * @param options the options to apply to the replace operation
     * @return the result of the replace one operation
     * @throws com.mongodb.MongoWriteException if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write
     *   concern
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException if the write failed due some other failure
     * @see [Modify Documents](https://www.mongodb.com/docs/manual/tutorial/modify-documents/#replace-the-document/)
     * @see [Update Command Behaviors](https://www.mongodb.com/docs/manual/reference/command/update/)
     */
    public fun replaceOne(
        clientSession: ClientSession,
        filter: Bson,
        replacement: T,
        options: ReplaceOptions = ReplaceOptions()
    ): UpdateResult = wrapped.replaceOne(clientSession.wrapped, filter, replacement, options)

    /**
     * Removes at most one document from the collection that matches the given filter.
     *
     * If no documents match, the collection is not modified.
     *
     * Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.
     *
     * @param filter the query filter to apply the delete operation
     * @param options the options to apply to the delete operation
     * @return the result of the remove one operation
     * @throws com.mongodb.MongoWriteException if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write
     *   concern
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException if the write failed due some other failure
     */
    public fun deleteOne(filter: Bson, options: DeleteOptions = DeleteOptions()): DeleteResult =
        wrapped.deleteOne(filter, options)

    /**
     * Removes at most one document from the collection that matches the given filter.
     *
     * If no documents match, the collection is not modified.
     *
     * Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter the query filter to apply the delete operation
     * @param options the options to apply to the delete operation
     * @return the result of the remove one operation
     * @throws com.mongodb.MongoWriteException if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write
     *   concern
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException if the write failed due some other failure
     */
    public fun deleteOne(
        clientSession: ClientSession,
        filter: Bson,
        options: DeleteOptions = DeleteOptions()
    ): DeleteResult = wrapped.deleteOne(clientSession.wrapped, filter, options)

    /**
     * Removes all documents from the collection that match the given query filter.
     *
     * If no documents match, the collection is not modified.
     *
     * @param filter the query filter to apply the delete operation
     * @param options the options to apply to the delete operation
     * @return the result of the remove many operation
     * @throws com.mongodb.MongoWriteException if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write
     *   concern
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException if the write failed due some other failure
     */
    public fun deleteMany(filter: Bson, options: DeleteOptions = DeleteOptions()): DeleteResult =
        wrapped.deleteMany(filter, options)

    /**
     * Removes all documents from the collection that match the given query filter.
     *
     * If no documents match, the collection is not modified.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter the query filter to apply the delete operation
     * @param options the options to apply to the delete operation
     * @return the result of the remove many operation
     * @throws com.mongodb.MongoWriteException if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write
     *   concern
     * @throws com.mongodb.MongoCommandException if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException if the write failed due some other failure
     */
    public fun deleteMany(
        clientSession: ClientSession,
        filter: Bson,
        options: DeleteOptions = DeleteOptions()
    ): DeleteResult = wrapped.deleteMany(clientSession.wrapped, filter, options)

    /**
     * Executes a mix of inserts, updates, replaces, and deletes.
     *
     * Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.
     * The eligibility for retryable write support for bulk operations is determined on the whole bulk write. If the
     * `requests` contain any `UpdateManyModels` or `DeleteManyModels` then the bulk operation will not support
     * retryable writes.
     *
     * @param requests the writes to execute
     * @param options the options to apply to the bulk write operation
     * @return the result of the bulk write
     * @throws com.mongodb.MongoBulkWriteException if there's an exception in the bulk write operation
     * @throws com.mongodb.MongoException if there's an exception running the operation
     */
    public fun bulkWrite(
        requests: List<WriteModel<out T>>,
        options: BulkWriteOptions = BulkWriteOptions()
    ): BulkWriteResult = wrapped.bulkWrite(requests, options)

    /**
     * Executes a mix of inserts, updates, replaces, and deletes.
     *
     * Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.
     * The eligibility for retryable write support for bulk operations is determined on the whole bulk write. If the
     * `requests` contain any `UpdateManyModels` or `DeleteManyModels` then the bulk operation will not support
     * retryable writes.
     *
     * @param clientSession the client session with which to associate this operation
     * @param requests the writes to execute
     * @param options the options to apply to the bulk write operation
     * @return the result of the bulk write
     * @throws com.mongodb.MongoBulkWriteException if there's an exception in the bulk write operation
     * @throws com.mongodb.MongoException if there's an exception running the operation
     */
    public fun bulkWrite(
        clientSession: ClientSession,
        requests: List<WriteModel<out T>>,
        options: BulkWriteOptions = BulkWriteOptions()
    ): BulkWriteResult = wrapped.bulkWrite(clientSession.wrapped, requests, options)

    /**
     * Atomically find a document and remove it.
     *
     * Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.
     *
     * @param filter the query filter to find the document with
     * @param options the options to apply to the operation
     * @return the document that was removed. If no documents matched the query filter, then null will be returned
     */
    public fun findOneAndDelete(filter: Bson, options: FindOneAndDeleteOptions = FindOneAndDeleteOptions()): T? =
        wrapped.findOneAndDelete(filter, options)

    /**
     * Atomically find a document and remove it.
     *
     * Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter the query filter to find the document with
     * @param options the options to apply to the operation
     * @return the document that was removed. If no documents matched the query filter, then null will be returned
     */
    public fun findOneAndDelete(
        clientSession: ClientSession,
        filter: Bson,
        options: FindOneAndDeleteOptions = FindOneAndDeleteOptions()
    ): T? = wrapped.findOneAndDelete(clientSession.wrapped, filter, options)

    /**
     * Atomically find a document and update it.
     *
     * Use this method to only update the corresponding fields in the document according to the update operators used in
     * the update document. To replace the entire document with a new document, use the corresponding
     * [findOneAndReplace] method.
     *
     * Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.
     *
     * @param filter a document describing the query filter, which may not be null.
     * @param update a document describing the update, which may not be null. The update to apply must include at least
     *   one update operator.
     * @param options the options to apply to the operation
     * @return the document that was updated. Depending on the value of the `returnOriginal` property, this will either
     *   be the document as it was before the update or as it is after the update. If no documents matched the query
     *   filter, then null will be returned
     * @see [Update Command Behaviors](https://www.mongodb.com/docs/manual/reference/command/update/)
     * @see com.mongodb.client.MongoCollection.findOneAndReplace
     */
    public fun findOneAndUpdate(
        filter: Bson,
        update: Bson,
        options: FindOneAndUpdateOptions = FindOneAndUpdateOptions()
    ): T? = wrapped.findOneAndUpdate(filter, update, options)

    /**
     * Atomically find a document and update it.
     *
     * Use this method to only update the corresponding fields in the document according to the update operators used in
     * the update document. To replace the entire document with a new document, use the corresponding
     * [findOneAndReplace] method.
     *
     * Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter a document describing the query filter, which may not be null.
     * @param update a document describing the update, which may not be null. The update to apply must include at least
     *   one update operator.
     * @param options the options to apply to the operation
     * @return the document that was updated. Depending on the value of the `returnOriginal` property, this will either
     *   be the document as it was before the update or as it is after the update. If no documents matched the query
     *   filter, then null will be returned
     * @see [Update Command Behaviors](https://www.mongodb.com/docs/manual/reference/command/update/)
     * @see com.mongodb.client.MongoCollection.findOneAndReplace
     */
    public fun findOneAndUpdate(
        clientSession: ClientSession,
        filter: Bson,
        update: Bson,
        options: FindOneAndUpdateOptions = FindOneAndUpdateOptions()
    ): T? = wrapped.findOneAndUpdate(clientSession.wrapped, filter, update, options)

    /**
     * Atomically find a document and update it.
     *
     * Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.
     *
     * @param filter a document describing the query filter, which may not be null.
     * @param update a pipeline describing the update, which may not be null.
     * @param options the options to apply to the operation
     * @return the document that was updated. Depending on the value of the `returnOriginal` property, this will either
     *   be the document as it was before the update or as it is after the update. If no documents matched the query
     *   filter, then null will be returned
     */
    public fun findOneAndUpdate(
        filter: Bson,
        update: List<Bson>,
        options: FindOneAndUpdateOptions = FindOneAndUpdateOptions()
    ): T? = wrapped.findOneAndUpdate(filter, update, options)

    /**
     * Atomically find a document and update it.
     *
     * Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter a document describing the query filter, which may not be null.
     * @param update a pipeline describing the update, which may not be null.
     * @param options the options to apply to the operation
     * @return the document that was updated. Depending on the value of the `returnOriginal` property, this will either
     *   be the document as it was before the update or as it is after the update. If no documents matched the query
     *   filter, then null will be returned
     */
    public fun findOneAndUpdate(
        clientSession: ClientSession,
        filter: Bson,
        update: List<Bson>,
        options: FindOneAndUpdateOptions = FindOneAndUpdateOptions()
    ): T? = wrapped.findOneAndUpdate(clientSession.wrapped, filter, update, options)

    /**
     * Atomically find a document and replace it.
     *
     * Use this method to replace a document using the specified replacement argument. To update the document with
     * update operators, use the corresponding [findOneAndUpdate] method.
     *
     * Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.
     *
     * @param filter the query filter to apply the replace operation
     * @param replacement the replacement document
     * @param options the options to apply to the operation
     * @return the document that was replaced. Depending on the value of the `returnOriginal` property, this will either
     *   be the document as it was before the update or as it is after the update. If no documents matched the query
     *   filter, then null will be returned
     * @see [Update Command Behaviors](https://www.mongodb.com/docs/manual/reference/command/update/)
     */
    public fun findOneAndReplace(
        filter: Bson,
        replacement: T,
        options: FindOneAndReplaceOptions = FindOneAndReplaceOptions()
    ): T? = wrapped.findOneAndReplace(filter, replacement, options)

    /**
     * Atomically find a document and replace it.
     *
     * Use this method to replace a document using the specified replacement argument. To update the document with
     * update operators, use the corresponding [findOneAndUpdate] method.
     *
     * Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter the query filter to apply the replace operation
     * @param replacement the replacement document
     * @param options the options to apply to the operation
     * @return the document that was replaced. Depending on the value of the `returnOriginal` property, this will either
     *   be the document as it was before the update or as it is after the update. If no documents matched the query
     *   filter, then null will be returned
     * @see [Update Command Behaviors](https://www.mongodb.com/docs/manual/reference/command/update/)
     */
    public fun findOneAndReplace(
        clientSession: ClientSession,
        filter: Bson,
        replacement: T,
        options: FindOneAndReplaceOptions = FindOneAndReplaceOptions()
    ): T? = wrapped.findOneAndReplace(clientSession.wrapped, filter, replacement, options)

    /**
     * Drops this collection from the Database.
     *
     * @param options various options for dropping the collection
     * @see [Drop Collection](https://www.mongodb.com/docs/manual/reference/command/drop/)
     */
    public fun drop(options: DropCollectionOptions = DropCollectionOptions()): Unit = wrapped.drop(options)

    /**
     * Drops this collection from the Database.
     *
     * @param clientSession the client session with which to associate this operation
     * @param options various options for dropping the collection
     * @see [Drop Collection](https://www.mongodb.com/docs/manual/reference/command/drop/)
     */
    public fun drop(clientSession: ClientSession, options: DropCollectionOptions = DropCollectionOptions()): Unit =
        wrapped.drop(clientSession.wrapped, options)

    /**
     * Create an Atlas Search index for the collection.
     *
     * @param indexName the name of the search index to create.
     * @param definition the search index mapping definition.
     * @return the search index name.
     * @see [Create search indexes](https://www.mongodb.com/docs/manual/reference/command/createSearchIndexes/)
     */
    public fun createSearchIndex(indexName: String, definition: Bson): String =
        wrapped.createSearchIndex(indexName, definition)

    /**
     * Create an Atlas Search index with `default` name for the collection.
     *
     * @param definition the search index mapping definition.
     * @return the search index name.
     * @see [Create search indexes](https://www.mongodb.com/docs/manual/reference/command/createSearchIndexes/)
     */
    public fun createSearchIndex(definition: Bson): String = wrapped.createSearchIndex(definition)

    /**
     * Create one or more Atlas Search indexes for the collection.
     *
     * <p>
     * The name can be omitted for a single index, in which case a name will be `default`. </p>
     *
     * @param searchIndexModels the search index models.
     * @return the search index names in the order specified by the given list of [SearchIndexModel]s.
     * @see [Create search indexes](https://www.mongodb.com/docs/manual/reference/command/createSearchIndexes/)
     */
    public fun createSearchIndexes(searchIndexModels: List<SearchIndexModel>): List<String> =
        wrapped.createSearchIndexes(searchIndexModels)

    /**
     * Update an Atlas Search index in the collection.
     *
     * @param indexName the name of the search index to update.
     * @param definition the search index mapping definition.
     * @see [Update search index](https://www.mongodb.com/docs/manual/reference/command/updateSearchIndex/)
     */
    public fun updateSearchIndex(indexName: String, definition: Bson) {
        wrapped.updateSearchIndex(indexName, definition)
    }

    /**
     * Drop an Atlas Search index given its name.
     *
     * @param indexName the name of the search index to drop.
     * @see [Drop search index](https://www.mongodb.com/docs/manual/reference/command/dropSearchIndex/)
     */
    public fun dropSearchIndex(indexName: String) {
        wrapped.dropSearchIndex(indexName)
    }

    /**
     * Get all the Atlas Search indexes in this collection.
     *
     * @return the list search indexes iterable interface.
     * @see [List search indexes](https://www.mongodb.com/docs/manual/reference/operator/aggregation/listSearchIndexes)
     */
    @JvmName("listSearchIndexesAsDocument")
    public fun listSearchIndexes(): ListSearchIndexesIterable<Document> = listSearchIndexes<Document>()

    /**
     * Get all the Atlas Search indexes in this collection.
     *
     * @param R the class to decode each document into.
     * @param resultClass the target document type of the iterable.
     * @return the list search indexes iterable interface.
     * @see [List search indexes](https://www.mongodb.com/docs/manual/reference/operator/aggregation/listSearchIndexes)
     */
    public fun <R : Any> listSearchIndexes(resultClass: Class<R>): ListSearchIndexesIterable<R> =
        ListSearchIndexesIterable(wrapped.listSearchIndexes(resultClass))

    /**
     * Get all the Atlas Search indexes in this collection.
     *
     * @param R the class to decode each document into.
     * @return the list search indexes iterable interface.
     * @see [List Atlas Search indexes]]
     *   (https://www.mongodb.com/docs/manual/reference/operator/aggregation/listSearchIndexes)
     */
    public inline fun <reified R : Any> listSearchIndexes(): ListSearchIndexesIterable<R> =
        listSearchIndexes(R::class.java)

    /**
     * Create an index with the given keys and options.
     *
     * @param keys an object describing the index key(s), which may not be null.
     * @param options the options for the index
     * @return the index name
     * @see [Create indexes](https://www.mongodb.com/docs/manual/reference/command/createIndexes/)
     */
    public fun createIndex(keys: Bson, options: IndexOptions = IndexOptions()): String =
        wrapped.createIndex(keys, options)

    /**
     * Create an index with the given keys and options.
     *
     * @param clientSession the client session with which to associate this operation
     * @param keys an object describing the index key(s), which may not be null.
     * @param options the options for the index
     * @return the index name
     * @see [Create indexes](https://www.mongodb.com/docs/manual/reference/command/createIndexes/)
     */
    public fun createIndex(clientSession: ClientSession, keys: Bson, options: IndexOptions = IndexOptions()): String =
        wrapped.createIndex(clientSession.wrapped, keys, options)

    /**
     * Create multiple indexes.
     *
     * @param indexes the list of indexes
     * @param options options to use when creating indexes
     * @return the list of index names
     * @see [Create indexes](https://www.mongodb.com/docs/manual/reference/command/createIndexes/)
     */
    public fun createIndexes(
        indexes: List<IndexModel>,
        options: CreateIndexOptions = CreateIndexOptions()
    ): List<String> = wrapped.createIndexes(indexes, options)

    /**
     * Create multiple indexes.
     *
     * @param clientSession the client session with which to associate this operation
     * @param indexes the list of indexes
     * @param options: options to use when creating indexes
     * @return the list of index names
     * @see [Create indexes](https://www.mongodb.com/docs/manual/reference/command/createIndexes/)
     */
    public fun createIndexes(
        clientSession: ClientSession,
        indexes: List<IndexModel>,
        options: CreateIndexOptions = CreateIndexOptions()
    ): List<String> = wrapped.createIndexes(clientSession.wrapped, indexes, options)

    /**
     * Get all the indexes in this collection.
     *
     * @return the list indexes iterable interface
     * @see [List indexes](https://www.mongodb.com/docs/manual/reference/command/listIndexes/)
     */
    @JvmName("listIndexesAsDocument") public fun listIndexes(): ListIndexesIterable<Document> = listIndexes<Document>()

    /**
     * Get all the indexes in this collection.
     *
     * @param clientSession the client session with which to associate this operation
     * @return the list indexes iterable interface
     * @see [List indexes](https://www.mongodb.com/docs/manual/reference/command/listIndexes/)
     */
    @JvmName("listIndexesAsDocumentWithSession")
    public fun listIndexes(clientSession: ClientSession): ListIndexesIterable<Document> =
        listIndexes<Document>(clientSession)

    /**
     * Get all the indexes in this collection.
     *
     * @param R the class to decode each document into
     * @param resultClass the target document type of the iterable.
     * @return the list indexes iterable interface
     * @see [List indexes](https://www.mongodb.com/docs/manual/reference/command/listIndexes/)
     */
    public fun <R : Any> listIndexes(resultClass: Class<R>): ListIndexesIterable<R> =
        ListIndexesIterable(wrapped.listIndexes(resultClass))

    /**
     * Get all the indexes in this collection.
     *
     * @param R the class to decode each document into
     * @param clientSession the client session with which to associate this operation
     * @param resultClass the target document type of the iterable.
     * @return the list indexes iterable interface
     * @see [List indexes](https://www.mongodb.com/docs/manual/reference/command/listIndexes/)
     */
    public fun <R : Any> listIndexes(clientSession: ClientSession, resultClass: Class<R>): ListIndexesIterable<R> =
        ListIndexesIterable(wrapped.listIndexes(clientSession.wrapped, resultClass))

    /**
     * Get all the indexes in this collection.
     *
     * @param R the class to decode each document into
     * @return the list indexes iterable interface
     * @see [List indexes](https://www.mongodb.com/docs/manual/reference/command/listIndexes/)
     */
    public inline fun <reified R : Any> listIndexes(): ListIndexesIterable<R> = listIndexes(R::class.java)

    /**
     * Get all the indexes in this collection.
     *
     * @param R the class to decode each document into
     * @param clientSession the client session with which to associate this operation
     * @return the list indexes iterable interface
     * @see [List indexes](https://www.mongodb.com/docs/manual/reference/command/listIndexes/)
     */
    public inline fun <reified R : Any> listIndexes(clientSession: ClientSession): ListIndexesIterable<R> =
        listIndexes(clientSession, R::class.java)

    /**
     * Drops the index given its name.
     *
     * @param indexName the name of the index to remove
     * @param options the options to use when dropping indexes
     * @see [Drop indexes](https://www.mongodb.com/docs/manual/reference/command/dropIndexes/)
     */
    public fun dropIndex(indexName: String, options: DropIndexOptions = DropIndexOptions()): Unit =
        wrapped.dropIndex(indexName, options)

    /**
     * Drops the index given the keys used to create it.
     *
     * @param keys the keys of the index to remove
     * @param options the options to use when dropping indexes
     * @see [Drop indexes](https://www.mongodb.com/docs/manual/reference/command/dropIndexes/)
     */
    public fun dropIndex(keys: Bson, options: DropIndexOptions = DropIndexOptions()): Unit =
        wrapped.dropIndex(keys, options)

    /**
     * Drops the index given its name.
     *
     * @param clientSession the client session with which to associate this operation
     * @param indexName the name of the index to remove
     * @param options the options to use when dropping indexes
     * @see [Drop indexes](https://www.mongodb.com/docs/manual/reference/command/dropIndexes/)
     */
    public fun dropIndex(
        clientSession: ClientSession,
        indexName: String,
        options: DropIndexOptions = DropIndexOptions()
    ): Unit = wrapped.dropIndex(clientSession.wrapped, indexName, options)

    /**
     * Drops the index given the keys used to create it.
     *
     * @param clientSession the client session with which to associate this operation
     * @param keys the keys of the index to remove
     * @param options the options to use when dropping indexes
     * @see [Drop indexes](https://www.mongodb.com/docs/manual/reference/command/dropIndexes/)
     */
    public fun dropIndex(
        clientSession: ClientSession,
        keys: Bson,
        options: DropIndexOptions = DropIndexOptions()
    ): Unit = wrapped.dropIndex(clientSession.wrapped, keys, options)

    /**
     * Drop all the indexes on this collection, except for the default on `_id`.
     *
     * @param options the options to use when dropping indexes
     * @see [Drop indexes](https://www.mongodb.com/docs/manual/reference/command/dropIndexes/)
     */
    public fun dropIndexes(options: DropIndexOptions = DropIndexOptions()): Unit = wrapped.dropIndexes(options)

    /**
     * Drop all the indexes on this collection, except for the default on `_id`.
     *
     * @param clientSession the client session with which to associate this operation
     * @param options the options to use when dropping indexes
     * @see [Drop indexes](https://www.mongodb.com/docs/manual/reference/command/dropIndexes/)
     */
    public fun dropIndexes(clientSession: ClientSession, options: DropIndexOptions = DropIndexOptions()): Unit =
        wrapped.dropIndexes(clientSession.wrapped, options)

    /**
     * Rename the collection with oldCollectionName to the newCollectionName.
     *
     * @param newCollectionNamespace the name the collection will be renamed to
     * @param options the options for renaming a collection
     * @throws com.mongodb.MongoServerException if you provide a newCollectionName that is the name of an existing
     *   collection and dropTarget is false, or if the oldCollectionName is the name of a collection that doesn't exist
     * @see [Rename collection](https://www.mongodb.com/docs/manual/reference/command/renameCollection/)
     */
    public fun renameCollection(
        newCollectionNamespace: MongoNamespace,
        options: RenameCollectionOptions = RenameCollectionOptions()
    ): Unit = wrapped.renameCollection(newCollectionNamespace, options)

    /**
     * Rename the collection with oldCollectionName to the newCollectionName.
     *
     * @param clientSession the client session with which to associate this operation
     * @param newCollectionNamespace the name the collection will be renamed to
     * @param options the options for renaming a collection
     * @throws com.mongodb.MongoServerException if you provide a newCollectionName that is the name of an existing
     *   collection and dropTarget is false, or if the oldCollectionName is the name of a collection that doesn't exist
     * @see [Rename collection](https://www.mongodb.com/docs/manual/reference/command/renameCollection/)
     */
    public fun renameCollection(
        clientSession: ClientSession,
        newCollectionNamespace: MongoNamespace,
        options: RenameCollectionOptions = RenameCollectionOptions()
    ): Unit = wrapped.renameCollection(clientSession.wrapped, newCollectionNamespace, options)
}

/**
 * maxTime extension function
 *
 * @param maxTime time in milliseconds
 * @return the options
 */
public fun CreateIndexOptions.maxTime(maxTime: Long): CreateIndexOptions =
    this.apply { maxTime(maxTime, TimeUnit.MILLISECONDS) }
/**
 * maxTime extension function
 *
 * @param maxTime time in milliseconds
 * @return the options
 */
public fun CountOptions.maxTime(maxTime: Long): CountOptions = this.apply { maxTime(maxTime, TimeUnit.MILLISECONDS) }
/**
 * maxTime extension function
 *
 * @param maxTime time in milliseconds
 * @return the options
 */
public fun DropIndexOptions.maxTime(maxTime: Long): DropIndexOptions =
    this.apply { maxTime(maxTime, TimeUnit.MILLISECONDS) }
/**
 * maxTime extension function
 *
 * @param maxTime time in milliseconds
 * @return the options
 */
public fun EstimatedDocumentCountOptions.maxTime(maxTime: Long): EstimatedDocumentCountOptions =
    this.apply { maxTime(maxTime, TimeUnit.MILLISECONDS) }
/**
 * maxTime extension function
 *
 * @param maxTime time in milliseconds
 * @return the options
 */
public fun FindOneAndDeleteOptions.maxTime(maxTime: Long): FindOneAndDeleteOptions =
    this.apply { maxTime(maxTime, TimeUnit.MILLISECONDS) }
/**
 * maxTime extension function
 *
 * @param maxTime time in milliseconds
 * @return the options
 */
public fun FindOneAndReplaceOptions.maxTime(maxTime: Long): FindOneAndReplaceOptions =
    this.apply { maxTime(maxTime, TimeUnit.MILLISECONDS) }
/**
 * maxTime extension function
 *
 * @param maxTime time in milliseconds
 * @return the options
 */
public fun FindOneAndUpdateOptions.maxTime(maxTime: Long): FindOneAndUpdateOptions =
    this.apply { maxTime(maxTime, TimeUnit.MILLISECONDS) }
/**
 * expireAfter extension function
 *
 * @param expireAfter time in seconds
 * @return the options
 */
public fun IndexOptions.expireAfter(expireAfter: Long): IndexOptions =
    this.apply { expireAfter(expireAfter, TimeUnit.SECONDS) }
