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

import com.mongodb.MongoNamespace
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.WriteConcern
import com.mongodb.bulk.BulkWriteResult
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
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.model.WriteModel
import com.mongodb.client.result.DeleteResult
import com.mongodb.client.result.InsertManyResult
import com.mongodb.client.result.InsertOneResult
import com.mongodb.client.result.UpdateResult
import com.mongodb.reactivestreams.client.MongoCollection as JMongoCollection
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import org.bson.BsonDocument
import org.bson.Document
import org.bson.codecs.configuration.CodecRegistry
import org.bson.conversions.Bson

/**
 * The MongoCollection representation.
 *
 * Note: Additions to this interface will not be considered to break binary compatibility.
 *
 * @param T The type that this collection will encode documents from and decode documents to.
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
    public suspend fun countDocuments(filter: Bson = BsonDocument(), options: CountOptions = CountOptions()): Long =
        wrapped.countDocuments(filter, options).awaitSingle()

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
    public suspend fun countDocuments(
        clientSession: ClientSession,
        filter: Bson = BsonDocument(),
        options: CountOptions = CountOptions()
    ): Long = wrapped.countDocuments(clientSession.wrapped, filter, options).awaitSingle()

    /**
     * Gets an estimate of the count of documents in a collection using collection metadata.
     *
     * Implementation note: this method is implemented using the MongoDB server's count command
     *
     * @param options the options describing the count
     * @return the number of documents in the collection
     * @see [Count behaviour](https://www.mongodb.com/docs/manual/reference/command/count/#behavior)
     */
    public suspend fun estimatedDocumentCount(
        options: EstimatedDocumentCountOptions = EstimatedDocumentCountOptions()
    ): Long = wrapped.estimatedDocumentCount(options).awaitSingle()

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
    public fun <R : Any> distinct(
        fieldName: String,
        filter: Bson = BsonDocument(),
        resultClass: Class<R>
    ): DistinctFlow<R> = DistinctFlow(wrapped.distinct(fieldName, filter, resultClass))

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
    public fun <R : Any> distinct(
        clientSession: ClientSession,
        fieldName: String,
        filter: Bson = BsonDocument(),
        resultClass: Class<R>
    ): DistinctFlow<R> = DistinctFlow(wrapped.distinct(clientSession.wrapped, fieldName, filter, resultClass))

    /**
     * Gets the distinct values of the specified field name.
     *
     * @param R the target type of the iterable.
     * @param fieldName the field name
     * @param filter the query filter
     * @return an iterable of distinct values
     * @see [Distinct command](https://www.mongodb.com/docs/manual/reference/command/distinct/)
     */
    public inline fun <reified R : Any> distinct(fieldName: String, filter: Bson = BsonDocument()): DistinctFlow<R> =
        distinct(fieldName, filter, R::class.java)

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
    public inline fun <reified R : Any> distinct(
        clientSession: ClientSession,
        fieldName: String,
        filter: Bson = BsonDocument()
    ): DistinctFlow<R> = distinct(clientSession, fieldName, filter, R::class.java)

    /**
     * Finds all documents in the collection.
     *
     * @param filter the query filter
     * @return the find iterable interface
     * @see [Query Documents](https://www.mongodb.com/docs/manual/tutorial/query-documents/)
     */
    @JvmName("findAsT") public fun find(filter: Bson = BsonDocument()): FindFlow<T> = find(filter, documentClass)

    /**
     * Finds all documents in the collection.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter the query filter
     * @return the find iterable interface
     * @see [Query Documents](https://www.mongodb.com/docs/manual/tutorial/query-documents/)
     */
    @JvmName("findAsTWithSession")
    public fun find(clientSession: ClientSession, filter: Bson = BsonDocument()): FindFlow<T> =
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
    public fun <R : Any> find(filter: Bson = BsonDocument(), resultClass: Class<R>): FindFlow<R> =
        FindFlow(wrapped.find(filter, resultClass))

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
    ): FindFlow<R> = FindFlow(wrapped.find(clientSession.wrapped, filter, resultClass))

    /**
     * Finds all documents in the collection.
     *
     * @param R the class to decode each document into
     * @param filter the query filter
     * @return the find iterable interface
     * @see [Query Documents](https://www.mongodb.com/docs/manual/tutorial/query-documents/)
     */
    public inline fun <reified R : Any> find(filter: Bson = BsonDocument()): FindFlow<R> = find(filter, R::class.java)

    /**
     * Finds all documents in the collection.
     *
     * @param R the class to decode each document into
     * @param clientSession the client session with which to associate this operation
     * @param filter the query filter
     * @return the find iterable interface
     * @see [Query Documents](https://www.mongodb.com/docs/manual/tutorial/query-documents/)
     */
    public inline fun <reified R : Any> find(clientSession: ClientSession, filter: Bson = BsonDocument()): FindFlow<R> =
        find(clientSession, filter, R::class.java)

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     *
     * @param pipeline the aggregation pipeline
     * @return an iterable containing the result of the aggregation operation
     * @see [Aggregate Command](https://www.mongodb.com/docs/manual/reference/command/aggregate/#dbcmd.aggregate/)
     */
    @JvmName("aggregateAsT")
    public fun aggregate(pipeline: List<Bson>): AggregateFlow<T> =
        AggregateFlow(wrapped.aggregate(pipeline, documentClass))

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     *
     * @param clientSession the client session with which to associate this operation
     * @param pipeline the aggregation pipeline
     * @return an iterable containing the result of the aggregation operation
     * @see [Aggregate Command](https://www.mongodb.com/docs/manual/reference/command/aggregate/#dbcmd.aggregate/)
     */
    @JvmName("aggregateAsTWithSession")
    public fun aggregate(clientSession: ClientSession, pipeline: List<Bson>): AggregateFlow<T> =
        AggregateFlow(wrapped.aggregate(clientSession.wrapped, pipeline, documentClass))

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     *
     * @param R the class to decode each document into
     * @param pipeline the aggregation pipeline
     * @param resultClass the target document type of the iterable.
     * @return an iterable containing the result of the aggregation operation
     * @see [Aggregate Command](https://www.mongodb.com/docs/manual/reference/command/aggregate/#dbcmd.aggregate/)
     */
    public fun <R : Any> aggregate(pipeline: List<Bson>, resultClass: Class<R>): AggregateFlow<R> =
        AggregateFlow(wrapped.aggregate(pipeline, resultClass))

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
    ): AggregateFlow<R> = AggregateFlow(wrapped.aggregate(clientSession.wrapped, pipeline, resultClass))

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     *
     * @param R the class to decode each document into
     * @param pipeline the aggregation pipeline
     * @return an iterable containing the result of the aggregation operation
     * @see [Aggregate Command](https://www.mongodb.com/docs/manual/reference/command/aggregate/#dbcmd.aggregate/)
     */
    public inline fun <reified R : Any> aggregate(pipeline: List<Bson>): AggregateFlow<R> =
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
    ): AggregateFlow<R> = aggregate(clientSession, pipeline, R::class.java)

    /**
     * Creates a change stream for this collection.
     *
     * @param pipeline the aggregation pipeline to apply to the change stream, defaults to an empty pipeline.
     * @return the change stream iterable
     * @see [Change Streams](https://dochub.mongodb.org/changestreams]
     */
    @JvmName("watchAsDocument")
    public fun watch(pipeline: List<Bson> = emptyList()): ChangeStreamFlow<T> = watch(pipeline, documentClass)

    /**
     * Creates a change stream for this collection.
     *
     * @param clientSession the client session with which to associate this operation
     * @param pipeline the aggregation pipeline to apply to the change stream, defaults to an empty pipeline.
     * @return the change stream iterable
     * @see [Change Streams](https://dochub.mongodb.org/changestreams]
     */
    @JvmName("watchAsDocumentWithSession")
    public fun watch(clientSession: ClientSession, pipeline: List<Bson> = emptyList()): ChangeStreamFlow<T> =
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
    public fun <R : Any> watch(pipeline: List<Bson> = emptyList(), resultClass: Class<R>): ChangeStreamFlow<R> =
        ChangeStreamFlow(wrapped.watch(pipeline, resultClass))

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
    ): ChangeStreamFlow<R> = ChangeStreamFlow(wrapped.watch(clientSession.wrapped, pipeline, resultClass))

    /**
     * Creates a change stream for this collection.
     *
     * @param R the target document type of the iterable.
     * @param pipeline the aggregation pipeline to apply to the change stream, defaults to an empty pipeline.
     * @return the change stream iterable
     * @see [Change Streams](https://dochub.mongodb.org/changestreams]
     */
    public inline fun <reified R : Any> watch(pipeline: List<Bson> = emptyList()): ChangeStreamFlow<R> =
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
    ): ChangeStreamFlow<R> = watch(clientSession, pipeline, R::class.java)

    /**
     * Aggregates documents according to the specified map-reduce function.
     *
     * @param mapFunction A JavaScript function that associates or "maps" a value with a key and emits the key and value
     *   pair.
     * @param reduceFunction A JavaScript function that "reduces" to a single object all the values associated with a
     *   particular key.
     * @return an iterable containing the result of the map-reduce operation
     * @see [map-reduce](https://www.mongodb.com/docs/manual/reference/command/mapReduce/)
     */
    @Suppress("DEPRECATION")
    @Deprecated("Map Reduce has been deprecated. Use Aggregation instead", replaceWith = ReplaceWith(""))
    @JvmName("mapReduceAsT")
    public fun mapReduce(mapFunction: String, reduceFunction: String): MapReduceFlow<T> =
        mapReduce(mapFunction, reduceFunction, documentClass)

    /**
     * Aggregates documents according to the specified map-reduce function.
     *
     * @param clientSession the client session with which to associate this operation
     * @param mapFunction A JavaScript function that associates or "maps" a value with a key and emits the key and value
     *   pair.
     * @param reduceFunction A JavaScript function that "reduces" to a single object all the values associated with a
     *   particular key.
     * @return an iterable containing the result of the map-reduce operation
     * @see [map-reduce](https://www.mongodb.com/docs/manual/reference/command/mapReduce/)
     */
    @Suppress("DEPRECATION")
    @Deprecated("Map Reduce has been deprecated. Use Aggregation instead", replaceWith = ReplaceWith(""))
    @JvmName("mapReduceAsTWithSession")
    public fun mapReduce(clientSession: ClientSession, mapFunction: String, reduceFunction: String): MapReduceFlow<T> =
        mapReduce(clientSession, mapFunction, reduceFunction, documentClass)

    /**
     * Aggregates documents according to the specified map-reduce function.
     *
     * @param R the class to decode each resulting document into.
     * @param mapFunction A JavaScript function that associates or "maps" a value with a key and emits the key and value
     *   pair.
     * @param reduceFunction A JavaScript function that "reduces" to a single object all the values associated with a
     *   particular key.
     * @param resultClass the target document type of the iterable.
     * @return an iterable containing the result of the map-reduce operation
     * @see [map-reduce](https://www.mongodb.com/docs/manual/reference/command/mapReduce/)
     */
    @Suppress("DEPRECATION")
    @Deprecated("Map Reduce has been deprecated. Use Aggregation instead", replaceWith = ReplaceWith(""))
    public fun <R : Any> mapReduce(
        mapFunction: String,
        reduceFunction: String,
        resultClass: Class<R>
    ): MapReduceFlow<R> = MapReduceFlow(wrapped.mapReduce(mapFunction, reduceFunction, resultClass))

    /**
     * Aggregates documents according to the specified map-reduce function.
     *
     * @param R the class to decode each resulting document into.
     * @param clientSession the client session with which to associate this operation
     * @param mapFunction A JavaScript function that associates or "maps" a value with a key and emits the key and value
     *   pair.
     * @param reduceFunction A JavaScript function that "reduces" to a single object all the values associated with a
     *   particular key.
     * @param resultClass the target document type of the iterable.
     * @return an iterable containing the result of the map-reduce operation
     * @see [map-reduce](https://www.mongodb.com/docs/manual/reference/command/mapReduce/)
     */
    @Suppress("DEPRECATION")
    @Deprecated("Map Reduce has been deprecated. Use Aggregation instead", replaceWith = ReplaceWith(""))
    public fun <R : Any> mapReduce(
        clientSession: ClientSession,
        mapFunction: String,
        reduceFunction: String,
        resultClass: Class<R>
    ): MapReduceFlow<R> =
        MapReduceFlow(wrapped.mapReduce(clientSession.wrapped, mapFunction, reduceFunction, resultClass))

    /**
     * Aggregates documents according to the specified map-reduce function.
     *
     * @param R the class to decode each resulting document into.
     * @param mapFunction A JavaScript function that associates or "maps" a value with a key and emits the key and value
     *   pair.
     * @param reduceFunction A JavaScript function that "reduces" to a single object all the values associated with a
     *   particular key.
     * @return an iterable containing the result of the map-reduce operation
     * @see [map-reduce](https://www.mongodb.com/docs/manual/reference/command/mapReduce/)
     */
    @Suppress("DEPRECATION")
    @Deprecated("Map Reduce has been deprecated. Use Aggregation instead", replaceWith = ReplaceWith(""))
    public inline fun <reified R : Any> mapReduce(mapFunction: String, reduceFunction: String): MapReduceFlow<R> =
        mapReduce(mapFunction, reduceFunction, R::class.java)

    /**
     * Aggregates documents according to the specified map-reduce function.
     *
     * @param R the class to decode each resulting document into.
     * @param clientSession the client session with which to associate this operation
     * @param mapFunction A JavaScript function that associates or "maps" a value with a key and emits the key and value
     *   pair.
     * @param reduceFunction A JavaScript function that "reduces" to a single object all the values associated with a
     *   particular key.
     * @return an iterable containing the result of the map-reduce operation
     * @see [map-reduce](https://www.mongodb.com/docs/manual/reference/command/mapReduce/)
     */
    @Suppress("DEPRECATION")
    @Deprecated("Map Reduce has been deprecated. Use Aggregation instead", replaceWith = ReplaceWith(""))
    public inline fun <reified R : Any> mapReduce(
        clientSession: ClientSession,
        mapFunction: String,
        reduceFunction: String
    ): MapReduceFlow<R> = mapReduce(clientSession, mapFunction, reduceFunction, R::class.java)

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
    public suspend fun insertOne(document: T, options: InsertOneOptions = InsertOneOptions()): InsertOneResult =
        wrapped.insertOne(document, options).awaitSingle()

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
    public suspend fun insertOne(
        clientSession: ClientSession,
        document: T,
        options: InsertOneOptions = InsertOneOptions()
    ): InsertOneResult = wrapped.insertOne(clientSession.wrapped, document, options).awaitSingle()

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
    public suspend fun insertMany(
        documents: List<T>,
        options: InsertManyOptions = InsertManyOptions()
    ): InsertManyResult = wrapped.insertMany(documents, options).awaitSingle()

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
    public suspend fun insertMany(
        clientSession: ClientSession,
        documents: List<T>,
        options: InsertManyOptions = InsertManyOptions()
    ): InsertManyResult = wrapped.insertMany(clientSession.wrapped, documents, options).awaitSingle()

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
    public suspend fun updateOne(filter: Bson, update: Bson, options: UpdateOptions = UpdateOptions()): UpdateResult =
        wrapped.updateOne(filter, update, options).awaitSingle()

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
    public suspend fun updateOne(
        clientSession: ClientSession,
        filter: Bson,
        update: Bson,
        options: UpdateOptions = UpdateOptions()
    ): UpdateResult = wrapped.updateOne(clientSession.wrapped, filter, update, options).awaitSingle()

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
    public suspend fun updateOne(
        filter: Bson,
        update: List<Bson>,
        options: UpdateOptions = UpdateOptions()
    ): UpdateResult = wrapped.updateOne(filter, update, options).awaitSingle()

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
    public suspend fun updateOne(
        clientSession: ClientSession,
        filter: Bson,
        update: List<Bson>,
        options: UpdateOptions = UpdateOptions()
    ): UpdateResult = wrapped.updateOne(clientSession.wrapped, filter, update, options).awaitSingle()

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
    public suspend fun updateMany(filter: Bson, update: Bson, options: UpdateOptions = UpdateOptions()): UpdateResult =
        wrapped.updateMany(filter, update, options).awaitSingle()

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
    public suspend fun updateMany(
        clientSession: ClientSession,
        filter: Bson,
        update: Bson,
        options: UpdateOptions = UpdateOptions()
    ): UpdateResult = wrapped.updateMany(clientSession.wrapped, filter, update, options).awaitSingle()

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
    public suspend fun updateMany(
        filter: Bson,
        update: List<Bson>,
        options: UpdateOptions = UpdateOptions()
    ): UpdateResult = wrapped.updateMany(filter, update, options).awaitSingle()

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
    public suspend fun updateMany(
        clientSession: ClientSession,
        filter: Bson,
        update: List<Bson>,
        options: UpdateOptions = UpdateOptions()
    ): UpdateResult = wrapped.updateMany(clientSession.wrapped, filter, update, options).awaitSingle()

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
     * @since 3.6
     */
    public suspend fun replaceOne(
        filter: Bson,
        replacement: T,
        options: ReplaceOptions = ReplaceOptions()
    ): UpdateResult = wrapped.replaceOne(filter, replacement, options).awaitSingle()

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
     * @since 3.6
     */
    public suspend fun replaceOne(
        clientSession: ClientSession,
        filter: Bson,
        replacement: T,
        options: ReplaceOptions = ReplaceOptions()
    ): UpdateResult = wrapped.replaceOne(clientSession.wrapped, filter, replacement, options).awaitSingle()

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
    public suspend fun deleteOne(filter: Bson, options: DeleteOptions = DeleteOptions()): DeleteResult =
        wrapped.deleteOne(filter, options).awaitSingle()

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
    public suspend fun deleteOne(
        clientSession: ClientSession,
        filter: Bson,
        options: DeleteOptions = DeleteOptions()
    ): DeleteResult = wrapped.deleteOne(clientSession.wrapped, filter, options).awaitSingle()

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
    public suspend fun deleteMany(filter: Bson, options: DeleteOptions = DeleteOptions()): DeleteResult =
        wrapped.deleteMany(filter, options).awaitSingle()

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
    public suspend fun deleteMany(
        clientSession: ClientSession,
        filter: Bson,
        options: DeleteOptions = DeleteOptions()
    ): DeleteResult = wrapped.deleteMany(clientSession.wrapped, filter, options).awaitSingle()

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
    public suspend fun bulkWrite(
        requests: List<WriteModel<out T>>,
        options: BulkWriteOptions = BulkWriteOptions()
    ): BulkWriteResult = wrapped.bulkWrite(requests, options).awaitSingle()

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
    public suspend fun bulkWrite(
        clientSession: ClientSession,
        requests: List<WriteModel<out T>>,
        options: BulkWriteOptions = BulkWriteOptions()
    ): BulkWriteResult = wrapped.bulkWrite(clientSession.wrapped, requests, options).awaitSingle()

    /**
     * Atomically find a document and remove it.
     *
     * Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.
     *
     * @param filter the query filter to find the document with
     * @param options the options to apply to the operation
     * @return the document that was removed. If no documents matched the query filter, then null will be returned
     */
    public suspend fun findOneAndDelete(
        filter: Bson,
        options: FindOneAndDeleteOptions = FindOneAndDeleteOptions()
    ): T? = wrapped.findOneAndDelete(filter, options).awaitFirstOrNull()

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
    public suspend fun findOneAndDelete(
        clientSession: ClientSession,
        filter: Bson,
        options: FindOneAndDeleteOptions = FindOneAndDeleteOptions()
    ): T? = wrapped.findOneAndDelete(clientSession.wrapped, filter, options).awaitFirstOrNull()

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
    public suspend fun findOneAndUpdate(
        filter: Bson,
        update: Bson,
        options: FindOneAndUpdateOptions = FindOneAndUpdateOptions()
    ): T? = wrapped.findOneAndUpdate(filter, update, options).awaitFirstOrNull()

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
    public suspend fun findOneAndUpdate(
        clientSession: ClientSession,
        filter: Bson,
        update: Bson,
        options: FindOneAndUpdateOptions = FindOneAndUpdateOptions()
    ): T? = wrapped.findOneAndUpdate(clientSession.wrapped, filter, update, options).awaitFirstOrNull()

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
    public suspend fun findOneAndUpdate(
        filter: Bson,
        update: List<Bson>,
        options: FindOneAndUpdateOptions = FindOneAndUpdateOptions()
    ): T? = wrapped.findOneAndUpdate(filter, update, options).awaitFirstOrNull()

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
    public suspend fun findOneAndUpdate(
        clientSession: ClientSession,
        filter: Bson,
        update: List<Bson>,
        options: FindOneAndUpdateOptions = FindOneAndUpdateOptions()
    ): T? = wrapped.findOneAndUpdate(clientSession.wrapped, filter, update, options).awaitFirstOrNull()

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
    public suspend fun findOneAndReplace(
        filter: Bson,
        replacement: T,
        options: FindOneAndReplaceOptions = FindOneAndReplaceOptions()
    ): T? = wrapped.findOneAndReplace(filter, replacement, options).awaitFirstOrNull()

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
    public suspend fun findOneAndReplace(
        clientSession: ClientSession,
        filter: Bson,
        replacement: T,
        options: FindOneAndReplaceOptions = FindOneAndReplaceOptions()
    ): T? = wrapped.findOneAndReplace(clientSession.wrapped, filter, replacement, options).awaitFirstOrNull()

    /**
     * Drops this collection from the Database.
     *
     * @param options various options for dropping the collection
     * @see [Drop Collection](https://www.mongodb.com/docs/manual/reference/command/drop/)
     */
    public suspend fun drop(options: DropCollectionOptions = DropCollectionOptions()) {
        wrapped.drop(options).awaitFirstOrNull()
    }
    /**
     * Drops this collection from the Database.
     *
     * @param clientSession the client session with which to associate this operation
     * @param options various options for dropping the collection
     * @see [Drop Collection](https://www.mongodb.com/docs/manual/reference/command/drop/)
     */
    public suspend fun drop(clientSession: ClientSession, options: DropCollectionOptions = DropCollectionOptions()) {
        wrapped.drop(clientSession.wrapped, options).awaitFirstOrNull()
    }

    /**
     * Create an index with the given keys and options.
     *
     * @param keys an object describing the index key(s), which may not be null.
     * @param options the options for the index
     * @return the index name
     * @see [Create indexes](https://www.mongodb.com/docs/manual/reference/command/createIndexes/)
     */
    public suspend fun createIndex(keys: Bson, options: IndexOptions = IndexOptions()): String =
        wrapped.createIndex(keys, options).awaitSingle()

    /**
     * Create an index with the given keys and options.
     *
     * @param clientSession the client session with which to associate this operation
     * @param keys an object describing the index key(s), which may not be null.
     * @param options the options for the index
     * @return the index name
     * @see [Create indexes](https://www.mongodb.com/docs/manual/reference/command/createIndexes/)
     */
    public suspend fun createIndex(
        clientSession: ClientSession,
        keys: Bson,
        options: IndexOptions = IndexOptions()
    ): String = wrapped.createIndex(clientSession.wrapped, keys, options).awaitSingle()

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
    ): Flow<String> = wrapped.createIndexes(indexes, options).asFlow()

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
    ): Flow<String> = wrapped.createIndexes(clientSession.wrapped, indexes, options).asFlow()

    /**
     * Get all the indexes in this collection.
     *
     * @return the list indexes iterable interface
     * @see [List indexes](https://www.mongodb.com/docs/manual/reference/command/listIndexes/)
     */
    @JvmName("listIndexesAsDocument") public fun listIndexes(): ListIndexesFlow<Document> = listIndexes<Document>()

    /**
     * Get all the indexes in this collection.
     *
     * @param clientSession the client session with which to associate this operation
     * @return the list indexes iterable interface
     * @see [List indexes](https://www.mongodb.com/docs/manual/reference/command/listIndexes/)
     */
    @JvmName("listIndexesAsDocumentWithSession")
    public fun listIndexes(clientSession: ClientSession): ListIndexesFlow<Document> =
        listIndexes<Document>(clientSession)

    /**
     * Get all the indexes in this collection.
     *
     * @param R the class to decode each document into
     * @param resultClass the target document type of the iterable.
     * @return the list indexes iterable interface
     * @see [List indexes](https://www.mongodb.com/docs/manual/reference/command/listIndexes/)
     */
    public fun <R : Any> listIndexes(resultClass: Class<R>): ListIndexesFlow<R> =
        ListIndexesFlow(wrapped.listIndexes(resultClass))

    /**
     * Get all the indexes in this collection.
     *
     * @param R the class to decode each document into
     * @param clientSession the client session with which to associate this operation
     * @param resultClass the target document type of the iterable.
     * @return the list indexes iterable interface
     * @see [List indexes](https://www.mongodb.com/docs/manual/reference/command/listIndexes/)
     */
    public fun <R : Any> listIndexes(clientSession: ClientSession, resultClass: Class<R>): ListIndexesFlow<R> =
        ListIndexesFlow(wrapped.listIndexes(clientSession.wrapped, resultClass))

    /**
     * Get all the indexes in this collection.
     *
     * @param R the class to decode each document into
     * @return the list indexes iterable interface
     * @see [List indexes](https://www.mongodb.com/docs/manual/reference/command/listIndexes/)
     */
    public inline fun <reified R : Any> listIndexes(): ListIndexesFlow<R> = listIndexes(R::class.java)

    /**
     * Get all the indexes in this collection.
     *
     * @param R the class to decode each document into
     * @param clientSession the client session with which to associate this operation
     * @return the list indexes iterable interface
     * @see [List indexes](https://www.mongodb.com/docs/manual/reference/command/listIndexes/)
     */
    public inline fun <reified R : Any> listIndexes(clientSession: ClientSession): ListIndexesFlow<R> =
        listIndexes(clientSession, R::class.java)

    /**
     * Drops the index given its name.
     *
     * @param indexName the name of the index to remove
     * @param options the options to use when dropping indexes
     * @see [Drop indexes](https://www.mongodb.com/docs/manual/reference/command/dropIndexes/)
     */
    public suspend fun dropIndex(indexName: String, options: DropIndexOptions = DropIndexOptions()) {
        wrapped.dropIndex(indexName, options).awaitFirstOrNull()
    }

    /**
     * Drops the index given the keys used to create it.
     *
     * @param keys the keys of the index to remove
     * @param options the options to use when dropping indexes
     * @see [Drop indexes](https://www.mongodb.com/docs/manual/reference/command/dropIndexes/)
     */
    public suspend fun dropIndex(keys: Bson, options: DropIndexOptions = DropIndexOptions()) {
        wrapped.dropIndex(keys, options).awaitFirstOrNull()
    }

    /**
     * Drops the index given its name.
     *
     * @param clientSession the client session with which to associate this operation
     * @param indexName the name of the index to remove
     * @param options the options to use when dropping indexes
     * @see [Drop indexes](https://www.mongodb.com/docs/manual/reference/command/dropIndexes/)
     */
    public suspend fun dropIndex(
        clientSession: ClientSession,
        indexName: String,
        options: DropIndexOptions = DropIndexOptions()
    ) {
        wrapped.dropIndex(clientSession.wrapped, indexName, options).awaitFirstOrNull()
    }

    /**
     * Drops the index given the keys used to create it.
     *
     * @param clientSession the client session with which to associate this operation
     * @param keys the keys of the index to remove
     * @param options the options to use when dropping indexes
     * @see [Drop indexes](https://www.mongodb.com/docs/manual/reference/command/dropIndexes/)
     */
    public suspend fun dropIndex(
        clientSession: ClientSession,
        keys: Bson,
        options: DropIndexOptions = DropIndexOptions()
    ) {
        wrapped.dropIndex(clientSession.wrapped, keys, options).awaitFirstOrNull()
    }

    /**
     * Drop all the indexes on this collection, except for the default on `_id`.
     *
     * @param options the options to use when dropping indexes
     * @see [Drop indexes](https://www.mongodb.com/docs/manual/reference/command/dropIndexes/)
     */
    public suspend fun dropIndexes(options: DropIndexOptions = DropIndexOptions()) {
        wrapped.dropIndexes(options).awaitFirstOrNull()
    }

    /**
     * Drop all the indexes on this collection, except for the default on `_id`.
     *
     * @param clientSession the client session with which to associate this operation
     * @param options the options to use when dropping indexes
     * @see [Drop indexes](https://www.mongodb.com/docs/manual/reference/command/dropIndexes/)
     */
    public suspend fun dropIndexes(clientSession: ClientSession, options: DropIndexOptions = DropIndexOptions()) {
        wrapped.dropIndexes(clientSession.wrapped, options).awaitFirstOrNull()
    }

    /**
     * Rename the collection with oldCollectionName to the newCollectionName.
     *
     * @param newCollectionNamespace the name the collection will be renamed to
     * @param options the options for renaming a collection
     * @throws com.mongodb.MongoServerException if you provide a newCollectionName that is the name of an existing
     *   collection and dropTarget is false, or if the oldCollectionName is the name of a collection that doesn't exist
     * @see [Rename collection](https://www.mongodb.com/docs/manual/reference/command/renameCollection/)
     */
    public suspend fun renameCollection(
        newCollectionNamespace: MongoNamespace,
        options: RenameCollectionOptions = RenameCollectionOptions()
    ) {
        wrapped.renameCollection(newCollectionNamespace, options).awaitFirstOrNull()
    }

    /**
     * Rename the collection with oldCollectionName to the newCollectionName.
     *
     * @param clientSession the client session with which to associate this operation
     * @param newCollectionNamespace the name the collection will be renamed to
     * @param options the options for renaming a collection
     * @throws com.mongodb.MongoServerException if you provide a newCollectionName that is the name of an existing
     *   collection and dropTarget is false, or if the oldCollectionName is the name of a collection that doesn't exist
     * @see [Rename collection](https://www.mongodb.com/docs/manual/reference/command/renameCollection/)
     * @since 3.6
     */
    public suspend fun renameCollection(
        clientSession: ClientSession,
        newCollectionNamespace: MongoNamespace,
        options: RenameCollectionOptions = RenameCollectionOptions()
    ) {
        wrapped.renameCollection(clientSession.wrapped, newCollectionNamespace, options).awaitFirstOrNull()
    }
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
