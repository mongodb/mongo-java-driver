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

package com.mongodb.client;

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.DropIndexOptions;
import com.mongodb.client.model.EstimatedDocumentCountOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.lang.Nullable;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.List;

/**
 * The MongoCollection interface.
 *
 * <p>Note: Additions to this interface will not be considered to break binary compatibility.</p>
 *
 * <p>MongoCollection is generic allowing for different types to represent documents. Any custom classes must have a
 * {@link org.bson.codecs.Codec} registered in the {@link CodecRegistry}.
 * </p>
 *
 * @param <TDocument> The type that this collection will encode documents from and decode documents to.
 * @since 3.0
 */
@ThreadSafe
public interface MongoCollection<TDocument> {

    /**
     * Gets the namespace of this collection.
     *
     * @return the namespace
     */
    MongoNamespace getNamespace();

    /**
     * Get the class of documents stored in this collection.
     *
     * @return the class
     */
    Class<TDocument> getDocumentClass();

    /**
     * Get the codec registry for the MongoCollection.
     *
     * @return the {@link org.bson.codecs.configuration.CodecRegistry}
     */
    CodecRegistry getCodecRegistry();

    /**
     * Get the read preference for the MongoCollection.
     *
     * @return the {@link com.mongodb.ReadPreference}
     */
    ReadPreference getReadPreference();

    /**
     * Get the write concern for the MongoCollection.
     *
     * @return the {@link com.mongodb.WriteConcern}
     */
    WriteConcern getWriteConcern();

    /**
     * Get the read concern for the MongoCollection.
     *
     * @return the {@link com.mongodb.ReadConcern}
     * @since 3.2
     * @mongodb.server.release 3.2
     * @mongodb.driver.manual reference/readConcern/ Read Concern
     */
    ReadConcern getReadConcern();

    /**
     * Create a new MongoCollection instance with a different default class to cast any documents returned from the database into..
     *
     * @param clazz the default class to cast any documents returned from the database into.
     * @param <NewTDocument> The type that the new collection will encode documents from and decode documents to
     * @return a new MongoCollection instance with the different default class
     */
    <NewTDocument> MongoCollection<NewTDocument> withDocumentClass(Class<NewTDocument> clazz);

    /**
     * Create a new MongoCollection instance with a different codec registry.
     *
     * @param codecRegistry the new {@link org.bson.codecs.configuration.CodecRegistry} for the collection
     * @return a new MongoCollection instance with the different codec registry
     */
    MongoCollection<TDocument> withCodecRegistry(CodecRegistry codecRegistry);

    /**
     * Create a new MongoCollection instance with a different read preference.
     *
     * @param readPreference the new {@link com.mongodb.ReadPreference} for the collection
     * @return a new MongoCollection instance with the different readPreference
     */
    MongoCollection<TDocument> withReadPreference(ReadPreference readPreference);

    /**
     * Create a new MongoCollection instance with a different write concern.
     *
     * @param writeConcern the new {@link com.mongodb.WriteConcern} for the collection
     * @return a new MongoCollection instance with the different writeConcern
     */
    MongoCollection<TDocument> withWriteConcern(WriteConcern writeConcern);

    /**
     * Create a new MongoCollection instance with a different read concern.
     *
     * @param readConcern the new {@link ReadConcern} for the collection
     * @return a new MongoCollection instance with the different ReadConcern
     * @since 3.2
     * @mongodb.server.release 3.2
     * @mongodb.driver.manual reference/readConcern/ Read Concern
     */
    MongoCollection<TDocument> withReadConcern(ReadConcern readConcern);

    /**
     * Counts the number of documents in the collection.
     *
     * @return the number of documents in the collection
     * @deprecated use {@link #countDocuments()} or {@link #estimatedDocumentCount()} instead
     */
    @Deprecated
    long count();

    /**
     * Counts the number of documents in the collection according to the given options.
     *
     * @param filter the query filter
     * @return the number of documents in the collection
     * @deprecated use {@link #countDocuments(Bson)} instead
     */
    @Deprecated
    long count(Bson filter);

    /**
     * Counts the number of documents in the collection according to the given options.
     *
     * @param filter  the query filter
     * @param options the options describing the count
     * @return the number of documents in the collection
     * @deprecated use {@link #countDocuments(Bson, CountOptions)} instead
     */
    @Deprecated
    long count(Bson filter, CountOptions options);

    /**
     * Counts the number of documents in the collection.
     *
     * @param clientSession the client session with which to associate this operation
     * @return the number of documents in the collection
     * @since 3.6
     * @mongodb.server.release 3.6
     * @deprecated use {@link #countDocuments(ClientSession)} instead
     */
    @Deprecated
    long count(ClientSession clientSession);

    /**
     * Counts the number of documents in the collection according to the given options.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter the query filter
     * @return the number of documents in the collection
     * @since 3.6
     * @mongodb.server.release 3.6
     * @deprecated use {@link #countDocuments(ClientSession, Bson)} instead
     */
    @Deprecated
    long count(ClientSession clientSession, Bson filter);

    /**
     * Counts the number of documents in the collection according to the given options.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter  the query filter
     * @param options the options describing the count
     * @return the number of documents in the collection
     * @since 3.6
     * @mongodb.server.release 3.6
     * @deprecated use {@link #countDocuments(ClientSession, Bson, CountOptions)} instead
     */
    @Deprecated
    long count(ClientSession clientSession, Bson filter, CountOptions options);

    /**
     * Counts the number of documents in the collection.
     *
     * <p>
     * Note: When migrating from {@code count()} to {@code countDocuments()} the following query operators must be replaced:
     * </p>
     * <pre>
     *
     *  +-------------+--------------------------------+
     *  | Operator    | Replacement                    |
     *  +=============+================================+
     *  | $where      |  $expr                         |
     *  +-------------+--------------------------------+
     *  | $near       |  $geoWithin with $center       |
     *  +-------------+--------------------------------+
     *  | $nearSphere |  $geoWithin with $centerSphere |
     *  +-------------+--------------------------------+
     * </pre>
     *
     * @return the number of documents in the collection
     * @since 3.8
     */
    long countDocuments();

    /**
     * Counts the number of documents in the collection according to the given options.
     *
     * <p>
     * Note: When migrating from {@code count()} to {@code countDocuments()} the following query operators must be replaced:
     * </p>
     * <pre>
     *
     *  +-------------+--------------------------------+
     *  | Operator    | Replacement                    |
     *  +=============+================================+
     *  | $where      |  $expr                         |
     *  +-------------+--------------------------------+
     *  | $near       |  $geoWithin with $center       |
     *  +-------------+--------------------------------+
     *  | $nearSphere |  $geoWithin with $centerSphere |
     *  +-------------+--------------------------------+
     * </pre>
     *
     * @param filter the query filter
     * @return the number of documents in the collection
     * @since 3.8
     */
    long countDocuments(Bson filter);

    /**
     * Counts the number of documents in the collection according to the given options.
     *
     * <p>
     * Note: When migrating from {@code count()} to {@code countDocuments()} the following query operators must be replaced:
     * </p>
     * <pre>
     *
     *  +-------------+--------------------------------+
     *  | Operator    | Replacement                    |
     *  +=============+================================+
     *  | $where      |  $expr                         |
     *  +-------------+--------------------------------+
     *  | $near       |  $geoWithin with $center       |
     *  +-------------+--------------------------------+
     *  | $nearSphere |  $geoWithin with $centerSphere |
     *  +-------------+--------------------------------+
     * </pre>
     *
     * @param filter  the query filter
     * @param options the options describing the count
     * @return the number of documents in the collection
     * @since 3.8
     */
    long countDocuments(Bson filter, CountOptions options);

    /**
     * Counts the number of documents in the collection.
     *
     * <p>
     * Note: When migrating from {@code count()} to {@code countDocuments()} the following query operators must be replaced:
     * </p>
     * <pre>
     *
     *  +-------------+--------------------------------+
     *  | Operator    | Replacement                    |
     *  +=============+================================+
     *  | $where      |  $expr                         |
     *  +-------------+--------------------------------+
     *  | $near       |  $geoWithin with $center       |
     *  +-------------+--------------------------------+
     *  | $nearSphere |  $geoWithin with $centerSphere |
     *  +-------------+--------------------------------+
     * </pre>
     *
     * @param clientSession the client session with which to associate this operation
     * @return the number of documents in the collection
     * @since 3.8
     * @mongodb.server.release 3.6
     */
    long countDocuments(ClientSession clientSession);

    /**
     * Counts the number of documents in the collection according to the given options.
     *
     * <p>
     * Note: When migrating from {@code count()} to {@code countDocuments()} the following query operators must be replaced:
     * </p>
     * <pre>
     *
     *  +-------------+--------------------------------+
     *  | Operator    | Replacement                    |
     *  +=============+================================+
     *  | $where      |  $expr                         |
     *  +-------------+--------------------------------+
     *  | $near       |  $geoWithin with $center       |
     *  +-------------+--------------------------------+
     *  | $nearSphere |  $geoWithin with $centerSphere |
     *  +-------------+--------------------------------+
     * </pre>
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter the query filter
     * @return the number of documents in the collection
     * @since 3.8
     * @mongodb.server.release 3.6
     */
    long countDocuments(ClientSession clientSession, Bson filter);

    /**
     * Counts the number of documents in the collection according to the given options.
     *
     * <p>
     * Note: When migrating from {@code count()} to {@code countDocuments()} the following query operators must be replaced:
     * </p>
     * <pre>
     *
     *  +-------------+--------------------------------+
     *  | Operator    | Replacement                    |
     *  +=============+================================+
     *  | $where      |  $expr                         |
     *  +-------------+--------------------------------+
     *  | $near       |  $geoWithin with $center       |
     *  +-------------+--------------------------------+
     *  | $nearSphere |  $geoWithin with $centerSphere |
     *  +-------------+--------------------------------+
     * </pre>
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter  the query filter
     * @param options the options describing the count
     * @return the number of documents in the collection
     * @since 3.8
     * @mongodb.server.release 3.6
     */
    long countDocuments(ClientSession clientSession, Bson filter, CountOptions options);

    /**
     * Gets an estimate of the count of documents in a collection using collection metadata.
     *
     * @return the number of documents in the collection
     * @since 3.8
     */
    long estimatedDocumentCount();

    /**
     * Gets an estimate of the count of documents in a collection using collection metadata.
     *
     * @param options the options describing the count
     * @return the number of documents in the collection
     * @since 3.8
     */
    long estimatedDocumentCount(EstimatedDocumentCountOptions options);

    /**
     * Gets the distinct values of the specified field name.
     *
     * @param fieldName   the field name
     * @param resultClass the class to cast any distinct items into.
     * @param <TResult>   the target type of the iterable.
     * @return an iterable of distinct values
     * @mongodb.driver.manual reference/command/distinct/ Distinct
     */
    <TResult> DistinctIterable<TResult> distinct(String fieldName, Class<TResult> resultClass);

    /**
     * Gets the distinct values of the specified field name.
     *
     * @param fieldName   the field name
     * @param filter      the query filter
     * @param resultClass the class to cast any distinct items into.
     * @param <TResult>   the target type of the iterable.
     * @return an iterable of distinct values
     * @mongodb.driver.manual reference/command/distinct/ Distinct
     */
    <TResult> DistinctIterable<TResult> distinct(String fieldName, Bson filter, Class<TResult> resultClass);

    /**
     * Gets the distinct values of the specified field name.
     *
     * @param clientSession the client session with which to associate this operation
     * @param fieldName   the field name
     * @param resultClass the class to cast any distinct items into.
     * @param <TResult>   the target type of the iterable.
     * @return an iterable of distinct values
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual reference/command/distinct/ Distinct
     */
    <TResult> DistinctIterable<TResult> distinct(ClientSession clientSession, String fieldName, Class<TResult> resultClass);

    /**
     * Gets the distinct values of the specified field name.
     *
     * @param clientSession the client session with which to associate this operation
     * @param fieldName   the field name
     * @param filter      the query filter
     * @param resultClass the class to cast any distinct items into.
     * @param <TResult>   the target type of the iterable.
     * @return an iterable of distinct values
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual reference/command/distinct/ Distinct
     */
    <TResult> DistinctIterable<TResult> distinct(ClientSession clientSession, String fieldName, Bson filter, Class<TResult> resultClass);

    /**
     * Finds all documents in the collection.
     *
     * @return the find iterable interface
     * @mongodb.driver.manual tutorial/query-documents/ Find
     */
    FindIterable<TDocument> find();

    /**
     * Finds all documents in the collection.
     *
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the find iterable interface
     * @mongodb.driver.manual tutorial/query-documents/ Find
     */
    <TResult> FindIterable<TResult> find(Class<TResult> resultClass);

    /**
     * Finds all documents in the collection.
     *
     * @param filter the query filter
     * @return the find iterable interface
     * @mongodb.driver.manual tutorial/query-documents/ Find
     */
    FindIterable<TDocument> find(Bson filter);

    /**
     * Finds all documents in the collection.
     *
     * @param filter      the query filter
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the find iterable interface
     * @mongodb.driver.manual tutorial/query-documents/ Find
     */
    <TResult> FindIterable<TResult> find(Bson filter, Class<TResult> resultClass);

    /**
     * Finds all documents in the collection.
     *
     * @param clientSession the client session with which to associate this operation
     * @return the find iterable interface
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual tutorial/query-documents/ Find
     */
    FindIterable<TDocument> find(ClientSession clientSession);

    /**
     * Finds all documents in the collection.
     *
     * @param clientSession the client session with which to associate this operation
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the find iterable interface
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual tutorial/query-documents/ Find
     */
    <TResult> FindIterable<TResult> find(ClientSession clientSession, Class<TResult> resultClass);

    /**
     * Finds all documents in the collection.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter the query filter
     * @return the find iterable interface
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual tutorial/query-documents/ Find
     */
    FindIterable<TDocument> find(ClientSession clientSession, Bson filter);

    /**
     * Finds all documents in the collection.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter      the query filter
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the find iterable interface
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual tutorial/query-documents/ Find
     */
    <TResult> FindIterable<TResult> find(ClientSession clientSession, Bson filter, Class<TResult> resultClass);

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     *
     * @param pipeline the aggregation pipeline
     * @return an iterable containing the result of the aggregation operation
     * @mongodb.driver.manual aggregation/ Aggregation
     * @mongodb.server.release 2.2
     */
    AggregateIterable<TDocument> aggregate(List<? extends Bson> pipeline);

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     *
     * @param pipeline    the aggregation pipeline
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return an iterable containing the result of the aggregation operation
     * @mongodb.driver.manual aggregation/ Aggregation
     * @mongodb.server.release 2.2
     */
    <TResult> AggregateIterable<TResult> aggregate(List<? extends Bson> pipeline, Class<TResult> resultClass);

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     *
     * @param clientSession the client session with which to associate this operation
     * @param pipeline the aggregation pipeline
     * @return an iterable containing the result of the aggregation operation
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual aggregation/ Aggregation
     */
    AggregateIterable<TDocument> aggregate(ClientSession clientSession, List<? extends Bson> pipeline);

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     *
     * @param clientSession the client session with which to associate this operation
     * @param pipeline    the aggregation pipeline
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return an iterable containing the result of the aggregation operation
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual aggregation/ Aggregation
     */
    <TResult> AggregateIterable<TResult> aggregate(ClientSession clientSession, List<? extends Bson> pipeline, Class<TResult> resultClass);

    /**
     * Creates a change stream for this collection.
     *
     * @return the change stream iterable
     * @mongodb.driver.dochub core/changestreams Change Streams
     * @since 3.6
     */
    ChangeStreamIterable<TDocument> watch();

    /**
     * Creates a change stream for this collection.
     *
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the change stream iterable
     * @mongodb.driver.dochub core/changestreams Change Streams
     * @since 3.6
     */
    <TResult> ChangeStreamIterable<TResult> watch(Class<TResult> resultClass);

    /**
     * Creates a change stream for this collection.
     *
     * @param pipeline the aggregation pipeline to apply to the change stream.
     * @return the change stream iterable
     * @mongodb.driver.dochub core/changestreams Change Streams
     * @since 3.6
     */
    ChangeStreamIterable<TDocument> watch(List<? extends Bson> pipeline);

    /**
     * Creates a change stream for this collection.
     *
     * @param pipeline    the aggregation pipeline to apply to the change stream
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the change stream iterable
     * @mongodb.driver.dochub core/changestreams Change Streams
     * @since 3.6
     */
    <TResult> ChangeStreamIterable<TResult> watch(List<? extends Bson> pipeline, Class<TResult> resultClass);

    /**
     * Creates a change stream for this collection.
     *
     * @param clientSession the client session with which to associate this operation
     * @return the change stream iterable
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.dochub core/changestreams Change Streams
     */
    ChangeStreamIterable<TDocument> watch(ClientSession clientSession);

    /**
     * Creates a change stream for this collection.
     *
     * @param clientSession the client session with which to associate this operation
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the change stream iterable
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.dochub core/changestreams Change Streams
     */
    <TResult> ChangeStreamIterable<TResult> watch(ClientSession clientSession, Class<TResult> resultClass);

    /**
     * Creates a change stream for this collection.
     *
     * @param clientSession the client session with which to associate this operation
     * @param pipeline the aggregation pipeline to apply to the change stream.
     * @return the change stream iterable
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.dochub core/changestreams Change Streams
     */
    ChangeStreamIterable<TDocument> watch(ClientSession clientSession, List<? extends Bson> pipeline);

    /**
     * Creates a change stream for this collection.
     *
     * @param clientSession the client session with which to associate this operation
     * @param pipeline    the aggregation pipeline to apply to the change stream
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the change stream iterable
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.dochub core/changestreams Change Streams
     */
    <TResult> ChangeStreamIterable<TResult> watch(ClientSession clientSession, List<? extends Bson> pipeline, Class<TResult> resultClass);

    /**
     * Aggregates documents according to the specified map-reduce function.
     *
     * @param mapFunction    A JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduceFunction A JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @return an iterable containing the result of the map-reduce operation
     * @mongodb.driver.manual reference/command/mapReduce/ map-reduce
     */
    MapReduceIterable<TDocument> mapReduce(String mapFunction, String reduceFunction);

    /**
     * Aggregates documents according to the specified map-reduce function.
     *
     * @param mapFunction    A JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduceFunction A JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @param resultClass    the class to decode each resulting document into.
     * @param <TResult>      the target document type of the iterable.
     * @return an iterable containing the result of the map-reduce operation
     * @mongodb.driver.manual reference/command/mapReduce/ map-reduce
     */
    <TResult> MapReduceIterable<TResult> mapReduce(String mapFunction, String reduceFunction, Class<TResult> resultClass);

    /**
     * Aggregates documents according to the specified map-reduce function.
     *
     * @param clientSession the client session with which to associate this operation
     * @param mapFunction    A JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduceFunction A JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @return an iterable containing the result of the map-reduce operation
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual reference/command/mapReduce/ map-reduce
     */
    MapReduceIterable<TDocument> mapReduce(ClientSession clientSession, String mapFunction, String reduceFunction);

    /**
     * Aggregates documents according to the specified map-reduce function.
     *
     * @param clientSession the client session with which to associate this operation
     * @param mapFunction    A JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduceFunction A JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @param resultClass    the class to decode each resulting document into.
     * @param <TResult>      the target document type of the iterable.
     * @return an iterable containing the result of the map-reduce operation
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual reference/command/mapReduce/ map-reduce
     */
    <TResult> MapReduceIterable<TResult> mapReduce(ClientSession clientSession, String mapFunction, String reduceFunction,
                                                   Class<TResult> resultClass);

    /**
     * Executes a mix of inserts, updates, replaces, and deletes.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.
     * The eligibility for retryable write support for bulk operations is determined on the whole bulk write. If the {@code requests}
     * contain any {@code UpdateManyModels} or {@code DeleteManyModels} then the bulk operation will not support retryable writes.</p>
     * @param requests the writes to execute
     * @return the result of the bulk write
     * @throws com.mongodb.MongoBulkWriteException if there's an exception in the bulk write operation
     * @throws com.mongodb.MongoException          if there's an exception running the operation
     */
    BulkWriteResult bulkWrite(List<? extends WriteModel<? extends TDocument>> requests);

    /**
     * Executes a mix of inserts, updates, replaces, and deletes.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.
     * The eligibility for retryable write support for bulk operations is determined on the whole bulk write. If the {@code requests}
     * contain any {@code UpdateManyModels} or {@code DeleteManyModels} then the bulk operation will not support retryable writes.</p>
     * @param requests the writes to execute
     * @param options  the options to apply to the bulk write operation
     * @return the result of the bulk write
     * @throws com.mongodb.MongoBulkWriteException if there's an exception in the bulk write operation
     * @throws com.mongodb.MongoException          if there's an exception running the operation
     */
    BulkWriteResult bulkWrite(List<? extends WriteModel<? extends TDocument>> requests, BulkWriteOptions options);

    /**
     * Executes a mix of inserts, updates, replaces, and deletes.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.
     * The eligibility for retryable write support for bulk operations is determined on the whole bulk write. If the {@code requests}
     * contain any {@code UpdateManyModels} or {@code DeleteManyModels} then the bulk operation will not support retryable writes.</p>
     * @param clientSession the client session with which to associate this operation
     * @param requests the writes to execute
     * @return the result of the bulk write
     * @throws com.mongodb.MongoBulkWriteException if there's an exception in the bulk write operation
     * @throws com.mongodb.MongoException          if there's an exception running the operation
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    BulkWriteResult bulkWrite(ClientSession clientSession, List<? extends WriteModel<? extends TDocument>> requests);

    /**
     * Executes a mix of inserts, updates, replaces, and deletes.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.
     * The eligibility for retryable write support for bulk operations is determined on the whole bulk write. If the {@code requests}
     * contain any {@code UpdateManyModels} or {@code DeleteManyModels} then the bulk operation will not support retryable writes.</p>
     * @param clientSession the client session with which to associate this operation
     * @param requests the writes to execute
     * @param options  the options to apply to the bulk write operation
     * @return the result of the bulk write
     * @throws com.mongodb.MongoBulkWriteException if there's an exception in the bulk write operation
     * @throws com.mongodb.MongoException          if there's an exception running the operation
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    BulkWriteResult bulkWrite(ClientSession clientSession, List<? extends WriteModel<? extends TDocument>> requests,
                              BulkWriteOptions options);

    /**
     * Inserts the provided document. If the document is missing an identifier, the driver should generate one.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param document the document to insert
     * @throws com.mongodb.MongoWriteException        if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write concern
     * @throws com.mongodb.MongoCommandException      if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     */
    void insertOne(TDocument document);

    /**
     * Inserts the provided document. If the document is missing an identifier, the driver should generate one.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param document the document to insert
     * @param options  the options to apply to the operation
     * @throws com.mongodb.MongoWriteException        if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write concern
     * @throws com.mongodb.MongoCommandException      if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     * @since 3.2
     */
    void insertOne(TDocument document, InsertOneOptions options);

    /**
     * Inserts the provided document. If the document is missing an identifier, the driver should generate one.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param clientSession the client session with which to associate this operation
     * @param document      the document to insert
     * @throws com.mongodb.MongoWriteException        if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write concern
     * @throws com.mongodb.MongoCommandException      if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void insertOne(ClientSession clientSession, TDocument document);

    /**
     * Inserts the provided document. If the document is missing an identifier, the driver should generate one.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param clientSession the client session with which to associate this operation
     * @param document      the document to insert
     * @param options       the options to apply to the operation
     * @throws com.mongodb.MongoWriteException        if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write concern
     * @throws com.mongodb.MongoCommandException      if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void insertOne(ClientSession clientSession, TDocument document, InsertOneOptions options);

    /**
     * Inserts one or more documents.  A call to this method is equivalent to a call to the {@code bulkWrite} method
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param documents the documents to insert
     * @throws com.mongodb.MongoBulkWriteException if there's an exception in the bulk write operation
     * @throws com.mongodb.MongoCommandException   if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException          if the write failed due some other failure
     * @see com.mongodb.client.MongoCollection#bulkWrite
     */
    void insertMany(List<? extends TDocument> documents);

    /**
     * Inserts one or more documents.  A call to this method is equivalent to a call to the {@code bulkWrite} method
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param documents the documents to insert
     * @param options   the options to apply to the operation
     * @throws com.mongodb.MongoBulkWriteException if there's an exception in the bulk write operation
     * @throws com.mongodb.MongoCommandException   if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException          if the write failed due some other failure
     */
    void insertMany(List<? extends TDocument> documents, InsertManyOptions options);

    /**
     * Inserts one or more documents.  A call to this method is equivalent to a call to the {@code bulkWrite} method
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param clientSession the client session with which to associate this operation
     * @param documents the documents to insert
     * @throws com.mongodb.MongoBulkWriteException if there's an exception in the bulk write operation
     * @throws com.mongodb.MongoCommandException   if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException          if the write failed due some other failure
     * @see com.mongodb.client.MongoCollection#bulkWrite
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void insertMany(ClientSession clientSession, List<? extends TDocument> documents);

    /**
     * Inserts one or more documents.  A call to this method is equivalent to a call to the {@code bulkWrite} method
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param clientSession the client session with which to associate this operation
     * @param documents the documents to insert
     * @param options   the options to apply to the operation
     * @throws com.mongodb.MongoBulkWriteException if there's an exception in the bulk write operation
     * @throws com.mongodb.MongoCommandException   if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException          if the write failed due some other failure
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void insertMany(ClientSession clientSession, List<? extends TDocument> documents, InsertManyOptions options);

    /**
     * Removes at most one document from the collection that matches the given filter.  If no documents match, the collection is not
     * modified.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param filter the query filter to apply the the delete operation
     * @return the result of the remove one operation
     * @throws com.mongodb.MongoWriteException        if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write concern
     * @throws com.mongodb.MongoCommandException      if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     */
    DeleteResult deleteOne(Bson filter);

    /**
     * Removes at most one document from the collection that matches the given filter.  If no documents match, the collection is not
     * modified.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param filter the query filter to apply the the delete operation
     * @param options  the options to apply to the delete operation
     * @return the result of the remove one operation
     * @throws com.mongodb.MongoWriteException        if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write concern
     * @throws com.mongodb.MongoCommandException      if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     * @since 3.4
     */
    DeleteResult deleteOne(Bson filter, DeleteOptions options);

    /**
     * Removes at most one document from the collection that matches the given filter.  If no documents match, the collection is not
     * modified.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param clientSession the client session with which to associate this operation
     * @param filter the query filter to apply the the delete operation
     * @return the result of the remove one operation
     * @throws com.mongodb.MongoWriteException        if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write concern
     * @throws com.mongodb.MongoCommandException      if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    DeleteResult deleteOne(ClientSession clientSession, Bson filter);

    /**
     * Removes at most one document from the collection that matches the given filter.  If no documents match, the collection is not
     * modified.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param clientSession the client session with which to associate this operation
     * @param filter the query filter to apply the the delete operation
     * @param options  the options to apply to the delete operation
     * @return the result of the remove one operation
     * @throws com.mongodb.MongoWriteException        if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write concern
     * @throws com.mongodb.MongoCommandException      if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    DeleteResult deleteOne(ClientSession clientSession, Bson filter, DeleteOptions options);

    /**
     * Removes all documents from the collection that match the given query filter.  If no documents match, the collection is not modified.
     *
     * @param filter the query filter to apply the the delete operation
     * @return the result of the remove many operation
     * @throws com.mongodb.MongoWriteException        if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write concern
     * @throws com.mongodb.MongoCommandException      if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     */
    DeleteResult deleteMany(Bson filter);

    /**
     * Removes all documents from the collection that match the given query filter.  If no documents match, the collection is not modified.
     *
     * @param filter the query filter to apply the the delete operation
     * @param options  the options to apply to the delete operation
     * @return the result of the remove many operation
     * @throws com.mongodb.MongoWriteException        if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write concern
     * @throws com.mongodb.MongoCommandException      if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     * @since 3.4
     */
    DeleteResult deleteMany(Bson filter, DeleteOptions options);

    /**
     * Removes all documents from the collection that match the given query filter.  If no documents match, the collection is not modified.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter the query filter to apply the the delete operation
     * @return the result of the remove many operation
     * @throws com.mongodb.MongoWriteException        if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write concern
     * @throws com.mongodb.MongoCommandException      if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    DeleteResult deleteMany(ClientSession clientSession, Bson filter);

    /**
     * Removes all documents from the collection that match the given query filter.  If no documents match, the collection is not modified.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter the query filter to apply the the delete operation
     * @param options  the options to apply to the delete operation
     * @return the result of the remove many operation
     * @throws com.mongodb.MongoWriteException        if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write concern
     * @throws com.mongodb.MongoCommandException      if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    DeleteResult deleteMany(ClientSession clientSession, Bson filter, DeleteOptions options);

    /**
     * Replace a document in the collection according to the specified arguments.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param filter      the query filter to apply the the replace operation
     * @param replacement the replacement document
     * @return the result of the replace one operation
     * @throws com.mongodb.MongoWriteException        if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write concern
     * @throws com.mongodb.MongoCommandException      if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     * @mongodb.driver.manual tutorial/modify-documents/#replace-the-document Replace
     */
    UpdateResult replaceOne(Bson filter, TDocument replacement);

    /**
     * Replace a document in the collection according to the specified arguments.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param filter        the query filter to apply the the replace operation
     * @param replacement   the replacement document
     * @param updateOptions the options to apply to the replace operation
     * @return the result of the replace one operation
     * @throws com.mongodb.MongoWriteException        if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write concern
     * @throws com.mongodb.MongoCommandException      if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     * @mongodb.driver.manual tutorial/modify-documents/#replace-the-document Replace
     * @deprecated use {@link #replaceOne(Bson, Object, ReplaceOptions)} instead
     */
    @Deprecated
    UpdateResult replaceOne(Bson filter, TDocument replacement, UpdateOptions updateOptions);

    /**
     * Replace a document in the collection according to the specified arguments.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param filter        the query filter to apply the the replace operation
     * @param replacement   the replacement document
     * @param replaceOptions the options to apply to the replace operation
     * @return the result of the replace one operation
     * @throws com.mongodb.MongoWriteException        if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write concern
     * @throws com.mongodb.MongoCommandException      if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     * @mongodb.driver.manual tutorial/modify-documents/#replace-the-document Replace
     * @since 3.7
     */
    UpdateResult replaceOne(Bson filter, TDocument replacement, ReplaceOptions replaceOptions);

    /**
     * Replace a document in the collection according to the specified arguments.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param clientSession the client session with which to associate this operation
     * @param filter      the query filter to apply the the replace operation
     * @param replacement the replacement document
     * @return the result of the replace one operation
     * @throws com.mongodb.MongoWriteException        if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write concern
     * @throws com.mongodb.MongoCommandException      if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual tutorial/modify-documents/#replace-the-document Replace
     */
    UpdateResult replaceOne(ClientSession clientSession, Bson filter, TDocument replacement);

    /**
     * Replace a document in the collection according to the specified arguments.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param clientSession the client session with which to associate this operation
     * @param filter        the query filter to apply the the replace operation
     * @param replacement   the replacement document
     * @param updateOptions the options to apply to the replace operation
     * @return the result of the replace one operation
     * @throws com.mongodb.MongoWriteException        if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write concern
     * @throws com.mongodb.MongoCommandException      if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual tutorial/modify-documents/#replace-the-document Replace
     * @deprecated use {@link #replaceOne(ClientSession, Bson, Object, ReplaceOptions)} instead
     */
    @Deprecated
    UpdateResult replaceOne(ClientSession clientSession, Bson filter, TDocument replacement, UpdateOptions updateOptions);

    /**
     * Replace a document in the collection according to the specified arguments.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param clientSession the client session with which to associate this operation
     * @param filter        the query filter to apply the the replace operation
     * @param replacement   the replacement document
     * @param replaceOptions the options to apply to the replace operation
     * @return the result of the replace one operation
     * @throws com.mongodb.MongoWriteException        if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write concern
     * @throws com.mongodb.MongoCommandException      if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     * @since 3.7
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual tutorial/modify-documents/#replace-the-document Replace
     */
    UpdateResult replaceOne(ClientSession clientSession, Bson filter, TDocument replacement, ReplaceOptions replaceOptions);

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param filter a document describing the query filter, which may not be null.
     * @param update a document describing the update, which may not be null. The update to apply must include only update operators.
     * @return the result of the update one operation
     * @throws com.mongodb.MongoWriteException        if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write concern
     * @throws com.mongodb.MongoCommandException      if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    UpdateResult updateOne(Bson filter, Bson update);

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param filter        a document describing the query filter, which may not be null.
     * @param update        a document describing the update, which may not be null. The update to apply must include only update operators.
     * @param updateOptions the options to apply to the update operation
     * @return the result of the update one operation
     * @throws com.mongodb.MongoWriteException        if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write concern
     * @throws com.mongodb.MongoCommandException      if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    UpdateResult updateOne(Bson filter, Bson update, UpdateOptions updateOptions);

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param clientSession the client session with which to associate this operation
     * @param filter a document describing the query filter, which may not be null.
     * @param update a document describing the update, which may not be null. The update to apply must include only update operators.
     * @return the result of the update one operation
     * @throws com.mongodb.MongoWriteException        if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write concern
     * @throws com.mongodb.MongoCommandException      if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    UpdateResult updateOne(ClientSession clientSession, Bson filter, Bson update);

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param clientSession the client session with which to associate this operation
     * @param filter        a document describing the query filter, which may not be null.
     * @param update        a document describing the update, which may not be null. The update to apply must include only update operators.
     * @param updateOptions the options to apply to the update operation
     * @return the result of the update one operation
     * @throws com.mongodb.MongoWriteException        if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write concern
     * @throws com.mongodb.MongoCommandException      if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    UpdateResult updateOne(ClientSession clientSession, Bson filter, Bson update, UpdateOptions updateOptions);

    /**
     * Update all documents in the collection according to the specified arguments.
     *
     * @param filter a document describing the query filter, which may not be null.
     * @param update a document describing the update, which may not be null. The update to apply must include only update operators.
     * @return the result of the update many operation
     * @throws com.mongodb.MongoWriteException        if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write concern
     * @throws com.mongodb.MongoCommandException      if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    UpdateResult updateMany(Bson filter, Bson update);

    /**
     * Update all documents in the collection according to the specified arguments.
     *
     * @param filter        a document describing the query filter, which may not be null.
     * @param update        a document describing the update, which may not be null. The update to apply must include only update operators.
     * @param updateOptions the options to apply to the update operation
     * @return the result of the update many operation
     * @throws com.mongodb.MongoWriteException        if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write concern
     * @throws com.mongodb.MongoCommandException      if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    UpdateResult updateMany(Bson filter, Bson update, UpdateOptions updateOptions);

    /**
     * Update all documents in the collection according to the specified arguments.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter a document describing the query filter, which may not be null.
     * @param update a document describing the update, which may not be null. The update to apply must include only update operators.
     * @return the result of the update many operation
     * @throws com.mongodb.MongoWriteException        if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write concern
     * @throws com.mongodb.MongoCommandException      if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    UpdateResult updateMany(ClientSession clientSession, Bson filter, Bson update);

    /**
     * Update all documents in the collection according to the specified arguments.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter        a document describing the query filter, which may not be null.
     * @param update        a document describing the update, which may not be null. The update to apply must include only update operators.
     * @param updateOptions the options to apply to the update operation
     * @return the result of the update many operation
     * @throws com.mongodb.MongoWriteException        if the write failed due to some specific write exception
     * @throws com.mongodb.MongoWriteConcernException if the write failed due to being unable to fulfil the write concern
     * @throws com.mongodb.MongoCommandException      if the write failed due to a specific command exception
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    UpdateResult updateMany(ClientSession clientSession, Bson filter, Bson update, UpdateOptions updateOptions);

    /**
     * Atomically find a document and remove it.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param filter the query filter to find the document with
     * @return the document that was removed.  If no documents matched the query filter, then null will be returned
     */
    @Nullable
    TDocument findOneAndDelete(Bson filter);

    /**
     * Atomically find a document and remove it.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param filter  the query filter to find the document with
     * @param options the options to apply to the operation
     * @return the document that was removed.  If no documents matched the query filter, then null will be returned
     */
    @Nullable
    TDocument findOneAndDelete(Bson filter, FindOneAndDeleteOptions options);

    /**
     * Atomically find a document and remove it.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param clientSession the client session with which to associate this operation
     * @param filter the query filter to find the document with
     * @return the document that was removed.  If no documents matched the query filter, then null will be returned
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    @Nullable
    TDocument findOneAndDelete(ClientSession clientSession, Bson filter);

    /**
     * Atomically find a document and remove it.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param clientSession the client session with which to associate this operation
     * @param filter  the query filter to find the document with
     * @param options the options to apply to the operation
     * @return the document that was removed.  If no documents matched the query filter, then null will be returned
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    @Nullable
    TDocument findOneAndDelete(ClientSession clientSession, Bson filter, FindOneAndDeleteOptions options);

    /**
     * Atomically find a document and replace it.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param filter      the query filter to apply the the replace operation
     * @param replacement the replacement document
     * @return the document that was replaced.  Depending on the value of the {@code returnOriginal} property, this will either be the
     * document as it was before the update or as it is after the update.  If no documents matched the query filter, then null will be
     * returned
     */
    @Nullable
    TDocument findOneAndReplace(Bson filter, TDocument replacement);

    /**
     * Atomically find a document and replace it.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param filter      the query filter to apply the the replace operation
     * @param replacement the replacement document
     * @param options     the options to apply to the operation
     * @return the document that was replaced.  Depending on the value of the {@code returnOriginal} property, this will either be the
     * document as it was before the update or as it is after the update.  If no documents matched the query filter, then null will be
     * returned
     */
    @Nullable
    TDocument findOneAndReplace(Bson filter, TDocument replacement, FindOneAndReplaceOptions options);

    /**
     * Atomically find a document and replace it.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param clientSession the client session with which to associate this operation
     * @param filter      the query filter to apply the the replace operation
     * @param replacement the replacement document
     * @return the document that was replaced.  Depending on the value of the {@code returnOriginal} property, this will either be the
     * document as it was before the update or as it is after the update.  If no documents matched the query filter, then null will be
     * returned
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    @Nullable
    TDocument findOneAndReplace(ClientSession clientSession, Bson filter, TDocument replacement);

    /**
     * Atomically find a document and replace it.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param clientSession the client session with which to associate this operation
     * @param filter      the query filter to apply the the replace operation
     * @param replacement the replacement document
     * @param options     the options to apply to the operation
     * @return the document that was replaced.  Depending on the value of the {@code returnOriginal} property, this will either be the
     * document as it was before the update or as it is after the update.  If no documents matched the query filter, then null will be
     * returned
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    @Nullable
    TDocument findOneAndReplace(ClientSession clientSession, Bson filter, TDocument replacement, FindOneAndReplaceOptions options);

    /**
     * Atomically find a document and update it.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param filter a document describing the query filter, which may not be null.
     * @param update a document describing the update, which may not be null. The update to apply must include only update operators.
     * @return the document that was updated before the update was applied.  If no documents matched the query filter, then null will be
     * returned
     */
    @Nullable
    TDocument findOneAndUpdate(Bson filter, Bson update);

    /**
     * Atomically find a document and update it.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param filter  a document describing the query filter, which may not be null.
     * @param update  a document describing the update, which may not be null. The update to apply must include only update operators.
     * @param options the options to apply to the operation
     * @return the document that was updated.  Depending on the value of the {@code returnOriginal} property, this will either be the
     * document as it was before the update or as it is after the update.  If no documents matched the query filter, then null will be
     * returned
     */
    @Nullable
    TDocument findOneAndUpdate(Bson filter, Bson update, FindOneAndUpdateOptions options);

    /**
     * Atomically find a document and update it.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param clientSession the client session with which to associate this operation
     * @param filter a document describing the query filter, which may not be null.
     * @param update a document describing the update, which may not be null. The update to apply must include only update operators.
     * @return the document that was updated before the update was applied.  If no documents matched the query filter, then null will be
     * returned
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    @Nullable
    TDocument findOneAndUpdate(ClientSession clientSession, Bson filter, Bson update);

    /**
     * Atomically find a document and update it.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param clientSession the client session with which to associate this operation
     * @param filter  a document describing the query filter, which may not be null.
     * @param update  a document describing the update, which may not be null. The update to apply must include only update operators.
     * @param options the options to apply to the operation
     * @return the document that was updated.  Depending on the value of the {@code returnOriginal} property, this will either be the
     * document as it was before the update or as it is after the update.  If no documents matched the query filter, then null will be
     * returned
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    @Nullable
    TDocument findOneAndUpdate(ClientSession clientSession, Bson filter, Bson update, FindOneAndUpdateOptions options);

    /**
     * Drops this collection from the Database.
     *
     * @mongodb.driver.manual reference/command/drop/ Drop Collection
     */
    void drop();

    /**
     * Drops this collection from the Database.
     *
     * @param clientSession the client session with which to associate this operation
     * @mongodb.driver.manual reference/command/drop/ Drop Collection
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void drop(ClientSession clientSession);

    /**
     * Create an index with the given keys.
     *
     * @param keys an object describing the index key(s), which may not be null.
     * @return the index name
     * @mongodb.driver.manual reference/command/createIndexes Create indexes
     */
    String createIndex(Bson keys);

    /**
     * Create an index with the given keys and options.
     *
     * @param keys                an object describing the index key(s), which may not be null.
     * @param indexOptions the options for the index
     * @return the index name
     * @mongodb.driver.manual reference/command/createIndexes Create indexes
     */
    String createIndex(Bson keys, IndexOptions indexOptions);

    /**
     * Create an index with the given keys.
     *
     * @param clientSession the client session with which to associate this operation
     * @param keys an object describing the index key(s), which may not be null.
     * @return the index name
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual reference/command/createIndexes Create indexes
     */
    String createIndex(ClientSession clientSession, Bson keys);

    /**
     * Create an index with the given keys and options.
     *
     * @param clientSession the client session with which to associate this operation
     * @param keys                an object describing the index key(s), which may not be null.
     * @param indexOptions the options for the index
     * @return the index name
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual reference/command/createIndexes Create indexes
     */
    String createIndex(ClientSession clientSession, Bson keys, IndexOptions indexOptions);

    /**
     * Create multiple indexes.
     *
     * @param indexes the list of indexes
     * @return the list of index names
     * @mongodb.driver.manual reference/command/createIndexes Create indexes
     * @mongodb.server.release 2.6
     */
    List<String> createIndexes(List<IndexModel> indexes);

    /**
     * Create multiple indexes.
     *
     * @param indexes the list of indexes
     * @param createIndexOptions options to use when creating indexes
     * @return the list of index names
     * @mongodb.driver.manual reference/command/createIndexes Create indexes
     * @since 3.6
     */
    List<String> createIndexes(List<IndexModel> indexes, CreateIndexOptions createIndexOptions);

    /**
     * Create multiple indexes.
     *
     * @param clientSession the client session with which to associate this operation
     * @param indexes the list of indexes
     * @return the list of index names
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual reference/command/createIndexes Create indexes
     */
    List<String> createIndexes(ClientSession clientSession, List<IndexModel> indexes);

    /**
     * Create multiple indexes.
     *
     * @param clientSession the client session with which to associate this operation
     * @param indexes the list of indexes
     * @param createIndexOptions options to use when creating indexes
     * @return the list of index names
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual reference/command/createIndexes Create indexes
     */
    List<String> createIndexes(ClientSession clientSession, List<IndexModel> indexes, CreateIndexOptions createIndexOptions);

    /**
     * Get all the indexes in this collection.
     *
     * @return the list indexes iterable interface
     * @mongodb.driver.manual reference/command/listIndexes/ List indexes
     */
    ListIndexesIterable<Document> listIndexes();

    /**
     * Get all the indexes in this collection.
     *
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the list indexes iterable interface
     * @mongodb.driver.manual reference/command/listIndexes/ List indexes
     */
    <TResult> ListIndexesIterable<TResult> listIndexes(Class<TResult> resultClass);

    /**
     * Get all the indexes in this collection.
     *
     * @param clientSession the client session with which to associate this operation
     * @return the list indexes iterable interface
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual reference/command/listIndexes/ List indexes
     */
    ListIndexesIterable<Document> listIndexes(ClientSession clientSession);

    /**
     * Get all the indexes in this collection.
     *
     * @param clientSession the client session with which to associate this operation
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the list indexes iterable interface
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual reference/command/listIndexes/ List indexes
     */
    <TResult> ListIndexesIterable<TResult> listIndexes(ClientSession clientSession, Class<TResult> resultClass);

    /**
     * Drops the index given its name.
     *
     * @param indexName the name of the index to remove
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop indexes
     */
    void dropIndex(String indexName);

    /**
     * Drops the index given its name.
     *
     * @param indexName the name of the index to remove
     * @param dropIndexOptions the options to use when dropping indexes
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop indexes
     * @since 3.6
     */
    void dropIndex(String indexName, DropIndexOptions dropIndexOptions);

    /**
     * Drops the index given the keys used to create it.
     *
     * @param keys the keys of the index to remove
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop indexes
     */
    void dropIndex(Bson keys);

    /**
     * Drops the index given the keys used to create it.
     *
     * @param keys the keys of the index to remove
     * @param dropIndexOptions the options to use when dropping indexes
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop indexes
     * @since 3.6
     */
    void dropIndex(Bson keys, DropIndexOptions dropIndexOptions);

    /**
     * Drops the index given its name.
     *
     * @param clientSession the client session with which to associate this operation
     * @param indexName the name of the index to remove
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop indexes
     */
    void dropIndex(ClientSession clientSession, String indexName);

    /**
     * Drops the index given the keys used to create it.
     *
     * @param clientSession the client session with which to associate this operation
     * @param keys the keys of the index to remove
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop indexes
     */
    void dropIndex(ClientSession clientSession, Bson keys);

    /**
     * Drops the index given its name.
     *
     * @param clientSession the client session with which to associate this operation
     * @param indexName the name of the index to remove
     * @param dropIndexOptions the options to use when dropping indexes
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop indexes
     */
    void dropIndex(ClientSession clientSession, String indexName, DropIndexOptions dropIndexOptions);

    /**
     * Drops the index given the keys used to create it.
     *
     * @param clientSession the client session with which to associate this operation
     * @param keys the keys of the index to remove
     * @param dropIndexOptions the options to use when dropping indexes
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop indexes
     */
    void dropIndex(ClientSession clientSession, Bson keys, DropIndexOptions dropIndexOptions);

    /**
     * Drop all the indexes on this collection, except for the default on _id.
     *
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop indexes
     */
    void dropIndexes();

    /**
     * Drop all the indexes on this collection, except for the default on _id.
     *
     * @param clientSession the client session with which to associate this operation
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop indexes
     */
    void dropIndexes(ClientSession clientSession);

    /**
     * Drop all the indexes on this collection, except for the default on _id.
     *
     * @param dropIndexOptions the options to use when dropping indexes
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop indexes
     * @since 3.6
     */
    void dropIndexes(DropIndexOptions dropIndexOptions);

    /**
     * Drop all the indexes on this collection, except for the default on _id.
     *
     * @param clientSession the client session with which to associate this operation
     * @param dropIndexOptions the options to use when dropping indexes
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop indexes
     */
    void dropIndexes(ClientSession clientSession, DropIndexOptions dropIndexOptions);

    /**
     * Rename the collection with oldCollectionName to the newCollectionName.
     *
     * @param newCollectionNamespace the namespace the collection will be renamed to
     * @throws com.mongodb.MongoServerException if you provide a newCollectionName that is the name of an existing collection, or if the
     *                                          oldCollectionName is the name of a collection that doesn't exist
     * @mongodb.driver.manual reference/command/renameCollection Rename collection
     */
    void renameCollection(MongoNamespace newCollectionNamespace);

    /**
     * Rename the collection with oldCollectionName to the newCollectionName.
     *
     * @param newCollectionNamespace  the name the collection will be renamed to
     * @param renameCollectionOptions the options for renaming a collection
     * @throws com.mongodb.MongoServerException if you provide a newCollectionName that is the name of an existing collection and dropTarget
     *                                          is false, or if the oldCollectionName is the name of a collection that doesn't exist
     * @mongodb.driver.manual reference/command/renameCollection Rename collection
     */
    void renameCollection(MongoNamespace newCollectionNamespace, RenameCollectionOptions renameCollectionOptions);

    /**
     * Rename the collection with oldCollectionName to the newCollectionName.
     *
     * @param clientSession the client session with which to associate this operation
     * @param newCollectionNamespace the namespace the collection will be renamed to
     * @throws com.mongodb.MongoServerException if you provide a newCollectionName that is the name of an existing collection, or if the
     *                                          oldCollectionName is the name of a collection that doesn't exist
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual reference/command/renameCollection Rename collection
     */
    void renameCollection(ClientSession clientSession, MongoNamespace newCollectionNamespace);

    /**
     * Rename the collection with oldCollectionName to the newCollectionName.
     *
     * @param clientSession the client session with which to associate this operation
     * @param newCollectionNamespace  the name the collection will be renamed to
     * @param renameCollectionOptions the options for renaming a collection
     * @throws com.mongodb.MongoServerException if you provide a newCollectionName that is the name of an existing collection and dropTarget
     *                                          is false, or if the oldCollectionName is the name of a collection that doesn't exist
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual reference/command/renameCollection Rename collection
     */
    void renameCollection(ClientSession clientSession, MongoNamespace newCollectionNamespace,
                          RenameCollectionOptions renameCollectionOptions);
}
