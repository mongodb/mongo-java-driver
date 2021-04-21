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

package com.mongodb.reactivestreams.client;

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
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

import java.util.List;

/**
 * The MongoCollection interface.
 *
 * <p>Note: Additions to this interface will not be considered to break binary compatibility.</p>
 *
 * @param <TDocument> The type that this collection will encode documents from and decode documents to.
 * @since 1.0
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
     * @mongodb.server.release 3.2
     * @since 1.2
     */
    ReadConcern getReadConcern();

    /**
     * Create a new MongoCollection instance with a different default class to cast any documents returned from the database into..
     *
     * @param clazz          the default class to cast any documents returned from the database into.
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
     * @mongodb.server.release 3.2
     * @since 1.2
     */
    MongoCollection<TDocument> withReadConcern(ReadConcern readConcern);

    /**
     * Gets an estimate of the count of documents in a collection using collection metadata.
     *
     * @return a publisher with a single element indicating the estimated number of documents
     * @since 1.9
     */
    Publisher<Long> estimatedDocumentCount();

    /**
     * Gets an estimate of the count of documents in a collection using collection metadata.
     *
     * @param options the options describing the count
     * @return a publisher with a single element indicating the estimated number of documents
     * @since 1.9
     */
    Publisher<Long> estimatedDocumentCount(EstimatedDocumentCountOptions options);

    /**
     * Counts the number of documents in the collection.
     *
     * <p>
     * Note: For a fast count of the total documents in a collection see {@link #estimatedDocumentCount()}.<br>
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
     * @return a publisher with a single element indicating the number of documents
     * @since 1.9
     */
    Publisher<Long> countDocuments();

    /**
     * Counts the number of documents in the collection according to the given options.
     *
     * <p>
     * Note: For a fast count of the total documents in a collection see {@link #estimatedDocumentCount()}.<br>
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
     * @return a publisher with a single element indicating the number of documents
     * @since 1.9
     */
    Publisher<Long> countDocuments(Bson filter);

    /**
     * Counts the number of documents in the collection according to the given options.
     *
     * <p>
     * Note: For a fast count of the total documents in a collection see {@link #estimatedDocumentCount()}.<br>
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
     * @return a publisher with a single element indicating the number of documents
     * @since 1.9
     */
    Publisher<Long> countDocuments(Bson filter, CountOptions options);

    /**
     * Counts the number of documents in the collection.
     *
     * <p>
     * Note: For a fast count of the total documents in a collection see {@link #estimatedDocumentCount()}.<br>
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
     * @return a publisher with a single element indicating the number of documents
     * @mongodb.server.release 3.6
     * @since 1.9
     */
    Publisher<Long> countDocuments(ClientSession clientSession);

    /**
     * Counts the number of documents in the collection according to the given options.
     *
     * <p>
     * Note: For a fast count of the total documents in a collection see {@link #estimatedDocumentCount()}.
     * </p>
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter the query filter
     * @return a publisher with a single element indicating the number of documents
     * @mongodb.server.release 3.6
     * @since 1.9
     */
    Publisher<Long> countDocuments(ClientSession clientSession, Bson filter);

    /**
     * Counts the number of documents in the collection according to the given options.
     *
     * <p>
     * Note: For a fast count of the total documents in a collection see {@link #estimatedDocumentCount()}.<br>
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
     * @return a publisher with a single element indicating the number of documents
     * @mongodb.server.release 3.6
     * @since 1.9
     */
    Publisher<Long> countDocuments(ClientSession clientSession, Bson filter, CountOptions options);

    /**
     * Gets the distinct values of the specified field name.
     *
     * @param fieldName   the field name
     * @param resultClass the default class to cast any distinct items into.
     * @param <TResult>   the target type of the iterable.
     * @return a publisher emitting the sequence of distinct values
     * @mongodb.driver.manual reference/command/distinct/ Distinct
     */
    <TResult> DistinctPublisher<TResult> distinct(String fieldName, Class<TResult> resultClass);

    /**
     * Gets the distinct values of the specified field name.
     *
     * @param fieldName   the field name
     * @param filter      the query filter
     * @param resultClass the default class to cast any distinct items into.
     * @param <TResult>   the target type of the iterable.
     * @return an iterable of distinct values
     * @mongodb.driver.manual reference/command/distinct/ Distinct
     */
    <TResult> DistinctPublisher<TResult> distinct(String fieldName, Bson filter, Class<TResult> resultClass);

    /**
     * Gets the distinct values of the specified field name.
     *
     * @param clientSession the client session with which to associate this operation
     * @param fieldName   the field name
     * @param resultClass the default class to cast any distinct items into.
     * @param <TResult>   the target type of the iterable.
     * @return a publisher emitting the sequence of distinct values
     * @mongodb.driver.manual reference/command/distinct/ Distinct
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    <TResult> DistinctPublisher<TResult> distinct(ClientSession clientSession, String fieldName, Class<TResult> resultClass);

    /**
     * Gets the distinct values of the specified field name.
     *
     * @param clientSession the client session with which to associate this operation
     * @param fieldName   the field name
     * @param filter      the query filter
     * @param resultClass the default class to cast any distinct items into.
     * @param <TResult>   the target type of the iterable.
     * @return an iterable of distinct values
     * @mongodb.driver.manual reference/command/distinct/ Distinct
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    <TResult> DistinctPublisher<TResult> distinct(ClientSession clientSession, String fieldName, Bson filter, Class<TResult> resultClass);

    /**
     * Finds all documents in the collection.
     *
     * @return the fluent find interface
     * @mongodb.driver.manual tutorial/query-documents/ Find
     */
    FindPublisher<TDocument> find();

    /**
     * Finds all documents in the collection.
     *
     * @param clazz     the class to decode each document into
     * @param <TResult> the target document type of the iterable.
     * @return the fluent find interface
     * @mongodb.driver.manual tutorial/query-documents/ Find
     */
    <TResult> FindPublisher<TResult> find(Class<TResult> clazz);

    /**
     * Finds all documents in the collection.
     *
     * @param filter the query filter
     * @return the fluent find interface
     * @mongodb.driver.manual tutorial/query-documents/ Find
     */
    FindPublisher<TDocument> find(Bson filter);

    /**
     * Finds all documents in the collection.
     *
     * @param filter    the query filter
     * @param clazz     the class to decode each document into
     * @param <TResult> the target document type of the iterable.
     * @return the fluent find interface
     * @mongodb.driver.manual tutorial/query-documents/ Find
     */
    <TResult> FindPublisher<TResult> find(Bson filter, Class<TResult> clazz);

    /**
     * Finds all documents in the collection.
     *
     * @param clientSession the client session with which to associate this operation
     * @return the fluent find interface
     * @mongodb.driver.manual tutorial/query-documents/ Find
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    FindPublisher<TDocument> find(ClientSession clientSession);

    /**
     * Finds all documents in the collection.
     *
     * @param clientSession the client session with which to associate this operation
     * @param clazz     the class to decode each document into
     * @param <TResult> the target document type of the iterable.
     * @return the fluent find interface
     * @mongodb.driver.manual tutorial/query-documents/ Find
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    <TResult> FindPublisher<TResult> find(ClientSession clientSession, Class<TResult> clazz);

    /**
     * Finds all documents in the collection.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter the query filter
     * @return the fluent find interface
     * @mongodb.driver.manual tutorial/query-documents/ Find
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    FindPublisher<TDocument> find(ClientSession clientSession, Bson filter);

    /**
     * Finds all documents in the collection.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter    the query filter
     * @param clazz     the class to decode each document into
     * @param <TResult> the target document type of the iterable.
     * @return the fluent find interface
     * @mongodb.driver.manual tutorial/query-documents/ Find
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    <TResult> FindPublisher<TResult> find(ClientSession clientSession, Bson filter, Class<TResult> clazz);

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     *
     * @param pipeline the aggregate pipeline
     * @return a publisher containing the result of the aggregation operation
     * @mongodb.driver.manual aggregation/ Aggregation
     */
    AggregatePublisher<TDocument> aggregate(List<? extends Bson> pipeline);

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     *
     * @param pipeline  the aggregate pipeline
     * @param clazz     the class to decode each document into
     * @param <TResult> the target document type of the iterable.
     * @return a publisher containing the result of the aggregation operation
     * @mongodb.driver.manual aggregation/ Aggregation
     */
    <TResult> AggregatePublisher<TResult> aggregate(List<? extends Bson> pipeline, Class<TResult> clazz);

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     *
     * @param clientSession the client session with which to associate this operation
     * @param pipeline the aggregate pipeline
     * @return a publisher containing the result of the aggregation operation
     * @mongodb.driver.manual aggregation/ Aggregation
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    AggregatePublisher<TDocument> aggregate(ClientSession clientSession, List<? extends Bson> pipeline);

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     *
     * @param clientSession the client session with which to associate this operation
     * @param pipeline  the aggregate pipeline
     * @param clazz     the class to decode each document into
     * @param <TResult> the target document type of the iterable.
     * @return a publisher containing the result of the aggregation operation
     * @mongodb.driver.manual aggregation/ Aggregation
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    <TResult> AggregatePublisher<TResult> aggregate(ClientSession clientSession, List<? extends Bson> pipeline, Class<TResult> clazz);

    /**
     * Creates a change stream for this collection.
     *
     * @return the change stream iterable
     * @mongodb.driver.manual reference/operator/aggregation/changeStream $changeStream
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    ChangeStreamPublisher<Document> watch();

    /**
     * Creates a change stream for this collection.
     *
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the change stream iterable
     * @mongodb.driver.manual reference/operator/aggregation/changeStream $changeStream
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    <TResult> ChangeStreamPublisher<TResult> watch(Class<TResult> resultClass);

    /**
     * Creates a change stream for this collection.
     *
     * @param pipeline the aggregation pipeline to apply to the change stream
     * @return the change stream iterable
     * @mongodb.driver.manual reference/operator/aggregation/changeStream $changeStream
     * @since 1.6
     */
    ChangeStreamPublisher<Document> watch(List<? extends Bson> pipeline);

    /**
     * Creates a change stream for this collection.
     *
     * @param pipeline    the aggregation pipeline to apply to the change stream
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the change stream iterable
     * @mongodb.driver.manual reference/operator/aggregation/changeStream $changeStream
     * @since 1.6
     */
    <TResult> ChangeStreamPublisher<TResult> watch(List<? extends Bson> pipeline, Class<TResult> resultClass);

    /**
     * Creates a change stream for this collection.
     *
     * @param clientSession the client session with which to associate this operation
     * @return the change stream iterable
     * @mongodb.driver.manual reference/operator/aggregation/changeStream $changeStream
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    ChangeStreamPublisher<Document> watch(ClientSession clientSession);

    /**
     * Creates a change stream for this collection.
     *
     * @param clientSession the client session with which to associate this operation
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the change stream iterable
     * @mongodb.driver.manual reference/operator/aggregation/changeStream $changeStream
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    <TResult> ChangeStreamPublisher<TResult> watch(ClientSession clientSession, Class<TResult> resultClass);

    /**
     * Creates a change stream for this collection.
     *
     * @param clientSession the client session with which to associate this operation
     * @param pipeline the aggregation pipeline to apply to the change stream
     * @return the change stream iterable
     * @mongodb.driver.manual reference/operator/aggregation/changeStream $changeStream
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    ChangeStreamPublisher<Document> watch(ClientSession clientSession, List<? extends Bson> pipeline);

    /**
     * Creates a change stream for this collection.
     *
     * @param clientSession the client session with which to associate this operation
     * @param pipeline    the aggregation pipeline to apply to the change stream
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the change stream iterable
     * @mongodb.driver.manual reference/operator/aggregation/changeStream $changeStream
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    <TResult> ChangeStreamPublisher<TResult> watch(ClientSession clientSession, List<? extends Bson> pipeline, Class<TResult> resultClass);

    /**
     * Aggregates documents according to the specified map-reduce function.
     *
     * @param mapFunction    A JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduceFunction A JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @return an publisher containing the result of the map-reduce operation
     * @mongodb.driver.manual reference/command/mapReduce/ map-reduce
     */
    MapReducePublisher<TDocument> mapReduce(String mapFunction, String reduceFunction);

    /**
     * Aggregates documents according to the specified map-reduce function.
     *
     * @param mapFunction    A JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduceFunction A JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @param clazz          the class to decode each resulting document into.
     * @param <TResult>      the target document type of the iterable.
     * @return a publisher containing the result of the map-reduce operation
     * @mongodb.driver.manual reference/command/mapReduce/ map-reduce
     */
    <TResult> MapReducePublisher<TResult> mapReduce(String mapFunction, String reduceFunction, Class<TResult> clazz);

    /**
     * Aggregates documents according to the specified map-reduce function.
     *
     * @param clientSession the client session with which to associate this operation
     * @param mapFunction    A JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduceFunction A JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @return an publisher containing the result of the map-reduce operation
     * @mongodb.driver.manual reference/command/mapReduce/ map-reduce
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    MapReducePublisher<TDocument> mapReduce(ClientSession clientSession, String mapFunction, String reduceFunction);

    /**
     * Aggregates documents according to the specified map-reduce function.
     *
     * @param clientSession the client session with which to associate this operation
     * @param mapFunction    A JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduceFunction A JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @param clazz          the class to decode each resulting document into.
     * @param <TResult>      the target document type of the iterable.
     * @return a publisher containing the result of the map-reduce operation
     * @mongodb.driver.manual reference/command/mapReduce/ map-reduce
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    <TResult> MapReducePublisher<TResult> mapReduce(ClientSession clientSession, String mapFunction, String reduceFunction,
                                                    Class<TResult> clazz);

    /**
     * Executes a mix of inserts, updates, replaces, and deletes.
     *
     * @param requests the writes to execute
     * @return a publisher with a single element the BulkWriteResult
     */
    Publisher<BulkWriteResult> bulkWrite(List<? extends WriteModel<? extends TDocument>> requests);

    /**
     * Executes a mix of inserts, updates, replaces, and deletes.
     *
     * @param requests the writes to execute
     * @param options  the options to apply to the bulk write operation
     * @return a publisher with a single element the BulkWriteResult
     */
    Publisher<BulkWriteResult> bulkWrite(List<? extends WriteModel<? extends TDocument>> requests, BulkWriteOptions options);

    /**
     * Executes a mix of inserts, updates, replaces, and deletes.
     *
     * @param clientSession the client session with which to associate this operation
     * @param requests the writes to execute
     * @return a publisher with a single element the BulkWriteResult
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<BulkWriteResult> bulkWrite(ClientSession clientSession, List<? extends WriteModel<? extends TDocument>> requests);

    /**
     * Executes a mix of inserts, updates, replaces, and deletes.
     *
     * @param clientSession the client session with which to associate this operation
     * @param requests the writes to execute
     * @param options  the options to apply to the bulk write operation
     * @return a publisher with a single element the BulkWriteResult
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<BulkWriteResult> bulkWrite(ClientSession clientSession, List<? extends WriteModel<? extends TDocument>> requests,
                                         BulkWriteOptions options);

    /**
     * Inserts the provided document. If the document is missing an identifier, the driver should generate one.
     *
     * @param document the document to insert
     * @return a publisher with a single element with the InsertOneResult or with either a
     * com.mongodb.DuplicateKeyException or com.mongodb.MongoException
     */
    Publisher<InsertOneResult> insertOne(TDocument document);

    /**
     * Inserts the provided document. If the document is missing an identifier, the driver should generate one.
     *
     * @param document the document to insert
     * @param options  the options to apply to the operation
     * @return a publisher with a single element with the InsertOneResult or with either a
     * com.mongodb.DuplicateKeyException or com.mongodb.MongoException
     * @since 1.2
     */
    Publisher<InsertOneResult> insertOne(TDocument document, InsertOneOptions options);

    /**
     * Inserts the provided document. If the document is missing an identifier, the driver should generate one.
     *
     * @param clientSession the client session with which to associate this operation
     * @param document the document to insert
     * @return a publisher with a single element with the InsertOneResult or with either a
     * com.mongodb.DuplicateKeyException or com.mongodb.MongoException
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<InsertOneResult> insertOne(ClientSession clientSession, TDocument document);

    /**
     * Inserts the provided document. If the document is missing an identifier, the driver should generate one.
     *
     * @param clientSession the client session with which to associate this operation
     * @param document the document to insert
     * @param options  the options to apply to the operation
     * @return a publisher with a single element with the InsertOneResult or with either a
     * com.mongodb.DuplicateKeyException or com.mongodb.MongoException
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<InsertOneResult> insertOne(ClientSession clientSession, TDocument document, InsertOneOptions options);

    /**
     * Inserts a batch of documents.
     *
     * @param documents the documents to insert
     * @return a publisher with a single element with the InsertManyResult or with either a
     * com.mongodb.DuplicateKeyException or com.mongodb.MongoException
     */
    Publisher<InsertManyResult> insertMany(List<? extends TDocument> documents);

    /**
     * Inserts a batch of documents.
     *
     * @param documents the documents to insert
     * @param options   the options to apply to the operation
     * @return a publisher with a single element with the InsertManyResult or with either a
     * com.mongodb.DuplicateKeyException or com.mongodb.MongoException
     */
    Publisher<InsertManyResult> insertMany(List<? extends TDocument> documents, InsertManyOptions options);

    /**
     * Inserts a batch of documents.
     *
     * @param clientSession the client session with which to associate this operation
     * @param documents the documents to insert
     * @return a publisher with a single element with the InsertManyResult or with either a
     * com.mongodb.DuplicateKeyException or com.mongodb.MongoException
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<InsertManyResult> insertMany(ClientSession clientSession, List<? extends TDocument> documents);

    /**
     * Inserts a batch of documents.
     *
     * @param clientSession the client session with which to associate this operation
     * @param documents the documents to insert
     * @param options   the options to apply to the operation
     * @return a publisher with a single element with the InsertManyResult or with either a
     * com.mongodb.DuplicateKeyException or com.mongodb.MongoException
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<InsertManyResult> insertMany(ClientSession clientSession, List<? extends TDocument> documents, InsertManyOptions options);

    /**
     * Removes at most one document from the collection that matches the given filter.  If no documents match, the collection is not
     * modified.
     *
     * @param filter the query filter to apply the delete operation
     * @return a publisher with a single element the DeleteResult or with an com.mongodb.MongoException
     */
    Publisher<DeleteResult> deleteOne(Bson filter);

    /**
     * Removes at most one document from the collection that matches the given filter.  If no documents match, the collection is not
     * modified.
     *
     * @param filter the query filter to apply the delete operation
     * @param options the options to apply to the delete operation
     * @return a publisher with a single element the DeleteResult or with an com.mongodb.MongoException
     * @since 1.5
     */
    Publisher<DeleteResult> deleteOne(Bson filter, DeleteOptions options);

    /**
     * Removes at most one document from the collection that matches the given filter.  If no documents match, the collection is not
     * modified.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter the query filter to apply the delete operation
     * @return a publisher with a single element the DeleteResult or with an com.mongodb.MongoException
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<DeleteResult> deleteOne(ClientSession clientSession, Bson filter);

    /**
     * Removes at most one document from the collection that matches the given filter.  If no documents match, the collection is not
     * modified.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter the query filter to apply the delete operation
     * @param options the options to apply to the delete operation
     * @return a publisher with a single element the DeleteResult or with an com.mongodb.MongoException
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<DeleteResult> deleteOne(ClientSession clientSession, Bson filter, DeleteOptions options);

    /**
     * Removes all documents from the collection that match the given query filter.  If no documents match, the collection is not modified.
     *
     * @param filter the query filter to apply the delete operation
     * @return a publisher with a single element the DeleteResult or with an com.mongodb.MongoException
     */
    Publisher<DeleteResult> deleteMany(Bson filter);

    /**
     * Removes all documents from the collection that match the given query filter.  If no documents match, the collection is not modified.
     *
     * @param filter the query filter to apply the delete operation
     * @param options the options to apply to the delete operation
     * @return a publisher with a single element the DeleteResult or with an com.mongodb.MongoException
     * @since 1.5
     */
    Publisher<DeleteResult> deleteMany(Bson filter, DeleteOptions options);

    /**
     * Removes all documents from the collection that match the given query filter.  If no documents match, the collection is not modified.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter the query filter to apply the delete operation
     * @return a publisher with a single element the DeleteResult or with an com.mongodb.MongoException
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<DeleteResult> deleteMany(ClientSession clientSession, Bson filter);

    /**
     * Removes all documents from the collection that match the given query filter.  If no documents match, the collection is not modified.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter the query filter to apply the delete operation
     * @param options the options to apply to the delete operation
     * @return a publisher with a single element the DeleteResult or with an com.mongodb.MongoException
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<DeleteResult> deleteMany(ClientSession clientSession, Bson filter, DeleteOptions options);

    /**
     * Replace a document in the collection according to the specified arguments.
     *
     * @param filter      the query filter to apply the replace operation
     * @param replacement the replacement document
     * @return a publisher with a single element the UpdateResult
     * @mongodb.driver.manual tutorial/modify-documents/#replace-the-document Replace
     */
    Publisher<UpdateResult> replaceOne(Bson filter, TDocument replacement);

    /**
     * Replace a document in the collection according to the specified arguments.
     *
     * @param filter      the query filter to apply the replace operation
     * @param replacement the replacement document
     * @param options     the options to apply to the replace operation
     * @return a publisher with a single element the UpdateResult
     * @mongodb.driver.manual tutorial/modify-documents/#replace-the-document Replace
     * @since 1.8
     */
    Publisher<UpdateResult> replaceOne(Bson filter, TDocument replacement, ReplaceOptions options);

    /**
     * Replace a document in the collection according to the specified arguments.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter      the query filter to apply the replace operation
     * @param replacement the replacement document
     * @return a publisher with a single element the UpdateResult
     * @mongodb.driver.manual tutorial/modify-documents/#replace-the-document Replace
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<UpdateResult> replaceOne(ClientSession clientSession, Bson filter, TDocument replacement);

    /**
     * Replace a document in the collection according to the specified arguments.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter      the query filter to apply the replace operation
     * @param replacement the replacement document
     * @param options     the options to apply to the replace operation
     * @return a publisher with a single element the UpdateResult
     * @mongodb.driver.manual tutorial/modify-documents/#replace-the-document Replace
     * @mongodb.server.release 3.6
     * @since 1.8
     */
    Publisher<UpdateResult> replaceOne(ClientSession clientSession, Bson filter, TDocument replacement, ReplaceOptions options);

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * @param filter a document describing the query filter, which may not be null.
     * @param update a document describing the update, which may not be null. The update to apply must include only update operators.
     * @return a publisher with a single element the UpdateResult
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    Publisher<UpdateResult> updateOne(Bson filter, Bson update);

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * @param filter  a document describing the query filter, which may not be null.
     * @param update  a document describing the update, which may not be null. The update to apply must include only update operators.
     * @param options the options to apply to the update operation
     * @return a publisher with a single element the UpdateResult
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    Publisher<UpdateResult> updateOne(Bson filter, Bson update, UpdateOptions options);

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter a document describing the query filter, which may not be null.
     * @param update a document describing the update, which may not be null. The update to apply must include only update operators.
     * @return a publisher with a single element the UpdateResult
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<UpdateResult> updateOne(ClientSession clientSession, Bson filter, Bson update);

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter  a document describing the query filter, which may not be null.
     * @param update  a document describing the update, which may not be null. The update to apply must include only update operators.
     * @param options the options to apply to the update operation
     * @return a publisher with a single element the UpdateResult
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<UpdateResult> updateOne(ClientSession clientSession, Bson filter, Bson update, UpdateOptions options);

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param filter a document describing the query filter, which may not be null.
     * @param update a pipeline describing the update, which may not be null.
     * @return a publisher with a single element the UpdateResult
     * @since 1.12
     * @mongodb.server.release 4.2
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    Publisher<UpdateResult> updateOne(Bson filter, List<? extends Bson> update);

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param filter        a document describing the query filter, which may not be null.
     * @param update        a pipeline describing the update, which may not be null.
     * @param options the options to apply to the update operation
     * @return a publisher with a single element the UpdateResult
     * @since 1.12
     * @mongodb.server.release 4.2
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    Publisher<UpdateResult> updateOne(Bson filter, List<? extends Bson> update, UpdateOptions options);

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param clientSession the client session with which to associate this operation
     * @param filter a document describing the query filter, which may not be null.
     * @param update a pipeline describing the update, which may not be null.
     * @return a publisher with a single element the UpdateResult
     * @since 1.12
     * @mongodb.server.release 4.2
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    Publisher<UpdateResult> updateOne(ClientSession clientSession, Bson filter, List<? extends Bson> update);

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param clientSession the client session with which to associate this operation
     * @param filter        a document describing the query filter, which may not be null.
     * @param update        a pipeline describing the update, which may not be null.
     * @param options the options to apply to the update operation
     * @return a publisher with a single element the UpdateResult
     * @since 1.12
     * @mongodb.server.release 4.2
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    Publisher<UpdateResult> updateOne(ClientSession clientSession, Bson filter, List<? extends Bson> update, UpdateOptions options);

    /**
     * Update all documents in the collection according to the specified arguments.
     *
     * @param filter a document describing the query filter, which may not be null.
     * @param update a document describing the update, which may not be null. The update to apply must include only update operators.
     * @return a publisher with a single element the UpdateResult
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    Publisher<UpdateResult> updateMany(Bson filter, Bson update);

    /**
     * Update all documents in the collection according to the specified arguments.
     *
     * @param filter  a document describing the query filter, which may not be null.
     * @param update  a document describing the update, which may not be null. The update to apply must include only update operators.
     * @param options the options to apply to the update operation
     * @return a publisher with a single element the UpdateResult
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    Publisher<UpdateResult> updateMany(Bson filter, Bson update, UpdateOptions options);

    /**
     * Update all documents in the collection according to the specified arguments.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter a document describing the query filter, which may not be null.
     * @param update a document describing the update, which may not be null. The update to apply must include only update operators.
     * @return a publisher with a single element the UpdateResult
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<UpdateResult> updateMany(ClientSession clientSession, Bson filter, Bson update);

    /**
     * Update all documents in the collection according to the specified arguments.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter  a document describing the query filter, which may not be null.
     * @param update  a document describing the update, which may not be null. The update to apply must include only update operators.
     * @param options the options to apply to the update operation
     * @return a publisher with a single element the UpdateResult
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<UpdateResult> updateMany(ClientSession clientSession, Bson filter, Bson update, UpdateOptions options);

    /**
     * Update all documents in the collection according to the specified arguments.
     *
     * @param filter a document describing the query filter, which may not be null.
     * @param update a pipeline describing the update, which may not be null.
     * @return a publisher with a single element the UpdateResult
     * @since 1.12
     * @mongodb.server.release 4.2
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    Publisher<UpdateResult> updateMany(Bson filter, List<? extends Bson> update);

    /**
     * Update all documents in the collection according to the specified arguments.
     *
     * @param filter        a document describing the query filter, which may not be null.
     * @param update        a pipeline describing the update, which may not be null.
     * @param options the options to apply to the update operation
     * @return a publisher with a single element the UpdateResult
     * @since 1.12
     * @mongodb.server.release 4.2
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    Publisher<UpdateResult> updateMany(Bson filter, List<? extends Bson> update, UpdateOptions options);

    /**
     * Update all documents in the collection according to the specified arguments.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter a document describing the query filter, which may not be null.
     * @param update a pipeline describing the update, which may not be null.
     * @return a publisher with a single element the UpdateResult
     * @since 1.12
     * @mongodb.server.release 4.2
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    Publisher<UpdateResult> updateMany(ClientSession clientSession, Bson filter, List<? extends Bson> update);

    /**
     * Update all documents in the collection according to the specified arguments.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter        a document describing the query filter, which may not be null.
     * @param update        a pipeline describing the update, which may not be null.
     * @param options the options to apply to the update operation
     * @return a publisher with a single element the UpdateResult
     * @since 1.12
     * @mongodb.server.release 4.2
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    Publisher<UpdateResult> updateMany(ClientSession clientSession, Bson filter, List<? extends Bson> update, UpdateOptions options);

    /**
     * Atomically find a document and remove it.
     *
     * @param filter the query filter to find the document with
     * @return a publisher with a single element the document that was removed.  If no documents matched the query filter, then null will be
     * returned
     */
    Publisher<TDocument> findOneAndDelete(Bson filter);

    /**
     * Atomically find a document and remove it.
     *
     * @param filter  the query filter to find the document with
     * @param options the options to apply to the operation
     * @return a publisher with a single element the document that was removed.  If no documents matched the query filter, then null will be
     * returned
     */
    Publisher<TDocument> findOneAndDelete(Bson filter, FindOneAndDeleteOptions options);

    /**
     * Atomically find a document and remove it.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter the query filter to find the document with
     * @return a publisher with a single element the document that was removed.  If no documents matched the query filter, then null will be
     * returned
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<TDocument> findOneAndDelete(ClientSession clientSession, Bson filter);

    /**
     * Atomically find a document and remove it.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter  the query filter to find the document with
     * @param options the options to apply to the operation
     * @return a publisher with a single element the document that was removed.  If no documents matched the query filter, then null will be
     * returned
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<TDocument> findOneAndDelete(ClientSession clientSession, Bson filter, FindOneAndDeleteOptions options);

    /**
     * Atomically find a document and replace it.
     *
     * @param filter      the query filter to apply the replace operation
     * @param replacement the replacement document
     * @return a publisher with a single element the document that was replaced.  Depending on the value of the {@code returnOriginal}
     * property, this will either be the document as it was before the update or as it is after the update.  If no documents matched the
     * query filter, then null will be returned
     */
    Publisher<TDocument> findOneAndReplace(Bson filter, TDocument replacement);

    /**
     * Atomically find a document and replace it.
     *
     * @param filter      the query filter to apply the replace operation
     * @param replacement the replacement document
     * @param options     the options to apply to the operation
     * @return a publisher with a single element the document that was replaced.  Depending on the value of the {@code returnOriginal}
     * property, this will either be the document as it was before the update or as it is after the update.  If no documents matched the
     * query filter, then null will be returned
     */
    Publisher<TDocument> findOneAndReplace(Bson filter, TDocument replacement, FindOneAndReplaceOptions options);

    /**
     * Atomically find a document and replace it.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter      the query filter to apply the replace operation
     * @param replacement the replacement document
     * @return a publisher with a single element the document that was replaced.  Depending on the value of the {@code returnOriginal}
     * property, this will either be the document as it was before the update or as it is after the update.  If no documents matched the
     * query filter, then null will be returned
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<TDocument> findOneAndReplace(ClientSession clientSession, Bson filter, TDocument replacement);

    /**
     * Atomically find a document and replace it.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter      the query filter to apply the replace operation
     * @param replacement the replacement document
     * @param options     the options to apply to the operation
     * @return a publisher with a single element the document that was replaced.  Depending on the value of the {@code returnOriginal}
     * property, this will either be the document as it was before the update or as it is after the update.  If no documents matched the
     * query filter, then null will be returned
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<TDocument> findOneAndReplace(ClientSession clientSession, Bson filter, TDocument replacement,
                                           FindOneAndReplaceOptions options);

    /**
     * Atomically find a document and update it.
     *
     * @param filter a document describing the query filter, which may not be null.
     * @param update a document describing the update, which may not be null. The update to apply must include only update operators.
     * @return a publisher with a single element the document that was updated before the update was applied.  If no documents matched the
     * query filter, then null will be returned
     */
    Publisher<TDocument> findOneAndUpdate(Bson filter, Bson update);

    /**
     * Atomically find a document and update it.
     *
     * @param filter  a document describing the query filter, which may not be null.
     * @param update  a document describing the update, which may not be null. The update to apply must include only update operators.
     * @param options the options to apply to the operation
     * @return a publisher with a single element the document that was updated.  Depending on the value of the {@code returnOriginal}
     * property, this will either be the document as it was before the update or as it is after the update.  If no documents matched the
     * query filter, then null will be returned
     */
    Publisher<TDocument> findOneAndUpdate(Bson filter, Bson update, FindOneAndUpdateOptions options);

    /**
     * Atomically find a document and update it.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter a document describing the query filter, which may not be null.
     * @param update a document describing the update, which may not be null. The update to apply must include only update operators.
     * @return a publisher with a single element the document that was updated before the update was applied.  If no documents matched the
     * query filter, then null will be returned
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<TDocument> findOneAndUpdate(ClientSession clientSession, Bson filter, Bson update);

    /**
     * Atomically find a document and update it.
     *
     * @param clientSession the client session with which to associate this operation
     * @param filter  a document describing the query filter, which may not be null.
     * @param update  a document describing the update, which may not be null. The update to apply must include only update operators.
     * @param options the options to apply to the operation
     * @return a publisher with a single element the document that was updated.  Depending on the value of the {@code returnOriginal}
     * property, this will either be the document as it was before the update or as it is after the update.  If no documents matched the
     * query filter, then null will be returned
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<TDocument> findOneAndUpdate(ClientSession clientSession, Bson filter, Bson update, FindOneAndUpdateOptions options);

    /**
     * Atomically find a document and update it.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param filter a document describing the query filter, which may not be null.
     * @param update a pipeline describing the update, which may not be null.
     * @return a publisher with a single element the document that was updated.  Depending on the value of the {@code returnOriginal}
     * property, this will either be the document as it was before the update or as it is after the update.  If no documents matched the
     * query filter, then null will be returned
     * @since 1.12
     * @mongodb.server.release 4.2
     */
    Publisher<TDocument> findOneAndUpdate(Bson filter, List<? extends Bson> update);

    /**
     * Atomically find a document and update it.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param filter  a document describing the query filter, which may not be null.
     * @param update  a pipeline describing the update, which may not be null.
     * @param options the options to apply to the operation
     * @return a publisher with a single element the document that was updated.  Depending on the value of the {@code returnOriginal}
     * property, this will either be the document as it was before the update or as it is after the update.  If no documents matched the
     * query filter, then null will be returned
     * @since 1.12
     * @mongodb.server.release 4.2
     */
    Publisher<TDocument> findOneAndUpdate(Bson filter, List<? extends Bson> update, FindOneAndUpdateOptions options);

    /**
     * Atomically find a document and update it.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param clientSession the client session with which to associate this operation
     * @param filter a document describing the query filter, which may not be null.
     * @param update a pipeline describing the update, which may not be null.
     * @return a publisher with a single element the document that was updated.  Depending on the value of the {@code returnOriginal}
     * property, this will either be the document as it was before the update or as it is after the update.  If no documents matched the
     * query filter, then null will be returned
     * @since 1.12
     * @mongodb.server.release 4.2
     */
    Publisher<TDocument> findOneAndUpdate(ClientSession clientSession, Bson filter, List<? extends Bson> update);

    /**
     * Atomically find a document and update it.
     *
     * <p>Note: Supports retryable writes on MongoDB server versions 3.6 or higher when the retryWrites setting is enabled.</p>
     * @param clientSession the client session with which to associate this operation
     * @param filter  a document describing the query filter, which may not be null.
     * @param update  a pipeline describing the update, which may not be null.
     * @param options the options to apply to the operation
     * @return a publisher with a single element the document that was updated.  Depending on the value of the {@code returnOriginal}
     * property, this will either be the document as it was before the update or as it is after the update.  If no documents matched the
     * query filter, then null will be returned
     * @since 1.12
     * @mongodb.server.release 4.2
     */
    Publisher<TDocument> findOneAndUpdate(ClientSession clientSession, Bson filter, List<? extends Bson> update,
                                           FindOneAndUpdateOptions options);

    /**
     * Drops this collection from the Database.
     *
     * @return an empty publisher that indicates when the operation has completed
     * @mongodb.driver.manual reference/command/drop/ Drop Collection
     */
    Publisher<Void> drop();

    /**
     * Drops this collection from the Database.
     *
     * @param clientSession the client session with which to associate this operation
     * @return an empty publisher that indicates when the operation has completed
     * @mongodb.driver.manual reference/command/drop/ Drop Collection
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<Void> drop(ClientSession clientSession);

    /**
     * Creates an index.
     *
     * @param key an object describing the index key(s), which may not be null.
     * @return an empty publisher that indicates when the operation has completed
     * @mongodb.driver.manual reference/method/db.collection.ensureIndex Ensure Index
     */
    Publisher<String> createIndex(Bson key);

    /**
     * Creates an index.
     *
     * @param key     an object describing the index key(s), which may not be null.
     * @param options the options for the index
     * @return an empty publisher that indicates when the operation has completed
     * @mongodb.driver.manual reference/method/db.collection.ensureIndex Ensure Index
     */
    Publisher<String> createIndex(Bson key, IndexOptions options);

    /**
     * Creates an index.
     *
     * @param clientSession the client session with which to associate this operation
     * @param key an object describing the index key(s), which may not be null.
     * @return an empty publisher that indicates when the operation has completed
     * @mongodb.driver.manual reference/method/db.collection.ensureIndex Ensure Index
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<String> createIndex(ClientSession clientSession, Bson key);

    /**
     * Creates an index.
     *
     * @param clientSession the client session with which to associate this operation
     * @param key     an object describing the index key(s), which may not be null.
     * @param options the options for the index
     * @return an empty publisher that indicates when the operation has completed
     * @mongodb.driver.manual reference/method/db.collection.ensureIndex Ensure Index
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<String> createIndex(ClientSession clientSession, Bson key, IndexOptions options);

    /**
     * Create multiple indexes.
     *
     * @param indexes the list of indexes
     * @return an empty publisher that indicates when the operation has completed
     * @mongodb.driver.manual reference/command/createIndexes Create indexes
     */
    Publisher<String> createIndexes(List<IndexModel> indexes);

    /**
     * Create multiple indexes.
     *
     * @param indexes the list of indexes
     * @param createIndexOptions options to use when creating indexes
     * @return an empty publisher that indicates when the operation has completed
     * @mongodb.driver.manual reference/command/createIndexes Create indexes
     * @since 1.7
     */
    Publisher<String> createIndexes(List<IndexModel> indexes, CreateIndexOptions createIndexOptions);

    /**
     * Create multiple indexes.
     *
     * @param clientSession the client session with which to associate this operation
     * @param indexes the list of indexes
     * @return an empty publisher that indicates when the operation has completed
     * @mongodb.driver.manual reference/command/createIndexes Create indexes
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<String> createIndexes(ClientSession clientSession, List<IndexModel> indexes);

    /**
     * Create multiple indexes.
     *
     * @param clientSession the client session with which to associate this operation
     * @param indexes the list of indexes
     * @param createIndexOptions options to use when creating indexes
     * @return an empty publisher that indicates when the operation has completed
     * @mongodb.driver.manual reference/command/createIndexes Create indexes
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<String> createIndexes(ClientSession clientSession, List<IndexModel> indexes, CreateIndexOptions createIndexOptions);

    /**
     * Get all the indexes in this collection.
     *
     * @return the fluent list indexes interface
     * @mongodb.driver.manual reference/command/listIndexes/ listIndexes
     */
    ListIndexesPublisher<Document> listIndexes();

    /**
     * Get all the indexes in this collection.
     *
     * @param clazz     the class to decode each document into
     * @param <TResult> the target document type of the iterable.
     * @return the fluent list indexes interface
     * @mongodb.driver.manual reference/command/listIndexes/ listIndexes
     */
    <TResult> ListIndexesPublisher<TResult> listIndexes(Class<TResult> clazz);

    /**
     * Get all the indexes in this collection.
     *
     * @param clientSession the client session with which to associate this operation
     * @return the fluent list indexes interface
     * @mongodb.driver.manual reference/command/listIndexes/ listIndexes
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    ListIndexesPublisher<Document> listIndexes(ClientSession clientSession);

    /**
     * Get all the indexes in this collection.
     *
     * @param clientSession the client session with which to associate this operation
     * @param clazz     the class to decode each document into
     * @param <TResult> the target document type of the iterable.
     * @return the fluent list indexes interface
     * @mongodb.driver.manual reference/command/listIndexes/ listIndexes
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    <TResult> ListIndexesPublisher<TResult> listIndexes(ClientSession clientSession, Class<TResult> clazz);

    /**
     * Drops the given index.
     *
     * @param indexName the name of the index to remove
     * @return an empty publisher that indicates when the operation has completed
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop Indexes
     */
    Publisher<Void> dropIndex(String indexName);

    /**
     * Drops the index given the keys used to create it.
     *
     * @param keys the keys of the index to remove
     * @return an empty publisher that indicates when the operation has completed
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop indexes
     */
    Publisher<Void> dropIndex(Bson keys);

    /**
     * Drops the given index.
     *
     * @param indexName the name of the index to remove
     * @param dropIndexOptions options to use when dropping indexes
     * @return an empty publisher that indicates when the operation has completed
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop Indexes
     * @since 1.7
     */
    Publisher<Void> dropIndex(String indexName, DropIndexOptions dropIndexOptions);

    /**
     * Drops the index given the keys used to create it.
     *
     * @param keys the keys of the index to remove
     * @param dropIndexOptions options to use when dropping indexes
     * @return an empty publisher that indicates when the operation has completed
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop indexes
     * @since 1.7
     */
    Publisher<Void> dropIndex(Bson keys, DropIndexOptions dropIndexOptions);

    /**
     * Drops the given index.
     *
     * @param clientSession the client session with which to associate this operation
     * @param indexName the name of the index to remove
     * @return an empty publisher that indicates when the operation has completed
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop Indexes
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<Void> dropIndex(ClientSession clientSession, String indexName);

    /**
     * Drops the index given the keys used to create it.
     *
     * @param clientSession the client session with which to associate this operation
     * @param keys the keys of the index to remove
     * @return an empty publisher that indicates when the operation has completed
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop indexes
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<Void> dropIndex(ClientSession clientSession, Bson keys);

    /**
     * Drops the given index.
     *
     * @param clientSession the client session with which to associate this operation
     * @param indexName the name of the index to remove
     * @param dropIndexOptions options to use when dropping indexes
     * @return an empty publisher that indicates when the operation has completed
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop Indexes
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<Void> dropIndex(ClientSession clientSession, String indexName, DropIndexOptions dropIndexOptions);

    /**
     * Drops the index given the keys used to create it.
     *
     * @param clientSession the client session with which to associate this operation
     * @param keys the keys of the index to remove
     * @param dropIndexOptions options to use when dropping indexes
     * @return an empty publisher that indicates when the operation has completed
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop indexes
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<Void> dropIndex(ClientSession clientSession, Bson keys, DropIndexOptions dropIndexOptions);

    /**
     * Drop all the indexes on this collection, except for the default on _id.
     *
     * @return an empty publisher that indicates when the operation has completed
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop Indexes
     */
    Publisher<Void> dropIndexes();

    /**
     * Drop all the indexes on this collection, except for the default on _id.
     *
     * @param dropIndexOptions options to use when dropping indexes
     * @return an empty publisher that indicates when the operation has completed
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop Indexes
     * @since 1.7
     */
    Publisher<Void> dropIndexes(DropIndexOptions dropIndexOptions);

    /**
     * Drop all the indexes on this collection, except for the default on _id.
     *
     * @param clientSession the client session with which to associate this operation
     * @return an empty publisher that indicates when the operation has completed
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop Indexes
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<Void> dropIndexes(ClientSession clientSession);

    /**
     * Drop all the indexes on this collection, except for the default on _id.
     *
     * @param clientSession the client session with which to associate this operation
     * @param dropIndexOptions options to use when dropping indexes
     * @return an empty publisher that indicates when the operation has completed
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop Indexes
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<Void> dropIndexes(ClientSession clientSession, DropIndexOptions dropIndexOptions);

    /**
     * Rename the collection with oldCollectionName to the newCollectionName.
     *
     * @param newCollectionNamespace the namespace the collection will be renamed to
     * @return an empty publisher that indicates when the operation has completed
     * @mongodb.driver.manual reference/commands/renameCollection Rename collection
     */
    Publisher<Void> renameCollection(MongoNamespace newCollectionNamespace);

    /**
     * Rename the collection with oldCollectionName to the newCollectionName.
     *
     * @param newCollectionNamespace the name the collection will be renamed to
     * @param options                the options for renaming a collection
     * @return an empty publisher that indicates when the operation has completed
     * @mongodb.driver.manual reference/commands/renameCollection Rename collection
     */
    Publisher<Void> renameCollection(MongoNamespace newCollectionNamespace, RenameCollectionOptions options);


    /**
     * Rename the collection with oldCollectionName to the newCollectionName.
     *
     * @param clientSession the client session with which to associate this operation
     * @param newCollectionNamespace the namespace the collection will be renamed to
     * @return an empty publisher that indicates when the operation has completed
     * @mongodb.driver.manual reference/commands/renameCollection Rename collection
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<Void> renameCollection(ClientSession clientSession, MongoNamespace newCollectionNamespace);

    /**
     * Rename the collection with oldCollectionName to the newCollectionName.
     *
     * @param clientSession the client session with which to associate this operation
     * @param newCollectionNamespace the name the collection will be renamed to
     * @param options                the options for renaming a collection
     * @return an empty publisher that indicates when the operation has completed
     * @mongodb.driver.manual reference/commands/renameCollection Rename collection
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<Void> renameCollection(ClientSession clientSession, MongoNamespace newCollectionNamespace,
                                        RenameCollectionOptions options);
}
