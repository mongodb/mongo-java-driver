/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

package com.mongodb.async.client;

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.session.ClientSession;
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
 * {@link org.bson.codecs.Codec} registered in the {@link CodecRegistry}. The default {@code CodecRegistry} includes built-in support for:
 * {@link org.bson.BsonDocument} and {@link Document}.
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
     * @param newDocumentClass the default class to cast any documents returned from the database into.
     * @param <NewTDocument>   the type that the new collection will encode documents from and decode documents to
     * @return a new MongoCollection instance with the different default class
     */
    <NewTDocument> MongoCollection<NewTDocument> withDocumentClass(Class<NewTDocument> newDocumentClass);

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
     * @param callback the callback passed the number of documents in the collection
     */
    void count(SingleResultCallback<Long> callback);

    /**
     * Counts the number of documents in the collection according to the given options.
     *
     * @param filter   the query filter
     * @param callback the callback passed the number of documents in the collection
     */
    void count(Bson filter, SingleResultCallback<Long> callback);

    /**
     * Counts the number of documents in the collection according to the given options.
     *
     * @param filter   the query filter
     * @param options  the options describing the count
     * @param callback the callback passed the number of documents in the collection
     */
    void count(Bson filter, CountOptions options, SingleResultCallback<Long> callback);

    /**
     * Counts the number of documents in the collection.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param callback the callback passed the number of documents in the collection
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void count(ClientSession clientSession, SingleResultCallback<Long> callback);

    /**
     * Counts the number of documents in the collection according to the given options.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param filter   the query filter
     * @param callback the callback passed the number of documents in the collection
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void count(ClientSession clientSession, Bson filter, SingleResultCallback<Long> callback);

    /**
     * Counts the number of documents in the collection according to the given options.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param filter   the query filter
     * @param options  the options describing the count
     * @param callback the callback passed the number of documents in the collection
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void count(ClientSession clientSession, Bson filter, CountOptions options, SingleResultCallback<Long> callback);

    /**
     * Gets the distinct values of the specified field name.
     *
     * @param fieldName   the field name
     * @param resultClass the default class to cast any distinct items into.
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
     * @param resultClass the default class to cast any distinct items into.
     * @param <TResult>   the target type of the iterable.
     * @return an iterable of distinct values
     * @mongodb.driver.manual reference/command/distinct/ Distinct
     */
    <TResult> DistinctIterable<TResult> distinct(String fieldName, Bson filter, Class<TResult> resultClass);

    /**
     * Gets the distinct values of the specified field name.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param fieldName   the field name
     * @param resultClass the default class to cast any distinct items into.
     * @param <TResult>   the target type of the iterable.
     * @return an iterable of distinct values
     * @mongodb.driver.manual reference/command/distinct/ Distinct
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    <TResult> DistinctIterable<TResult> distinct(ClientSession clientSession, String fieldName, Class<TResult> resultClass);

    /**
     * Gets the distinct values of the specified field name.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param fieldName   the field name
     * @param filter      the query filter
     * @param resultClass the default class to cast any distinct items into.
     * @param <TResult>   the target type of the iterable.
     * @return an iterable of distinct values
     * @mongodb.driver.manual reference/command/distinct/ Distinct
     * @since 3.6
     * @mongodb.server.release 3.6
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
     * @param clientSession  the client session with which to associate this operation
     * @return the find iterable interface
     * @mongodb.driver.manual tutorial/query-documents/ Find
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    FindIterable<TDocument> find(ClientSession clientSession);

    /**
     * Finds all documents in the collection.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the find iterable interface
     * @mongodb.driver.manual tutorial/query-documents/ Find
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    <TResult> FindIterable<TResult> find(ClientSession clientSession, Class<TResult> resultClass);

    /**
     * Finds all documents in the collection.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param filter the query filter
     * @return the find iterable interface
     * @mongodb.driver.manual tutorial/query-documents/ Find
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    FindIterable<TDocument> find(ClientSession clientSession, Bson filter);

    /**
     * Finds all documents in the collection.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param filter      the query filter
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the find iterable interface
     * @mongodb.driver.manual tutorial/query-documents/ Find
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    <TResult> FindIterable<TResult> find(ClientSession clientSession, Bson filter, Class<TResult> resultClass);

    /**
     * Aggregates documents according to the specified aggregation pipeline.  If the pipeline ends with a $out stage, the returned
     * iterable will be a query of the collection that the aggregation was written to.  Note that in this case the pipeline will be
     * executed even if the iterable is never iterated.
     *
     * @param pipeline the aggregate pipeline
     * @return an iterable containing the result of the aggregation operation
     * @mongodb.driver.manual aggregation/ Aggregation
     */
    AggregateIterable<TDocument> aggregate(List<? extends Bson> pipeline);

    /**
     * Aggregates documents according to the specified aggregation pipeline.  If the pipeline ends with a $out stage, the returned
     * iterable will be a query of the collection that the aggregation was written to.  Note that in this case the pipeline will be
     * executed even if the iterable is never iterated.
     *
     * @param pipeline    the aggregate pipeline
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return an iterable containing the result of the aggregation operation
     * @mongodb.driver.manual aggregation/ Aggregation
     */
    <TResult> AggregateIterable<TResult> aggregate(List<? extends Bson> pipeline, Class<TResult> resultClass);

    /**
     * Aggregates documents according to the specified aggregation pipeline.  If the pipeline ends with a $out stage, the returned
     * iterable will be a query of the collection that the aggregation was written to.  Note that in this case the pipeline will be
     * executed even if the iterable is never iterated.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param pipeline the aggregate pipeline
     * @return an iterable containing the result of the aggregation operation
     * @mongodb.driver.manual aggregation/ Aggregation
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    AggregateIterable<TDocument> aggregate(ClientSession clientSession, List<? extends Bson> pipeline);

    /**
     * Aggregates documents according to the specified aggregation pipeline.  If the pipeline ends with a $out stage, the returned
     * iterable will be a query of the collection that the aggregation was written to.  Note that in this case the pipeline will be
     * executed even if the iterable is never iterated.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param pipeline    the aggregate pipeline
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return an iterable containing the result of the aggregation operation
     * @mongodb.driver.manual aggregation/ Aggregation
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    <TResult> AggregateIterable<TResult> aggregate(ClientSession clientSession, List<? extends Bson> pipeline, Class<TResult> resultClass);

    /**
     * Creates a change stream for this collection.
     *
     * @return the change stream iterable
     * @mongodb.driver.dochub core/changestreams Change Streams
     * @since 3.6
     * @mongodb.server.release 3.6
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
     * @mongodb.server.release 3.6
     */
    <TResult> ChangeStreamIterable<TResult> watch(Class<TResult> resultClass);

    /**
     * Creates a change stream for this collection.
     *
     * @param pipeline the aggregation pipeline to apply to the change stream
     * @return the change stream iterable
     * @mongodb.driver.dochub core/changestreams Change Streams
     * @since 3.6
     * @mongodb.server.release 3.6
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
     * @mongodb.server.release 3.6
     */
    <TResult> ChangeStreamIterable<TResult> watch(List<? extends Bson> pipeline, Class<TResult> resultClass);

    /**
     * Creates a change stream for this collection.
     *
     * @param clientSession  the client session with which to associate this operation
     * @return the change stream iterable
     * @mongodb.driver.manual reference/operator/aggregation/changeStream $changeStream
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    ChangeStreamIterable<TDocument> watch(ClientSession clientSession);

    /**
     * Creates a change stream for this collection.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the change stream iterable
     * @mongodb.driver.manual reference/operator/aggregation/changeStream $changeStream
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    <TResult> ChangeStreamIterable<TResult> watch(ClientSession clientSession, Class<TResult> resultClass);

    /**
     * Creates a change stream for this collection.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param pipeline the aggregation pipeline to apply to the change stream
     * @return the change stream iterable
     * @mongodb.driver.manual reference/operator/aggregation/changeStream $changeStream
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    ChangeStreamIterable<TDocument> watch(ClientSession clientSession, List<? extends Bson> pipeline);

    /**
     * Creates a change stream for this collection.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param pipeline    the aggregation pipeline to apply to the change stream
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the change stream iterable
     * @mongodb.driver.manual reference/operator/aggregation/changeStream $changeStream
     * @since 3.6
     * @mongodb.server.release 3.6
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
     * @param clientSession  the client session with which to associate this operation
     * @param mapFunction    A JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduceFunction A JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @return an iterable containing the result of the map-reduce operation
     * @mongodb.driver.manual reference/command/mapReduce/ map-reduce
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    MapReduceIterable<TDocument> mapReduce(ClientSession clientSession, String mapFunction, String reduceFunction);

    /**
     * Aggregates documents according to the specified map-reduce function.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param mapFunction    A JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduceFunction A JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @param resultClass    the class to decode each resulting document into.
     * @param <TResult>      the target document type of the iterable.
     * @return an iterable containing the result of the map-reduce operation
     * @mongodb.driver.manual reference/command/mapReduce/ map-reduce
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    <TResult> MapReduceIterable<TResult> mapReduce(ClientSession clientSession, String mapFunction, String reduceFunction,
                                                   Class<TResult> resultClass);
    /**
     * Executes a mix of inserts, updates, replaces, and deletes.
     *
     * @param requests the writes to execute
     * @param callback the callback passed the result of the bulk write
     */
    void bulkWrite(List<? extends WriteModel<? extends TDocument>> requests, SingleResultCallback<BulkWriteResult> callback);

    /**
     * Executes a mix of inserts, updates, replaces, and deletes.
     *
     * @param requests the writes to execute
     * @param options  the options to apply to the bulk write operation
     * @param callback the callback passed the result of the bulk write
     */
    void bulkWrite(List<? extends WriteModel<? extends TDocument>> requests, BulkWriteOptions options,
                   SingleResultCallback<BulkWriteResult> callback);

    /**
     * Executes a mix of inserts, updates, replaces, and deletes.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param requests the writes to execute
     * @param callback the callback passed the result of the bulk write
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void bulkWrite(ClientSession clientSession, List<? extends WriteModel<? extends TDocument>> requests,
                   SingleResultCallback<BulkWriteResult> callback);

    /**
     * Executes a mix of inserts, updates, replaces, and deletes.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param requests the writes to execute
     * @param options  the options to apply to the bulk write operation
     * @param callback the callback passed the result of the bulk write
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void bulkWrite(ClientSession clientSession, List<? extends WriteModel<? extends TDocument>> requests, BulkWriteOptions options,
                   SingleResultCallback<BulkWriteResult> callback);

    /**
     * Inserts the provided document. If the document is missing an identifier, the driver should generate one.
     *
     * @param document the document to insert
     * @param callback the callback that is completed once the insert has completed
     * @throws com.mongodb.MongoWriteException        returned via the callback
     * @throws com.mongodb.MongoWriteConcernException returned via the callback
     * @throws com.mongodb.MongoException             returned via the callback
     */
    void insertOne(TDocument document, SingleResultCallback<Void> callback);

    /**
     * Inserts the provided document. If the document is missing an identifier, the driver should generate one.
     *
     * @param document the document to insert
     * @param options  the options to apply to the operation
     * @param callback the callback that is completed once the insert has completed
     * @throws com.mongodb.MongoWriteException        returned via the callback
     * @throws com.mongodb.MongoWriteConcernException returned via the callback
     * @throws com.mongodb.MongoCommandException      returned via the callback
     * @throws com.mongodb.MongoException             returned via the callback
     * @since 3.2
     */
    void insertOne(TDocument document, InsertOneOptions options, SingleResultCallback<Void> callback);

    /**
     * Inserts the provided document. If the document is missing an identifier, the driver should generate one.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param document the document to insert
     * @param callback the callback that is completed once the insert has completed
     * @throws com.mongodb.MongoWriteException        returned via the callback
     * @throws com.mongodb.MongoWriteConcernException returned via the callback
     * @throws com.mongodb.MongoException             returned via the callback
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void insertOne(ClientSession clientSession, TDocument document, SingleResultCallback<Void> callback);

    /**
     * Inserts the provided document. If the document is missing an identifier, the driver should generate one.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param document the document to insert
     * @param options  the options to apply to the operation
     * @param callback the callback that is completed once the insert has completed
     * @throws com.mongodb.MongoWriteException        returned via the callback
     * @throws com.mongodb.MongoWriteConcernException returned via the callback
     * @throws com.mongodb.MongoCommandException      returned via the callback
     * @throws com.mongodb.MongoException             returned via the callback
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void insertOne(ClientSession clientSession, TDocument document, InsertOneOptions options, SingleResultCallback<Void> callback);

    /**
     * Inserts one or more documents.  A call to this method is equivalent to a call to the {@code bulkWrite} method
     *
     * @param documents the documents to insert
     * @param callback  the callback that is completed once the insert has completed
     * @throws com.mongodb.MongoBulkWriteException if there's an exception in the bulk write operation
     * @throws com.mongodb.MongoException          if the write failed due some other failure
     * @see com.mongodb.async.client.MongoCollection#bulkWrite
     */
    void insertMany(List<? extends TDocument> documents, SingleResultCallback<Void> callback);

    /**
     * Inserts one or more documents.  A call to this method is equivalent to a call to the {@code bulkWrite} method
     *
     * @param documents the documents to insert
     * @param options   the options to apply to the operation
     * @param callback  the callback that is completed once the insert has completed
     * @throws com.mongodb.MongoBulkWriteException if there's an exception in the bulk write operation
     * @throws com.mongodb.MongoException          if the write failed due some other failure
     * @see com.mongodb.async.client.MongoCollection#bulkWrite
     */
    void insertMany(List<? extends TDocument> documents, InsertManyOptions options, SingleResultCallback<Void> callback);

    /**
     * Inserts one or more documents.  A call to this method is equivalent to a call to the {@code bulkWrite} method
     *
     * @param clientSession  the client session with which to associate this operation
     * @param documents the documents to insert
     * @param callback  the callback that is completed once the insert has completed
     * @throws com.mongodb.MongoBulkWriteException if there's an exception in the bulk write operation
     * @throws com.mongodb.MongoException          if the write failed due some other failure
     * @see com.mongodb.async.client.MongoCollection#bulkWrite
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void insertMany(ClientSession clientSession, List<? extends TDocument> documents, SingleResultCallback<Void> callback);

    /**
     * Inserts one or more documents.  A call to this method is equivalent to a call to the {@code bulkWrite} method
     *
     * @param clientSession  the client session with which to associate this operation
     * @param documents the documents to insert
     * @param options   the options to apply to the operation
     * @param callback  the callback that is completed once the insert has completed
     * @throws com.mongodb.MongoBulkWriteException if there's an exception in the bulk write operation
     * @throws com.mongodb.MongoException          if the write failed due some other failure
     * @see com.mongodb.async.client.MongoCollection#bulkWrite
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void insertMany(ClientSession clientSession, List<? extends TDocument> documents, InsertManyOptions options,
                    SingleResultCallback<Void> callback);

    /**
     * Removes at most one document from the collection that matches the given filter.  If no documents match, the collection is not
     * modified.
     *
     * @param filter   the query filter to apply the the delete operation
     * @param callback the callback passed the result of the remove one operation
     * @throws com.mongodb.MongoWriteException        returned via the callback
     * @throws com.mongodb.MongoWriteConcernException returned via the callback
     * @throws com.mongodb.MongoException             returned via the callback
     */
    void deleteOne(Bson filter, SingleResultCallback<DeleteResult> callback);

    /**
     * Removes at most one document from the collection that matches the given filter.  If no documents match, the collection is not
     * modified.
     *
     * @param filter   the query filter to apply the the delete operation
     * @param options  the options to apply to the delete operation
     * @param callback the callback passed the result of the remove one operation
     * @throws com.mongodb.MongoWriteException        returned via the callback
     * @throws com.mongodb.MongoWriteConcernException returned via the callback
     * @throws com.mongodb.MongoException             returned via the callback
     */
    void deleteOne(Bson filter, DeleteOptions options, SingleResultCallback<DeleteResult> callback);

    /**
     * Removes at most one document from the collection that matches the given filter.  If no documents match, the collection is not
     * modified.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param filter   the query filter to apply the the delete operation
     * @param callback the callback passed the result of the remove one operation
     * @throws com.mongodb.MongoWriteException        returned via the callback
     * @throws com.mongodb.MongoWriteConcernException returned via the callback
     * @throws com.mongodb.MongoException             returned via the callback
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void deleteOne(ClientSession clientSession, Bson filter, SingleResultCallback<DeleteResult> callback);

    /**
     * Removes at most one document from the collection that matches the given filter.  If no documents match, the collection is not
     * modified.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param filter   the query filter to apply the the delete operation
     * @param options  the options to apply to the delete operation
     * @param callback the callback passed the result of the remove one operation
     * @throws com.mongodb.MongoWriteException        returned via the callback
     * @throws com.mongodb.MongoWriteConcernException returned via the callback
     * @throws com.mongodb.MongoException             returned via the callback
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void deleteOne(ClientSession clientSession, Bson filter, DeleteOptions options, SingleResultCallback<DeleteResult> callback);

    /**
     * Removes all documents from the collection that match the given query filter.  If no documents match, the collection is not modified.
     *
     * @param filter   the query filter to apply the the delete operation
     * @param callback the callback passed the result of the remove many operation
     * @throws com.mongodb.MongoWriteException        returned via the callback
     * @throws com.mongodb.MongoWriteConcernException returned via the callback
     * @throws com.mongodb.MongoException             returned via the callback
     */
    void deleteMany(Bson filter, SingleResultCallback<DeleteResult> callback);

    /**
     * Removes all documents from the collection that match the given query filter.  If no documents match, the collection is not modified.
     *
     * @param filter   the query filter to apply the the delete operation
     * @param options  the options to apply to the delete operation
     * @param callback the callback passed the result of the remove many operation
     * @throws com.mongodb.MongoWriteException        returned via the callback
     * @throws com.mongodb.MongoWriteConcernException returned via the callback
     * @throws com.mongodb.MongoException             returned via the callback
     */
    void deleteMany(Bson filter, DeleteOptions options, SingleResultCallback<DeleteResult> callback);

    /**
     * Removes all documents from the collection that match the given query filter.  If no documents match, the collection is not modified.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param filter   the query filter to apply the the delete operation
     * @param callback the callback passed the result of the remove many operation
     * @throws com.mongodb.MongoWriteException        returned via the callback
     * @throws com.mongodb.MongoWriteConcernException returned via the callback
     * @throws com.mongodb.MongoException             returned via the callback
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void deleteMany(ClientSession clientSession, Bson filter, SingleResultCallback<DeleteResult> callback);

    /**
     * Removes all documents from the collection that match the given query filter.  If no documents match, the collection is not modified.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param filter   the query filter to apply the the delete operation
     * @param options  the options to apply to the delete operation
     * @param callback the callback passed the result of the remove many operation
     * @throws com.mongodb.MongoWriteException        returned via the callback
     * @throws com.mongodb.MongoWriteConcernException returned via the callback
     * @throws com.mongodb.MongoException             returned via the callback
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void deleteMany(ClientSession clientSession, Bson filter, DeleteOptions options, SingleResultCallback<DeleteResult> callback);

    /**
     * Replace a document in the collection according to the specified arguments.
     *
     * @param filter      the query filter to apply the the replace operation
     * @param replacement the replacement document
     * @param callback    the callback passed the result of the replace one operation
     * @throws com.mongodb.MongoWriteException        returned via the callback
     * @throws com.mongodb.MongoWriteConcernException returned via the callback
     * @throws com.mongodb.MongoException             returned via the callback
     * @mongodb.driver.manual tutorial/modify-documents/#replace-the-document Replace
     */
    void replaceOne(Bson filter, TDocument replacement, SingleResultCallback<UpdateResult> callback);

    /**
     * Replace a document in the collection according to the specified arguments.
     *
     * @param filter      the query filter to apply the the replace operation
     * @param replacement the replacement document
     * @param options     the options to apply to the replace operation
     * @param callback    the callback passed the result of the replace one operation
     * @throws com.mongodb.MongoWriteException        returned via the callback
     * @throws com.mongodb.MongoWriteConcernException returned via the callback
     * @throws com.mongodb.MongoException             returned via the callback
     * @mongodb.driver.manual tutorial/modify-documents/#replace-the-document Replace
     */
    void replaceOne(Bson filter, TDocument replacement, UpdateOptions options, SingleResultCallback<UpdateResult> callback);

    /**
     * Replace a document in the collection according to the specified arguments.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param filter      the query filter to apply the the replace operation
     * @param replacement the replacement document
     * @param callback    the callback passed the result of the replace one operation
     * @throws com.mongodb.MongoWriteException        returned via the callback
     * @throws com.mongodb.MongoWriteConcernException returned via the callback
     * @throws com.mongodb.MongoException             returned via the callback
     * @mongodb.driver.manual tutorial/modify-documents/#replace-the-document Replace
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void replaceOne(ClientSession clientSession, Bson filter, TDocument replacement, SingleResultCallback<UpdateResult> callback);

    /**
     * Replace a document in the collection according to the specified arguments.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param filter      the query filter to apply the the replace operation
     * @param replacement the replacement document
     * @param options     the options to apply to the replace operation
     * @param callback    the callback passed the result of the replace one operation
     * @throws com.mongodb.MongoWriteException        returned via the callback
     * @throws com.mongodb.MongoWriteConcernException returned via the callback
     * @throws com.mongodb.MongoException             returned via the callback
     * @mongodb.driver.manual tutorial/modify-documents/#replace-the-document Replace
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void replaceOne(ClientSession clientSession, Bson filter, TDocument replacement, UpdateOptions options,
                    SingleResultCallback<UpdateResult> callback);

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * @param filter   a document describing the query filter, which may not be null.
     * @param update   a document describing the update, which may not be null. The update to apply must include only update operators.
     * @param callback the callback passed the result of the update one operation
     * @throws com.mongodb.MongoWriteException        returned via the callback
     * @throws com.mongodb.MongoWriteConcernException returned via the callback
     * @throws com.mongodb.MongoException             returned via the callback
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    void updateOne(Bson filter, Bson update, SingleResultCallback<UpdateResult> callback);

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * @param filter   a document describing the query filter, which may not be null.
     * @param update   a document describing the update, which may not be null. The update to apply must include only update operators.
     * @param options  the options to apply to the update operation
     * @param callback the callback passed the result of the update one operation
     * @throws com.mongodb.MongoWriteException        returned via the callback
     * @throws com.mongodb.MongoWriteConcernException returned via the callback
     * @throws com.mongodb.MongoException             returned via the callback
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    void updateOne(Bson filter, Bson update, UpdateOptions options, SingleResultCallback<UpdateResult> callback);

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param filter   a document describing the query filter, which may not be null.
     * @param update   a document describing the update, which may not be null. The update to apply must include only update operators.
     * @param callback the callback passed the result of the update one operation
     * @throws com.mongodb.MongoWriteException        returned via the callback
     * @throws com.mongodb.MongoWriteConcernException returned via the callback
     * @throws com.mongodb.MongoException             returned via the callback
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void updateOne(ClientSession clientSession, Bson filter, Bson update, SingleResultCallback<UpdateResult> callback);

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param filter   a document describing the query filter, which may not be null.
     * @param update   a document describing the update, which may not be null. The update to apply must include only update operators.
     * @param options  the options to apply to the update operation
     * @param callback the callback passed the result of the update one operation
     * @throws com.mongodb.MongoWriteException        returned via the callback
     * @throws com.mongodb.MongoWriteConcernException returned via the callback
     * @throws com.mongodb.MongoException             returned via the callback
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void updateOne(ClientSession clientSession, Bson filter, Bson update, UpdateOptions options,
                   SingleResultCallback<UpdateResult> callback);

    /**
     * Update all documents in the collection according to the specified arguments.
     *
     * @param filter   a document describing the query filter, which may not be null.
     * @param update   a document describing the update, which may not be null. The update to apply must include only update operators. T
     * @param callback the callback passed the result of the update many operation
     * @throws com.mongodb.MongoWriteException        returned via the callback
     * @throws com.mongodb.MongoWriteConcernException returned via the callback
     * @throws com.mongodb.MongoException             returned via the callback
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    void updateMany(Bson filter, Bson update, SingleResultCallback<UpdateResult> callback);

    /**
     * Update all documents in the collection according to the specified arguments.
     *
     * @param filter   a document describing the query filter, which may not be null.
     * @param update   a document describing the update, which may not be null. The update to apply must include only update operators.
     * @param options  the options to apply to the update operation
     * @param callback the callback passed the result of the update many operation
     * @throws com.mongodb.MongoWriteException        returned via the callback
     * @throws com.mongodb.MongoWriteConcernException returned via the callback
     * @throws com.mongodb.MongoException             returned via the callback
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    void updateMany(Bson filter, Bson update, UpdateOptions options, SingleResultCallback<UpdateResult> callback);

    /**
     * Update all documents in the collection according to the specified arguments.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param filter   a document describing the query filter, which may not be null.
     * @param update   a document describing the update, which may not be null. The update to apply must include only update operators. T
     * @param callback the callback passed the result of the update many operation
     * @throws com.mongodb.MongoWriteException        returned via the callback
     * @throws com.mongodb.MongoWriteConcernException returned via the callback
     * @throws com.mongodb.MongoException             returned via the callback
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void updateMany(ClientSession clientSession, Bson filter, Bson update, SingleResultCallback<UpdateResult> callback);

    /**
     * Update all documents in the collection according to the specified arguments.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param filter   a document describing the query filter, which may not be null.
     * @param update   a document describing the update, which may not be null. The update to apply must include only update operators.
     * @param options  the options to apply to the update operation
     * @param callback the callback passed the result of the update many operation
     * @throws com.mongodb.MongoWriteException        returned via the callback
     * @throws com.mongodb.MongoWriteConcernException returned via the callback
     * @throws com.mongodb.MongoException             returned via the callback
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void updateMany(ClientSession clientSession, Bson filter, Bson update, UpdateOptions options,
                    SingleResultCallback<UpdateResult> callback);

    /**
     * Atomically find a document and remove it.
     *
     * @param filter   the query filter to find the document with
     * @param callback the callback passed the document that was removed.  If no documents matched the query filter, then null will be
     *                 returned
     */
    void findOneAndDelete(Bson filter, SingleResultCallback<TDocument> callback);

    /**
     * Atomically find a document and remove it.
     *
     * @param filter   the query filter to find the document with
     * @param options  the options to apply to the operation
     * @param callback the callback passed the document that was removed.  If no documents matched the query filter, then null will be
     *                 returned
     */
    void findOneAndDelete(Bson filter, FindOneAndDeleteOptions options, SingleResultCallback<TDocument> callback);

    /**
     * Atomically find a document and remove it.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param filter   the query filter to find the document with
     * @param callback the callback passed the document that was removed.  If no documents matched the query filter, then null will be
     *                 returned
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void findOneAndDelete(ClientSession clientSession, Bson filter, SingleResultCallback<TDocument> callback);

    /**
     * Atomically find a document and remove it.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param filter   the query filter to find the document with
     * @param options  the options to apply to the operation
     * @param callback the callback passed the document that was removed.  If no documents matched the query filter, then null will be
     *                 returned
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void findOneAndDelete(ClientSession clientSession, Bson filter, FindOneAndDeleteOptions options,
                          SingleResultCallback<TDocument> callback);

    /**
     * Atomically find a document and replace it.
     *
     * @param filter      the query filter to apply the the replace operation
     * @param replacement the replacement document
     * @param callback    the callback passed the document that was replaced.  Depending on the value of the {@code returnOriginal}
     *                    property, this will either be the document as it was before the update or as it is after the update.  If no
     *                    documents matched the query filter, then null will be returned
     */
    void findOneAndReplace(Bson filter, TDocument replacement, SingleResultCallback<TDocument> callback);

    /**
     * Atomically find a document and replace it.
     *
     * @param filter      the query filter to apply the the replace operation
     * @param replacement the replacement document
     * @param options     the options to apply to the operation
     * @param callback    the callback passed the document that was replaced.  Depending on the value of the {@code returnOriginal}
     *                    property, this will either be the document as it was before the update or as it is after the update.  If no
     *                    documents matched the query filter, then null will be returned
     */
    void findOneAndReplace(Bson filter, TDocument replacement, FindOneAndReplaceOptions options, SingleResultCallback<TDocument> callback);

    /**
     * Atomically find a document and replace it.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param filter      the query filter to apply the the replace operation
     * @param replacement the replacement document
     * @param callback    the callback passed the document that was replaced.  Depending on the value of the {@code returnOriginal}
     *                    property, this will either be the document as it was before the update or as it is after the update.  If no
     *                    documents matched the query filter, then null will be returned
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void findOneAndReplace(ClientSession clientSession, Bson filter, TDocument replacement, SingleResultCallback<TDocument> callback);

    /**
     * Atomically find a document and replace it.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param filter      the query filter to apply the the replace operation
     * @param replacement the replacement document
     * @param options     the options to apply to the operation
     * @param callback    the callback passed the document that was replaced.  Depending on the value of the {@code returnOriginal}
     *                    property, this will either be the document as it was before the update or as it is after the update.  If no
     *                    documents matched the query filter, then null will be returned
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void findOneAndReplace(ClientSession clientSession, Bson filter, TDocument replacement, FindOneAndReplaceOptions options,
                           SingleResultCallback<TDocument> callback);

    /**
     * Atomically find a document and update it.
     *
     * @param filter   a document describing the query filter, which may not be null.
     * @param update   a document describing the update, which may not be null. The update to apply must include only update operators.
     * @param callback the callback passed the document that was updated before the update was applied.  If no documents matched the query
     *                 filter, then null will be returned
     */
    void findOneAndUpdate(Bson filter, Bson update, SingleResultCallback<TDocument> callback);

    /**
     * Atomically find a document and update it.
     *
     * @param filter   a document describing the query filter, which may not be null.
     * @param update   a document describing the update, which may not be null. The update to apply must include only update operators.
     * @param options  the options to apply to the operation
     * @param callback the callback passed the document that was updated.  Depending on the value of the {@code returnOriginal} property,
     *                 this will either be the document as it was before the update or as it is after the update.  If no documents matched
     *                 the query filter, then null will be returned
     */
    void findOneAndUpdate(Bson filter, Bson update, FindOneAndUpdateOptions options, SingleResultCallback<TDocument> callback);

    /**
     * Atomically find a document and update it.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param filter   a document describing the query filter, which may not be null.
     * @param update   a document describing the update, which may not be null. The update to apply must include only update operators.
     * @param callback the callback passed the document that was updated before the update was applied.  If no documents matched the query
     *                 filter, then null will be returned
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void findOneAndUpdate(ClientSession clientSession, Bson filter, Bson update, SingleResultCallback<TDocument> callback);

    /**
     * Atomically find a document and update it.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param filter   a document describing the query filter, which may not be null.
     * @param update   a document describing the update, which may not be null. The update to apply must include only update operators.
     * @param options  the options to apply to the operation
     * @param callback the callback passed the document that was updated.  Depending on the value of the {@code returnOriginal} property,
     *                 this will either be the document as it was before the update or as it is after the update.  If no documents matched
     *                 the query filter, then null will be returned
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void findOneAndUpdate(ClientSession clientSession, Bson filter, Bson update, FindOneAndUpdateOptions options,
                          SingleResultCallback<TDocument> callback);

    /**
     * Drops this collection from the Database.
     *
     * @param callback the callback that is completed once the collection has been dropped
     * @mongodb.driver.manual reference/command/drop/ Drop Collection
     */
    void drop(SingleResultCallback<Void> callback);

    /**
     * Drops this collection from the Database.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param callback the callback that is completed once the collection has been dropped
     * @mongodb.driver.manual reference/command/drop/ Drop Collection
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void drop(ClientSession clientSession, SingleResultCallback<Void> callback);

    /**
     * Creates an index.  If successful, the callback will be executed with the name of the created index as the result.
     *
     * @param key      an object describing the index key(s), which may not be null.
     * @param callback the callback that is completed once the index has been created
     * @mongodb.driver.manual reference/command/createIndexes/ Create indexes
     */
    void createIndex(Bson key, SingleResultCallback<String> callback);

    /**
     * Creates an index.  If successful, the callback will be executed with the name of the created index as the result.
     *
     * @param key      an object describing the index key(s), which may not be null.
     * @param options  the options for the index
     * @param callback the callback that is completed once the index has been created
     * @mongodb.driver.manual reference/command/createIndexes/ Create indexes
     */
    void createIndex(Bson key, IndexOptions options, SingleResultCallback<String> callback);

    /**
     * Creates an index.  If successful, the callback will be executed with the name of the created index as the result.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param key      an object describing the index key(s), which may not be null.
     * @param callback the callback that is completed once the index has been created
     * @mongodb.driver.manual reference/command/createIndexes/ Create indexes
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void createIndex(ClientSession clientSession, Bson key, SingleResultCallback<String> callback);

    /**
     * Creates an index.  If successful, the callback will be executed with the name of the created index as the result.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param key      an object describing the index key(s), which may not be null.
     * @param options  the options for the index
     * @param callback the callback that is completed once the index has been created
     * @mongodb.driver.manual reference/command/createIndexes/ Create indexes
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void createIndex(ClientSession clientSession, Bson key, IndexOptions options, SingleResultCallback<String> callback);

    /**
     * Create multiple indexes. If successful, the callback will be executed with a list of the namess of the created index as the result.
     *
     * @param indexes the list of indexes
     * @param callback the callback that is completed once the indexes has been created
     * @mongodb.driver.manual reference/command/createIndexes Create indexes
     * @mongodb.server.release 2.6
     */
    void createIndexes(List<IndexModel> indexes, SingleResultCallback<List<String>> callback);

    /**
     * Create multiple indexes. If successful, the callback will be executed with a list of the namess of the created index as the result.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param indexes the list of indexes
     * @param callback the callback that is completed once the indexes has been created
     * @mongodb.driver.manual reference/command/createIndexes Create indexes
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void createIndexes(ClientSession clientSession, List<IndexModel> indexes, SingleResultCallback<List<String>> callback);

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
     * @param clientSession  the client session with which to associate this operation
     * @return the list indexes iterable interface
     * @mongodb.driver.manual reference/command/listIndexes/ List indexes
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    ListIndexesIterable<Document> listIndexes(ClientSession clientSession);

    /**
     * Get all the indexes in this collection.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the list indexes iterable interface
     * @mongodb.driver.manual reference/command/listIndexes/ List indexes
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    <TResult> ListIndexesIterable<TResult> listIndexes(ClientSession clientSession, Class<TResult> resultClass);

    /**
     * Drops the index given its name.
     *
     * @param indexName the name of the index to remove
     * @param callback  the callback that is completed once the index has been dropped
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop indexes
     */
    void dropIndex(String indexName, SingleResultCallback<Void> callback);

    /**
     * Drops the index given the keys used to create it.
     *
     * @param keys the keys of the index to remove
     * @param callback  the callback that is completed once the index has been dropped
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop indexes
     */
    void dropIndex(Bson keys, SingleResultCallback<Void> callback);

    /**
     * Drops the index given its name.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param indexName the name of the index to remove
     * @param callback  the callback that is completed once the index has been dropped
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop indexes
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void dropIndex(ClientSession clientSession, String indexName, SingleResultCallback<Void> callback);

    /**
     * Drops the index given the keys used to create it.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param keys the keys of the index to remove
     * @param callback  the callback that is completed once the index has been dropped
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop indexes
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void dropIndex(ClientSession clientSession, Bson keys, SingleResultCallback<Void> callback);

    /**
     * Drop all the indexes on this collection, except for the default on _id.
     *
     * @param callback the callback that is completed once all the indexes have been dropped
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop indexes
     */
    void dropIndexes(SingleResultCallback<Void> callback);

    /**
     * Drop all the indexes on this collection, except for the default on _id.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param callback the callback that is completed once all the indexes have been dropped
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop indexes
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void dropIndexes(ClientSession clientSession, SingleResultCallback<Void> callback);

    /**
     * Rename the collection with oldCollectionName to the newCollectionName.
     *
     * @param newCollectionNamespace the namespace the collection will be renamed to
     * @param callback               the callback that is completed once the collection has been renamed
     * @throws com.mongodb.MongoServerException if you provide a newCollectionName that is the name of an existing collection, or if the
     *                                          oldCollectionName is the name of a collection that doesn't exist
     * @mongodb.driver.manual reference/command/renameCollection Rename collection
     */
    void renameCollection(MongoNamespace newCollectionNamespace, SingleResultCallback<Void> callback);

    /**
     * Rename the collection with oldCollectionName to the newCollectionName.
     *
     * @param newCollectionNamespace the name the collection will be renamed to
     * @param options                the options for renaming a collection
     * @param callback               the callback that is completed once the collection has been renamed
     * @throws com.mongodb.MongoServerException if you provide a newCollectionName that is the name of an existing collection and dropTarget
     *                                          is false, or if the oldCollectionName is the name of a collection that doesn't exist
     * @mongodb.driver.manual reference/command/renameCollection Rename collection
     */
    void renameCollection(MongoNamespace newCollectionNamespace, RenameCollectionOptions options, SingleResultCallback<Void> callback);

    /**
     * Rename the collection with oldCollectionName to the newCollectionName.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param newCollectionNamespace the namespace the collection will be renamed to
     * @param callback               the callback that is completed once the collection has been renamed
     * @throws com.mongodb.MongoServerException if you provide a newCollectionName that is the name of an existing collection, or if the
     *                                          oldCollectionName is the name of a collection that doesn't exist
     * @mongodb.driver.manual reference/command/renameCollection Rename collection
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void renameCollection(ClientSession clientSession, MongoNamespace newCollectionNamespace, SingleResultCallback<Void> callback);

    /**
     * Rename the collection with oldCollectionName to the newCollectionName.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param newCollectionNamespace the name the collection will be renamed to
     * @param options                the options for renaming a collection
     * @param callback               the callback that is completed once the collection has been renamed
     * @throws com.mongodb.MongoServerException if you provide a newCollectionName that is the name of an existing collection and dropTarget
     *                                          is false, or if the oldCollectionName is the name of a collection that doesn't exist
     * @mongodb.driver.manual reference/command/renameCollection Rename collection
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void renameCollection(ClientSession clientSession, MongoNamespace newCollectionNamespace, RenameCollectionOptions options,
                          SingleResultCallback<Void> callback);

}
