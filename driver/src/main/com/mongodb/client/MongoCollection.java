/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import com.mongodb.MongoCursor;
import com.mongodb.MongoNamespace;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.AggregateOptions;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.DistinctOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.FindOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.MapReduceOptions;
import com.mongodb.client.model.ParallelCollectionScanOptions;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;

import java.util.List;

/**
 * The MongoCollection interface.
 *
 * <p>Note: Additions to this interface will not be considered to break binary compatibility.</p>
 *
 * @param <T> The type that this collection will encode documents from and decode documents to.
 * @since 3.0
 */
@ThreadSafe
public interface MongoCollection<T> {

    /**
     * Gets the namespace of this collection.
     *
     * @return the namespace
     */
    MongoNamespace getNamespace();

    /**
     * Gets the options to apply by default to all operations executed via this instance.
     *
     * @return the collection options
     */
    MongoCollectionOptions getOptions();

    /**
     * Counts the number of documents in the collection.
     *
     * @return the number of documents in the collection
     */
    long count();

    /**
     * Counts the number of documents in the collection according to the given options.
     *
     * @param criteria the query criteria
     * @return the number of documents in the collection
     */
    long count(Object criteria);

    /**
     * Counts the number of documents in the collection according to the given options.
     *
     * @param criteria the query criteria
     * @param options the options describing the count
     * @return the number of documents in the collection
     */
    long count(Object criteria, CountOptions options);

    /**
     * Gets the distinct values of the specified field name.
     *
     * @param fieldName the field name
     * @return a non-null list of distinct values
     * @mongodb.driver.manual reference/command/distinct/ Distinct
     */
    List<Object> distinct(String fieldName);

    /**
     * Gets the distinct values of the specified field name.
     *
     * @param fieldName the field name
     * @param distinctOptions the options to apply to the distinct operation
     * @return a non-null list of distinct values
     * @mongodb.driver.manual reference/command/distinct/ Distinct
     */
    List<Object> distinct(String fieldName, DistinctOptions distinctOptions);

    /**
     * Finds all documents in the collection.
     *
     * @return an iterable containing the result of the find operation
     * @mongodb.driver.manual tutorial/query-documents/ Find
     */
    MongoIterable<T> find();

    /**
     * Finds all documents in the collection.
     *
     * @param clazz the class to decode each document into
     * @param <C> the target document type of the iterable.
     * @return an iterable containing the result of the find operation
     * @mongodb.driver.manual tutorial/query-documents/ Find
     */
    <C> MongoIterable<C> find(Class<C> clazz);

    /**
     * Finds all documents in the collection.
     *
     * @param criteria the query criteria
     * @return an iterable containing the result of the find operation
     * @mongodb.driver.manual tutorial/query-documents/ Find
     */
    MongoIterable<T> find(Object criteria);

    /**
     * Finds all documents in the collection.
     *
     * @param criteria the query criteria
     * @param clazz the class to decode each document into
     * @param <C> the target document type of the iterable.
     * @return an iterable containing the result of the find operation
     * @mongodb.driver.manual tutorial/query-documents/ Find
     */
    <C> MongoIterable<C> find(Object criteria, Class<C> clazz);

    /**
     * Finds documents in the collection according to the specified options.
     *
     * @param criteria the query criteria
     * @param findOptions the options to apply to the find operation
     * @return an iterable containing the result of the find operation
     * @mongodb.driver.manual tutorial/query-documents/ Find
     */
    MongoIterable<T> find(Object criteria, FindOptions findOptions);

    /**
     * Finds documents according to the specified criteria.
     *
     * @param criteria the query criteria
     * @param findOptions the options describing the find operation
     * @param clazz the class to decode each document into
     * @param <C> the target document type of the iterable.
     * @return an iterable containing the result of the find operation
     * @mongodb.driver.manual tutorial/query-documents/ Find
     */
    <C> MongoIterable<C> find(Object criteria, FindOptions findOptions, Class<C> clazz);

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     *
     * @param pipeline the aggregate pipeline
     * @return an iterable containing the result of the aggregation operation
     * @mongodb.driver.manual aggregation/ Aggregation
     * @mongodb.server.release 2.2
     */
    MongoIterable<Document> aggregate(List<?> pipeline);

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     *
     * @param pipeline the aggregate pipeline
     * @param clazz the class to decode each document into
     * @param <C> the target document type of the iterable.
     * @return an iterable containing the result of the aggregation operation
     * @mongodb.driver.manual aggregation/ Aggregation
     * @mongodb.server.release 2.2
     */
    <C> MongoIterable<C> aggregate(List<?> pipeline, Class<C> clazz);

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     *
     * @param pipeline the aggregate pipeline
     * @param options the options to apply to the aggregation operation
     * @return an iterable containing the result of the aggregation operation
     * @mongodb.driver.manual aggregation/ Aggregation
     * @mongodb.server.release 2.2
     */
    MongoIterable<Document> aggregate(List<?> pipeline, AggregateOptions options);

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     *
     * @param pipeline the aggregate pipeline
     * @param options the options to apply to the aggregation operation
     * @param clazz the class to decode each document into
     * @param <C> the target document type of the iterable.
     * @return an iterable containing the result of the aggregation operation
     * @mongodb.driver.manual aggregation/ Aggregation
     * @mongodb.server.release 2.2
     */
    <C> MongoIterable<C> aggregate(List<?> pipeline, AggregateOptions options, Class<C> clazz);

    /**
     * Aggregates documents according to the specified map-reduce function.
     *
     * @param mapFunction A JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduceFunction A JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @return an iterable containing the result of the map-reduce operation
     * @mongodb.driver.manual reference/command/mapReduce/ map-reduce
     */
    MongoIterable<Document> mapReduce(String mapFunction, String reduceFunction);

    /**
     * Aggregates documents according to the specified map-reduce function.
     *
     * @param mapFunction A JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduceFunction A JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @param options The specific options for the map-reduce command.
     * @return an iterable containing the result of the map-reduce operation
     * @mongodb.driver.manual reference/command/mapReduce/ map-reduce
     */
    MongoIterable<Document> mapReduce(String mapFunction, String reduceFunction, MapReduceOptions options);

    /**
     * Aggregates documents according to the specified map-reduce function.
     *
     * @param mapFunction A JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduceFunction A JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @param clazz the class to decode each resulting document into.
     * @param <C> the target document type of the iterable.
     * @return an iterable containing the result of the map-reduce operation
     * @mongodb.driver.manual reference/command/mapReduce/ map-reduce
     */
    <C> MongoIterable<C> mapReduce(String mapFunction, String reduceFunction, Class<C> clazz);

    /**
     * Aggregates documents according to the specified map-reduce function.
     *
     * @param mapFunction A JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduceFunction A JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @param options The specific options for the map-reduce command.
     * @param clazz the class to decode each resulting document into.
     * @param <C> the target document type of the iterable.
     * @return an iterable containing the result of the map-reduce operation
     * @mongodb.driver.manual reference/command/mapReduce/ map-reduce
     */
    <C> MongoIterable<C> mapReduce(String mapFunction, String reduceFunction, MapReduceOptions options, Class<C> clazz);

    /**
     * Executes a mix of inserts, updates, replaces, and deletes.
     *
     * @param requests the writes to execute
     * @throws com.mongodb.BulkWriteException
     * @throws com.mongodb.MongoException
     * @return the result of the bulk write
     */
    BulkWriteResult bulkWrite(List<? extends WriteModel<? extends T>> requests);

    /**
     * Executes a mix of inserts, updates, replaces, and deletes.
     *
     * @param requests the writes to execute
     * @param options the options to apply to the bulk write operation
     * @throws com.mongodb.BulkWriteException
     * @throws com.mongodb.MongoException
     * @return the result of the bulk write
     */
    BulkWriteResult bulkWrite(List<? extends WriteModel<? extends T>> requests, BulkWriteOptions options);

    /**
     * Inserts the provided document. If the document is missing an identifier,
     * the driver should generate one.
     *
     * @param document the document to insert
     * @throws com.mongodb.DuplicateKeyException
     * @throws com.mongodb.MongoException
     */
    void insertOne(T document);

    /**
     * Inserts a batch of documents. The preferred way to perform bulk
     * inserts is to use the BulkWrite API. However, when talking with
     * a server &lt; 2.6, using this method will be faster due to constraints
     * in the bulk API related to error handling.
     *
     * @param documents the documents to insert
     * @throws com.mongodb.DuplicateKeyException
     * @throws com.mongodb.MongoException
     */
    void insertMany(List<? extends T> documents);

    /**
     * Inserts a batch of documents. The preferred way to perform bulk
     * inserts is to use the BulkWrite API. However, when talking with
     * a server &lt; 2.6, using this method will be faster due to constraints
     * in the bulk API related to error handling.
     *
     * @param documents the documents to insert
     * @param options the options to apply to the operation
     * @throws com.mongodb.DuplicateKeyException
     * @throws com.mongodb.MongoException
     */
    void insertMany(List<? extends T> documents, InsertManyOptions options);

    /**
     * Removes at most one document from the collection that matches the given criteria.  If no documents match,
     * the collection is not modified.
     *
     * @param criteria the query criteria to apply the the delete operation
     * @return the result of the remove one operation
     * @throws com.mongodb.MongoException
     */
    DeleteResult deleteOne(Object criteria);

    /**
     * Removes all documents from the collection that match the given criteria.  If no documents match, the collection is not modified.
     *
     * @param criteria the query criteria to apply the the delete operation
     * @return the result of the remove many operation
     * @throws com.mongodb.MongoException
     */
    DeleteResult deleteMany(Object criteria);

    /**
     * Replace a document in the collection according to the specified arguments.
     *
     * @return the result of the replace one operation
     * @mongodb.driver.manual tutorial/modify-documents/#replace-the-document Replace
     * @param criteria the query criteria to apply the the replace operation
     * @param replacement the replacement document
     */
    UpdateResult replaceOne(Object criteria, T replacement);

    /**
     * Replace a document in the collection according to the specified arguments.
     *
     * @return the result of the replace one operation
     * @mongodb.driver.manual tutorial/modify-documents/#replace-the-document Replace
     * @param criteria the query criteria to apply the the replace operation
     * @param replacement the replacement document
     * @param options the options to apply to the replace operation
     */
    UpdateResult replaceOne(Object criteria, T replacement, UpdateOptions options);

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * @param criteria a document describing the query criteria, which may not be null. This can be of any type for which a
     * {@code Codec} is registered
     * @param update a document describing the update, which may not be null. The update to apply must include only update
     * operators. This can be of any type for which a {@code Codec} is registered
     * @return the result of the update one operation
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    UpdateResult updateOne(Object criteria, Object update);

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * @param criteria a document describing the query criteria, which may not be null. This can be of any type for which a
     * {@code Codec} is registered
     * @param update a document describing the update, which may not be null. The update to apply must include only update
     * operators. This can be of any type for which a {@code Codec} is registered
     * @param options the options to apply to the update operation
     * @return the result of the update one operation
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    UpdateResult updateOne(Object criteria, Object update, UpdateOptions options);

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * @param model the model describing the update
     * @return the result of the update one operation
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    UpdateResult updateOne(UpdateOneModel<T> model);

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * @param criteria a document describing the query criteria, which may not be null. This can be of any type for which a
     * {@code Codec} is registered
     * @param update a document describing the update, which may not be null. The update to apply must include only update
     * operators. This can be of any type for which a {@code Codec} is registered
     * @return the result of the update one operation
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    UpdateResult updateMany(Object criteria, Object update);

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * @param criteria a document describing the query criteria, which may not be null. This can be of any type for which a
     * {@code Codec} is registered
     * @param update a document describing the update, which may not be null. The update to apply must include only update
     * operators. This can be of any type for which a {@code Codec} is registered
     * @param options the options to apply to the update operation
     * @return the result of the update one operation
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    UpdateResult updateMany(Object criteria, Object update, UpdateOptions options);

    /**
     * Atomically find a document and remove it.
     *
     * @param criteria the query criteria to find the document with
     * @return the document that was removed.  If no documents matched the criteria, then null will be returned
     */
    T findOneAndDelete(Object criteria);

    /**
     * Atomically find a document and remove it.
     *
     * @param criteria the query criteria to find the document with
     * @param options the options to apply to the operation
     * @return the document that was removed.  If no documents matched the criteria, then null will be returned
     */
    T findOneAndDelete(Object criteria, FindOneAndDeleteOptions options);

    /**
     * Atomically find a document and replace it.
     *
     * @param criteria the query criteria to apply the the replace operation
     * @param replacement the replacement document
     * @return the document that was replaced.  Depending on the value of the {@code returnReplaced} property,
     * this will either be the document as it was before the update or as it is after the update.  If no documents matched the criteria,
     * then null will be returned
     */
    T findOneAndReplace(Object criteria, T replacement);

    /**
     * Atomically find a document and replace it.
     *
     * @param criteria the query criteria to apply the the replace operation
     * @param replacement the replacement document
     * @param options the options to apply to the operation
     * @return the document that was replaced.  Depending on the value of the {@code returnReplaced} property,
     * this will either be the document as it was before the update or as it is after the update.  If no documents matched the criteria,
     * then null will be returned
     */
    T findOneAndReplace(Object criteria, T replacement, FindOneAndReplaceOptions options);

    /**
     * Atomically find a document and update it.
     *
     * @param criteria a document describing the query criteria, which may not be null. This can be of any type for which a
     * {@code Codec} is registered
     * @param update a document describing the update, which may not be null. The update to apply must include only update
     * operators. This can be of any type for which a {@code Codec} is registered
     * @return the document that was updated.  Depending on the value of the {@code returnUpdated} property,
     * this will either be the document as it was before the update or as it is after the update.  If no documents matched the criteria,
     * then null will be returned
     */
    T findOneAndUpdate(Object criteria, Object update);

    /**
     * Atomically find a document and update it.
     *
     * @param criteria a document describing the query criteria, which may not be null. This can be of any type for which a
     * {@code Codec} is registered
     * @param update a document describing the update, which may not be null. The update to apply must include only update
     * operators. This can be of any type for which a {@code Codec} is registered
     * @param options the options to apply to the operation
     * @return the document that was updated.  Depending on the value of the {@code returnUpdated} property,
     * this will either be the document as it was before the update or as it is after the update.  If no documents matched the criteria,
     * then null will be returned
     */
    T findOneAndUpdate(Object criteria, Object update, FindOneAndUpdateOptions options);

    /**
     * Create multiple parallel cursors for reading all the documents from a collection, thereby increasing throughput.
     *
     * @param numCursors the number of cursors requested to iterate in parallel.
     * @return a list of MongoIterable, whose size may be less than the number requested
     * @mongodb.server.release 2.6
     */
    List<MongoCursor<T>> parallelCollectionScan(int numCursors);

    /**
     * Create multiple parallel cursors for reading all the documents from a collection, thereby increasing throughput.
     *
     * @param numCursors the number of cursors requested to iterate in parallel.
     * @param parallelCollectionScanOptions the options to use with operation.
     * @return a list of MongoIterable, whose size may be less than the number requested
     * @mongodb.server.release 2.6
     */
    List<MongoCursor<T>> parallelCollectionScan(int numCursors, ParallelCollectionScanOptions parallelCollectionScanOptions);

    /**
     * Create multiple parallel cursors for reading all the documents from a collection, thereby increasing throughput.
     *
     * @param numCursors the number of cursors requested to iterate in parallel.
     * @param clazz the class to decode each document into.
     * @param <C> the target document type of the iterable.
     * @return a list of MongoIterable, whose size may be less than the number requested
     * @mongodb.server.release 2.6
     */
    <C> List<MongoCursor<C>> parallelCollectionScan(int numCursors, Class<C> clazz);

    /**
     * Create multiple parallel cursors for reading all the documents from a collection, thereby increasing throughput.
     *
     * @param numCursors the number of cursors requested to iterate in parallel.
     * @param parallelCollectionScanOptions the options to use with operation.
     * @param clazz the class to decode each document into.
     * @param <C> the target document type of the iterable.
     * @return a list of MongoIterable, whose size may be less than the number requested
     * @mongodb.server.release 2.6
     */
    <C> List<MongoCursor<C>> parallelCollectionScan(int numCursors, ParallelCollectionScanOptions parallelCollectionScanOptions,
                                                      Class<C> clazz);

    /**
     * Drops this collection from the Database.
     *
     * @mongodb.driver.manual reference/command/drop/ Drop Collection
     */
    void dropCollection();

    /**
     * @param key an object describing the index key(s), which may not be null. This can be of any type for which a {@code Codec} is
     *            registered
     * @mongodb.driver.manual reference/method/db.collection.ensureIndex Ensure Index
     */
    void createIndex(Object key);

    /**
     * @param key an object describing the index key(s), which may not be null. This can be of any type for which a {@code Codec} is
     *            registered
     * @param createIndexOptions the options for the index
     * @mongodb.driver.manual reference/method/db.collection.ensureIndex Ensure Index
     */
    void createIndex(Object key, CreateIndexOptions createIndexOptions);

    /**
     * @return all the indexes on this collection
     * @mongodb.driver.manual reference/method/db.collection.getIndexes/ getIndexes
     */
    List<Document> getIndexes();

    /**
     * Drops the given index.
     *
     * @param indexName the name of the index to remove
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop Indexes
     */
    void dropIndex(String indexName);

    /**
     * Drop all the indexes on this collection, except for the default on _id.
     *
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop Indexes
     */
    void dropIndexes();

    /**
     * Rename the collection with oldCollectionName to the newCollectionName.
     *
     * @param newCollectionNamespace the namespace the collection will be renamed to
     * @throws com.mongodb.MongoServerException if you provide a newCollectionName that is the name of an existing collection, or if the
     *                                          oldCollectionName is the name of a collection that doesn't exist
     * @mongodb.driver.manual reference/commands/renameCollection Rename collection
     */
    void renameCollection(MongoNamespace newCollectionNamespace);

    /**
     * Rename the collection with oldCollectionName to the newCollectionName.
     *
     * @param newCollectionNamespace  the name the collection will be renamed to
     * @param renameCollectionOptions the options for renaming a collection
     * @throws com.mongodb.MongoServerException if you provide a newCollectionName that is the name of an existing collection and dropTarget
     *                                          is false, or if the oldCollectionName is the name of a collection that doesn't exist
     * @mongodb.driver.manual reference/commands/renameCollection Rename collection
     */
    void renameCollection(MongoNamespace newCollectionNamespace, RenameCollectionOptions renameCollectionOptions);

}
