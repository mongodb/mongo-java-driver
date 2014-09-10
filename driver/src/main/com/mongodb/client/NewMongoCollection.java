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

import com.mongodb.ExplainVerbosity;
import com.mongodb.MongoNamespace;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.client.model.AggregateOptions;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.DistinctOptions;
import com.mongodb.client.model.ExplainableModel;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.FindOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.ReplaceOneOptions;
import com.mongodb.client.model.UpdateManyOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOneOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.mongodb.BulkWriteResult;
import org.mongodb.Document;

import java.util.List;

/**
 * Additions to this interface will not be considered to break binary compatibility.
 *
 * @param <T> The type that this collection will encode documents from and decode documents to.
 *
 * @since 3.0
 */
@ThreadSafe
public interface NewMongoCollection<T> {
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
     * @param options the options describing the count
     * @return the number of documents in the collection
     */
    long count(CountOptions options);

    /**
     * Gets the distinct values of the specified field name.
     *
     * @param fieldName the field name
     * @return a non-null list of distinct values
     * @mongodb.driver.manual manual/reference/command/distinct/ Distinct
     */
     List<Object> distinct(String fieldName);

    /**
     * Gets the distinct values of the specified field name.
     *
     * @param fieldName the field name
     * @return a non-null list of distinct values
     * @mongodb.driver.manual manual/reference/command/distinct/ Distinct
     */
    List<Object> distinct(String fieldName, DistinctOptions options);

    /**
     * Finds all documents in the collection.
     *
     * @return an iterable containing the result of the find operation
     * @mongodb.driver.manual manual/tutorial/query-documents/ Find
     */
    MongoIterable<T> find();

    /**
     * Finds all documents in the collection.
     *
     * @param clazz the class to decode each document into
     * @return an iterable containing the result of the find operation
     * @mongodb.driver.manual manual/tutorial/query-documents/ Find
     */
    <C> MongoIterable<C> find(final Class<C> clazz);

    /**
     * Finds documents in the collection according to the specified options.
     *
     * @param findOptions the options to apply to the find operation
     * @return an iterable containing the result of the find operation
     * @mongodb.driver.manual manual/tutorial/query-documents/ Find
     */
     MongoIterable<T> find(FindOptions findOptions);

    /**
     * Finds documents according to the specified criteria.
     *
     * @param findOptions the options describing the find operation
     * @param clazz the class to decode each document into
     * @return an iterable containing the result of the find operation
     * @mongodb.driver.manual manual/tutorial/query-documents/ Find
     */
    <C> MongoIterable<C> find(FindOptions findOptions, Class<C> clazz);

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     *
     * @param pipeline the aggregate pipeline
     * @return an iterable containing the result of the find operation
     * @mongodb.driver.manual manual/aggregation/ Aggregation
     * @mongodb.server.release 2.2
     */
    MongoIterable<Document> aggregate(List<?> pipeline);

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     *
     * @param pipeline the aggregate pipeline
     * @param clazz the class to decode each document into
     * @return an iterable containing the result of the find operation
     * @mongodb.driver.manual manual/aggregation/ Aggregation
     * @mongodb.server.release 2.2
     */
    <C> MongoIterable<C> aggregate(List<?> pipeline, Class<C> clazz);

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     *
     * @param pipeline the aggregate pipeline
     * @param options the options to apply to the aggregation operation
     * @return an iterable containing the result of the find operation
     * @mongodb.driver.manual manual/aggregation/ Aggregation
     * @mongodb.server.release 2.2
     */
    MongoIterable<Document> aggregate(List<?> pipeline, AggregateOptions options);

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     *
     * @param pipeline the aggregate pipeline
     * @param options the options to apply to the aggregation operation
     * @param clazz the class to decode each document into
     * @return an iterable containing the result of the find operation
     * @mongodb.driver.manual manual/aggregation/ Aggregation
     * @mongodb.server.release 2.2
     */
    <C> MongoIterable<C> aggregate(List<?> pipeline, AggregateOptions options, Class<C> clazz);

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
     * a server < 2.6, using this method will be faster due to constraints
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
     * a server < 2.6, using this method will be faster due to constraints
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
     * @mongodb.driver.manual manual/tutorial/modify-documents/#replace-the-document Replace
     * @param criteria the query criteria to apply the the replace operation
     * @param replacement the replacement document
     */
    UpdateResult replaceOne(Object criteria, T replacement);

    /**
     * Replace a document in the collection according to the specified arguments.
     *
     * @return the result of the replace one operation
     * @mongodb.driver.manual manual/tutorial/modify-documents/#replace-the-document Replace
     * @param criteria the query criteria to apply the the replace operation
     * @param replacement the replacement document
     * @param options the options to apply to the replace operation
     */
    UpdateResult replaceOne(Object criteria, T replacement, ReplaceOneOptions options);

     /**
     * Update a single document in the collection according to the specified arguments.
     *
     * @param criteria a document describing the query criteria, which may not be null. This can be of any type for which a
     * {@code Codec} is registered
     * @param update a document describing the update, which may not be null. The update to apply must include only update
     * operators. This can be of any type for which a {@code Codec} is registered
     * @return the result of the update one operation
     * @mongodb.driver.manual manual/tutorial/modify-documents/ Updates
     * @mongodb.driver.manual manual/reference/operator/update/ Update Operators
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
     * @mongodb.driver.manual manual/tutorial/modify-documents/ Updates
     * @mongodb.driver.manual manual/reference/operator/update/ Update Operators
     */
    UpdateResult updateOne(Object criteria, Object update, UpdateOneOptions options);

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * @param model the model describing the update
     * @return the result of the update one operation
     * @mongodb.driver.manual manual/tutorial/modify-documents/ Updates
     * @mongodb.driver.manual manual/reference/operator/update/ Update Operators
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
     * @mongodb.driver.manual manual/tutorial/modify-documents/ Updates
     * @mongodb.driver.manual manual/reference/operator/update/ Update Operators
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
     * @mongodb.driver.manual manual/tutorial/modify-documents/ Updates
     * @mongodb.driver.manual manual/reference/operator/update/ Update Operators
     */
    UpdateResult updateMany(Object criteria, Object update, UpdateManyOptions options);

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
     * Explain the specified operation with the specified verbosity.
     *
     * @param explainableModel the operation to explain
     * @param verbosity the verbosity
     * @return a document explaining how the server would perform the given operation
     * @mongodb.server.release 2.8
     */
     Document explain(ExplainableModel explainableModel, ExplainVerbosity verbosity);
}
