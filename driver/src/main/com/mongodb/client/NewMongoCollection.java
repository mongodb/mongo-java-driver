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
import com.mongodb.client.model.AggregateModel;
import com.mongodb.client.model.BulkWriteModel;
import com.mongodb.client.model.CountModel;
import com.mongodb.client.model.DistinctModel;
import com.mongodb.client.model.ExplainableModel;
import com.mongodb.client.model.FindModel;
import com.mongodb.client.model.FindOneAndRemoveModel;
import com.mongodb.client.model.FindOneAndReplaceModel;
import com.mongodb.client.model.FindOneAndUpdateModel;
import com.mongodb.client.model.InsertManyModel;
import com.mongodb.client.model.RemoveManyModel;
import com.mongodb.client.model.RemoveOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.result.RemoveResult;
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
     * Counts the number of documents in the collection according to the given model.
     *
     * @param model the model describing the count
     * @return the number of documents in the collection
     */
    <D> long count(CountModel<D> model);

    /**
     * Gets the distinct values of the specified field name.
     *
     * @param model the model describing the distinct operation
     * @return a non-null list of distict values
     * @mongodb.driver.manual manual/reference/command/distinct/ Distinct
     */
    <D> List<Object> distinct(DistinctModel<D> model);

    /**
     * Finds documents according to the specified criteria.
     *
     * @param model the model describing the find operation
     * @return an iterable containing the result of the find operation
     * @mongodb.driver.manual manual/tutorial/query-documents/ Find
     */
    <D> MongoIterable<T> find(FindModel<D> model);

    /**
     * Finds documents according to the specified criteria.
     *
     * @param model the model describing the find operation
     * @param clazz the class to decode each document into
     * @return an iterable containing the result of the find operation
     * @mongodb.driver.manual manual/tutorial/query-documents/ Find
     */
    <D, C> MongoIterable<C> find(FindModel<D> model, Class<C> clazz);

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     *
     * @param model the model describing the aggregate operation
     * @return an iterable containing the result of the find operation
     * @mongodb.driver.manual manual/aggregation/ Aggregation
     * @mongodb.server.release 2.2
     */
    <D> MongoIterable<Document> aggregate(AggregateModel<D> model);

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     *
     * @param model the model describing the aggregate operation
     * @param clazz the class to decode each document into
     * @return an iterable containing the result of the find operation
     * @mongodb.driver.manual manual/aggregation/ Aggregation
     * @mongodb.server.release 2.2
     */
    <D, C> MongoIterable<C> aggregate(AggregateModel<D> model, Class<C> clazz);

    /**
     *
     * @param model additional options to apply to the bulk write
     * @throws com.mongodb.BulkWriteException
     * @throws com.mongodb.MongoException
     * @return the result of the bulk write
     */
    <D> BulkWriteResult bulkWrite(BulkWriteModel<? extends T, D> model);

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
     * @param model the model describing the insert
     * @throws com.mongodb.DuplicateKeyException
     * @throws com.mongodb.MongoException
     */
    void insertMany(InsertManyModel<T> model);

    /**
     * Removes at most one document from the collection that matches the given query filter.  If no documents match,
     * the collection is not modified.
     *
     * @param model the model describing the remove
     * @return the result of the remove one operation
     * @throws com.mongodb.MongoException
     */
    <D> RemoveResult removeOne(RemoveOneModel<T, D> model);

    /**
     * Removes all documents from the collection that match the given query filter.  If no documents match, the collection is not modified.
     *
     * @param model the model describing the remove
     * @return the result of the remove many operation
     * @throws com.mongodb.MongoException
     */
    <D> RemoveResult removeMany(RemoveManyModel<T, D> model);

    /**
     * Replace a document in the collection according to the specified arguments.
     *
     * @param model the model describing the replace
     * @return the result of the replace one operation
     * @mongodb.driver.manual manual/tutorial/modify-documents/#replace-the-document Replace
     */
    <D> UpdateResult replaceOne(ReplaceOneModel<T, D> model);

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * @param model the model describing the update
     * @return the result of the update one operation
     * @mongodb.driver.manual manual/tutorial/modify-documents/ Updates
     * @mongodb.driver.manual manual/reference/operator/update/ Update Operators
     */
    <D> UpdateResult updateOne(UpdateOneModel<T, D> model);

    /**
     * Update all documents in the collection according to the specified arguments.
     *
     * @param model the model describing the update
     * @return the result of the update many operation
     * @mongodb.driver.manual manual/tutorial/modify-documents/ Updates
     * @mongodb.driver.manual manual/reference/operator/update/ Update Operators
     */
    <D> UpdateResult updateMany(UpdateManyModel<T, D> model);

    /**
     * Atomically find a document and remove it.
     *
     * @param model the model describing the find one and remove
     * @return the document that was removed.  If no documents matched the criteria, then null will be returned
     */
    <D> T findOneAndRemove(FindOneAndRemoveModel<D> model);

    /**
     * Atomically find a document and update it.
     *
     * @param model the model describing the find one and update
     * @return the document that was updated.  Depending on the value of the {@code returnUpdated} property,
     * this will either be the document as it was before the update or as it is after the update.  If no documents matched the criteria,
     * then null will be returned
     */
    <D> T findOneAndUpdate(FindOneAndUpdateModel<D> model);

    /**
     * Atomically find a document and replace it.
     *
     * @param model the model describing the find one and replace
     * @return the document that was replaced.  Depending on the value of the {@code returnReplaced} property,
     * this will either be the document as it was before the update or as it is after the update.  If no documents matched the criteria,
     * then null will be returned
     */
    <D> T findOneAndReplace(FindOneAndReplaceModel<T, D> model);


    /**
     * Explain the specified operation with the specified verbosity.
     *
     * @param explainableModel the operation to explain
     * @param verbosity the verbosity
     * @return a document explaining how the server would perform the given operation
     * @mongodb.server.release 2.8
     */
    <D> Document explain(ExplainableModel<D> explainableModel, ExplainVerbosity verbosity);
}
