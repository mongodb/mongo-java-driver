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
import com.mongodb.client.result.ReplaceOneResult;
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
    MongoNamespace getNamespace();

    MongoCollectionOptions getOptions();

    // READ

    /**
     * Counts the number of documents in the collection according to the given model.
     *
     * @param model the model describing the count
     * @return the number of documents in the collection
     */
    <D> long count(CountModel<D> model);

    /**
     *
     * @param model
     * @return
     */
    <D> List<Object> distinct(DistinctModel<D> model);

    /**
     *
     * @param model the model describing the find operation
     * @return an iterable containing the result of the find operation
     */
    <D> MongoIterable<T> find(FindModel<D> model);

    <D, C> MongoIterable<C> find(FindModel<D> model, Class<C> clazz);

    <D> MongoIterable<Document> aggregate(AggregateModel<D> model);

    <D, C> MongoIterable<C> aggregate(AggregateModel<D> model, Class<C> clazz);

    // WRITE

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
     *
     * @param model
     * @return the result of the replace one operation
     */
    <D> ReplaceOneResult replaceOne(ReplaceOneModel<T, D> model);

    /**
     *
     * @param model
     * @return the result of the update one operation
     */
    <D> UpdateResult updateOne(UpdateOneModel<T, D> model);

    /**
     *
     * @param model
     * @return the result of the update many operation
     */
    <D> UpdateResult updateMany(UpdateManyModel<T, D> model);

    <D> T findOneAndRemove(FindOneAndRemoveModel<D> model);

    <D> T findOneAndUpdate(FindOneAndUpdateModel<D> model);

    <D> T findOneAndReplace(FindOneAndReplaceModel<T, D> model);


    // explain

    /**
     *
     * @param explainableModel
     * @param verbosity
     * @return
     * @mongodb.server.release 2.8
     */
    <D> Document explain(ExplainableModel<D> explainableModel, ExplainVerbosity verbosity);
}
