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
import com.mongodb.client.model.BulkWriteModel;
import com.mongodb.client.model.CountModel;
import com.mongodb.client.model.DistinctModel;
import com.mongodb.client.model.ExplainableModel;
import com.mongodb.client.model.FindModel;
import com.mongodb.client.model.InsertManyModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
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

    MongoIterable<Document> aggregate(List<?> pipeline);

    <D> MongoIterable<D> aggregate(List<?> pipeline, Class<D> clazz);

    /**
     *
     * @return
     */
    long count();

    /**
     *
     * @param model
     * @return
     */
    long count(CountModel model);

    /**
     *
     * @param fieldName
     * @return
     */
    List<Object> distinct(String fieldName);

    /**
     *
     * @param model
     * @return
     */
    List<Object> distinct(DistinctModel model);

    /**
     *
     * @param model
     * @return
     */
    MongoIterable<T> find(FindModel model);

    <D> MongoIterable<D> find(FindModel model, Class<D> clazz);

    // WRITE

    /**
     *
     * @param operations the write operations
     * @throws com.mongodb.BulkWriteException
     * @throws com.mongodb.MongoException
     * @return the result of the bulk write
     */
    BulkWriteResult bulkWrite(List<? extends WriteModel<? extends T>> operations);

    /**
     *
     * @param model additional options to apply to the bulk write
     * @throws com.mongodb.BulkWriteException
     * @throws com.mongodb.MongoException
     * @return the result of the bulk write
     */
    BulkWriteResult bulkWrite(BulkWriteModel<? extends T> model);

    /**
     * Inserts the provided document. If the document is missing an identifier,
     * the driver should generate one.
     *
     * @param document the document to insert
     * @return the result of the insertion
     * @throws com.mongodb.DuplicateKeyException
     * @throws com.mongodb.MongoException
     */
    InsertOneResult insertOne(T document);

    /**
     * Inserts a batch of documents. The preferred way to perform bulk
     * inserts is to use the BulkWrite API. However, when talking with
     * a server < 2.6, using this method will be faster due to constraints
     * in the bulk API related to error handling.
     *
     * @param documents the documents to insert
     * @throws com.mongodb.DuplicateKeyException
     * @throws com.mongodb.MongoException
     * @return the result of the insertion
     */
    InsertManyResult insertMany(List<? extends T> documents);

    /**
     * Inserts a batch of documents. The preferred way to perform bulk
     * inserts is to use the BulkWrite API. However, when talking with
     * a server < 2.6, using this method will be faster due to constraints
     * in the bulk API related to error handling.
     *
     * @param model the model describing the insert
     * @throws com.mongodb.DuplicateKeyException
     * @throws com.mongodb.MongoException
     * @return the result of the insertion
     */
    InsertManyResult insertMany(InsertManyModel<T> model);

    /**
     * Removes at most one document from the collection that matches the given query filter.  If no documents match,
     * the collection is not modified.
     *
     * @param filter the query filter to apply
     * @return the result of the removal
     * @throws com.mongodb.MongoException
     */
    RemoveResult removeOne(Object filter);

    /**
     * Removes all documents from the collection that match the given query filter.  If no documents match, the collection is not modified.
     *
     * @param filter the query filter to apply
     * @return the result of the removal
     * @throws com.mongodb.MongoException
     */
    RemoveResult removeMany(Object filter);

    /**
     *
     * @param filter
     * @param replacement
     * @return
     */
    ReplaceOneResult replaceOne(Object filter, T replacement);

    /**
     *
     * @param model
     * @return
     */
    ReplaceOneResult replaceOne(ReplaceOneModel<T> model);

    /**
     *
     * @param filter
     * @param update
     * @return
     */
    UpdateResult updateOne(Object filter, Object update);

    /**
     *
     * @param model
     * @return
     */
    UpdateResult updateOne(UpdateOneModel<T> model);

    /**
     *
     * @param filter
     * @param update
     * @return
     */
    UpdateResult updateMany(Object filter, Object update);

    /**
     *
     * @param model
     * @return
     */
    UpdateResult updateMany(UpdateManyModel<T> model);



    // explain

    /**
     *
     * @param explainableModel
     * @param verbosity
     * @return
     * @mongodb.server.release 2.8
     */
    Document explain(ExplainableModel explainableModel, ExplainVerbosity verbosity);
}
