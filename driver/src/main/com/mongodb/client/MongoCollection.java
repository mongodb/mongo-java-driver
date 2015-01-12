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

import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
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
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.MapReduceOptions;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;

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
     * Get the default class to cast any documents returned from the database into.
     *
     * @return the default class to cast any documents into
     */
    Class<T> getDefaultClass();

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
     * Create a new MongoCollection instance with a different default class to cast any documents returned from the database into..
     *
     * @param clazz the default class to cast any documents returned from the database into.
     * @param <C> The type that the new collection will encode documents from and decode documents to
     * @return a new MongoCollection instance with the different default class
     */
    <C> MongoCollection<C> withDefaultClass(Class<C> clazz);

    /**
     * Create a new MongoCollection instance with a different codec registry.
     *
     * @param codecRegistry the new {@link org.bson.codecs.configuration.CodecRegistry} for the collection
     * @return a new MongoCollection instance with the different codec registry
     */
    MongoCollection<T> withCodecRegistry(CodecRegistry codecRegistry);

    /**
     * Create a new MongoCollection instance with a different read preference.
     *
     * @param readPreference the new {@link com.mongodb.ReadPreference} for the collection
     * @return a new MongoCollection instance with the different readPreference
     */
    MongoCollection<T> withReadPreference(ReadPreference readPreference);

    /**
     * Create a new MongoCollection instance with a different write concern.
     *
     * @param writeConcern the new {@link com.mongodb.WriteConcern} for the collection
     * @return a new MongoCollection instance with the different writeConcern
     */
    MongoCollection<T> withWriteConcern(WriteConcern writeConcern);

    /**
     * Counts the number of documents in the collection.
     *
     * @return the number of documents in the collection
     */
    long count();

    /**
     * Counts the number of documents in the collection according to the given options.
     *
     * @param filter the query filter
     * @return the number of documents in the collection
     */
    long count(Object filter);

    /**
     * Counts the number of documents in the collection according to the given options.
     *
     * @param filter  the query filter
     * @param options the options describing the count
     * @return the number of documents in the collection
     */
    long count(Object filter, CountOptions options);

    /**
     * Gets the distinct values of the specified field name.
     *
     * @param fieldName the field name
     * @param filter    the query filter
     * @return a non-null list of distinct values
     * @mongodb.driver.manual reference/command/distinct/ Distinct
     */
    List<Object> distinct(String fieldName, Object filter);

    /**
     * Gets the distinct values of the specified field name.
     *
     * @param fieldName       the field name
     * @param filter          the query filter
     * @param distinctOptions the options to apply to the distinct operation
     * @return a non-null list of distinct values
     * @mongodb.driver.manual reference/command/distinct/ Distinct
     */
    List<Object> distinct(String fieldName, Object filter, DistinctOptions distinctOptions);

    /**
     * Finds all documents in the collection.
     *
     * @return the fluent find interface
     * @mongodb.driver.manual tutorial/query-documents/ Find
     */
    FindFluent<T> find();

    /**
     * Finds all documents in the collection.
     *
     * @param clazz the class to decode each document into
     * @param <C>   the target document type of the iterable.
     * @return the fluent find interface
     * @mongodb.driver.manual tutorial/query-documents/ Find
     */
    <C> FindFluent<C> find(Class<C> clazz);

    /**
     * Finds all documents in the collection.
     *
     * @param filter the query filter
     * @return the fluent find interface
     * @mongodb.driver.manual tutorial/query-documents/ Find
     */
    FindFluent<T> find(Object filter);

    /**
     * Finds all documents in the collection.
     *
     * @param filter the query filter
     * @param clazz  the class to decode each document into
     * @param <C>    the target document type of the iterable.
     * @return the fluent find interface
     * @mongodb.driver.manual tutorial/query-documents/ Find
     */
    <C> FindFluent<C> find(Object filter, Class<C> clazz);

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
     * @param clazz    the class to decode each document into
     * @param <C>      the target document type of the iterable.
     * @return an iterable containing the result of the aggregation operation
     * @mongodb.driver.manual aggregation/ Aggregation
     * @mongodb.server.release 2.2
     */
    <C> MongoIterable<C> aggregate(List<?> pipeline, Class<C> clazz);

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     *
     * @param pipeline the aggregate pipeline
     * @param options  the options to apply to the aggregation operation
     * @return an iterable containing the result of the aggregation operation
     * @mongodb.driver.manual aggregation/ Aggregation
     * @mongodb.server.release 2.2
     */
    MongoIterable<Document> aggregate(List<?> pipeline, AggregateOptions options);

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     *
     * @param pipeline the aggregate pipeline
     * @param options  the options to apply to the aggregation operation
     * @param clazz    the class to decode each document into
     * @param <C>      the target document type of the iterable.
     * @return an iterable containing the result of the aggregation operation
     * @mongodb.driver.manual aggregation/ Aggregation
     * @mongodb.server.release 2.2
     */
    <C> MongoIterable<C> aggregate(List<?> pipeline, AggregateOptions options, Class<C> clazz);

    /**
     * Aggregates documents according to the specified map-reduce function.
     *
     * @param mapFunction    A JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduceFunction A JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @return an iterable containing the result of the map-reduce operation
     * @mongodb.driver.manual reference/command/mapReduce/ map-reduce
     */
    MongoIterable<Document> mapReduce(String mapFunction, String reduceFunction);

    /**
     * Aggregates documents according to the specified map-reduce function.
     *
     * @param mapFunction    A JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduceFunction A JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @param options        The specific options for the map-reduce command.
     * @return an iterable containing the result of the map-reduce operation
     * @mongodb.driver.manual reference/command/mapReduce/ map-reduce
     */
    MongoIterable<Document> mapReduce(String mapFunction, String reduceFunction, MapReduceOptions options);

    /**
     * Aggregates documents according to the specified map-reduce function.
     *
     * @param mapFunction    A JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduceFunction A JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @param clazz          the class to decode each resulting document into.
     * @param <C>            the target document type of the iterable.
     * @return an iterable containing the result of the map-reduce operation
     * @mongodb.driver.manual reference/command/mapReduce/ map-reduce
     */
    <C> MongoIterable<C> mapReduce(String mapFunction, String reduceFunction, Class<C> clazz);

    /**
     * Aggregates documents according to the specified map-reduce function.
     *
     * @param mapFunction    A JavaScript function that associates or "maps" a value with a key and emits the key and value pair.
     * @param reduceFunction A JavaScript function that "reduces" to a single object all the values associated with a particular key.
     * @param options        The specific options for the map-reduce command.
     * @param clazz          the class to decode each resulting document into.
     * @param <C>            the target document type of the iterable.
     * @return an iterable containing the result of the map-reduce operation
     * @mongodb.driver.manual reference/command/mapReduce/ map-reduce
     */
    <C> MongoIterable<C> mapReduce(String mapFunction, String reduceFunction, MapReduceOptions options, Class<C> clazz);

    /**
     * Executes a mix of inserts, updates, replaces, and deletes.
     *
     * @param requests the writes to execute
     * @return the result of the bulk write
     * @throws com.mongodb.MongoBulkWriteException if there's an exception in the bulk write operation
     * @throws com.mongodb.MongoException          if there's an exception running the operation
     */
    BulkWriteResult bulkWrite(List<? extends WriteModel<? extends T>> requests);

    /**
     * Executes a mix of inserts, updates, replaces, and deletes.
     *
     * @param requests the writes to execute
     * @param options  the options to apply to the bulk write operation
     * @return the result of the bulk write
     * @throws com.mongodb.MongoBulkWriteException if there's an exception in the bulk write operation
     * @throws com.mongodb.MongoException          if there's an exception running the operation
     */
    BulkWriteResult bulkWrite(List<? extends WriteModel<? extends T>> requests, BulkWriteOptions options);

    /**
     * Inserts the provided document. If the document is missing an identifier, the driver should generate one.
     *
     * @param document the document to insert
     * @throws com.mongodb.MongoWriteException        if the write failed due some other failure specific to the insert command
     * @throws com.mongodb.MongoWriteConcernException if the write failed due being unable to fulfil the write concern
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     */
    void insertOne(T document);

    /**
     * Inserts one or more documents.  A call to this method is equivalent to a call to the {@code bulkWrite} method
     *
     * @param documents the documents to insert
     * @throws com.mongodb.MongoBulkWriteException if there's an exception in the bulk write operation
     * @throws com.mongodb.MongoException          if the write failed due some other failure
     * @see com.mongodb.client.MongoCollection#bulkWrite
     */
    void insertMany(List<? extends T> documents);

    /**
     * Inserts one or more documents.  A call to this method is equivalent to a call to the {@code bulkWrite} method
     *
     * @param documents the documents to insert
     * @param options   the options to apply to the operation
     * @throws com.mongodb.DuplicateKeyException if the write failed to a duplicate unique key
     * @throws com.mongodb.WriteConcernException if the write failed due being unable to fulfil the write concern
     * @throws com.mongodb.MongoException        if the write failed due some other failure
     */
    void insertMany(List<? extends T> documents, InsertManyOptions options);

    /**
     * Removes at most one document from the collection that matches the given filter.  If no documents match, the collection is not
     * modified.
     *
     * @param filter the query filter to apply the the delete operation
     * @return the result of the remove one operation
     * @throws com.mongodb.MongoWriteException        if the write failed due some other failure specific to the delete command
     * @throws com.mongodb.MongoWriteConcernException if the write failed due being unable to fulfil the write concern
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     */
    DeleteResult deleteOne(Object filter);

    /**
     * Removes all documents from the collection that match the given query filter.  If no documents match, the collection is not modified.
     *
     * @param filter the query filter to apply the the delete operation
     * @return the result of the remove many operation
     * @throws com.mongodb.MongoWriteException        if the write failed due some other failure specific to the delete command
     * @throws com.mongodb.MongoWriteConcernException if the write failed due being unable to fulfil the write concern
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     */
    DeleteResult deleteMany(Object filter);

    /**
     * Replace a document in the collection according to the specified arguments.
     *
     * @param filter      the query filter to apply the the replace operation
     * @param replacement the replacement document
     * @return the result of the replace one operation
     * @throws com.mongodb.MongoWriteException        if the write failed due some other failure specific to the replace command
     * @throws com.mongodb.MongoWriteConcernException if the write failed due being unable to fulfil the write concern
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     * @mongodb.driver.manual tutorial/modify-documents/#replace-the-document Replace
     */
    UpdateResult replaceOne(Object filter, T replacement);

    /**
     * Replace a document in the collection according to the specified arguments.
     *
     * @param filter        the query filter to apply the the replace operation
     * @param replacement   the replacement document
     * @param updateOptions the options to apply to the replace operation
     * @return the result of the replace one operation
     * @throws com.mongodb.MongoWriteException        if the write failed due some other failure specific to the replace command
     * @throws com.mongodb.MongoWriteConcernException if the write failed due being unable to fulfil the write concern
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     * @mongodb.driver.manual tutorial/modify-documents/#replace-the-document Replace
     */
    UpdateResult replaceOne(Object filter, T replacement, UpdateOptions updateOptions);

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * @param filter a document describing the query filter, which may not be null. This can be of any type for which a {@code Codec} is
     *               registered
     * @param update a document describing the update, which may not be null. The update to apply must include only update operators. This
     *               can be of any type for which a {@code Codec} is registered
     * @return the result of the update one operation
     * @throws com.mongodb.MongoWriteException        if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoWriteConcernException if the write failed due being unable to fulfil the write concern
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    UpdateResult updateOne(Object filter, Object update);

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * @param filter        a document describing the query filter, which may not be null. This can be of any type for which a {@code Codec}
     *                      is registered
     * @param update        a document describing the update, which may not be null. The update to apply must include only update operators.
     *                      This can be of any type for which a {@code Codec} is registered
     * @param updateOptions the options to apply to the update operation
     * @return the result of the update one operation
     * @throws com.mongodb.MongoWriteException        if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoWriteConcernException if the write failed due being unable to fulfil the write concern
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    UpdateResult updateOne(Object filter, Object update, UpdateOptions updateOptions);

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * @param filter a document describing the query filter, which may not be null. This can be of any type for which a {@code Codec} is
     *               registered
     * @param update a document describing the update, which may not be null. The update to apply must include only update operators. This
     *               can be of any type for which a {@code Codec} is registered
     * @return the result of the update one operation
     * @throws com.mongodb.MongoWriteException        if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoWriteConcernException if the write failed due being unable to fulfil the write concern
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    UpdateResult updateMany(Object filter, Object update);

    /**
     * Update a single document in the collection according to the specified arguments.
     *
     * @param filter        a document describing the query filter, which may not be null. This can be of any type for which a {@code Codec}
     *                      is registered
     * @param update        a document describing the update, which may not be null. The update to apply must include only update operators.
     *                      This can be of any type for which a {@code Codec} is registered
     * @param updateOptions the options to apply to the update operation
     * @return the result of the update one operation
     * @throws com.mongodb.MongoWriteException        if the write failed due some other failure specific to the update command
     * @throws com.mongodb.MongoWriteConcernException if the write failed due being unable to fulfil the write concern
     * @throws com.mongodb.MongoException             if the write failed due some other failure
     * @mongodb.driver.manual tutorial/modify-documents/ Updates
     * @mongodb.driver.manual reference/operator/update/ Update Operators
     */
    UpdateResult updateMany(Object filter, Object update, UpdateOptions updateOptions);

    /**
     * Atomically find a document and remove it.
     *
     * @param filter the query filter to find the document with
     * @return the document that was removed.  If no documents matched the query filter, then null will be returned
     */
    T findOneAndDelete(Object filter);

    /**
     * Atomically find a document and remove it.
     *
     * @param filter  the query filter to find the document with
     * @param options the options to apply to the operation
     * @return the document that was removed.  If no documents matched the query filter, then null will be returned
     */
    T findOneAndDelete(Object filter, FindOneAndDeleteOptions options);

    /**
     * Atomically find a document and replace it.
     *
     * @param filter      the query filter to apply the the replace operation
     * @param replacement the replacement document
     * @return the document that was replaced.  Depending on the value of the {@code returnOriginal} property, this will either be the
     * document as it was before the update or as it is after the update.  If no documents matched the query filter, then null will be
     * returned
     */
    T findOneAndReplace(Object filter, T replacement);

    /**
     * Atomically find a document and replace it.
     *
     * @param filter      the query filter to apply the the replace operation
     * @param replacement the replacement document
     * @param options     the options to apply to the operation
     * @return the document that was replaced.  Depending on the value of the {@code returnOriginal} property, this will either be the
     * document as it was before the update or as it is after the update.  If no documents matched the query filter, then null will be
     * returned
     */
    T findOneAndReplace(Object filter, T replacement, FindOneAndReplaceOptions options);

    /**
     * Atomically find a document and update it.
     *
     * @param filter a document describing the query filter, which may not be null. This can be of any type for which a {@code Codec} is
     *               registered
     * @param update a document describing the update, which may not be null. The update to apply must include only update operators. This
     *               can be of any type for which a {@code Codec} is registered
     * @return the document that was updated before the update was applied.  If no documents matched the query filter, then null will be
     * returned
     */
    T findOneAndUpdate(Object filter, Object update);

    /**
     * Atomically find a document and update it.
     *
     * @param filter  a document describing the query filter, which may not be null. This can be of any type for which a {@code Codec} is
     *                registered
     * @param update  a document describing the update, which may not be null. The update to apply must include only update operators. This
     *                can be of any type for which a {@code Codec} is registered
     * @param options the options to apply to the operation
     * @return the document that was updated.  Depending on the value of the {@code returnOriginal} property, this will either be the
     * document as it was before the update or as it is after the update.  If no documents matched the query filter, then null will be
     * returned
     */
    T findOneAndUpdate(Object filter, Object update, FindOneAndUpdateOptions options);

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
     * @param key                an object describing the index key(s), which may not be null. This can be of any type for which a {@code
     *                           Codec} is registered
     * @param createIndexOptions the options for the index
     * @mongodb.driver.manual reference/method/db.collection.ensureIndex Ensure Index
     */
    void createIndex(Object key, CreateIndexOptions createIndexOptions);

    /**
     * Get all the indexes in this collection.
     *
     * @return an iterable containing all the indexes
     * @mongodb.driver.manual reference/command/listIndexes/ listIndexes
     */
    MongoIterable<Document> listIndexes();

    /**
     * Get all the indexes in this collection.
     *
     * @param clazz    the class to decode each document into
     * @param <C>      the target document type of the iterable.
     * @return an iterable containing all the indexes
     * @mongodb.driver.manual reference/command/listIndexes/ listIndexes
     */
    <C> MongoIterable<C> listIndexes(Class<C> clazz);

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
