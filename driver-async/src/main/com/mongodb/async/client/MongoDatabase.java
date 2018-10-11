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

package com.mongodb.async.client;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateViewOptions;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.List;

/**
 * The MongoDatabase interface.
 * <p>
 * Note: Additions to this interface will not be considered to break binary compatibility.</p>
 *
 * @since 3.0
 * @deprecated Prefer the Reactive Streams-based asynchronous driver (mongodb-driver-reactivestreams artifactId)
 */
@ThreadSafe
@Deprecated
public interface MongoDatabase {

    /**
     * Gets the name of the database.
     *
     * @return the database name
     */
    String getName();

    /**
     * Get the codec registry for the MongoDatabase.
     *
     * @return the {@link org.bson.codecs.configuration.CodecRegistry}
     */
    CodecRegistry getCodecRegistry();

    /**
     * Get the read preference for the MongoDatabase.
     *
     * @return the {@link com.mongodb.ReadPreference}
     */
    ReadPreference getReadPreference();

    /**
     * Get the write concern for the MongoDatabase.
     *
     * @return the {@link com.mongodb.WriteConcern}
     */
    WriteConcern getWriteConcern();

    /**
     * Get the read concern for the MongoDatabase.
     *
     * @return the {@link com.mongodb.ReadConcern}
     * @since 3.2
     * @mongodb.server.release 3.2
     * @mongodb.driver.manual reference/readConcern/ Read Concern
     */
    ReadConcern getReadConcern();

    /**
     * Create a new MongoDatabase instance with a different codec registry.
     *
     * @param codecRegistry the new {@link org.bson.codecs.configuration.CodecRegistry} for the database
     * @return a new MongoDatabase instance with the different codec registry
     */
    MongoDatabase withCodecRegistry(CodecRegistry codecRegistry);

    /**
     * Create a new MongoDatabase instance with a different read preference.
     *
     * @param readPreference the new {@link com.mongodb.ReadPreference} for the database
     * @return a new MongoDatabase instance with the different readPreference
     */
    MongoDatabase withReadPreference(ReadPreference readPreference);

    /**
     * Create a new MongoDatabase instance with a different write concern.
     *
     * @param writeConcern the new {@link com.mongodb.WriteConcern} for the database
     * @return a new MongoDatabase instance with the different writeConcern
     */
    MongoDatabase withWriteConcern(WriteConcern writeConcern);

    /**
     * Create a new MongoDatabase instance with a different read concern.
     *
     * @param readConcern the new {@link ReadConcern} for the database
     * @return a new MongoDatabase instance with the different ReadConcern
     * @since 3.2
     * @mongodb.server.release 3.2
     * @mongodb.driver.manual reference/readConcern/ Read Concern
     */
    MongoDatabase withReadConcern(ReadConcern readConcern);

    /**
     * Gets a collection.
     *
     * @param collectionName the name of the collection to return
     * @return the collection
     * @throws IllegalArgumentException if collectionName is invalid
     * @see com.mongodb.MongoNamespace#checkCollectionNameValidity(String)
     */
    MongoCollection<Document> getCollection(String collectionName);

    /**
     * Gets a collection, with a specific default document class.
     *
     * @param collectionName the name of the collection to return
     * @param documentClass the default class to cast any documents returned from the database into.
     * @param <TDocument>   the type of the class to use instead of {@code Document}.
     * @return the collection
     */
    <TDocument> MongoCollection<TDocument> getCollection(String collectionName, Class<TDocument> documentClass);

    /**
     * Executes the given command in the context of the current database with a read preference of {@link ReadPreference#primary()}.
     *
     * @param command  the command to be run
     * @param callback the callback that is passed the command result
     */
    void runCommand(Bson command, SingleResultCallback<Document> callback);

    /**
     * Executes the given command in the context of the current database with the given read preference.
     *
     * @param command        the command to be run
     * @param readPreference the {@link com.mongodb.ReadPreference} to be used when executing the command
     * @param callback       the callback that is passed the command result
     */
    void runCommand(Bson command, ReadPreference readPreference, SingleResultCallback<Document> callback);

    /**
     * Executes the given command in the context of the current database with a read preference of {@link ReadPreference#primary()}.
     *
     * @param command     the command to be run
     * @param resultClass the default class to cast any documents returned from the database into.
     * @param <TResult>   the type of the class to use instead of {@code Document}.
     * @param callback    the callback that is passed the command result
     */
    <TResult> void runCommand(Bson command, Class<TResult> resultClass, SingleResultCallback<TResult> callback);

    /**
     * Executes the given command in the context of the current database with the given read preference.
     *
     * @param command        the command to be run
     * @param readPreference the {@link com.mongodb.ReadPreference} to be used when executing the command
     * @param resultClass    the default class to cast any documents returned from the database into.
     * @param <TResult>      the type of the class to use instead of {@code Document}.
     * @param callback       the callback that is passed the command result
     */
    <TResult> void runCommand(Bson command, ReadPreference readPreference, Class<TResult> resultClass,
                              SingleResultCallback<TResult> callback);

    /**
     * Executes the given command in the context of the current database with a read preference of {@link ReadPreference#primary()}.
     *
     * @param clientSession the client session with which to associate this operation
     * @param command  the command to be run
     * @param callback the callback that is passed the command result
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void runCommand(ClientSession clientSession, Bson command, SingleResultCallback<Document> callback);

    /**
     * Executes the given command in the context of the current database with the given read preference.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param command        the command to be run
     * @param readPreference the {@link ReadPreference} to be used when executing the command
     * @param callback       the callback that is passed the command result
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void runCommand(ClientSession clientSession, Bson command, ReadPreference readPreference, SingleResultCallback<Document> callback);
    /**
     * Executes the given command in the context of the current database with a read preference of {@link ReadPreference#primary()}.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param command     the command to be run
     * @param resultClass the default class to cast any documents returned from the database into.
     * @param <TResult>   the type of the class to use instead of {@code Document}.
     * @param callback    the callback that is passed the command result
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    <TResult> void runCommand(ClientSession clientSession, Bson command, Class<TResult> resultClass,
                              SingleResultCallback<TResult> callback);

    /**
     * Executes the given command in the context of the current database with the given read preference.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param command        the command to be run
     * @param readPreference the {@link com.mongodb.ReadPreference} to be used when executing the command
     * @param resultClass    the default class to cast any documents returned from the database into.
     * @param <TResult>      the type of the class to use instead of {@code Document}.
     * @param callback       the callback that is passed the command result
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    <TResult> void runCommand(ClientSession clientSession, Bson command, ReadPreference readPreference, Class<TResult> resultClass,
                              SingleResultCallback<TResult> callback);

    /**
     * Drops this database.
     *
     * @param callback the callback that is completed once the database has been dropped
     * @mongodb.driver.manual reference/command/dropDatabase/#dbcmd.dropDatabase Drop database
     */
    void drop(SingleResultCallback<Void> callback);

    /**
     * Drops this database.
     *
     * @param callback the callback that is completed once the database has been dropped
     * @param clientSession the client session with which to associate this operation
     * @mongodb.driver.manual reference/command/dropDatabase/#dbcmd.dropDatabase Drop database
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void drop(ClientSession clientSession, SingleResultCallback<Void> callback);

    /**
     * Gets the names of all the collections in this database.
     *
     * @return an iterable containing all the names of all the collections in this database
     */
    MongoIterable<String> listCollectionNames();

    /**
     * Gets the names of all the collections in this database.
     *
     * @param clientSession the client session with which to associate this operation
     * @return an iterable containing all the names of all the collections in this database
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    MongoIterable<String> listCollectionNames(ClientSession clientSession);

    /**
     * Finds all the collections in this database.
     *
     * @return the list collections iterable interface
     * @mongodb.driver.manual reference/command/listCollections listCollections
     */
    ListCollectionsIterable<Document> listCollections();

    /**
     * Finds all the collections in this database.
     *
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the list collections iterable interface
     * @mongodb.driver.manual reference/command/listCollections listCollections
     */
    <TResult> ListCollectionsIterable<TResult> listCollections(Class<TResult> resultClass);

    /**
     * Finds all the collections in this database.
     *
     * @param clientSession the client session with which to associate this operation
     * @return the list collections iterable interface
     * @mongodb.driver.manual reference/command/listCollections listCollections
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    ListCollectionsIterable<Document> listCollections(ClientSession clientSession);

    /**
     * Finds all the collections in this database.
     *
     * @param clientSession the client session with which to associate this operation
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the list collections iterable interface
     * @mongodb.driver.manual reference/command/listCollections listCollections
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    <TResult> ListCollectionsIterable<TResult> listCollections(ClientSession clientSession, Class<TResult> resultClass);

    /**
     * Create a new collection with the given name.
     *
     * @param collectionName the name for the new collection to create
     * @param callback       the callback that is completed once the collection has been created
     * @mongodb.driver.manual reference/command/create Create Command
     */
    void createCollection(String collectionName, SingleResultCallback<Void> callback);

    /**
     * Create a new collection with the selected options
     *
     * @param collectionName the name for the new collection to create
     * @param options        various options for creating the collection
     * @param callback       the callback that is completed once the collection has been created
     * @mongodb.driver.manual reference/command/create Create Command
     */
    void createCollection(String collectionName, CreateCollectionOptions options, SingleResultCallback<Void> callback);

    /**
     * Create a new collection with the given name.
     *
     * @param clientSession the client session with which to associate this operation
     * @param collectionName the name for the new collection to create
     * @param callback       the callback that is completed once the collection has been created
     * @mongodb.driver.manual reference/command/create Create Command
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void createCollection(ClientSession clientSession, String collectionName, SingleResultCallback<Void> callback);

    /**
     * Create a new collection with the selected options
     *
     * @param clientSession the client session with which to associate this operation
     * @param collectionName the name for the new collection to create
     * @param options        various options for creating the collection
     * @param callback       the callback that is completed once the collection has been created
     * @mongodb.driver.manual reference/command/create Create Command
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void createCollection(ClientSession clientSession, String collectionName, CreateCollectionOptions options,
                          SingleResultCallback<Void> callback);

    /**
     * Creates a view with the given name, backing collection/view name, and aggregation pipeline that defines the view.
     *
     * @param viewName the name of the view to create
     * @param viewOn   the backing collection/view for the view
     * @param pipeline the pipeline that defines the view
     * @param callback the callback that is completed once the collection has been created
     * @mongodb.driver.manual reference/command/create Create Command
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    void createView(String viewName, String viewOn, List<? extends Bson> pipeline, SingleResultCallback<Void> callback);

    /**
     * Creates a view with the given name, backing collection/view name, aggregation pipeline, and options that defines the view.
     *
     * @param viewName the name of the view to create
     * @param viewOn   the backing collection/view for the view
     * @param pipeline the pipeline that defines the view
     * @param createViewOptions various options for creating the view
     * @param callback the callback that is completed once the collection has been created
     * @mongodb.driver.manual reference/command/create Create Command
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    void createView(String viewName, String viewOn, List<? extends Bson> pipeline, CreateViewOptions createViewOptions,
                    SingleResultCallback<Void> callback);

    /**
     * Creates a view with the given name, backing collection/view name, and aggregation pipeline that defines the view.
     *
     * @param clientSession the client session with which to associate this operation
     * @param viewName the name of the view to create
     * @param viewOn   the backing collection/view for the view
     * @param pipeline the pipeline that defines the view
     * @param callback the callback that is completed once the collection has been created
     * @mongodb.driver.manual reference/command/create Create Command
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void createView(ClientSession clientSession, String viewName, String viewOn, List<? extends Bson> pipeline,
                    SingleResultCallback<Void> callback);

    /**
     * Creates a view with the given name, backing collection/view name, aggregation pipeline, and options that defines the view.
     *
     * @param clientSession the client session with which to associate this operation
     * @param viewName the name of the view to create
     * @param viewOn   the backing collection/view for the view
     * @param pipeline the pipeline that defines the view
     * @param createViewOptions various options for creating the view
     * @param callback the callback that is completed once the collection has been created
     * @mongodb.driver.manual reference/command/create Create Command
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    void createView(ClientSession clientSession, String viewName, String viewOn, List<? extends Bson> pipeline,
                    CreateViewOptions createViewOptions, SingleResultCallback<Void> callback);

    /**
     * Creates a change stream for this database.
     *
     * @return the change stream iterable
     * @mongodb.driver.dochub core/changestreams Change Streams
     * @since 3.8
     * @mongodb.server.release 4.0
     */
    ChangeStreamIterable<Document> watch();

    /**
     * Creates a change stream for this database.
     *
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the change stream iterable
     * @mongodb.driver.dochub core/changestreams Change Streams
     * @since 3.8
     * @mongodb.server.release 4.0
     */
    <TResult> ChangeStreamIterable<TResult> watch(Class<TResult> resultClass);

    /**
     * Creates a change stream for this database.
     *
     * @param pipeline the aggregation pipeline to apply to the change stream.
     * @return the change stream iterable
     * @mongodb.driver.dochub core/changestreams Change Streams
     * @since 3.8
     * @mongodb.server.release 4.0
     */
    ChangeStreamIterable<Document> watch(List<? extends Bson> pipeline);

    /**
     * Creates a change stream for this database.
     *
     * @param pipeline    the aggregation pipeline to apply to the change stream
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the change stream iterable
     * @mongodb.driver.dochub core/changestreams Change Streams
     * @since 3.8
     * @mongodb.server.release 4.0
     */
    <TResult> ChangeStreamIterable<TResult> watch(List<? extends Bson> pipeline, Class<TResult> resultClass);

    /**
     * Creates a change stream for this database.
     *
     * @param clientSession the client session with which to associate this operation
     * @return the change stream iterable
     * @since 3.8
     * @mongodb.server.release 4.0
     * @mongodb.driver.dochub core/changestreams Change Streams
     */
    ChangeStreamIterable<Document> watch(ClientSession clientSession);

    /**
     * Creates a change stream for this database.
     *
     * @param clientSession the client session with which to associate this operation
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the change stream iterable
     * @since 3.8
     * @mongodb.server.release 4.0
     * @mongodb.driver.dochub core/changestreams Change Streams
     */
    <TResult> ChangeStreamIterable<TResult> watch(ClientSession clientSession, Class<TResult> resultClass);

    /**
     * Creates a change stream for this database.
     *
     * @param clientSession the client session with which to associate this operation
     * @param pipeline the aggregation pipeline to apply to the change stream.
     * @return the change stream iterable
     * @since 3.8
     * @mongodb.server.release 4.0
     * @mongodb.driver.dochub core/changestreams Change Streams
     */
    ChangeStreamIterable<Document> watch(ClientSession clientSession, List<? extends Bson> pipeline);

    /**
     * Creates a change stream for this database.
     *
     * @param clientSession the client session with which to associate this operation
     * @param pipeline    the aggregation pipeline to apply to the change stream
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the change stream iterable
     * @since 3.8
     * @mongodb.server.release 4.0
     * @mongodb.driver.dochub core/changestreams Change Streams
     */
    <TResult> ChangeStreamIterable<TResult> watch(ClientSession clientSession, List<? extends Bson> pipeline, Class<TResult> resultClass);

}
