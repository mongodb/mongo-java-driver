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

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateViewOptions;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.List;

/**
 * The MongoDatabase interface.
 *
 * <p>Note: Additions to this interface will not be considered to break binary compatibility.</p>
 *
 * @since 3.0
 */
@ThreadSafe
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
     * @param readPreference the new {@link ReadPreference} for the database
     * @return a new MongoDatabase instance with the different readPreference
     */
    MongoDatabase withReadPreference(ReadPreference readPreference);

    /**
     * Create a new MongoDatabase instance with a different write concern.
     *
     * @param writeConcern the new {@link WriteConcern} for the database
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
     * @param documentClass  the default class to cast any documents returned from the database into.
     * @param <TDocument>    the type of the class to use instead of {@code Document}.
     * @return the collection
     */
    <TDocument> MongoCollection<TDocument> getCollection(String collectionName, Class<TDocument> documentClass);

    /**
     * Executes the given command in the context of the current database with a read preference of {@link ReadPreference#primary()}.
     *
     * @param command the command to be run
     * @return the command result
     */
    Document runCommand(Bson command);

    /**
     * Executes the given command in the context of the current database with the given read preference.
     *
     * @param command        the command to be run
     * @param readPreference the {@link ReadPreference} to be used when executing the command
     * @return the command result
     */
    Document runCommand(Bson command, ReadPreference readPreference);

    /**
     * Executes the given command in the context of the current database with a read preference of {@link ReadPreference#primary()}.
     *
     * @param command     the command to be run
     * @param resultClass the class to decode each document into
     * @param <TResult> the type of the class to use instead of {@code Document}.
     * @return the command result
     */
    <TResult> TResult runCommand(Bson command, Class<TResult> resultClass);

    /**
     * Executes the given command in the context of the current database with the given read preference.
     *
     * @param command        the command to be run
     * @param readPreference the {@link ReadPreference} to be used when executing the command
     * @param resultClass    the class to decode each document into
     * @param <TResult>      the type of the class to use instead of {@code Document}.
     * @return the command result
     */
    <TResult> TResult runCommand(Bson command, ReadPreference readPreference, Class<TResult> resultClass);

    /**
     * Executes the given command in the context of the current database with a read preference of {@link ReadPreference#primary()}.
     *
     * @param clientSession the client session with which to associate this operation
     * @param command the command to be run
     * @return the command result
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    Document runCommand(ClientSession clientSession, Bson command);

    /**
     * Executes the given command in the context of the current database with the given read preference.
     *
     * @param clientSession the client session with which to associate this operation
     * @param command        the command to be run
     * @param readPreference the {@link ReadPreference} to be used when executing the command
     * @return the command result
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    Document runCommand(ClientSession clientSession, Bson command, ReadPreference readPreference);

    /**
     * Executes the given command in the context of the current database with a read preference of {@link ReadPreference#primary()}.
     *
     * @param clientSession the client session with which to associate this operation
     * @param command     the command to be run
     * @param resultClass the class to decode each document into
     * @param <TResult> the type of the class to use instead of {@code Document}.
     * @return the command result
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    <TResult> TResult runCommand(ClientSession clientSession, Bson command, Class<TResult> resultClass);

    /**
     * Executes the given command in the context of the current database with the given read preference.
     *
     * @param clientSession  the client session with which to associate this operation
     * @param command        the command to be run
     * @param readPreference the {@link ReadPreference} to be used when executing the command
     * @param resultClass    the class to decode each document into
     * @param <TResult>      the type of the class to use instead of {@code Document}.
     * @return the command result
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    <TResult> TResult runCommand(ClientSession clientSession, Bson command, ReadPreference readPreference, Class<TResult> resultClass);

    /**
     * Drops this database.
     *
     * @mongodb.driver.manual reference/command/dropDatabase/#dbcmd.dropDatabase Drop database
     */
    void drop();

    /**
     * Drops this database.
     *
     * @param clientSession the client session with which to associate this operation
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual reference/command/dropDatabase/#dbcmd.dropDatabase Drop database
     */
    void drop(ClientSession clientSession);

    /**
     * Gets the names of all the collections in this database.
     *
     * @return an iterable containing all the names of all the collections in this database
     */
    MongoIterable<String> listCollectionNames();

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
     * @param clientSession the client session with which to associate this operation
     * @return the list collections iterable interface
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual reference/command/listCollections listCollections
     */
    ListCollectionsIterable<Document> listCollections(ClientSession clientSession);

    /**
     * Finds all the collections in this database.
     *
     * @param clientSession the client session with which to associate this operation
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the list collections iterable interface
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual reference/command/listCollections listCollections
     */
    <TResult> ListCollectionsIterable<TResult> listCollections(ClientSession clientSession, Class<TResult> resultClass);


    /**
     * Create a new collection with the given name.
     *
     * @param collectionName the name for the new collection to create
     * @mongodb.driver.manual reference/command/create Create Command
     */
    void createCollection(String collectionName);

    /**
     * Create a new collection with the selected options
     *
     * @param collectionName          the name for the new collection to create
     * @param createCollectionOptions various options for creating the collection
     * @mongodb.driver.manual reference/command/create Create Command
     */
    void createCollection(String collectionName, CreateCollectionOptions createCollectionOptions);

    /**
     * Create a new collection with the given name.
     *
     * @param clientSession the client session with which to associate this operation
     * @param collectionName the name for the new collection to create
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual reference/command/create Create Command
     */
    void createCollection(ClientSession clientSession, String collectionName);

    /**
     * Create a new collection with the selected options
     *
     * @param clientSession the client session with which to associate this operation
     * @param collectionName          the name for the new collection to create
     * @param createCollectionOptions various options for creating the collection
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual reference/command/create Create Command
     */
    void createCollection(ClientSession clientSession, String collectionName, CreateCollectionOptions createCollectionOptions);

    /**
     * Creates a view with the given name, backing collection/view name, and aggregation pipeline that defines the view.
     *
     * @param viewName the name of the view to create
     * @param viewOn   the backing collection/view for the view
     * @param pipeline the pipeline that defines the view
     * @since 3.4
     * @mongodb.server.release 3.4
     * @mongodb.driver.manual reference/command/create Create Command
     */
    void createView(String viewName, String viewOn, List<? extends Bson> pipeline);

    /**
     * Creates a view with the given name, backing collection/view name, aggregation pipeline, and options that defines the view.
     *
     * @param viewName the name of the view to create
     * @param viewOn   the backing collection/view for the view
     * @param pipeline the pipeline that defines the view
     * @param createViewOptions various options for creating the view
     * @since 3.4
     * @mongodb.server.release 3.4
     * @mongodb.driver.manual reference/command/create Create Command
     */
    void createView(String viewName, String viewOn, List<? extends Bson> pipeline, CreateViewOptions createViewOptions);

    /**
     * Creates a view with the given name, backing collection/view name, and aggregation pipeline that defines the view.
     *
     * @param clientSession the client session with which to associate this operation
     * @param viewName the name of the view to create
     * @param viewOn   the backing collection/view for the view
     * @param pipeline the pipeline that defines the view
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual reference/command/create Create Command
     */
    void createView(ClientSession clientSession, String viewName, String viewOn, List<? extends Bson> pipeline);

    /**
     * Creates a view with the given name, backing collection/view name, aggregation pipeline, and options that defines the view.
     *
     * @param clientSession the client session with which to associate this operation
     * @param viewName the name of the view to create
     * @param viewOn   the backing collection/view for the view
     * @param pipeline the pipeline that defines the view
     * @param createViewOptions various options for creating the view
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual reference/command/create Create Command
     */
    void createView(ClientSession clientSession, String viewName, String viewOn, List<? extends Bson> pipeline,
                    CreateViewOptions createViewOptions);
}
