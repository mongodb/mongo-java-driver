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

package com.mongodb.async.client;

import com.mongodb.ReadPreference;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.async.MongoFuture;
import com.mongodb.client.options.OperationOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import org.bson.Document;

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
     * Executes command in the context of the current database.
     *
     * @param command the command to be run
     * @return the command result
     */
    MongoFuture<Document> executeCommand(Object command);

    /**
     * Executes command in the context of the current database.
     *
     * @param command        the command to be run
     * @param readPreference the {@link com.mongodb.ReadPreference} to be used when executing the command
     * @return the command result
     */
    MongoFuture<Document> executeCommand(Object command, ReadPreference readPreference);

    /**
     * Executes command in the context of the current database.
     *
     * @param command the command to be run
     * @param clazz   the default class to cast any documents returned from the database into.
     * @param <T>     the type of the class to use instead of {@code Document}.
     * @return the command result
     */
    <T> MongoFuture<T> executeCommand(Object command, Class<T> clazz);

    /**
     * Executes command in the context of the current database.
     *
     * @param command        the command to be run
     * @param readPreference the {@link com.mongodb.ReadPreference} to be used when executing the command
     * @param clazz          the default class to cast any documents returned from the database into.
     * @param <T>            the type of the class to use instead of {@code Document}.
     * @return the command result
     */
    <T> MongoFuture<T> executeCommand(Object command, ReadPreference readPreference, Class<T> clazz);

    /**
     * Gets the options that are used with the database.
     *
     * <p>Note: {@link OperationOptions} is immutable.</p>
     *
     * @return the options
     */
    OperationOptions getOptions();

    /**
     * Gets a collection.
     *
     * @param collectionName the name of the collection to return
     * @return the collection
     */
    MongoCollection<Document> getCollection(String collectionName);

    /**
     * Gets a collection, with the specific {@code OperationOptions}.
     *
     * @param collectionName   the name of the collection to return
     * @param operationOptions the options to be used with the {@code MongoCollection}
     * @return the collection
     */
    MongoCollection<Document> getCollection(String collectionName, OperationOptions operationOptions);

    /**
     * Gets a collection, with a specific document class.
     *
     * @param collectionName the name of the collection to return
     * @param clazz          the default class to cast any documents returned from the database into.
     * @param <T>            the type of the class to use instead of {@code Document}.
     * @return the collection
     */
    <T> MongoCollection<T> getCollection(String collectionName, Class<T> clazz);

    /**
     * Gets a collection, with a specific document class and {@code OperationOptions}.
     *
     * @param collectionName the name of the collection to return
     * @param clazz          the default class to cast any documents returned from the database into
     * @param options        the options to be used with the {@code MongoCollection}
     * @param <T>            the type of the class to use instead of {@code Document}
     * @return the collection
     */
    <T> MongoCollection<T> getCollection(String collectionName, Class<T> clazz, OperationOptions options);

    /**
     * Drops this database.
     *
     * @mongodb.driver.manual reference/commands/dropDatabase/#dbcmd.dropDatabase Drop database
     */
    MongoFuture<Void> dropDatabase();

    /**
     * Gets the names of all the collections in this database.
     *
     * @return a future set of the names of all the collections in this database
     */
    MongoFuture<List<String>> getCollectionNames();

    /**
     * Create a new collection with the given name.
     *
     * @param collectionName the name for the new collection to create
     * @mongodb.driver.manual reference/commands/create Create Command
     */
    MongoFuture<Void> createCollection(String collectionName);

    /**
     * Create a new collection with the selected options
     *
     * @param collectionName the name for the new collection to create
     * @param options        various options for creating the collection
     * @mongodb.driver.manual reference/commands/create Create Command
     */
    MongoFuture<Void> createCollection(String collectionName, CreateCollectionOptions options);
}
