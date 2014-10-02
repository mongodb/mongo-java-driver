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

import com.mongodb.ReadPreference;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.client.model.CreateCollectionOptions;
import org.mongodb.Document;

import java.util.List;

/**
 * Additions to this interface will not be considered to break binary compatibility.
 */
@ThreadSafe
public interface MongoDatabase {
    String getName();

    Document executeCommand(Document command);

    Document executeCommand(Document command, ReadPreference readPreference);

    MongoDatabaseOptions getOptions();

    MongoCollection<Document> getCollection(String collectionName);

    MongoCollection<Document> getCollection(String collectionName, MongoCollectionOptions options);

    <T> MongoCollection<T> getCollection(String collectionName, Class<T> clazz);

    <T> MongoCollection<T> getCollection(String collectionName, Class<T> clazz, MongoCollectionOptions options);


    /**
     * Drops this database.
     *
     * @mongodb.driver.manual reference/commands/dropDatabase/#dbcmd.dropDatabase Drop database
     */
    void dropDatabase();

    /**
     * Gets the names of all the collections in this database.
     *
     * @return a set of the names of all the collections in this database
     */
    List<String> getCollectionNames();

    /**
     * Create a new collection with the given name.
     *
     * @param collectionName the name for the new collection to create
     * @mongodb.driver.manual reference/commands/create Create Command
     */
    void createCollection(String collectionName);

    /**
     * Create a new collection with the selected options
     *
     * @param collectionName          the name for the new collection to create
     * @param createCollectionOptions various options for creating the collection
     * @mongodb.driver.manual reference/commands/create Create Command
     */
    void createCollection(String collectionName, CreateCollectionOptions createCollectionOptions);

}
