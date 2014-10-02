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

import com.mongodb.async.MongoFuture;
import com.mongodb.client.model.CreateCollectionOptions;

import java.util.List;

/**
 * The administrative commands that can be run against a selected database.  Application developers should not normally need to call these
 * methods.
 *
 * @since 3.0
 */
public interface DatabaseAdministration {
    /**
     * Drops this database.
     *
     * @mongodb.driver.manual reference/commands/dropDatabase/#dbcmd.dropDatabase Drop database
     */
    MongoFuture<Void> drop();

    /**
     * @return a Set of the names of all the collections in this database
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
     * @param createCollectionOptions various options for creating the collection
     * @mongodb.driver.manual reference/commands/create Create Command
     */
    MongoFuture<Void> createCollection(String collectionName, CreateCollectionOptions createCollectionOptions);

}
