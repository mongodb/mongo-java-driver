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
import com.mongodb.operation.Index;
import org.mongodb.Document;

import java.util.List;

/**
 * Provides the functionality for a collection that is useful for administration, but not necessarily in the course of normal use of a
 * collection.
 *
 * @since 3.0
 */
public interface CollectionAdministration {
    /**
     * @param indexes all the details of the index to add
     * @mongodb.driver.manual reference/method/db.collection.createIndex/ Ensure Index
     * @see Index
     */
    MongoFuture<Void> createIndexes(List<Index> indexes);

    /**
     * @return all the indexes on this collection
     * @mongodb.driver.manual reference/method/db.collection.getIndexes/ getIndexes
     */
    MongoFuture<List<Document>> getIndexes();

    /**
     * Drops this collection from the Database.
     *
     * @mongodb.driver.manual reference/command/drop/ Drop Collection
     */
    MongoFuture<Void> drop();

    /**
     * Drops the given index.
     *
     * @param index the details of the index to remove
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop Indexes
     */
    MongoFuture<Void> dropIndex(Index index);

    /**
     * Drop all the indexes on this collection, except for the default on _id.
     *
     * @mongodb.driver.manual reference/command/dropIndexes/ Drop Indexes
     */
    MongoFuture<Void> dropIndexes();

}
