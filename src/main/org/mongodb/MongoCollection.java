/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mongodb;

import org.mongodb.operation.MongoDelete;
import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoQuery;

/**
 * Additions to this interface will not be considered to break binary compatibility.
 *
 * @param <T> The type that this collection will serialize documents from and to
 */
public interface MongoCollection<T> {

    /**
     * Gets the database in which this collection resides.
     *
     * @return the database
     */
    MongoDatabase getDatabase();

    /**
     * Gets the name of this collection.  This is the simple name of the collection and is not prefixed with the database name.
     *
     * @return the collection name
     */
    String getName();

    MongoNamespace getNamespace();

    MongoClient getClient();

    WriteConcern getWriteConcern();

    /**
     * The same collection but with a different MongoClient.  Useful when binding to a channel.
     * @see org.mongodb.MongoClient#bindToChannel()
     */
    MongoCollection<T> withClient(MongoClient client);

    /**
     * The same collection but with a different default write concern.
     */
    MongoCollection<T> withWriteConcern(WriteConcern writeConcern);

    MongoCursor<T> find(MongoQuery query);

    InsertResult insert(MongoInsert<T> insert);

    RemoveResult remove(MongoDelete delete);

    // TODO: add
    // update
    // count
    // findAndModify
    // group
    // distinct
    // mapReduce
    // aggregate


}


//    void save(T document);

//    void save(T document, WriteConcern writeConcern);

//    UpdateResult update(MongoQueryFilter filter, UpdateOperations ops);
//
//    /**
//     * updates all entities found with the operations
//     */
//    UpdateResult update(MongoQueryFilter filter, UpdateOperations ops, WriteConcern writeConcern);
//
//    /**
//     * updates the first entity found with the operations
//     */
//    UpdateResult updateFirst(MongoQueryFilter filter, UpdateOperations ops);
//
//    /**
//     * updates the first entity found with the operations, if nothing is found insert the update as an entity if "upsert" is true
//     */
//    UpdateResult updateFirst(MongoQueryFilter filter, UpdateOperations ops, boolean upsert);
//
//    UpdateResult updateFirst(MongoQueryFilter filter, UpdateOperations ops, boolean upsert, WriteConcern writeConcern);
//
//    /**
//     * updates the first entity found with the operations, if nothing is found insert the update as an entity if "upsert" is true
//     */
//    UpdateResult replace(MongoQueryFilter filter, T document, boolean upsert);
//
//    /**
//     * updates the first entity found with the operations, if nothing is found insert the update as an entity if "upsert" is true
//     */
//    UpdateResult replace(MongoQueryFilter filter, T document, boolean upsert, WriteConcern writeConcern);


