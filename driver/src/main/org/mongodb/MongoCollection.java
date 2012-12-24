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

import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoSave;
import org.mongodb.result.InsertResult;
import org.mongodb.result.UpdateResult;

// TODO: add these
// update
// group
// distinct
// mapReduce
// aggregate


/**
 * Additions to this interface will not be considered to break binary compatibility.
 *
 * @param <T> The type that this collection will serialize documents from and to
 */
public interface MongoCollection<T> extends MongoCollectionBase<T>, MongoStream<T> {

    InsertResult insert(MongoInsert<T> insert);

    InsertResult insert(T doc);

    InsertResult insert(Iterable<T> doc);

    UpdateResult save(T doc);

    UpdateResult save(MongoSave<T> save);

    CollectionAdmin admin();
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


