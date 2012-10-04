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

public interface Collection {


    /**
     * Gets the name of this collection.
     *
     * @return the collection name
     */
    String getName();

    /**
     * Gets the database that this collection is a part of.
     *
     * @return the database
     */
//    Database getDatabase();

    /**
     * @return
     */
//    Query query();

//    void save(Map<String, Object> document);

//    void save(Map<String, Object> document, WriteConcern writeConcern);

//    void insert(Iterable<Map<String, Object>> document);
//
//    void insert(Iterable<Map<String, Object>> document, WriteConcern writeConcern);
//
//    void delete(Query query);
//
//    void delete(Query query, WriteConcern writeConcern);
//
//    UpdateResults update(Query query, UpdateOperations ops);
//
//    /**
//     * updates all entities found with the operations, if nothing is found insert the update as an entity if "createIfMissing" is true; this is an atomic operation per entity
//     */
//    UpdateResults update(Query query, UpdateOperations ops, WriteConcern writeConcern);
//
//    /**
//     * updates the first entity found with the operations; this is an atomic operation
//     */
//    UpdateResults updateFirst(Query query, UpdateOperations ops);
//
//    /**
//     * updates the first entity found with the operations, if nothing is found insert the update as an entity if "createIfMissing" is true; this is an atomic operation per entity
//     */
//    UpdateResults updateFirst(Query query, UpdateOperations ops, boolean createIfMissing);
//
//    UpdateResults updateFirst(Query query, UpdateOperations ops, boolean createIfMissing, WriteConcern writeConcern);
//
//    /**
//     * updates the first entity found with the operations, if nothing is found insert the update as an entity if "createIfMissing" is true; this is an atomic operation per entity
//     */
//    UpdateResults updateFirst(Query query, Map<String, Object> document, boolean createIfMissing);
//
//    /**
//     * updates the first entity found with the operations, if nothing is found insert the update as an entity if "createIfMissing" is true; this is an atomic operation per entity
//     */
//    UpdateResults updateFirst(Query query, Map<String, Object> document, boolean createIfMissing, WriteConcern writeConcern);

//    Database getDatabase();
}
