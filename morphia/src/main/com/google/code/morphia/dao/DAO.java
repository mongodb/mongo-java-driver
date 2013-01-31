/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package com.google.code.morphia.dao;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Key;
import com.google.code.morphia.query.Query;
import com.google.code.morphia.query.QueryResults;
import com.google.code.morphia.query.UpdateOperations;
import com.google.code.morphia.query.UpdateResults;
import com.mongodb.DBCollection;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

import java.util.List;

public interface DAO<T, K> {
    /**
     * Starts a query for this DAO entities type
     */
    Query<T> createQuery();

    /**
     * Starts a update-operations def for this DAO entities type
     */
    UpdateOperations<T> createUpdateOperations();

    /**
     * The type of entities for this DAO
     */
    Class<T> getEntityClass();

    /**
     * Saves the entity; either inserting or overriding the existing document
     */
    Key<T> save(T entity);

    /**
     * Saves the entity; either inserting or overriding the existing document
     */
    Key<T> save(T entity, WriteConcern wc);

    /**
     * Updates the first entity matched by the constraints with the modifiers supplied.
     */
    UpdateResults<T> updateFirst(Query<T> q, UpdateOperations<T> ops);

    /**
     * Updates all entities matched by the constraints with the modifiers supplied.
     */
    UpdateResults<T> update(Query<T> q, UpdateOperations<T> ops);

    /**
     * Deletes the entity
     */
    WriteResult delete(T entity);

    /**
     * Deletes the entity
     *
     * @return a WriteResult representing the success or failure of this operation
     */
    WriteResult delete(T entity, WriteConcern wc);

    /**
     * Delete the entity by id value
     */
    WriteResult deleteById(K id);

    /**
     * Saves the entities given the query
     */
    WriteResult deleteByQuery(Query<T> q);

    /**
     * Loads the entity by id value
     */
    T get(K id);

    /**
     * Finds the entities Key<T> by the criteria {key:value}
     */
    List<K> findIds(String key, Object value);

    /**
     * Finds the entities Ts
     */
    List<K> findIds();

    /**
     * Finds the entities Ts by the criteria {key:value}
     */
    List<K> findIds(Query<T> q);

    /**
     * checks for entities which match criteria {key:value}
     */
    boolean exists(String key, Object value);

    /**
     * checks for entities which match the criteria
     */
    boolean exists(Query<T> q);

    /**
     * returns the total count
     */
    long count();

    /**
     * returns the count which match criteria {key:value}
     */
    long count(String key, Object value);

    /**
     * returns the count which match the criteria
     */
    long count(Query<T> q);

    /**
     * returns the entity which match criteria {key:value}
     */
    T findOne(String key, Object value);

    /**
     * returns the entity which match the criteria
     */
    T findOne(Query<T> q);

    /**
     * returns the entities
     */
    QueryResults<T> find();

    /**
     * returns the entities which match the criteria
     */
    QueryResults<T> find(Query<T> q);

    /**
     * ensures indexed for this DAO
     */
    void ensureIndexes();

    /**
     * gets the collection
     */
    DBCollection getCollection();

    /**
     * returns the underlying datastore
     */
    Datastore getDatastore();
}