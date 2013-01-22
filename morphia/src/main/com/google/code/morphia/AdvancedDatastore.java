/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

package com.google.code.morphia;

import com.google.code.morphia.query.Query;
import com.google.code.morphia.query.UpdateOperations;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

/**
 * <p> This interface exposes advanced {@link Datastore} features, like interacting with DBObject and low-level options.
 * <p/> <ul> <li>Implements matching methods from the {@code Datastore} interface but with a specified kind (collection
 * name), or raw types (DBObject). </li> </ul> </p>
 *
 * @author ScottHernandez
 */
public interface AdvancedDatastore extends Datastore {
    /**
     * Creates a reference to the entity (using the current DB -can be null-, the collectionName, and id)
     */
    <T, V> DBRef createRef(Class<T> clazz, V id);

    /**
     * Creates a reference to the entity (using the current DB -can be null-, the collectionName, and id)
     */
    <T> DBRef createRef(T entity);

    /**
     * Find the given entity (by collectionName/id);
     */
    <T> T get(Class<T> clazz, DBRef ref);

    /**
     * Gets the count this kind
     */
    long getCount(String kind);

    <T, V> T get(String kind, Class<T> clazz, V id);

    <T> Query<T> find(String kind, Class<T> clazz);

    <T, V> Query<T> find(String kind, Class<T> clazz, String property, V value, int offset, int size);

    <T> Key<T> save(String kind, T entity);

    /**
     * No validation or conversion is done to the id
     */
    @Deprecated
    <T> WriteResult delete(String kind, T id);

    <T, V> WriteResult delete(String kind, Class<T> clazz, V id);

    <T> Key<T> insert(String kind, T entity);

    <T> Key<T> insert(T entity);

    <T> Key<T> insert(T entity, WriteConcern wc);

    <T> Iterable<Key<T>> insert(T... entities);

    <T> Iterable<Key<T>> insert(Iterable<T> entities, WriteConcern wc);

    <T> Iterable<Key<T>> insert(String kind, Iterable<T> entities);

    <T> Iterable<Key<T>> insert(String kind, Iterable<T> entities, WriteConcern wc);


    <T> Query<T> createQuery(String kind, Class<T> clazz);

    //DBObject implementations; in case we don't have features impl'd yet
    <T> Query<T> createQuery(Class<T> kind, DBObject q);

    <T> Query<T> createQuery(String kind, Class<T> clazz, DBObject q);

    /**
     * Returns a new query based on the example object
     */
    <T> Query<T> queryByExample(String kind, T example);


    <T> UpdateOperations<T> createUpdateOperations(Class<T> kind, DBObject ops);

    // TODO: Removed these until there is something better to replace it
//    DBDecoderFactory setDecoderFact(DBDecoderFactory fact);
//
//    DBDecoderFactory getDecoderFact();

}