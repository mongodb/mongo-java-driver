/**
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
 *
 */

package org.mongodb.impl;

import org.mongodb.InsertResult;
import org.mongodb.MongoClient;
import org.mongodb.MongoCollection;
import org.mongodb.MongoNamespace;
import org.mongodb.MongoCursor;
import org.mongodb.RemoveResult;
import org.mongodb.WriteConcern;
import org.mongodb.operation.MongoDelete;
import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoQuery;

class MongoCollectionImpl<T> implements MongoCollection<T> {
    private final String name;
    private final MongoDatabaseImpl database;
    private final Class<T> clazz;
    private WriteConcern writeConcern;

    public MongoCollectionImpl(final String name, MongoDatabaseImpl database, Class<T> clazz) {
        this(name, database, clazz, null);
    }

    public MongoCollectionImpl(final String name, final MongoDatabaseImpl database, final Class<T> clazz,
                               final WriteConcern writeConcern) {
        this.name = name;
        this.database = database;
        this.clazz = clazz;
        this.writeConcern = writeConcern;
    }

    @Override
    public MongoDatabaseImpl getDatabase() {
        return database;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public MongoCursor<T> find(MongoQuery query) {
        return new MongoCursor<T>(this, query, clazz);
    }

    @Override
    public InsertResult insert(final MongoInsert<T> insert) {
        return getClient().getOperations().insert(getNamespace(), insert, clazz);
    }

    @Override
    public RemoveResult remove(final MongoDelete delete) {
        return getClient().getOperations().delete(getNamespace(), delete);
    }

    @Override
    public MongoCollection<T> withClient(final MongoClient client) {
        return new MongoCollectionImpl<T>(name, database.withClient(client), clazz, writeConcern);
    }

    @Override
    public MongoCollection<T> withWriteConcern(final WriteConcern writeConcern) {
        return new MongoCollectionImpl<T>(name, database, clazz, writeConcern);
    }

    @Override
    public MongoClient getClient() {
        return getDatabase().getClient();
    }

    @Override
    public WriteConcern getWriteConcern() {
        if (writeConcern != null) {
            return writeConcern;
        }
        return getDatabase().getWriteConcern();
    }

    @Override
    public MongoNamespace getNamespace() {
        return new MongoNamespace(getDatabase().getName(), getName());
    }

}
