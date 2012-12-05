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

import org.mongodb.MongoClient;
import org.mongodb.MongoCollection;
import org.mongodb.MongoCollectionName;
import org.mongodb.WriteConcern;

class MongoCollectionImpl<T> implements MongoCollection<T> {
    private final String name;
    private final MongoDatabaseImpl database;

    public MongoCollectionImpl(final String name, MongoDatabaseImpl database) {
        this.name = name;
        this.database = database;
    }

    @Override
    public MongoClient getMongoClient() {
        return getDatabase().getMongoClient();
    }

    @Override
    public MongoDatabaseImpl getDatabase() {
        return database;
    }

    @Override
    public String getName() {
        return name;
    }

    public <T> void insert(T doc, WriteConcern writeConcern) {
        getMongoClient().getOperations().insert(getFullName(), doc, writeConcern);
    }

    private MongoCollectionName getFullName() {
        return new MongoCollectionName(getDatabase().getName(), getName());
    }
}
