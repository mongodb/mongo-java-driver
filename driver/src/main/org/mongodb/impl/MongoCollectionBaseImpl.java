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

package org.mongodb.impl;

import org.mongodb.MongoCollection;
import org.mongodb.MongoCollectionBase;
import org.mongodb.MongoCollectionOptions;
import org.mongodb.MongoDatabase;
import org.mongodb.MongoNamespace;
import org.mongodb.serialization.CollectibleSerializer;

public class MongoCollectionBaseImpl<T> implements MongoCollectionBase<T> {
    private final String name;
    private final MongoDatabase database;
    private final MongoCollectionOptions options;
    private final CollectibleSerializer<T> serializer;

    public MongoCollectionBaseImpl(final MongoCollection<T> from) {
        this.serializer = from.getSerializer();
        this.name = from.getName();
        this.database = from.getDatabase();
        this.options = from.getOptions();
    }

    public MongoCollectionBaseImpl(final CollectibleSerializer<T> serializer, final String name,
                                   final MongoDatabaseImpl database, final MongoCollectionOptions options) {

        this.serializer = serializer;
        this.name = name;
        this.database = database;
        this.options = options;
    }

    @Override
    public MongoDatabase getDatabase() {
        return database;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public CollectibleSerializer<T> getSerializer() {
        return serializer;
    }

//    @Override
//    public MongoClient getClient() {
//        return getDatabase().getClient();
//    }

    @Override
    public MongoCollectionOptions getOptions() {
        return options;
    }

    @Override
    public MongoNamespace getNamespace() {
        return new MongoNamespace(getDatabase().getName(), getName());
    }
}
