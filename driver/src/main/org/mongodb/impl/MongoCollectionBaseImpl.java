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

package org.mongodb.impl;

import org.mongodb.MongoClient;
import org.mongodb.MongoCollection;
import org.mongodb.MongoCollectionBase;
import org.mongodb.MongoDatabase;
import org.mongodb.MongoNamespace;
import org.mongodb.ReadPreference;
import org.mongodb.WriteConcern;
import org.mongodb.serialization.CollectibleSerializer;
import org.mongodb.serialization.PrimitiveSerializers;

public class MongoCollectionBaseImpl<T> implements MongoCollectionBase<T> {
    protected final String name;
    protected final MongoDatabase database;
    protected final PrimitiveSerializers primitiveSerializers;
    protected final CollectibleSerializer<T> serializer;
    protected final WriteConcern writeConcern;
    protected final ReadPreference readPreference;

    public MongoCollectionBaseImpl(final CollectibleSerializer<T> serializer, final String name, final MongoDatabaseImpl database,
                                   final WriteConcern writeConcern, final ReadPreference readPreference,
                                   final PrimitiveSerializers primitiveSerializers) {
        this.serializer = serializer;
        this.name = name;
        this.database = database;
        this.writeConcern = writeConcern;
        this.primitiveSerializers = primitiveSerializers;
        this.readPreference = readPreference;
    }

    public MongoCollectionBaseImpl(final MongoCollection<T> from) {
        this.serializer = from.getSerializer();
        this.name = from.getName();
        this.database = from.getDatabase();
        this.writeConcern = from.getWriteConcern();
        this.readPreference = from.getReadPreference();
        this.primitiveSerializers = from.getPrimitiveSerializers();
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
    public PrimitiveSerializers getPrimitiveSerializers() {
        if (primitiveSerializers != null) {
            return primitiveSerializers;
        }
        return getDatabase().getPrimitiveSerializers();
    }

    @Override
    public CollectibleSerializer<T> getSerializer() {
        return serializer;
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
    public ReadPreference getReadPreference() {
        if (readPreference != null) {
            return readPreference;
        }
        return getDatabase().getReadPreference();
    }

    @Override
    public MongoNamespace getNamespace() {
        return new MongoNamespace(getDatabase().getName(), getName());
    }
}
