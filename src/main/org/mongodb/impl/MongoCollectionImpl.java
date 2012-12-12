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
import org.mongodb.MongoCursor;
import org.mongodb.MongoDocument;
import org.mongodb.MongoNamespace;
import org.mongodb.ReadPreference;
import org.mongodb.WriteConcern;
import org.mongodb.command.CountCommand;
import org.mongodb.command.FindAndRemoveCommand;
import org.mongodb.command.FindAndReplaceCommand;
import org.mongodb.command.FindAndUpdateCommand;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoFindAndRemove;
import org.mongodb.operation.MongoFindAndReplace;
import org.mongodb.operation.MongoFindAndUpdate;
import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoRemove;
import org.mongodb.result.InsertResult;
import org.mongodb.result.RemoveResult;
import org.mongodb.serialization.Serializer;
import org.mongodb.serialization.Serializers;
import org.mongodb.serialization.serializers.MongoDocumentSerializer;

class MongoCollectionImpl<T> implements MongoCollection<T> {
    private final String name;
    private final MongoDatabaseImpl database;
    private WriteConcern writeConcern;
    private ReadPreference readPreference;
    private final Serializers baseSerializers;
    private final Serializer<T> serializer;

    public MongoCollectionImpl(final String name, final MongoDatabaseImpl database,
                               final Serializers baseSerializers, final Serializer<T> serializer,
                               final WriteConcern writeConcern, final ReadPreference readPreference) {
        this.name = name;
        this.database = database;
        this.baseSerializers = baseSerializers;
        this.serializer = serializer;
        this.writeConcern = writeConcern;
        this.readPreference = readPreference;
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
    public MongoCursor<T> find(MongoFind find) {
        return new MongoCursor<T>(this, find.readPreferenceIfAbsent(readPreference));
    }

    @Override
    public T findOne(final MongoFind find) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long count() {
        return new CountCommand(getClient(), getNamespace()).execute().getCount();
    }

    @Override
    public long count(final MongoFind find) {
        return new CountCommand(getClient(), getNamespace(), find).execute().getCount();
    }

    @Override
    public T findAndUpdate(final MongoFindAndUpdate findAndUpdate) {
        return new FindAndUpdateCommand<T>(getClient(), getNamespace(), findAndUpdate, getBaseSerializers(), getSerializer()).execute().getValue();
    }

    @Override
    public T findAndReplace(final MongoFindAndReplace<T> findAndReplace) {
        return new FindAndReplaceCommand<T>(getClient(), getNamespace(), findAndReplace, getBaseSerializers(), getSerializer()).execute().getValue();
    }

    @Override
    public T findAndRemove(final MongoFindAndRemove findAndRemove) {
        return new FindAndRemoveCommand<T>(getClient(), getNamespace(), findAndRemove, getBaseSerializers(), getSerializer()).execute().getValue();
    }

    @Override
    public InsertResult insert(final MongoInsert<T> insert) {
        return getClient().getOperations().insert(getNamespace(), insert.writeConcernIfAbsent(getWriteConcern()), getSerializer());
    }

    @Override
    public RemoveResult remove(final MongoRemove remove) {
        // TODO: need a serializer to pass in here
        return getClient().getOperations().remove(getNamespace(), remove.writeConcernIfAbsent(getWriteConcern()),
                getMongoDocumentSerializer());
    }

    @Override
    public Serializers getBaseSerializers() {
        if (baseSerializers != null) {
            return baseSerializers;
        }
        return getDatabase().getSerializers();
    }

    @Override
    public Serializer<T> getSerializer() {
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

    private Serializer<MongoDocument> getMongoDocumentSerializer() {
        return new MongoDocumentSerializer(baseSerializers);
    }
}
