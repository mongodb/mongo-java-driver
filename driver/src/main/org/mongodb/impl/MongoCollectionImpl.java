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

import org.bson.types.Document;
import org.mongodb.CollectionAdmin;
import org.mongodb.MongoCollection;
import org.mongodb.MongoCursor;
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
import org.mongodb.operation.MongoReplace;
import org.mongodb.operation.MongoUpdate;
import org.mongodb.result.InsertResult;
import org.mongodb.result.QueryResult;
import org.mongodb.result.RemoveResult;
import org.mongodb.result.UpdateResult;
import org.mongodb.serialization.PrimitiveSerializers;
import org.mongodb.serialization.Serializer;
import org.mongodb.serialization.serializers.DocumentSerializer;

class MongoCollectionImpl<T> extends MongoCollectionBaseImpl<T> implements MongoCollection<T> {

    private CollectionAdmin admin;

    public MongoCollectionImpl(final String name, final MongoDatabaseImpl database,
                               final PrimitiveSerializers primitiveSerializers, final Serializer<T> serializer,
                               final WriteConcern writeConcern, final ReadPreference readPreference) {
        super(serializer, name, database, writeConcern, readPreference, primitiveSerializers);
        admin = new CollectionAdminImpl(database.getClient().getOperations(), primitiveSerializers, database.getName(),
                                        name);
    }

    @Override
    public MongoCursor<T> find(MongoFind find) {
        return new MongoCursor<T>(this, find.readPreferenceIfAbsent(readPreference));
    }

    @Override
    public T findOne(final MongoFind find) {
        QueryResult<T> res = getClient().getOperations().query(getNamespace(), find.batchSize(-1),
                                                               new DocumentSerializer(getPrimitiveSerializers()),
                                                               getSerializer());
        if (res.getResults().isEmpty()) {
            return null;
        }

        return res.getResults().get(0);
    }

    @Override
    public long count() {
        return new CountCommand(this, new MongoFind()).execute().getCount();
    }

    @Override
    public long count(final MongoFind find) {
        return new CountCommand(this, find).execute().getCount();
    }

    @Override
    public T findAndUpdate(final MongoFindAndUpdate findAndUpdate) {
        return new FindAndUpdateCommand<T>(this, findAndUpdate, getPrimitiveSerializers(),
                                           getSerializer()).execute().getValue();
    }

    @Override
    public T findAndReplace(final MongoFindAndReplace<T> findAndReplace) {
        return new FindAndReplaceCommand<T>(this, findAndReplace, getPrimitiveSerializers(),
                                            getSerializer()).execute().getValue();
    }

    @Override
    public T findAndRemove(final MongoFindAndRemove findAndRemove) {
        return new FindAndRemoveCommand<T>(this, findAndRemove, getPrimitiveSerializers(),
                                           getSerializer()).execute().getValue();
    }

    @Override
    public InsertResult insert(final MongoInsert<T> insert) {
        return getClient().getOperations().insert(getNamespace(), insert.writeConcernIfAbsent(getWriteConcern()),
                                                  getSerializer());
    }

    @Override
    public UpdateResult update(final MongoUpdate update) {
        return getClient().getOperations().update(getNamespace(), update.writeConcernIfAbsent(getWriteConcern()),
                                                  getDocumentSerializer());
    }

    @Override
    public UpdateResult replace(final MongoReplace<T> replace) {
        return getClient().getOperations().replace(getNamespace(), replace.writeConcernIfAbsent(getWriteConcern()),
                                                   getDocumentSerializer(), getSerializer());
    }

    @Override
    public RemoveResult remove(final MongoRemove remove) {
        // TODO: need a serializer to pass in here
        return getClient().getOperations().remove(getNamespace(), remove.writeConcernIfAbsent(getWriteConcern()),
                                                  getDocumentSerializer());
    }

    @Override
    public CollectionAdmin admin() {
        return admin;
    }

    private Serializer<Document> getDocumentSerializer() {
        return new DocumentSerializer(primitiveSerializers);
    }
}
