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
import org.mongodb.MongoCollectionOptions;
import org.mongodb.MongoCursor;
import org.mongodb.QueryFilterDocument;
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
import org.mongodb.operation.MongoSave;
import org.mongodb.operation.MongoUpdate;
import org.mongodb.result.InsertResult;
import org.mongodb.result.QueryResult;
import org.mongodb.result.RemoveResult;
import org.mongodb.result.UpdateResult;
import org.mongodb.serialization.CollectibleSerializer;
import org.mongodb.serialization.Serializer;
import org.mongodb.serialization.serializers.DocumentSerializer;

class MongoCollectionImpl<T> extends MongoCollectionBaseImpl<T> implements MongoCollection<T> {

    private final CollectionAdmin admin;
    private final DocumentSerializer documentSerializer;

    public MongoCollectionImpl(final String name, final MongoDatabaseImpl database,
                               final CollectibleSerializer<T> serializer, final MongoCollectionOptions options) {
        super(serializer, name, database, options);
        admin = new CollectionAdminImpl(database.getClient().getOperations(), options.getPrimitiveSerializers(),
                                        database.getName(), name);
        documentSerializer = new DocumentSerializer(options.getPrimitiveSerializers());
    }

    @Override
    public MongoCursor<T> find(final MongoFind find) {
        return new MongoCursor<T>(this, find.readPreferenceIfAbsent(options.getReadPreference()));
    }

    @Override
    public T findOne(final MongoFind find) {
        QueryResult<T> res = getClient().getOperations().query(getNamespace(), find.batchSize(-1),
                                                               documentSerializer, getSerializer());
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
        return new FindAndUpdateCommand<T>(this, findAndUpdate, options.getPrimitiveSerializers(),
                                           getSerializer()).execute().getValue();
    }

    @Override
    public T findAndReplace(final MongoFindAndReplace<T> findAndReplace) {
        return new FindAndReplaceCommand<T>(this, findAndReplace, options.getPrimitiveSerializers(),
                                            getSerializer()).execute().getValue();
    }

    @Override
    public T findAndRemove(final MongoFindAndRemove findAndRemove) {
        return new FindAndRemoveCommand<T>(this, findAndRemove, options.getPrimitiveSerializers(),
                                           getSerializer()).execute().getValue();
    }

    @Override
    public InsertResult insert(final MongoInsert<T> insert) {
        return getClient().getOperations().insert(getNamespace(),
                                                  insert.writeConcernIfAbsent(options.getWriteConcern()),
                                                  getSerializer());
    }

    @Override
    public UpdateResult update(final MongoUpdate update) {
        return getClient().getOperations().update(getNamespace(),
                                                  update.writeConcernIfAbsent(options.getWriteConcern()),
                                                  getDocumentSerializer());
    }

    @Override
    @SuppressWarnings("unchecked")
    public UpdateResult save(final MongoSave<T> save) {
        Object id = serializer.getId(save.getDocument());
        if (id == null) {
            return insert(new MongoInsert<T>(save.getDocument()).writeConcern(save.getWriteConcern()));
        }
        else {
            return replace(new MongoReplace<T>(new QueryFilterDocument("_id", id),
                                               save.getDocument()).isUpsert(true).writeConcern(save.getWriteConcern()));
        }
    }

    @Override
    public UpdateResult replace(final MongoReplace<T> replace) {
        return getClient().getOperations().replace(getNamespace(),
                                                   replace.writeConcernIfAbsent(options.getWriteConcern()),
                                                   getDocumentSerializer(), getSerializer());
    }

    @Override
    public RemoveResult remove(final MongoRemove remove) {
        return getClient().getOperations().remove(getNamespace(),
                                                  remove.writeConcernIfAbsent(options.getWriteConcern()),
                                                  getDocumentSerializer());
    }

    @Override
    public CollectionAdmin admin() {
        return admin;
    }

    private Serializer<Document> getDocumentSerializer() {
        return options.getDocumentSerializer();
    }
}
