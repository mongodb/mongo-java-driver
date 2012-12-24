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
import org.mongodb.Block;
import org.mongodb.CollectionAdmin;
import org.mongodb.MongoCollection;
import org.mongodb.MongoCursor;
import org.mongodb.MongoReadableStream;
import org.mongodb.MongoStream;
import org.mongodb.MongoWritableStream;
import org.mongodb.QueryFilterDocument;
import org.mongodb.ReadPreference;
import org.mongodb.WriteConcern;
import org.mongodb.command.CountCommand;
import org.mongodb.command.FindAndRemoveCommand;
import org.mongodb.command.FindAndReplaceCommand;
import org.mongodb.command.FindAndUpdateCommand;
import org.mongodb.operation.MongoFieldSelector;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoFindAndRemove;
import org.mongodb.operation.MongoFindAndReplace;
import org.mongodb.operation.MongoFindAndUpdate;
import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoQueryFilter;
import org.mongodb.operation.MongoRemove;
import org.mongodb.operation.MongoReplace;
import org.mongodb.operation.MongoSave;
import org.mongodb.operation.MongoSortCriteria;
import org.mongodb.operation.MongoUpdate;
import org.mongodb.operation.MongoUpdateOperations;
import org.mongodb.result.InsertResult;
import org.mongodb.result.QueryResult;
import org.mongodb.result.RemoveResult;
import org.mongodb.result.UpdateResult;
import org.mongodb.serialization.CollectibleSerializer;
import org.mongodb.serialization.PrimitiveSerializers;
import org.mongodb.serialization.Serializer;
import org.mongodb.serialization.serializers.DocumentSerializer;

import java.util.Collection;

class MongoCollectionImpl<T> extends MongoCollectionBaseImpl<T> implements MongoCollection<T> {

    private CollectionAdmin admin;

    public MongoCollectionImpl(final String name, final MongoDatabaseImpl database,
                               final PrimitiveSerializers primitiveSerializers,
                               final CollectibleSerializer<T> serializer, final WriteConcern writeConcern,
                               final ReadPreference readPreference) {
        super(serializer, name, database, writeConcern, readPreference, primitiveSerializers);
        admin = new CollectionAdminImpl(database.getClient().getOperations(), primitiveSerializers, database.getName(),
                                        name);
    }

    public MongoCursor<T> find(MongoFind find) {
        return new MongoCursor<T>(this, find.readPreferenceIfAbsent(readPreference));
    }

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
    public MongoReadableStream<T> batchSize(final int batchSize) {
        return new MongoStreamImpl().batchSize(batchSize);
    }

    @Override
    public MongoReadableStream<T> readPreference(final ReadPreference readPreference) {
        return new MongoStreamImpl().readPreference(readPreference);
    }

    @Override
    public MongoCursor<T> iterator() {
        return find();
    }

    @Override
    public MongoCursor<T> find() {
        return new MongoStreamImpl().find();
    }

    @Override
    public T findOne() {
        return new MongoStreamImpl().findOne();
    }

    @Override
    public long count() {
        return new CountCommand(this, new MongoFind()).execute().getCount();
    }

    @Override
    public void forEach(Block<? super T> block) {
        new MongoStreamImpl().forEach(block);
    }

    @Override
    public <A extends Collection<? super T>> A into(final A target) {
        return new MongoStreamImpl().into(target);
    }

    public long count(final MongoFind find) {
        return new CountCommand(this, find).execute().getCount();
    }

    public T findAndUpdate(final MongoFindAndUpdate findAndUpdate) {
        return new FindAndUpdateCommand<T>(this, findAndUpdate, getPrimitiveSerializers(),
                                           getSerializer()).execute().getValue();
    }

    public T findAndReplace(final MongoFindAndReplace<T> findAndReplace) {
        return new FindAndReplaceCommand<T>(this, findAndReplace, getPrimitiveSerializers(),
                                            getSerializer()).execute().getValue();
    }

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
    public InsertResult insert(final T doc) {
        return insert(new MongoInsert<T>(doc));
    }

    @Override
    public InsertResult insert(final Iterable<T> doc) {
        return insert(new MongoInsert<T>(doc));
    }

    @Override
    public UpdateResult save(final T doc) {
        return save(new MongoSave<T>(doc));
    }

    public UpdateResult update(final MongoUpdate update) {
        return getClient().getOperations().update(getNamespace(), update.writeConcernIfAbsent(getWriteConcern()),
                                                  getDocumentSerializer());
    }

    @Override
    public UpdateResult save(final MongoSave<T> save) {
        Object id = serializer.getId(save.getDocument());
        if (id == null) {
            return insert(save.getDocument());
        }
        else {
            return replace(new MongoReplace<T>(new QueryFilterDocument("_id", id), save.getDocument()).upsert(
                    true).writeConcernIfAbsent(getWriteConcern()));
        }
    }

    public UpdateResult replace(final MongoReplace<T> replace) {
        return getClient().getOperations().replace(getNamespace(), replace.writeConcernIfAbsent(getWriteConcern()),
                                                   getDocumentSerializer(), getSerializer());
    }

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

    @Override
    public MongoStream<T> filter(final MongoQueryFilter filter) {
        return new MongoStreamImpl().filter(filter);
    }

    @Override
    public MongoStream<T> sort(final MongoSortCriteria sortCriteria) {
        return new MongoStreamImpl().sort(sortCriteria);
    }

    @Override
    public MongoStream<T> skip(final int skip) {
        return new MongoStreamImpl().skip(skip);
    }

    @Override
    public MongoStream<T> limit(final int limit) {
        return new MongoStreamImpl().limit(limit);
    }

    @Override
    public MongoStream<T> noLimit() {
        return new MongoStreamImpl().noLimit();
    }

    @Override
    public MongoStream<T> select(final MongoFieldSelector selector) {
        return new MongoStreamImpl().select(selector);
    }

    @Override
    public MongoWritableStream<T> writeConcern(final WriteConcern writeConcern) {
        return new MongoStreamImpl().writeConcern(writeConcern);
    }

    @Override
    public MongoWritableStream<T> upsert() {
        return new MongoStreamImpl().upsert();
    }

    @Override
    public RemoveResult remove() {
        return new MongoStreamImpl().remove();
    }

    @Override
    public UpdateResult update(final MongoUpdateOperations updateOperations) {
        return new MongoStreamImpl().update(updateOperations);
    }

    @Override
    public UpdateResult replace(final T replacement) {
        return new MongoStreamImpl().replace(replacement);
    }

    @Override
    public MongoWritableStream<T> returnNew() {
        return new MongoStreamImpl().returnNew();
    }

    @Override
    public T findAndUpdate(final MongoUpdateOperations updateOperations) {
        return new MongoStreamImpl().findAndUpdate(updateOperations);
    }

    @Override
    public T findAndReplace(final T replacement) {
        return new MongoStreamImpl().findAndReplace(replacement);
    }

    @Override
    public T findAndRemove() {
        return new MongoStreamImpl().findAndRemove();
    }

    private class MongoStreamImpl implements MongoStream<T> {
        private final MongoFind find = new MongoFind();
        private WriteConcern writeConcern = getWriteConcern();
        private boolean upsert;
        private boolean limitSet;
        private boolean returnNew;  // TODO: Only for findAndModify

        @Override
        public MongoCursor<T> iterator() {
            return find();
        }

        @Override
        public MongoStream<T> filter(final MongoQueryFilter filter) {
            find.filter(filter);
            return this;
        }

        @Override
        public MongoStream<T> sort(final MongoSortCriteria sortCriteria) {
            find.order(sortCriteria);
            return this;
        }

        @Override
        public MongoStream<T> select(MongoFieldSelector selector) {
            find.select(selector);
            return this;
        }

        @Override
        public MongoStream<T> skip(final int skip) {
            find.skip(skip);
            return this;
        }

        @Override
        public MongoStream<T> limit(final int limit) {
            find.limit(limit);
            limitSet = true;
            return this;
        }

        @Override
        public MongoStream<T> noLimit() {
            return limit(0);
        }

        @Override
        public MongoStream<T> batchSize(final int batchSize) {
            find.batchSize(batchSize);
            return this;
        }

        @Override
        public MongoStream<T> readPreference(final ReadPreference readPreference) {
            find.readPreference(readPreference);
            return this;
        }

        @Override
        public MongoCursor<T> find() {
            return MongoCollectionImpl.this.find(find);
        }

        @Override
        public T findOne() {
            return MongoCollectionImpl.this.findOne(find);
        }

        @Override
        public long count() {
            return new CountCommand(MongoCollectionImpl.this, find).execute().getCount();
        }

        @Override
        public void forEach(final Block<? super T> block) {
            MongoCursor<T> cursor = find();
            try {
                for (; cursor.hasNext(); ) {
                    block.run(cursor.next());
                }
            } finally {
                cursor.close();
            }
        }

        @Override
        public <A extends Collection<? super T>> A into(final A target) {
            MongoCursor<T> cursor = find();
            try {
                for (; cursor.hasNext(); ) {
                    target.add(cursor.next());
                }
            } finally {
                cursor.close();
            }
            return target;
        }

        @Override
        public MongoStream<T> writeConcern(WriteConcern writeConcern) {
            this.writeConcern = writeConcern;
            return this;
        }

        @Override
        public MongoStream<T> upsert() {
            this.upsert = true;
            return this;
        }

        @Override
        public RemoveResult remove() {
            return MongoCollectionImpl.this.remove(
                    new MongoRemove(find.getFilter()).multi(getMultiFromLimit()).writeConcern(writeConcern));
        }

        @Override
        public UpdateResult update(MongoUpdateOperations updateOperations) {
            MongoUpdate update = new MongoUpdate(find.getFilter(), updateOperations).upsert(upsert).multi(
                    getMultiFromLimit()).writeConcern(writeConcern);
            return getClient().getOperations().update(getNamespace(), update.writeConcernIfAbsent(getWriteConcern()),
                                                      getDocumentSerializer());

        }

        @Override
        public UpdateResult replace(T replacement) {
            return MongoCollectionImpl.this.replace(
                    new MongoReplace<T>(find.getFilter(), replacement).upsert(upsert).writeConcern(writeConcern));
        }

        @Override
        public MongoWritableStream<T> returnNew() {
            this.returnNew = true;
            return this;
        }

        @Override
        public T findAndUpdate(final MongoUpdateOperations updateOperations) {
            return MongoCollectionImpl.this.findAndUpdate(
                    new MongoFindAndUpdate().where(find.getFilter()).updateWith(updateOperations).returnNew(
                            returnNew).select(find.getFields()).upsert(upsert).sortBy(find.getOrder()));
        }

        @Override
        public T findAndReplace(final T replacement) {
            return MongoCollectionImpl.this.findAndReplace(
                    new MongoFindAndReplace<T>(replacement).where(find.getFilter()).returnNew(returnNew).select(
                            find.getFields()).upsert(upsert).sortBy(find.getOrder()));
        }

        @Override
        public T findAndRemove() {
            return MongoCollectionImpl.this.findAndRemove(
                    new MongoFindAndRemove().where(find.getFilter()).select(find.getFields()).sortBy(find.getOrder()));
        }

        private boolean getMultiFromLimit() {
            if (limitSet) {
                if (find.getLimit() == 1) {
                    return false;
                }
                else if (find.getLimit() == 0) {
                    return true;
                }
                else {
                    throw new IllegalArgumentException("Update currently only supports a limit of either none or 1");
                }
            }
            else {
                return false;
            }
        }
    }

}
