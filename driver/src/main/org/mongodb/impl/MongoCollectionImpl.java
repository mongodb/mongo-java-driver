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
import org.mongodb.Function;
import org.mongodb.MongoCollection;
import org.mongodb.MongoCollectionOptions;
import org.mongodb.MongoCursor;
import org.mongodb.MongoIterable;
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
import org.mongodb.serialization.Serializer;

import java.util.Collection;

class MongoCollectionImpl<T> extends MongoCollectionBaseImpl<T> implements MongoCollection<T> {

    private CollectionAdmin admin;

    public MongoCollectionImpl(final String name, final MongoDatabaseImpl database,
                               final CollectibleSerializer<T> serializer,
                               final MongoCollectionOptions options) {
        super(serializer, name, database, options);
        admin = new CollectionAdminImpl(database.getClient().getOperations(), options.getPrimitiveSerializers(), database.getName(),
                                        name);
    }

    @Override
    public MongoReadableStream<T> batchSize(final int batchSize) {
        return new MongoCollectionStream().batchSize(batchSize);
    }

    @Override
    public MongoReadableStream<T> readPreference(final ReadPreference readPreference) {
        return new MongoCollectionStream().readPreference(readPreference);
    }

    @Override
    public MongoCursor<T> iterator() {
        return find();
    }

    @Override
    public MongoCursor<T> find() {
        return new MongoCollectionStream().find();
    }

    @Override
    public T findOne() {
        return new MongoCollectionStream().findOne();
    }

    @Override
    public long count() {
        return new MongoCollectionStream().count();
    }

    @Override
    public void forEach(Block<? super T> block) {
        new MongoCollectionStream().forEach(block);
    }

    @Override
    public <A extends Collection<? super T>> A into(final A target) {
        return new MongoCollectionStream().into(target);
    }

    @Override
    public <U> MongoIterable<U> map(final Function<T, U> mapper) {
        return new MongoCollectionStream().map(mapper);
    }

    @Override
    public InsertResult insert(final T document) {
        return new MongoCollectionStream().insert(document);
    }

    @Override
    public InsertResult insert(final Iterable<T> documents) {
        return new MongoCollectionStream().insert(documents);
    }

    @Override
    public UpdateResult save(final T document) {
        return new MongoCollectionStream().save(document);
    }

    @Override
    public MongoStream<T> filter(final MongoQueryFilter filter) {
        return new MongoCollectionStream().filter(filter);
    }

    @Override
    public MongoStream<T> sort(final MongoSortCriteria sortCriteria) {
        return new MongoCollectionStream().sort(sortCriteria);
    }

    @Override
    public MongoStream<T> skip(final int skip) {
        return new MongoCollectionStream().skip(skip);
    }

    @Override
    public MongoStream<T> limit(final int limit) {
        return new MongoCollectionStream().limit(limit);
    }

    @Override
    public MongoStream<T> noLimit() {
        return new MongoCollectionStream().noLimit();
    }

    @Override
    public MongoStream<T> select(final MongoFieldSelector selector) {
        return new MongoCollectionStream().select(selector);
    }

    @Override
    public MongoWritableStream<T> writeConcern(final WriteConcern writeConcern) {
        return new MongoCollectionStream().writeConcern(writeConcern);
    }

    @Override
    public MongoWritableStream<T> upsert() {
        return new MongoCollectionStream().upsert();
    }

    @Override
    public RemoveResult remove() {
        return new MongoCollectionStream().remove();
    }

    @Override
    public UpdateResult update(final MongoUpdateOperations updateOperations) {
        return new MongoCollectionStream().update(updateOperations);
    }

    @Override
    public UpdateResult replace(final T replacement) {
        return new MongoCollectionStream().replace(replacement);
    }

    @Override
    public MongoWritableStream<T> returnNew() {
        return new MongoCollectionStream().returnNew();
    }

    @Override
    public T findAndUpdate(final MongoUpdateOperations updateOperations) {
        return new MongoCollectionStream().findAndUpdate(updateOperations);
    }

    @Override
    public T findAndReplace(final T replacement) {
        return new MongoCollectionStream().findAndReplace(replacement);
    }

    @Override
    public T findAndRemove() {
        return new MongoCollectionStream().findAndRemove();
    }

    @Override
    public CollectionAdmin admin() {
        return admin;
    }

    private Serializer<Document> getDocumentSerializer() {
        return options.getDocumentSerializer();
    }

    private class MongoCollectionStream implements MongoStream<T> {
        private MongoFind findOp;
        private WriteConcern writeConcern;
        private boolean upsert;
        private boolean limitSet;
        private boolean returnNew;  // TODO: Only for findAndModify

        private MongoCollectionStream() {
            findOp = new MongoFind();
            findOp.readPreference(getOptions().getReadPreference());
            writeConcern = getOptions().getWriteConcern();
        }

        private MongoCollectionStream(MongoCollectionStream from) {
            findOp = new MongoFind(from.findOp);
            writeConcern = getOptions().getWriteConcern();
            upsert = from.upsert;
            limitSet = from.limitSet;
            returnNew = from.returnNew;
        }

        @Override
        public MongoCursor<T> iterator() {
            return find();
        }

        @Override
        public MongoStream<T> filter(final MongoQueryFilter filter) {
            MongoCollectionStream newStream = new MongoCollectionStream(this);
            newStream.findOp.filter(filter);
            return newStream;
        }

        @Override
        public MongoStream<T> sort(final MongoSortCriteria sortCriteria) {
            MongoCollectionStream newStream = new MongoCollectionStream(this);
            newStream.findOp.order(sortCriteria);
            return newStream;
        }

        @Override
        public MongoStream<T> select(MongoFieldSelector selector) {
            MongoCollectionStream newStream = new MongoCollectionStream(this);
            newStream.findOp.select(selector);
            return newStream;
        }

        @Override
        public MongoStream<T> skip(final int skip) {
            MongoCollectionStream newStream = new MongoCollectionStream(this);
            newStream.findOp.skip(skip);
            return newStream;
        }

        @Override
        public MongoStream<T> limit(final int limit) {
            MongoCollectionStream newStream = new MongoCollectionStream(this);
            newStream.findOp.limit(limit);
            newStream.limitSet = true;
            return newStream;
        }

        @Override
        public MongoStream<T> noLimit() {
            return limit(0);
        }

        @Override
        public MongoStream<T> batchSize(final int batchSize) {
            MongoCollectionStream newStream = new MongoCollectionStream(this);
            newStream.findOp.batchSize(batchSize);
            return newStream;
        }

        @Override
        public MongoStream<T> readPreference(final ReadPreference readPreference) {
            MongoCollectionStream newStream = new MongoCollectionStream(this);
            newStream.findOp.readPreference(readPreference);
            return newStream;
        }

        @Override
        public MongoCursor<T> find() {
            return new MongoCollectionCursor<T>(MongoCollectionImpl.this, findOp);
        }

        @Override
        public T findOne() {
            QueryResult<T> res = getClient().getOperations().query(getNamespace(), findOp.batchSize(-1),
                                                                   getDocumentSerializer(),
                                                                   getSerializer());
            if (res.getResults().isEmpty()) {
                return null;
            }

            return res.getResults().get(0);
        }

        @Override
        public long count() {
            return new CountCommand(MongoCollectionImpl.this, findOp).execute().getCount();
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
            forEach(new Block<T>() {
                @Override
                public void run(final T t) {
                    target.add(t);
                }
            });
            return target;
        }

        @Override
        public <U> MongoIterable<U> map(final Function<T, U> mapper) {
            return new MongoIterableCollection<T, U>(this, mapper);
        }

        @Override
        public MongoStream<T> writeConcern(WriteConcern writeConcern) {
            MongoCollectionStream newStream = new MongoCollectionStream(this);
            newStream.writeConcern = writeConcern;
            return newStream;
        }

        @Override
        public InsertResult insert(final T document) {
            return getClient().getOperations().insert(getNamespace(),
                                                      new MongoInsert<T>(document).writeConcern(writeConcern),
                                                      getSerializer());
        }

        @Override
        public InsertResult insert(final Iterable<T> documents) {
            return getClient().getOperations().insert(getNamespace(),
                                                      new MongoInsert<T>(documents).writeConcern(writeConcern),
                                                      getSerializer());
        }

        @Override
        public UpdateResult save(final T document) {
            Object id = serializer.getId(document);
            if (id == null) {
                return insert(document);
            }
            else {
                return filter(new QueryFilterDocument("_id", id)).upsert().replace(document);
            }
        }

        @Override
        public MongoStream<T> upsert() {
            MongoCollectionStream newStream = new MongoCollectionStream(this);
            newStream.upsert = true;
            return newStream;
        }

        @Override
        public RemoveResult remove() {
            MongoRemove remove = new MongoRemove(findOp.getFilter()).multi(getMultiFromLimit()).writeConcern(
                    writeConcern);
            return getClient().getOperations().remove(getNamespace(), remove, getDocumentSerializer());

        }

        @Override
        public UpdateResult update(MongoUpdateOperations updateOperations) {
            MongoUpdate update = new MongoUpdate(findOp.getFilter(), updateOperations).upsert(upsert).multi(
                    getMultiFromLimit()).writeConcern(writeConcern);
            return getClient().getOperations().update(getNamespace(), update, getDocumentSerializer());

        }

        @Override
        public UpdateResult replace(T replacement) {
            MongoReplace<T> replace = new MongoReplace<T>(findOp.getFilter(), replacement).upsert(upsert).writeConcern(
                    writeConcern);
            return getClient().getOperations().replace(getNamespace(), replace, getDocumentSerializer(),
                                                       getSerializer());

        }

        @Override
        public MongoWritableStream<T> returnNew() {
            MongoCollectionStream newStream = new MongoCollectionStream(this);
            newStream.returnNew = true;
            return newStream;
        }

        @Override
        public T findAndUpdate(final MongoUpdateOperations updateOperations) {
            MongoFindAndUpdate findAndUpdate = new MongoFindAndUpdate().where(findOp.getFilter()).updateWith(
                    updateOperations).returnNew(returnNew).select(findOp.getFields()).upsert(upsert).sortBy(
                    findOp.getOrder());
            return new FindAndUpdateCommand<T>(MongoCollectionImpl.this, findAndUpdate, getOptions().getPrimitiveSerializers(),
                                               getSerializer()).execute().getValue();
        }

        @Override
        public T findAndReplace(final T replacement) {
            MongoFindAndReplace findAndReplace = new MongoFindAndReplace<T>(replacement).where(
                    findOp.getFilter()).returnNew(returnNew).select(findOp.getFields()).upsert(upsert).sortBy(
                    findOp.getOrder());
            return new FindAndReplaceCommand<T>(MongoCollectionImpl.this, findAndReplace, getOptions().getPrimitiveSerializers(),
                                                getSerializer()).execute().getValue();
        }

        @Override
        public T findAndRemove() {
            MongoFindAndRemove findAndRemove = new MongoFindAndRemove().where(findOp.getFilter()).select(
                    findOp.getFields()).sortBy(findOp.getOrder());

            return new FindAndRemoveCommand<T>(MongoCollectionImpl.this, findAndRemove, getOptions().getPrimitiveSerializers(),
                                               getSerializer()).execute().getValue();
        }

        private boolean getMultiFromLimit() {
            if (limitSet) {
                if (findOp.getLimit() == 1) {
                    return false;
                }
                else if (findOp.getLimit() == 0) {
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

    private static class MongoIterableCollection<U, V> implements MongoIterable<V> {

        private final MongoIterable<U> iterable;
        private final Function<U, V> mapper;

        public MongoIterableCollection(final MongoIterable<U> iterable, final Function<U, V> mapper) {
            this.iterable = iterable;
            this.mapper = mapper;
        }

        @Override
        public MongoCursor<V> iterator() {
            return new MongoMappingCursor<U, V>(iterable.iterator(), mapper);
        }

        @Override
        public void forEach(final Block<? super V> block) {
            iterable.forEach(new Block<U>() {
                @Override
                public void run(final U document) {
                    block.run(mapper.apply(document));
                }
            });

        }

        @Override
        public <A extends Collection<? super V>> A into(final A target) {
            forEach(new Block<V>() {
                @Override
                public void run(final V v) {
                    target.add(v);
                }
            });
            return target;
        }

        @Override
        public <W> MongoIterable<W> map(final Function<V, W> mapper) {
            return new MongoIterableCollection<V, W>(this, mapper);
        }
    }
}
