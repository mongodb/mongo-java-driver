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

import org.bson.types.Document;
import org.mongodb.Block;
import org.mongodb.CollectionAdmin;
import org.mongodb.Function;
import org.mongodb.Get;
import org.mongodb.MongoCollection;
import org.mongodb.MongoCollectionOptions;
import org.mongodb.MongoCursor;
import org.mongodb.MongoIterable;
import org.mongodb.MongoOperations;
import org.mongodb.MongoStream;
import org.mongodb.QueryFilterDocument;
import org.mongodb.ReadPreference;
import org.mongodb.WriteConcern;
import org.mongodb.async.SingleResultCallback;
import org.mongodb.command.Count;
import org.mongodb.command.CountCommandResult;
import org.mongodb.command.Distinct;
import org.mongodb.command.DistinctCommandResult;
import org.mongodb.command.FindAndModifyCommandResult;
import org.mongodb.command.FindAndModifyCommandResultSerializer;
import org.mongodb.command.FindAndRemove;
import org.mongodb.command.FindAndReplace;
import org.mongodb.command.FindAndUpdate;
import org.mongodb.operation.MongoFieldSelector;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoFindAndRemove;
import org.mongodb.operation.MongoFindAndReplace;
import org.mongodb.operation.MongoFindAndUpdate;
import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoQueryFilter;
import org.mongodb.operation.MongoRemove;
import org.mongodb.operation.MongoReplace;
import org.mongodb.operation.MongoSortCriteria;
import org.mongodb.operation.MongoUpdate;
import org.mongodb.operation.MongoUpdateOperations;
import org.mongodb.result.QueryResult;
import org.mongodb.result.WriteResult;
import org.mongodb.serialization.CollectibleSerializer;
import org.mongodb.serialization.Serializer;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;

class MongoCollectionImpl<T> extends MongoCollectionBaseImpl<T> implements MongoCollection<T> {

    private final CollectionAdmin admin;

    public MongoCollectionImpl(final String name, final MongoDatabaseImpl database,
                               final CollectibleSerializer<T> serializer, final MongoCollectionOptions options) {
        super(serializer, name, database, options);
        admin = new CollectionAdminImpl(database.getClient().getOperations(), options.getPrimitiveSerializers(),
                                       getNamespace(), getDatabase());
    }

    @Override
    public MongoStream<T> batchSize(final int batchSize) {
        return new MongoCollectionStream().batchSize(batchSize);
    }

    @Override
    public MongoStream<T> readPreference(final ReadPreference readPreference) {
        return new MongoCollectionStream().readPreference(readPreference);
    }

    @Override
    public MongoCursor<T> iterator() {
        return all();
    }

    @Override
    public MongoCursor<T> all() {
        return new MongoCollectionStream().all();
    }

    @Override
    public T one() {
        return new MongoCollectionStream().one();
    }

    @Override
    public long count() {
        return new MongoCollectionStream().count();
    }

    @Override
    public List<String> distinct(final String field) {
        return new MongoCollectionStream().distinct(field);
    }

    @Override
    public void forEach(final Block<? super T> block) {
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
    public WriteResult insert(final T document) {
        return new MongoCollectionStream().insert(document);
    }

    @Override
    public WriteResult insert(final Iterable<T> documents) {
        return new MongoCollectionStream().insert(documents);
    }

    @Override
    public WriteResult save(final T document) {
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
    public MongoStream<T> writeConcern(final WriteConcern writeConcern) {
        return new MongoCollectionStream().writeConcern(writeConcern);
    }

    @Override
    public WriteResult remove() {
        return new MongoCollectionStream().remove();
    }

    @Override
    public WriteResult modify(final MongoUpdateOperations updateOperations) {
        return new MongoCollectionStream().modify(updateOperations);
    }

    @Override
    public WriteResult modifyOrInsert(final MongoUpdateOperations updateOperations) {
        return new MongoCollectionStream().modifyOrInsert(updateOperations);
    }

    @Override
    public WriteResult replace(final T replacement) {
        return new MongoCollectionStream().replace(replacement);
    }

    @Override
    public WriteResult replaceOrInsert(final T replacement) {
        return new MongoCollectionStream().replaceOrInsert(replacement);
    }

    @Override
    public T modifyAndGet(final MongoUpdateOperations updateOperations, final Get beforeOrAfter) {
        return new MongoCollectionStream().modifyAndGet(updateOperations, beforeOrAfter);
    }

    @Override
    public T modifyOrInsertAndGet(final MongoUpdateOperations updateOperations, final Get beforeOrAfter) {
        return new MongoCollectionStream().modifyOrInsertAndGet(updateOperations, beforeOrAfter);
    }

    @Override
    public T replaceAndGet(final T replacement, final Get beforeOrAfter) {
        return new MongoCollectionStream().replaceAndGet(replacement, beforeOrAfter);
    }

    @Override
    public T replaceOrInsertAndGet(final T replacement, final Get beforeOrAfter) {
        return new MongoCollectionStream().replaceOrInsertAndGet(replacement, beforeOrAfter);
    }

    @Override
    public T removeAndGet() {
        return new MongoCollectionStream().removeAndGet();
    }

    @Override
    public Future<WriteResult> asyncReplaceOrInsert(final T replacement) {
        return new MongoCollectionStream().asyncReplaceOrInsert(replacement);
    }

    @Override
    public void asyncReplaceOrInsert(final T replacement, final SingleResultCallback<WriteResult> callback) {
         new MongoCollectionStream().asyncReplaceOrInsert(replacement, callback);
    }

    @Override
    public CollectionAdmin tools() {
        return admin;
    }

    private Serializer<Document> getDocumentSerializer() {
        return getOptions().getDocumentSerializer();
    }

    private final class MongoCollectionStream implements MongoStream<T> {
        private final MongoFind findOp;
        private WriteConcern writeConcern;
        private boolean limitSet;

        private MongoCollectionStream() {
            findOp = new MongoFind();
            findOp.readPreference(getOptions().getReadPreference());
            writeConcern = getOptions().getWriteConcern();
        }

        private MongoCollectionStream(final MongoCollectionStream from) {
            findOp = new MongoFind(from.findOp);
            writeConcern = from.writeConcern;
            limitSet = from.limitSet;
        }

        @Override
        public MongoCursor<T> iterator() {
            return all();
        }

        @Override
        public MongoStream<T> filter(final MongoQueryFilter filter) {
            final MongoCollectionStream newStream = new MongoCollectionStream(this);
            newStream.findOp.filter(filter);
            return newStream;
        }

        @Override
        public MongoStream<T> sort(final MongoSortCriteria sortCriteria) {
            final MongoCollectionStream newStream = new MongoCollectionStream(this);
            newStream.findOp.order(sortCriteria);
            return newStream;
        }

        @Override
        public MongoStream<T> select(final MongoFieldSelector selector) {
            final MongoCollectionStream newStream = new MongoCollectionStream(this);
            newStream.findOp.select(selector);
            return newStream;
        }

        @Override
        public MongoStream<T> skip(final int skip) {
            final MongoCollectionStream newStream = new MongoCollectionStream(this);
            newStream.findOp.skip(skip);
            return newStream;
        }

        @Override
        public MongoStream<T> limit(final int limit) {
            final MongoCollectionStream newStream = new MongoCollectionStream(this);
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
            final MongoCollectionStream newStream = new MongoCollectionStream(this);
            newStream.findOp.batchSize(batchSize);
            return newStream;
        }

        @Override
        public MongoStream<T> readPreference(final ReadPreference readPreference) {
            final MongoCollectionStream newStream = new MongoCollectionStream(this);
            newStream.findOp.readPreference(readPreference);
            return newStream;
        }

        @Override
        public MongoCursor<T> all() {
            return new MongoCollectionCursor<T>(MongoCollectionImpl.this, findOp);
        }

        @Override
        public T one() {
            final QueryResult<T> res = getClient().getOperations().query(getNamespace(), findOp.batchSize(-1),
                                                                        getDocumentSerializer(),
                                                                        getSerializer());
            if (res.getResults().isEmpty()) {
                return null;
            }

            return res.getResults().get(0);
        }

        @Override
        public long count() {
            return new CountCommandResult(getDatabase().executeCommand(new Count(findOp, getName()))).getCount();
        }

        @Override
        public List<String> distinct(final String field) {
            final Distinct distinctOperation = new Distinct(getName(), field, findOp);
            return new DistinctCommandResult(getDatabase().executeCommand(distinctOperation)).getValue();
        }

        @Override
        public void forEach(final Block<? super T> block) {
            final MongoCursor<T> cursor = all();
            try {
                while (cursor.hasNext()) {
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
        public MongoStream<T> writeConcern(final WriteConcern writeConcernForThisOperation) {
            final MongoCollectionStream newStream = new MongoCollectionStream(this);
            newStream.writeConcern = writeConcernForThisOperation;
            return newStream;
        }

        @Override
        public WriteResult insert(final T document) {
            return getClient().getOperations().insert(getNamespace(),
                                                     new MongoInsert<T>(document).writeConcern(writeConcern),
                                                     getSerializer(), getDocumentSerializer());
        }

        @Override
        public WriteResult insert(final Iterable<T> documents) {
            return getClient().getOperations().insert(getNamespace(),
                                                     new MongoInsert<T>(documents).writeConcern(writeConcern),
                                                     getSerializer(), getDocumentSerializer());
        }

        @Override
        public WriteResult save(final T document) {
            final Object id = getSerializer().getId(document);
            if (id == null) {
                return insert(document);
            }
            else {
                return filter(new QueryFilterDocument("_id", id)).replaceOrInsert(document);
            }
        }

        @Override
        public WriteResult remove() {
            final MongoRemove remove = new MongoRemove(findOp.getFilter()).multi(getMultiFromLimit())
                                                                          .writeConcern(writeConcern);
            return getClient().getOperations().remove(getNamespace(), remove, getDocumentSerializer());
        }

        @Override
        public WriteResult modify(final MongoUpdateOperations updateOperations) {
            final MongoUpdate update = new MongoUpdate(findOp.getFilter(), updateOperations).multi(getMultiFromLimit())
                                                                                            .writeConcern(writeConcern);
            return getClient().getOperations().update(getNamespace(), update, getDocumentSerializer());
        }

        @Override
        public WriteResult modifyOrInsert(final MongoUpdateOperations updateOperations) {
            final MongoUpdate update = new MongoUpdate(findOp.getFilter(), updateOperations).upsert(true)
                                                                                            .multi(getMultiFromLimit())
                                                                                            .writeConcern(writeConcern);
            return getClient().getOperations().update(getNamespace(), update, getDocumentSerializer());
        }

        @Override
        public WriteResult replace(final T replacement) {
            final MongoReplace<T> replace = new MongoReplace<T>(findOp.getFilter(), replacement)
                                            .writeConcern(writeConcern);
            return getClient().getOperations().replace(getNamespace(), replace, getDocumentSerializer(),
                                                      getSerializer());
        }

        @Override
        public WriteResult replaceOrInsert(final T replacement) {
            final MongoReplace<T> replace = new MongoReplace<T>(findOp.getFilter(), replacement)
                                            .upsert(true)
                                            .writeConcern(writeConcern);
            return getClient().getOperations().replace(getNamespace(), replace, getDocumentSerializer(),
                                                      getSerializer());
        }

        @Override
        //CHECKSTYLE:OFF
        //TODO: absolute disaster area
        public T modifyAndGet(final MongoUpdateOperations updateOperations, final Get beforeOrAfter) {
            final MongoFindAndUpdate<T> findAndUpdate;
            findAndUpdate = new MongoFindAndUpdate<T>().where(findOp.getFilter())
                                                    .updateWith(updateOperations)
                                                    .returnNew(asBoolean(beforeOrAfter))
                                                    .select(findOp.getFields())
                                                    .sortBy(findOp.getOrder());
            final MongoOperations operations = getClient().getOperations();

            //TODO: Still need to tidy up some of this command stuff, especially around return values
            final FindAndUpdate<T> findAndUpdateCommand = new FindAndUpdate<T>(MongoCollectionImpl.this, findAndUpdate);
            final FindAndModifyCommandResultSerializer<T> serializer = new
                                                                 FindAndModifyCommandResultSerializer<T>(
                                                                                                        getOptions()
                                                                                                        .getPrimitiveSerializers(),
                                                                                                        getSerializer());
            return new FindAndModifyCommandResult<T>(operations.executeCommand(getDatabase().getName(),
                                                                              findAndUpdateCommand,
                                                                              serializer)).getValue();
        }

        @Override
        public T modifyOrInsertAndGet(final MongoUpdateOperations updateOperations, final Get beforeOrAfter) {
            final MongoFindAndUpdate<T> findAndUpdate = new MongoFindAndUpdate<T>().where(findOp.getFilter()).updateWith(

                                                                                                                        updateOperations)
                                                                             .upsert(true)
                                                                             .returnNew(asBoolean(beforeOrAfter))
                                                                             .select(findOp.getFields()).sortBy(
                                                                                                               findOp
                                                                                                               .getOrder());
            return new FindAndModifyCommandResult<T>(getClient().getOperations().executeCommand(getDatabase().getName(),
                                                                                               new FindAndUpdate<T>(
                                                                                                                MongoCollectionImpl.this,
                                                                                                                findAndUpdate),
                                                                                               new FindAndModifyCommandResultSerializer<T>(
                                                                                                                                          getOptions()
                                                                                                                                          .getPrimitiveSerializers(),
                                                                                                                                          getSerializer())))
                   .getValue();
        }

        @Override
        public T replaceAndGet(final T replacement, final Get beforeOrAfter) {
            final MongoFindAndReplace<T> findAndReplace = new MongoFindAndReplace<T>(replacement).where(
                                                                                                       findOp
                                                                                                       .getFilter())
                                                                                                 .returnNew(asBoolean(
                                                                                                                     beforeOrAfter))
                                                                                                 .select(findOp
                                                                                                         .getFields())
                                                                                                 .sortBy(
                                                                                                        findOp
                                                                                                        .getOrder());
            return new FindAndModifyCommandResult<T>(getClient().getOperations().executeCommand(getDatabase().getName(),
                                                                                               new FindAndReplace<T>(
                                                                                                                 MongoCollectionImpl.this,
                                                                                                                 findAndReplace),
                                                                                               new FindAndModifyCommandResultSerializer<T>(
                                                                                                                                          getOptions()
                                                                                                                                          .getPrimitiveSerializers(),
                                                                                                                                          getSerializer())))
                   .getValue();
        }

        @Override
        public T replaceOrInsertAndGet(final T replacement, final Get beforeOrAfter) {
            final MongoFindAndReplace<T> findAndReplace = new MongoFindAndReplace<T>(replacement).where(
                                                                                                       findOp
                                                                                                       .getFilter())
                                                                                                 .returnNew(asBoolean(
                                                                                                                     beforeOrAfter))
                                                                                                 .upsert(true).select(
                                                                                                                     findOp
                                                                                                                     .getFields())
                                                                                                 .sortBy(
                                                                                                        findOp
                                                                                                        .getOrder());
            return new FindAndModifyCommandResult<T>(getClient().getOperations().executeCommand(getDatabase().getName(),
                                                                                               new FindAndReplace<T>(
                                                                                                                 MongoCollectionImpl.this,
                                                                                                                 findAndReplace),
                                                                                               new FindAndModifyCommandResultSerializer<T>(
                                                                                                                                          getOptions()
                                                                                                                                          .getPrimitiveSerializers(),
                                                                                                                                          getSerializer())))
                   .getValue();
        }

        @Override
        public T removeAndGet() {
            final MongoFindAndRemove<T> findAndRemove = new MongoFindAndRemove<T>().where(findOp.getFilter())
                                                                                   .select(findOp.getFields())
                                                                                   .sortBy(findOp.getOrder());

            final MongoOperations operations = getClient().getOperations();
            final FindAndModifyCommandResultSerializer<T> serializer
                = new FindAndModifyCommandResultSerializer<T>(getOptions().getPrimitiveSerializers(),
                                                             getSerializer());
            return new FindAndModifyCommandResult<T>(operations.executeCommand(getDatabase().getName(),
                                                                              new FindAndRemove<T>(
                                                                                               MongoCollectionImpl.this,
                                                                                               findAndRemove),
                                                                              serializer))
                   .getValue();
        }
        //CHECKSTYLE:OFF

        @Override
        public Future<WriteResult> asyncReplaceOrInsert(final T replacement) {
            final MongoReplace<T> replace = new MongoReplace<T>(findOp.getFilter(), replacement).upsert(true).writeConcern(writeConcern);
            return getClient().getAsyncOperations().asyncReplace(getNamespace(), replace, getDocumentSerializer(), getSerializer());
        }

        @Override
        public void asyncReplaceOrInsert(final T replacement, final SingleResultCallback<WriteResult> callback) {
            final MongoReplace<T> replace = new MongoReplace<T>(findOp.getFilter(), replacement).upsert(true).writeConcern(writeConcern);
            getClient().getAsyncOperations().asyncReplace(getNamespace(), replace, getDocumentSerializer(), getSerializer(), callback);
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

        boolean asBoolean(final Get get) {
            return get == Get.BeforeChangeApplied;
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
