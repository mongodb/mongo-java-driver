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
import org.mongodb.MongoConnection;
import org.mongodb.MongoCursor;
import org.mongodb.MongoException;
import org.mongodb.MongoIterable;
import org.mongodb.MongoStream;
import org.mongodb.QueryFilterDocument;
import org.mongodb.ReadPreference;
import org.mongodb.WriteConcern;
import org.mongodb.async.AsyncBlock;
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
import org.mongodb.operation.GetMore;
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
import org.mongodb.result.CommandResult;
import org.mongodb.result.QueryResult;
import org.mongodb.result.WriteResult;
import org.mongodb.serialization.CollectibleSerializer;
import org.mongodb.serialization.Serializer;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class MongoCollectionImpl<T> extends MongoCollectionBaseImpl<T> implements MongoCollection<T> {

    private final CollectionAdmin admin;
    private final MongoConnection operations;

    public MongoCollectionImpl(final String name, final MongoDatabaseImpl database,
                               final CollectibleSerializer<T> serializer, final MongoCollectionOptions options,
                               final MongoConnection operations) {
        super(serializer, name, database, options);
        this.operations = operations;
        admin = new CollectionAdminImpl(operations, options.getPrimitiveSerializers(),
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
    public Future<T> asyncOne() {
        return new MongoCollectionStream().asyncOne();
    }

    @Override
    public void asyncOne(final SingleResultCallback<T> callback) {
        new MongoCollectionStream().asyncOne(callback);
    }

    @Override
    public Future<Long> asyncCount() {
        return new MongoCollectionStream().asyncCount();
    }

    @Override
    public void asyncCount(final SingleResultCallback<Long> callback) {
        new MongoCollectionStream().asyncCount(callback);
    }

    @Override
    public void asyncForEach(final AsyncBlock<? super T> block) {
        new MongoCollectionStream().asyncForEach(block);
    }

    @Override
    public <A extends Collection<? super T>> Future<A> asyncInto(final A target) {
        return new MongoCollectionStream().asyncInto(target);
    }

    @Override
    public <A extends Collection<? super T>> void asyncInto(final A target, final SingleResultCallback<A> callback) {
        new MongoCollectionStream().asyncInto(target, callback);
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
            return new MongoCollectionCursor<T>(MongoCollectionImpl.this, findOp, operations);
        }

        @Override
        public T one() {
            final QueryResult<T> res = operations.query(getNamespace(), findOp.batchSize(-1),
                    getDocumentSerializer(), getSerializer());
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
                    if (!block.run(cursor.next())) {
                        break;
                    }
                }
            } finally {
                cursor.close();
            }
        }


        @Override
        public <A extends Collection<? super T>> A into(final A target) {
            forEach(new Block<T>() {
                @Override
                public boolean run(final T t) {
                    target.add(t);
                    return true;
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
            return operations.insert(getNamespace(),
                    new MongoInsert<T>(document).writeConcern(writeConcern),
                    getSerializer(), getDocumentSerializer());
        }

        @Override
        public WriteResult insert(final Iterable<T> documents) {
            return operations.insert(getNamespace(),
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
            return operations.remove(getNamespace(), remove, getDocumentSerializer());
        }

        @Override
        public WriteResult modify(final MongoUpdateOperations updateOperations) {
            final MongoUpdate update = new MongoUpdate(findOp.getFilter(), updateOperations).multi(getMultiFromLimit())
                    .writeConcern(writeConcern);
            return operations.update(getNamespace(), update, getDocumentSerializer());
        }

        @Override
        public WriteResult modifyOrInsert(final MongoUpdateOperations updateOperations) {
            final MongoUpdate update = new MongoUpdate(findOp.getFilter(), updateOperations).upsert(true)
                    .multi(getMultiFromLimit())
                    .writeConcern(writeConcern);
            return operations.update(getNamespace(), update, getDocumentSerializer());
        }

        @Override
        public WriteResult replace(final T replacement) {
            final MongoReplace<T> replace = new MongoReplace<T>(findOp.getFilter(), replacement)
                    .writeConcern(writeConcern);
            return operations.replace(getNamespace(), replace, getDocumentSerializer(),
                    getSerializer());
        }

        @Override
        public WriteResult replaceOrInsert(final T replacement) {
            final MongoReplace<T> replace = new MongoReplace<T>(findOp.getFilter(), replacement)
                    .upsert(true)
                    .writeConcern(writeConcern);
            return operations.replace(getNamespace(), replace, getDocumentSerializer(),
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
            return new FindAndModifyCommandResult<T>(operations.executeCommand(getDatabase().getName(),
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
            return new FindAndModifyCommandResult<T>(operations.executeCommand(getDatabase().getName(),
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
            return new FindAndModifyCommandResult<T>(operations.executeCommand(getDatabase().getName(),
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
            return operations.asyncReplace(getNamespace(), replace, getDocumentSerializer(), getSerializer());
        }

        @Override
        public void asyncReplaceOrInsert(final T replacement, final SingleResultCallback<WriteResult> callback) {
            final MongoReplace<T> replace = new MongoReplace<T>(findOp.getFilter(), replacement).upsert(true).writeConcern(writeConcern);
            operations.asyncReplace(getNamespace(), replace, getDocumentSerializer(), getSerializer(), callback);
        }

        boolean asBoolean(final Get get) {
            return get == Get.BeforeChangeApplied;
        }

        @Override
        public Future<T> asyncOne() {
            final Future<QueryResult<T>> queryResultFuture =
                    operations.asyncQuery(getNamespace(), findOp.batchSize(-1), getDocumentSerializer(),
                            getSerializer());
            return new Future<T>() {
                @Override
                public boolean cancel(final boolean mayInterruptIfRunning) {
                    return queryResultFuture.cancel(mayInterruptIfRunning);
                }

                @Override
                public boolean isCancelled() {
                    return queryResultFuture.isCancelled();
                }

                @Override
                public boolean isDone() {
                    return queryResultFuture.isDone();
                }

                @Override
                public T get() throws InterruptedException, ExecutionException {
                    QueryResult<T> queryResult = queryResultFuture.get();
                    if (queryResult.getResults().isEmpty()) {
                        return null;
                    }
                    else {
                        return queryResult.getResults().get(0);
                    }
                }

                @Override
                public T get(final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                    QueryResult<T> queryResult = queryResultFuture.get(timeout, unit);
                    if (queryResult.getResults().isEmpty()) {
                        return null;
                    }
                    else {
                        return queryResult.getResults().get(0);
                    }
                }
            };
        }

        @Override
        public void asyncOne(final SingleResultCallback<T> callback) {
            operations.asyncQuery(getNamespace(), findOp.batchSize(-1), getDocumentSerializer(),
                    getSerializer(), new SingleResultCallback<QueryResult<T>>() {
                @Override
                public void onResult(final QueryResult<T> result, final MongoException e) {
                    if (e != null) {
                        callback.onResult(null, e);
                    }
                    if (result.getResults().isEmpty()) {
                        callback.onResult(null, null);
                    }

                    callback.onResult(result.getResults().get(0), null);
                }
            });
        }

        @Override
        public Future<Long> asyncCount() {
            final Future<CommandResult> commandResultFuture = operations
                    .asyncExecuteCommand(getDatabase().getName(), new Count(findOp, getName()), getDocumentSerializer());
            return new Future<Long>() {
                @Override
                public boolean cancel(final boolean mayInterruptIfRunning) {
                    return commandResultFuture.cancel(mayInterruptIfRunning);
                }

                @Override
                public boolean isCancelled() {
                    return commandResultFuture.isCancelled();
                }

                @Override
                public boolean isDone() {
                    return commandResultFuture.isDone();
                }

                @Override
                public Long get() throws InterruptedException, ExecutionException {
                    CommandResult commandResult = commandResultFuture.get();
                    return new CountCommandResult(commandResult).getCount();
                }

                @Override
                public Long get(final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                    CommandResult commandResult = commandResultFuture.get(timeout, unit);
                    return new CountCommandResult(commandResult).getCount();
                }
            };
        }

        @Override
        public void asyncCount(final SingleResultCallback<Long> callback) {
            operations.asyncExecuteCommand(getDatabase().getName(), new Count(findOp, getName()),
                    getDocumentSerializer(),
                    new SingleResultCallback<CommandResult>() {
                        @Override
                        public void onResult(final CommandResult result, final MongoException e) {
                            if (e != null) {
                                callback.onResult(null, e);
                            }
                            else {
                                callback.onResult(new CountCommandResult(result).getCount(), null);
                            }
                        }
                    });
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

        @Override
        public void asyncForEach(final AsyncBlock<? super T> block) {
            operations.asyncQuery(getNamespace(), findOp, getDocumentSerializer(),
                    getSerializer(), new QueryResultSingleResultCallback(block));
        }

        @Override
        public <A extends Collection<? super T>> Future<A> asyncInto(final A target) {
            final SingleResultFuture<A> future = new SingleResultFuture<A>();

            asyncInto(target, new SingleResultFutureCallback<A>(future));
            return future;
        }

        @Override
        public <A extends Collection<? super T>> void asyncInto(final A target, final SingleResultCallback<A> callback) {
            asyncForEach(new AsyncBlock<T>() {
                @Override
                public void done() {
                    callback.onResult(target, null);
                }

                @Override
                public boolean run(final T t) {
                    target.add(t);
                    return true;
                }
            });
        }

        private class QueryResultSingleResultCallback implements SingleResultCallback<QueryResult<T>> {
            private final AsyncBlock<? super T> block;

            public QueryResultSingleResultCallback(final AsyncBlock<? super T> block) {
                this.block = block;
            }

            @Override
            public void onResult(final QueryResult<T> result, final MongoException e) {
                if (e != null) {
                   // TODO: Error handling.  Call done with an ExecutionException...
                }

                for (T cur : result.getResults()) {
                    if (!block.run(cur)) {
                        break;
                    }
                }
                if (result.getCursor() == null) {
                    block.done();
                }
                else {
                    operations
                            .asyncGetMore(getNamespace(), new GetMore(result.getCursor(), findOp.getBatchSize()),
                                    getSerializer(), new QueryResultSingleResultCallback(block));
                }
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
                public boolean run(final U document) {
                    return block.run(mapper.apply(document));
                }
            });

        }

        @Override
        public <A extends Collection<? super V>> A into(final A target) {
            forEach(new Block<V>() {
                @Override
                public boolean run(final V v) {
                    target.add(v);
                    return true;
                }
            });
            return target;
        }

        @Override
        public <W> MongoIterable<W> map(final Function<V, W> mapper) {
            return new MongoIterableCollection<V, W>(this, mapper);
        }

        @Override
        public void asyncForEach(final AsyncBlock<? super V> block) {
            iterable.asyncForEach(new AsyncBlock<U>() {
                @Override
                public void done() {
                    block.done();
                }

                @Override
                public boolean run(final U u) {
                    return block.run(mapper.apply(u));
                }
            });
        }

        @Override
        public <A extends Collection<? super V>> Future<A> asyncInto(final A target) {
            final SingleResultFuture<A> future = new SingleResultFuture<A>();
            asyncInto(target, new SingleResultFutureCallback<A>(future));
            return future;
        }

        @Override
        public <A extends Collection<? super V>> void asyncInto(final A target, final SingleResultCallback<A> callback) {
            asyncForEach(new AsyncBlock<V>() {
                @Override
                public void done() {
                    callback.onResult(target, null);
                }

                @Override
                public boolean run(final V v) {
                    target.add(v);
                    return true;
                }
            });
        }
    }
}
