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

package org.mongodb;

import org.mongodb.async.AsyncBlock;
import org.mongodb.async.MongoAsyncQueryCursor;
import org.mongodb.command.Count;
import org.mongodb.command.CountCommandResult;
import org.mongodb.command.Distinct;
import org.mongodb.command.DistinctCommandResult;
import org.mongodb.command.FindAndModifyCommandResult;
import org.mongodb.command.FindAndModifyCommandResultCodec;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.operation.AsyncCommandOperation;
import org.mongodb.operation.AsyncQueryOperation;
import org.mongodb.operation.AsyncReplaceOperation;
import org.mongodb.operation.CommandOperation;
import org.mongodb.operation.CommandResult;
import org.mongodb.operation.Find;
import org.mongodb.operation.FindAndRemove;
import org.mongodb.operation.FindAndReplace;
import org.mongodb.operation.FindAndUpdate;
import org.mongodb.operation.Insert;
import org.mongodb.operation.InsertOperation;
import org.mongodb.operation.MongoFuture;
import org.mongodb.operation.QueryOperation;
import org.mongodb.operation.QueryOption;
import org.mongodb.operation.QueryResult;
import org.mongodb.operation.Remove;
import org.mongodb.operation.RemoveOperation;
import org.mongodb.operation.Replace;
import org.mongodb.operation.ReplaceOperation;
import org.mongodb.operation.SingleResultFuture;
import org.mongodb.operation.SingleResultFutureCallback;
import org.mongodb.operation.Update;
import org.mongodb.operation.UpdateOperation;

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

class MongoCollectionImpl<T> implements MongoCollection<T> {

    private final CollectionAdministration admin;
    private final MongoClientImpl client;
    private final String name;
    private final MongoDatabase database;
    private final MongoCollectionOptions options;
    private final CollectibleCodec<T> codec;

    public MongoCollectionImpl(final String name, final MongoDatabaseImpl database,
                               final CollectibleCodec<T> codec, final MongoCollectionOptions options,
                               final MongoClientImpl client) {

        this.codec = codec;
        this.name = name;
        this.database = database;
        this.options = options;
        this.client = client;
        admin = new CollectionAdministrationImpl(client, options.getPrimitiveCodecs(), getNamespace(), getDatabase());
    }

    @Override
    public WriteResult insert(final T document) {
        return new MongoCollectionStream().insert(document);
    }

    @Override
    public WriteResult insert(final List<T> documents) {
        return new MongoCollectionStream().insert(documents);
    }

    @Override
    public WriteResult save(final T document) {
        return new MongoCollectionStream().save(document);
    }

    @Override
    public CollectionAdministration tools() {
        return admin;
    }

    @Override
    public MongoStream<T> find() {
        return new MongoCollectionStream();
    }

    @Override
    public MongoStream<T> find(final Document filter) {
        return new MongoCollectionStream().find(filter);
    }

    @Override
    public MongoStream<T> find(final ConvertibleToDocument filter) {
        return new MongoCollectionStream().find(filter);
    }

    @Override
    public MongoStream<T> withWriteConcern(final WriteConcern writeConcern) {
        return new MongoCollectionStream().withWriteConcern(writeConcern);
    }

    private Codec<Document> getDocumentCodec() {
        return getOptions().getDocumentCodec();
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
    public CollectibleCodec<T> getCodec() {
        return codec;
    }

    @Override
    public MongoCollectionOptions getOptions() {
        return options;
    }

    @Override
    public MongoNamespace getNamespace() {
        return new MongoNamespace(getDatabase().getName(), getName());
    }

    private enum UpdateType {
        modify {
            @Override
            boolean isMultiByDefault() {
                return false;
            }
        },

        remove {
            @Override
            boolean isMultiByDefault() {
                return true;
            }
        };

        abstract boolean isMultiByDefault();
    }

    private final class MongoCollectionStream implements MongoStream<T> {
        private final Find findOp;
        private WriteConcern writeConcern;
        private boolean limitSet;
        private boolean upsert;

        private MongoCollectionStream() {
            findOp = new Find();
            findOp.readPreference(getOptions().getReadPreference());
            writeConcern = getOptions().getWriteConcern();
        }

        @Override
        public MongoCursor<T> iterator() {
            return get();
        }

        @Override
        public MongoStream<T> find(final Document filter) {
            findOp.filter(filter);
            return this;
        }

        @Override
        public MongoStream<T> find(final ConvertibleToDocument filter) {
            return find(filter.toDocument());
        }

        @Override
        public MongoStream<T> sort(final ConvertibleToDocument sortCriteria) {
            return sort(sortCriteria.toDocument());
        }

        @Override
        public MongoStream<T> sort(final Document sortCriteria) {
            findOp.order(sortCriteria);
            return this;
        }

        @Override
        public MongoStream<T> fields(final Document selector) {
            findOp.select(selector);
            return this;
        }

        @Override
        public MongoStream<T> fields(final ConvertibleToDocument selector) {
            return fields(selector.toDocument());
        }

        @Override
        public MongoStream<T> upsert() {
            upsert = true;
            return this;
        }

        @Override
        public Find getCriteria() {
            return new Find(findOp);
        }

        @Override
        public MongoStream<T> skip(final int skip) {
            findOp.skip(skip);
            return this;
        }

        @Override
        public MongoStream<T> limit(final int limit) {
            findOp.limit(limit);
            limitSet = true;
            return this;
        }

        @Override
        public MongoStream<T> noLimit() {
            return limit(0);
        }

        @Override
        public MongoStream<T> batchSize(final int batchSize) {
            findOp.batchSize(batchSize);
            return this;
        }

        @Override
        public MongoStream<T> withOptions(final EnumSet<QueryOption> queryOptions) {
            findOp.addOptions(queryOptions);
            return this;
        }

        @Override
        public MongoStream<T> tail() {
            findOp.addOptions(EnumSet.of(QueryOption.Tailable, QueryOption.AwaitData));
            return this;
        }

        @Override
        public MongoStream<T> withReadPreference(final ReadPreference readPreference) {
            findOp.readPreference(readPreference);
            return this;
        }

        @Override
        public MongoCursor<T> get() {
            return new MongoQueryCursor<T>(getNamespace(), findOp, getDocumentCodec(), getCodec(), client.getSession(),
                    client.getBufferProvider());
        }

        @Override
        public T getOne() {
            final QueryResult<T> res = client.getSession().execute(
                    new QueryOperation<T>(getNamespace(), findOp.batchSize(-1), getDocumentCodec(), getCodec(),
                            client.getBufferProvider()));
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
        public void forEach(final Block<? super T> block) {
            final MongoCursor<T> cursor = get();
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
        public MongoStream<T> withWriteConcern(final WriteConcern writeConcernForThisOperation) {
            writeConcern = writeConcernForThisOperation;
            return this;
        }

        @Override
        public WriteResult insert(final T document) {
            return new WriteResult(client.getSession().execute(new InsertOperation<T>(getNamespace(),
                    new Insert<T>(writeConcern, document), getCodec(), client.getBufferProvider())), writeConcern);
        }

        @Override
        public WriteResult insert(final List<T> documents) {
            return new WriteResult(client.getSession().execute(new InsertOperation<T>(getNamespace(),
                    new Insert<T>(writeConcern, documents), getCodec(), client.getBufferProvider())), writeConcern);
        }

        @Override
        public WriteResult save(final T document) {
            final Object id = getCodec().getId(document);
            if (id == null) {
                return insert(document);
            }
            else {
                return upsert().find(new Document("_id", id)).replace(document);
            }
        }

        @Override
        public WriteResult remove() {
            final Remove remove = new Remove(writeConcern, findOp.getFilter()).multi(getMultiFromLimit(UpdateType.remove));
            return new WriteResult(client.getSession().execute(
                    new RemoveOperation(getNamespace(), remove, getDocumentCodec(), client.getBufferProvider())), writeConcern);
        }

        @Override
        public WriteResult update(final Document updateOperations) {
            final Update update = new Update(writeConcern, findOp.getFilter(), updateOperations).upsert(upsert)
                    .multi(getMultiFromLimit(UpdateType.modify));
            return new WriteResult(client.getSession().execute(new UpdateOperation(getNamespace(), update, getDocumentCodec(),
                    client.getBufferProvider())), writeConcern);
        }

        @Override
        public WriteResult update(final ConvertibleToDocument updateOperations) {
            return update(updateOperations.toDocument());
        }

        @Override
        public WriteResult replace(final T replacement) {
            final Replace<T> replace = new Replace<T>(writeConcern, findOp.getFilter(), replacement).upsert(upsert);
            return new WriteResult(client.getSession().execute(
                    new ReplaceOperation<T>(getNamespace(), replace, getDocumentCodec(), getCodec(), client.getBufferProvider())),
                    writeConcern);
        }

        @Override
        public T updateOneAndGet(final Document updateOperations) {
            return updateOneAndGet(updateOperations, Get.AfterChangeApplied);
        }

        @Override
        public T updateOneAndGet(final ConvertibleToDocument updateOperations) {
            return updateOneAndGet(updateOperations.toDocument());
        }

        @Override
        public T replaceOneAndGet(final T replacement) {
            return replaceOneAndGet(replacement, Get.AfterChangeApplied);
        }

        @Override
        public T updateOneAndGetOriginal(final Document updateOperations) {
            return updateOneAndGet(updateOperations, Get.BeforeChangeApplied);
        }

        @Override
        public T updateOneAndGetOriginal(final ConvertibleToDocument updateOperations) {
            return updateOneAndGetOriginal(updateOperations.toDocument());
        }

        @Override
        public T replaceOneAndGetOriginal(final T replacement) {
            return replaceOneAndGet(replacement, Get.BeforeChangeApplied);
        }

        //CHECKSTYLE:OFF
        //TODO: absolute disaster area
        public T updateOneAndGet(final Document updateOperations, final Get beforeOrAfter) {
            final FindAndUpdate<T> findAndUpdate = new FindAndUpdate<T>()
                    .where(findOp.getFilter()).updateWith(updateOperations).returnNew(asBoolean(beforeOrAfter)).select(findOp.getFields())
                    .sortBy(findOp.getOrder()).upsert(upsert);

            //TODO: Still need to tidy up some of this command stuff, especially around return values
            final org.mongodb.command.FindAndUpdate<T> findAndUpdateCommand =
                    new org.mongodb.command.FindAndUpdate<T>(findAndUpdate, getName());
            final FindAndModifyCommandResultCodec<T> codec = new FindAndModifyCommandResultCodec<T>(getOptions().getPrimitiveCodecs(),
                    getCodec());
            return new FindAndModifyCommandResult<T>(client.getSession().execute(
                    new CommandOperation(getDatabase().getName(), findAndUpdateCommand, codec, client.getCluster().getDescription(),
                            client.getBufferProvider()))).getValue();
        }

        public T replaceOneAndGet(final T replacement, final Get beforeOrAfter) {
            final FindAndReplace<T> findAndReplace = new FindAndReplace<T>(replacement).where(findOp.getFilter())
                    .returnNew(asBoolean(beforeOrAfter)).select(findOp.getFields()).sortBy(findOp.getOrder()).upsert(upsert);
            return new FindAndModifyCommandResult<T>(
                    client.getSession().execute(
                            new CommandOperation(getDatabase().getName(), new org.mongodb.command.FindAndReplace<T>(findAndReplace,
                                    getName()), new FindAndModifyCommandResultCodec<T>(getOptions().getPrimitiveCodecs(), getCodec()),
                                    client.getCluster().getDescription(), client.getBufferProvider()))).getValue();
        }

        @Override
        public T removeOneAndGet() {
            final FindAndRemove<T> findAndRemove = new FindAndRemove<T>().where(findOp.getFilter()).select(findOp.getFields())
                    .sortBy(findOp.getOrder());

            final FindAndModifyCommandResultCodec<T> codec
                    = new FindAndModifyCommandResultCodec<T>(getOptions().getPrimitiveCodecs(),
                    getCodec());
            return new FindAndModifyCommandResult<T>(client.getSession().execute(
                    new CommandOperation(getDatabase().getName(), new org.mongodb.command.FindAndRemove(findAndRemove, getName()), codec,
                            client.getCluster().getDescription(), client.getBufferProvider()))).getValue();
        }
        //CHECKSTYLE:OFF

        @Override
        public MongoFuture<WriteResult> asyncReplace(final T replacement) {
            final Replace<T> replace = new Replace<T>(writeConcern, findOp.getFilter(), replacement).upsert(upsert);
            final MongoFuture<CommandResult> commandResultFuture = client.getAsyncSession().execute(
                    new AsyncReplaceOperation<T>(getNamespace(), replace, getDocumentCodec(), getCodec(), client.getBufferProvider()));
            return new MappingFuture<CommandResult, WriteResult>(commandResultFuture, new Function<CommandResult, WriteResult>() {
                @Override
                public WriteResult apply(final CommandResult commandResult) {
                    return new WriteResult(commandResult, writeConcern);
                }
            });
        }

        boolean asBoolean(final Get get) {
            return get == Get.AfterChangeApplied;
        }

        @Override
        public MongoFuture<T> asyncOne() {
            final MongoFuture<QueryResult<T>> queryResultFuture =
                    client.getAsyncSession().execute(
                            new AsyncQueryOperation<T>(getNamespace(), findOp.batchSize(-1), getDocumentCodec(), getCodec(),
                                    client.getBufferProvider()));
            return new MappingFuture<QueryResult<T>, T>(queryResultFuture, new Function<QueryResult<T>, T>() {
                @Override
                public T apply(final QueryResult<T> queryResult) {
                    if (queryResult.getResults().isEmpty()) {
                        return null;
                    }
                    else {
                        return queryResult.getResults().get(0);
                    }
                }
            });
        }

        @Override
        public MongoFuture<Long> asyncCount() {
            final MongoFuture<CommandResult> commandResultFuture = client.getAsyncSession().execute(
                    new AsyncCommandOperation(getDatabase().getName(), new Count(findOp, getName()), getDocumentCodec(),
                            client.getCluster().getDescription(), client.getBufferProvider()));
            return new MappingFuture<CommandResult, Long>(commandResultFuture, new Function<CommandResult, Long>() {
                @Override
                public Long apply(final CommandResult commandResult) {
                    return new CountCommandResult(commandResult).getCount();
                }
            });
        }

        private boolean getMultiFromLimit(final UpdateType updateType) {
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
            else return updateType.isMultiByDefault();
        }

        @Override
        public void asyncForEach(final AsyncBlock<? super T> block) {
            new MongoAsyncQueryCursor<T>(getNamespace(), findOp, getDocumentCodec(), getCodec(), client.getBufferProvider(),
                    client.getAsyncSession(), block).start();
        }

        @Override
        public <A extends Collection<? super T>> MongoFuture<A> asyncInto(final A target) {
            final SingleResultFuture<A> future = new SingleResultFuture<A>();

            asyncInto(target, new SingleResultFutureCallback<A>(future));
            return future;
        }

        private <A extends Collection<? super T>> void asyncInto(final A target, final SingleResultCallback<A> callback) {
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
        public <A extends Collection<? super V>> MongoFuture<A> asyncInto(final A target) {
            final SingleResultFuture<A> future = new SingleResultFuture<A>();
            asyncInto(target, new SingleResultFutureCallback<A>(future));
            return future;
        }

        private <A extends Collection<? super V>> void asyncInto(final A target, final SingleResultCallback<A> callback) {
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
