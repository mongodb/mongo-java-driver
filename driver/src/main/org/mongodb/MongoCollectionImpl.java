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

// CHECKSTYLE:OFF

import org.mongodb.async.AsyncBlock;
import org.mongodb.async.MongoAsyncQueryCursor;
import org.mongodb.command.Count;
import org.mongodb.command.CountCommandResult;
import org.mongodb.command.Distinct;
import org.mongodb.command.DistinctCommandResult;
import org.mongodb.command.FindAndModifyCommandResult;
import org.mongodb.command.FindAndModifyCommandResultCodec;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.operation.*;

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

// CHECKSTYLE:ON

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
    public MongoStream<T> batchSize(final int batchSize) {
        return new MongoCollectionStream().batchSize(batchSize);
    }

    @Override
    public MongoStream<T> withOptions(final EnumSet<QueryOption> queryOptions) {
        return new MongoCollectionStream().withOptions(queryOptions);
    }

    @Override
    public MongoStream<T> tail() {
        return new MongoCollectionStream().tail();
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
    public WriteResult insert(final List<T> documents) {
        return new MongoCollectionStream().insert(documents);
    }

    @Override
    public WriteResult save(final T document) {
        return new MongoCollectionStream().save(document);
    }

    @Override
    public MongoStream<T> filter(final Document filter) {
        return new MongoCollectionStream().filter(filter);
    }

    @Override
    public MongoStream<T> filter(final ConvertibleToDocument filter) {
        return filter(filter.toDocument());
    }

    @Override
    public MongoStream<T> sort(final Document sortCriteria) {
        return new MongoCollectionStream().sort(sortCriteria);
    }

    @Override
    public MongoStream<T> sort(final ConvertibleToDocument sortCriteria) {
        return sort(sortCriteria.toDocument());
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
    public MongoStream<T> select(final Document selector) {
        return new MongoCollectionStream().select(selector);
    }

    @Override
    public MongoStream<T> select(final ConvertibleToDocument selector) {
        return new MongoCollectionStream().select(selector);
    }

    @Override
    public Find getCriteria() {
        return new MongoCollectionStream().getCriteria();
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
    public WriteResult modify(final Document updateOperations) {
        return new MongoCollectionStream().modify(updateOperations);
    }

    @Override
    public WriteResult modifyOrInsert(final Document updateOperations) {
        return new MongoCollectionStream().modifyOrInsert(updateOperations);
    }

    @Override
    public WriteResult modifyOrInsert(final ConvertibleToDocument updateOperations) {
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
    public T modifyAndGet(final Document updateOperations, final Get beforeOrAfter) {
        return new MongoCollectionStream().modifyAndGet(updateOperations, beforeOrAfter);
    }

    @Override
    public T modifyAndGet(final ConvertibleToDocument updateOperations, final Get beforeOrAfter) {
        return new MongoCollectionStream().modifyAndGet(updateOperations, beforeOrAfter);
    }

    @Override
    public T modifyOrInsertAndGet(final Document updateOperations, final Get beforeOrAfter) {
        return new MongoCollectionStream().modifyOrInsertAndGet(updateOperations, beforeOrAfter);
    }

    @Override
    public T modifyOrInsertAndGet(final ConvertibleToDocument updateOperations, final Get beforeOrAfter) {
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
    public MongoFuture<WriteResult> asyncReplaceOrInsert(final T replacement) {
        return new MongoCollectionStream().asyncReplaceOrInsert(replacement);
    }

    @Override
    public MongoFuture<T> asyncOne() {
        return new MongoCollectionStream().asyncOne();
    }

    @Override
    public MongoFuture<Long> asyncCount() {
        return new MongoCollectionStream().asyncCount();
    }

    @Override
    public void asyncForEach(final AsyncBlock<? super T> block) {
        new MongoCollectionStream().asyncForEach(block);
    }

    @Override
    public <A extends Collection<? super T>> MongoFuture<A> asyncInto(final A target) {
        return new MongoCollectionStream().asyncInto(target);
    }

    @Override
    public CollectionAdministration tools() {
        return admin;
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

        private MongoCollectionStream() {
            findOp = new Find();
            findOp.readPreference(getOptions().getReadPreference());
            writeConcern = getOptions().getWriteConcern();
        }

        private MongoCollectionStream(final MongoCollectionStream from) {
            findOp = new Find(from.findOp);
            writeConcern = from.writeConcern;
            limitSet = from.limitSet;
        }

        @Override
        public MongoCursor<T> iterator() {
            return all();
        }

        @Override
        public MongoStream<T> filter(final Document filter) {
            final MongoCollectionStream newStream = new MongoCollectionStream(this);
            newStream.findOp.filter(filter);
            return newStream;
        }

        @Override
        public MongoStream<T> filter(final ConvertibleToDocument filter) {
            return filter(filter.toDocument());
        }

        @Override
        public MongoStream<T> sort(final ConvertibleToDocument sortCriteria) {
            return sort(sortCriteria.toDocument());
        }

        @Override
        public MongoStream<T> sort(final Document sortCriteria) {
            final MongoCollectionStream newStream = new MongoCollectionStream(this);
            newStream.findOp.order(sortCriteria);
            return newStream;
        }

        @Override
        public MongoStream<T> select(final Document selector) {
            final MongoCollectionStream newStream = new MongoCollectionStream(this);
            newStream.findOp.select(selector);
            return newStream;
        }

        @Override
        public MongoStream<T> select(final ConvertibleToDocument selector) {
            return select(selector.toDocument());
        }

        @Override
        public Find getCriteria() {
            return new Find(findOp);
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
        public MongoStream<T> withOptions(final EnumSet<QueryOption> queryOptions) {
            final MongoCollectionStream newStream = new MongoCollectionStream(this);
            newStream.findOp.addOptions(queryOptions);
            return newStream;
        }

        @Override
        public MongoStream<T> tail() {
            final MongoCollectionStream newStream = new MongoCollectionStream(this);
            newStream.findOp.addOptions(EnumSet.of(QueryOption.Tailable, QueryOption.AwaitData));
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
            return new MongoQueryCursor<T>(getNamespace(), findOp, getDocumentCodec(), getCodec(), client.getSession(),
                    client.getBufferProvider());
        }

        @Override
        public T one() {
            final QueryResult<T> res = new QueryOperation<T>(getNamespace(), findOp.batchSize(-1), getDocumentCodec(), getCodec(),
                    client.getBufferProvider()).execute(client.getSession());
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
            return new WriteResult(new InsertOperation<T>(getNamespace(), new Insert<T>(document).writeConcern(writeConcern), getCodec(),
                    client.getBufferProvider()).execute(client.getSession()), writeConcern);
        }

        @Override
        public WriteResult insert(final List<T> documents) {
            return new WriteResult(new InsertOperation<T>(getNamespace(), new Insert<T>(documents).writeConcern(writeConcern), getCodec(),
                    client.getBufferProvider()).execute(client.getSession()), writeConcern);
        }

        @Override
        public WriteResult save(final T document) {
            final Object id = getCodec().getId(document);
            if (id == null) {
                return insert(document);
            }
            else {
                return filter(new Document("_id", id)).replaceOrInsert(document);
            }
        }

        @Override
        public WriteResult remove() {
            final Remove remove = new Remove(findOp.getFilter()).multi(getMultiFromLimit(UpdateType.remove))
                    .writeConcern(writeConcern);
            return new WriteResult(new RemoveOperation(getNamespace(), remove, getDocumentCodec(),
                    client.getBufferProvider()).execute(client.getSession()), writeConcern);
        }

        @Override
        public WriteResult modify(final Document updateOperations) {
            final Update update = new Update(findOp.getFilter(), updateOperations).multi(getMultiFromLimit(UpdateType.modify))
                    .writeConcern(writeConcern);
            return new WriteResult(new UpdateOperation(getNamespace(), update, getDocumentCodec(), client.getBufferProvider())
                    .execute(client.getSession()), writeConcern);
        }

        @Override
        public WriteResult modifyOrInsert(final Document updateOperations) {
            final Update update = new Update(findOp.getFilter(), updateOperations).upsert(true).multi(getMultiFromLimit(UpdateType.modify))
                    .writeConcern(writeConcern);
            return new WriteResult(new UpdateOperation(getNamespace(), update, getDocumentCodec(),
                    client.getBufferProvider()).execute(client.getSession()), writeConcern);
        }

        @Override
        public WriteResult modifyOrInsert(final ConvertibleToDocument updateOperations) {
            return modifyOrInsert(updateOperations.toDocument());
        }

        @Override
        public WriteResult replace(final T replacement) {
            final Replace<T> replace = new Replace<T>(findOp.getFilter(), replacement).writeConcern(writeConcern);
            return new WriteResult(new ReplaceOperation<T>(getNamespace(), replace, getDocumentCodec(), getCodec(),
                    client.getBufferProvider()).execute(client.getSession()), writeConcern);
        }

        @Override
        public WriteResult replaceOrInsert(final T replacement) {
            final Replace<T> replace = new Replace<T>(findOp.getFilter(), replacement).upsert(true).writeConcern(writeConcern);
            return new WriteResult(new ReplaceOperation<T>(getNamespace(), replace, getDocumentCodec(), getCodec(),
                    client.getBufferProvider()).execute(client.getSession()), writeConcern);
        }

        @Override
        //CHECKSTYLE:OFF
        //TODO: absolute disaster area
        public T modifyAndGet(final Document updateOperations, final Get beforeOrAfter) {
            final FindAndUpdate<T> findAndUpdate = new FindAndUpdate<T>()
                    .where(findOp.getFilter()).updateWith(updateOperations).returnNew(asBoolean(beforeOrAfter)).select(findOp.getFields())
                    .sortBy(findOp.getOrder());

            //TODO: Still need to tidy up some of this command stuff, especially around return values
            final org.mongodb.command.FindAndUpdate<T> findAndUpdateCommand =
                    new org.mongodb.command.FindAndUpdate<T>(findAndUpdate, getName());
            final FindAndModifyCommandResultCodec<T> codec = new FindAndModifyCommandResultCodec<T>(getOptions().getPrimitiveCodecs(),
                    getCodec());
            return new FindAndModifyCommandResult<T>(new CommandOperation(getDatabase().getName(), findAndUpdateCommand, codec,
                    client.getCluster().getDescription(), client.getBufferProvider()).execute(client.getSession())).getValue();
        }

        @Override
        public T modifyAndGet(final ConvertibleToDocument updateOperations, final Get beforeOrAfter) {
            return modifyAndGet(updateOperations.toDocument(), beforeOrAfter);
        }

        @Override
        public T modifyOrInsertAndGet(final Document updateOperations, final Get beforeOrAfter) {
            final FindAndUpdate<T> findAndUpdate = new FindAndUpdate<T>().where(findOp.getFilter()).updateWith(updateOperations)
                    .upsert(true).returnNew(asBoolean(beforeOrAfter)).select(findOp.getFields()).sortBy(findOp.getOrder());
            return new FindAndModifyCommandResult<T>(
                    new CommandOperation(getDatabase().getName(), new org.mongodb.command.FindAndUpdate<T>(findAndUpdate, getName()),
                            new FindAndModifyCommandResultCodec<T>(getOptions().getPrimitiveCodecs(), getCodec()),
                            client.getCluster().getDescription(), client.getBufferProvider()).execute(client.getSession())).getValue();
        }

        @Override
        public T modifyOrInsertAndGet(final ConvertibleToDocument updateOperations, final Get beforeOrAfter) {
            return modifyOrInsertAndGet(updateOperations.toDocument(), beforeOrAfter);
        }

        @Override
        public T replaceAndGet(final T replacement, final Get beforeOrAfter) {
            final FindAndReplace<T> findAndReplace = new FindAndReplace<T>(replacement).where(findOp.getFilter())
                    .returnNew(asBoolean(beforeOrAfter)).select(findOp.getFields()).sortBy(findOp.getOrder());
            return new FindAndModifyCommandResult<T>(
                    new CommandOperation(getDatabase().getName(), new org.mongodb.command.FindAndReplace<T>(findAndReplace, getName()),
                            new FindAndModifyCommandResultCodec<T>(getOptions().getPrimitiveCodecs(), getCodec()),
                            client.getCluster().getDescription(), client.getBufferProvider()).execute(client.getSession()))
                    .getValue();
        }

        @Override
        public T replaceOrInsertAndGet(final T replacement, final Get beforeOrAfter) {
            final FindAndReplace<T> findAndReplace = new FindAndReplace<T>(replacement).where(findOp.getFilter())
                    .returnNew(asBoolean(beforeOrAfter)).upsert(true).select(findOp.getFields()).sortBy(findOp.getOrder());
            return new FindAndModifyCommandResult<T>(
                    new CommandOperation(getDatabase().getName(), new org.mongodb.command.FindAndReplace<T>(findAndReplace, getName()),
                            new FindAndModifyCommandResultCodec<T>(getOptions().getPrimitiveCodecs(), getCodec()),
                            client.getCluster().getDescription(), client.getBufferProvider()).execute(client.getSession())).getValue();
        }

        @Override
        public T removeAndGet() {
            final FindAndRemove<T> findAndRemove = new FindAndRemove<T>().where(findOp.getFilter()).select(findOp.getFields())
                    .sortBy(findOp.getOrder());

            final FindAndModifyCommandResultCodec<T> codec
                    = new FindAndModifyCommandResultCodec<T>(getOptions().getPrimitiveCodecs(),
                    getCodec());
            return new FindAndModifyCommandResult<T>(new CommandOperation(getDatabase().getName(),
                    new org.mongodb.command.FindAndRemove(findAndRemove, getName()), codec, client.getCluster().getDescription(),
                    client.getBufferProvider()).execute(client.getSession())).getValue();
        }
        //CHECKSTYLE:OFF

        @Override
        public MongoFuture<WriteResult> asyncReplaceOrInsert(final T replacement) {
            final Replace<T> replace = new Replace<T>(findOp.getFilter(), replacement).upsert(true).writeConcern(writeConcern);
            final MongoFuture<CommandResult> commandResultFuture = new AsyncReplaceOperation<T>(getNamespace(), replace,
                    getDocumentCodec(), getCodec(), client.getBufferProvider()).execute(client.getAsyncSession());
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
                    new AsyncQueryOperation<T>(getNamespace(), findOp.batchSize(-1), getDocumentCodec(), getCodec(),
                            client.getBufferProvider()).execute(client.getAsyncSession());
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
            final MongoFuture<CommandResult> commandResultFuture = new AsyncCommandOperation(getDatabase().getName(), new Count(findOp,
                    getName()), getDocumentCodec(), client.getCluster().getDescription(), client.getBufferProvider()).execute(client
                    .getAsyncSession());
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
