/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import org.bson.types.Code;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.operation.AggregateOperation;
import org.mongodb.operation.AsyncOperation;
import org.mongodb.operation.CountOperation;
import org.mongodb.operation.Find;
import org.mongodb.operation.FindAndRemove;
import org.mongodb.operation.FindAndRemoveOperation;
import org.mongodb.operation.FindAndReplace;
import org.mongodb.operation.FindAndReplaceOperation;
import org.mongodb.operation.FindAndUpdate;
import org.mongodb.operation.FindAndUpdateOperation;
import org.mongodb.operation.InsertOperation;
import org.mongodb.operation.InsertRequest;
import org.mongodb.operation.MapReduce;
import org.mongodb.operation.MapReduceToCollectionOperation;
import org.mongodb.operation.MapReduceWithInlineResultsOperation;
import org.mongodb.operation.Operation;
import org.mongodb.operation.QueryOperation;
import org.mongodb.operation.RemoveOperation;
import org.mongodb.operation.RemoveRequest;
import org.mongodb.operation.ReplaceOperation;
import org.mongodb.operation.ReplaceRequest;
import org.mongodb.operation.SingleResultFuture;
import org.mongodb.operation.SingleResultFutureCallback;
import org.mongodb.operation.UpdateOperation;
import org.mongodb.operation.UpdateRequest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;

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
        admin = new CollectionAdministrationImpl(client, getNamespace(), getDatabase());
    }

    @Override
    public WriteResult insert(final T document) {
        return new MongoCollectionView().insert(document);
    }

    @Override
    public WriteResult insert(final List<T> documents) {
        return new MongoCollectionView().insert(documents);
    }

    @Override
    public MongoFuture<WriteResult> asyncInsert(final T document) {
        return new MongoCollectionView().asyncInsert(document);
    }

    @Override
    public MongoFuture<WriteResult> asyncInsert(final List<T> documents) {
        return new MongoCollectionView().asyncInsert(documents);
    }

    @Override
    public WriteResult save(final T document) {
        return new MongoCollectionView().save(document);
    }

    @Override
    public MongoPipeline<T> pipe() {
        return new MongoCollectionPipeline();
    }

    @Override
    public CollectionAdministration tools() {
        return admin;
    }

    @Override
    public MongoView<T> find() {
        return new MongoCollectionView();
    }

    @Override
    public MongoView<T> find(final Document filter) {
        return new MongoCollectionView().find(filter);
    }

    @Override
    public MongoView<T> find(final ConvertibleToDocument filter) {
        return new MongoCollectionView().find(filter);
    }

    @Override
    public MongoView<T> withWriteConcern(final WriteConcern writeConcern) {
        return new MongoCollectionView().withWriteConcern(writeConcern);
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

    <V> V execute(final Operation<V> operation) {
        return client.execute(operation);
    }

    <V> MongoFuture<V> executeAsync(final AsyncOperation<V> operation) {
        return client.executeAsync(operation);
    }

    private final class MongoCollectionView implements MongoView<T> {
        private final Find findOp;
        private WriteConcern writeConcern;
        private boolean limitSet;
        private boolean upsert;

        private MongoCollectionView() {
            findOp = new Find();
            findOp.readPreference(getOptions().getReadPreference());
            writeConcern = getOptions().getWriteConcern();
        }

        @Override
        public MongoCursor<T> iterator() {
            return get();
        }

        @Override
        public MongoView<T> find(final Document filter) {
            findOp.filter(filter);
            return this;
        }

        @Override
        public MongoView<T> find(final ConvertibleToDocument filter) {
            return find(filter.toDocument());
        }

        @Override
        public MongoView<T> sort(final ConvertibleToDocument sortCriteria) {
            return sort(sortCriteria.toDocument());
        }

        @Override
        public MongoView<T> sort(final Document sortCriteria) {
            findOp.order(sortCriteria);
            return this;
        }

        @Override
        public MongoView<T> fields(final Document selector) {
            findOp.select(selector);
            return this;
        }

        @Override
        public MongoView<T> fields(final ConvertibleToDocument selector) {
            return fields(selector.toDocument());
        }

        @Override
        public MongoView<T> upsert() {
            upsert = true;
            return this;
        }

        @Override
        public MongoView<T> withQueryOptions(final QueryOptions queryOptions) {
            findOp.options(queryOptions);
            return this;
        }

        @Override
        public MongoView<T> skip(final int skip) {
            findOp.skip(skip);
            return this;
        }

        @Override
        public MongoView<T> limit(final int limit) {
            findOp.limit(limit);
            limitSet = true;
            return this;
        }

        @Override
        public MongoView<T> withReadPreference(final ReadPreference readPreference) {
            findOp.readPreference(readPreference);
            return this;
        }

        @Override
        public MongoCursor<T> get() {
            return execute(new QueryOperation<T>(getNamespace(), findOp, getDocumentCodec(), getCodec()
            ));
        }

        @Override
        public T getOne() {
            MongoCursor<T> cursor = execute(new QueryOperation<T>(getNamespace(), findOp.batchSize(-1), getDocumentCodec(), getCodec()
            ));

            return cursor.hasNext() ? cursor.next() : null;
        }

        @Override
        public long count() {
            return execute(new CountOperation(getNamespace(), findOp, getDocumentCodec()));
        }

        @Override
        public MongoIterable<Document> mapReduce(final String map, final String reduce) {
            //TODO: support supplied read preferences?
            MapReduce mapReduce = new MapReduce(new Code(map), new Code(reduce)).filter(findOp.getFilter())
                                                                                .limit(findOp.getLimit());

            if (mapReduce.isInline()) {
                MapReduceWithInlineResultsOperation<Document> operation =
                new MapReduceWithInlineResultsOperation<Document>(getNamespace(), mapReduce, new DocumentCodec(),
                                                                  options.getReadPreference());
                return new MapReduceResultsIterable<T, Document>(operation, MongoCollectionImpl.this);
            } else {
                execute(new MapReduceToCollectionOperation(getNamespace(), mapReduce));
                return client.getDatabase(mapReduce.getOutput().getDatabaseName()).getCollection(mapReduce.getOutput().getCollectionName())
                             .find();
            }
        }

        @Override
        public void forEach(final Block<? super T> block) {
            MongoCursor<T> cursor = get();
            try {
                while (cursor.hasNext()) {
                    block.apply(cursor.next());
                }
            } finally {
                cursor.close();
            }
        }


        @Override
        public <A extends Collection<? super T>> A into(final A target) {
            forEach(new Block<T>() {
                @Override
                public void apply(final T t) {
                    target.add(t);
                }
            });
            return target;
        }

        @Override
        public <U> MongoIterable<U> map(final Function<T, U> mapper) {
            return new MappingIterable<T, U>(this, mapper);
        }

        @Override
        public MongoView<T> withWriteConcern(final WriteConcern writeConcernForThisOperation) {
            writeConcern = writeConcernForThisOperation;
            return this;
        }

        @Override
        @SuppressWarnings("unchecked")
        public WriteResult insert(final T document) {
            return execute(new InsertOperation<T>(getNamespace(), true, writeConcern, asList(new InsertRequest<T>(document)), getCodec()));
        }

        @Override
        public WriteResult insert(final List<T> documents) {
            List<InsertRequest<T>> insertRequestList = new ArrayList<InsertRequest<T>>(documents.size());
            for (T cur : documents) {
                insertRequestList.add(new InsertRequest<T>(cur));
            }
            return execute(new InsertOperation<T>(getNamespace(), true, writeConcern, insertRequestList, getCodec()));
        }

        @Override
        @SuppressWarnings("unchecked")
        public MongoFuture<WriteResult> asyncInsert(final T document) {
            return asyncInsert(asList(document));
        }

        @Override
        @SuppressWarnings("unchecked")
        public MongoFuture<WriteResult> asyncInsert(final List<T> documents) {
            List<InsertRequest<T>> insertRequestList = new ArrayList<InsertRequest<T>>(documents.size());
            for (T cur : documents) {
                insertRequestList.add(new InsertRequest<T>(cur));
            }
            return executeAsync(new InsertOperation<T>(getNamespace(), true, writeConcern, insertRequestList, getCodec()));
        }

        @Override
        public WriteResult save(final T document) {
            Object id = getCodec().getId(document);
            if (id == null) {
                return insert(document);
            } else {
                return upsert().find(new Document("_id", id)).replace(document);
            }
        }

        @Override
        public WriteResult remove() {
            RemoveRequest removeRequest = new RemoveRequest(findOp.getFilter()).multi(getMultiFromLimit());
            return execute(new RemoveOperation(getNamespace(), true, writeConcern, asList(removeRequest),
                                               getDocumentCodec()));
        }

        @Override
        public WriteResult removeOne() {
            RemoveRequest removeRequest = new RemoveRequest(findOp.getFilter()).multi(false);
            return execute(new RemoveOperation(getNamespace(), true, writeConcern, asList(removeRequest),
                                               getDocumentCodec()));
        }

        @Override
        public WriteResult update(final Document updateOperations) {
            UpdateRequest update = new UpdateRequest(findOp.getFilter(), updateOperations).upsert(upsert)
                                                                                          .multi(getMultiFromLimit());
            return execute(new UpdateOperation(getNamespace(), true, writeConcern, asList(update),
                                               getDocumentCodec()));
        }

        @Override
        public WriteResult update(final ConvertibleToDocument updateOperations) {
            return update(updateOperations.toDocument());
        }

        @Override
        public WriteResult updateOne(final Document updateOperations) {
            UpdateRequest update = new UpdateRequest(findOp.getFilter(), updateOperations).upsert(upsert).multi(false);
            return execute(new UpdateOperation(getNamespace(), true, writeConcern, asList(update), getDocumentCodec()));
        }

        @Override
        public WriteResult updateOne(final ConvertibleToDocument updateOperations) {
            return updateOne(updateOperations.toDocument());
        }

        @Override
        @SuppressWarnings("unchecked")
        public WriteResult replace(final T replacement) {
            ReplaceRequest<T> replaceRequest = new ReplaceRequest<T>(findOp.getFilter(), replacement).upsert(upsert);
            return execute(new ReplaceOperation<T>(getNamespace(), true, writeConcern, asList(replaceRequest), getDocumentCodec(),
                                                   getCodec()));
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
        public T getOneAndUpdate(final Document updateOperations) {
            return updateOneAndGet(updateOperations, Get.BeforeChangeApplied);
        }

        @Override
        public T getOneAndUpdate(final ConvertibleToDocument updateOperations) {
            return getOneAndUpdate(updateOperations.toDocument());
        }

        @Override
        public T getOneAndReplace(final T replacement) {
            return replaceOneAndGet(replacement, Get.BeforeChangeApplied);
        }

        public T updateOneAndGet(final Document updateOperations, final Get beforeOrAfter) {
            FindAndUpdate<T> findAndUpdate = new FindAndUpdate<T>().where(findOp.getFilter())
                                                                   .updateWith(updateOperations)
                                                                   .returnNew(asBoolean(beforeOrAfter))
                                                                   .select(findOp.getFields())
                                                                   .sortBy(findOp.getOrder())
                                                                   .upsert(upsert);

            return execute(new FindAndUpdateOperation<T>(getNamespace(), findAndUpdate, getCodec()));
        }

        public T replaceOneAndGet(final T replacement, final Get beforeOrAfter) {
            FindAndReplace<T> findAndReplace = new FindAndReplace<T>(replacement).where(findOp.getFilter())
                                                                                 .returnNew(asBoolean(beforeOrAfter))
                                                                                 .select(findOp.getFields())
                                                                                 .sortBy(findOp.getOrder())
                                                                                 .upsert(upsert);
            return execute(new FindAndReplaceOperation<T>(getNamespace(), findAndReplace, getCodec(), getCodec()));
        }

        @Override
        public T getOneAndRemove() {
            FindAndRemove<T> findAndRemove = new FindAndRemove<T>().where(findOp.getFilter())
                                                                   .select(findOp.getFields())
                                                                   .sortBy(findOp.getOrder());

            return execute(new FindAndRemoveOperation<T>(getNamespace(), findAndRemove, getCodec())); }

        @Override
        @SuppressWarnings("unchecked")
        public MongoFuture<WriteResult> asyncReplace(final T replacement) {
            ReplaceRequest<T> replaceRequest = new ReplaceRequest<T>(findOp.getFilter(), replacement).upsert(upsert);
            return executeAsync(new ReplaceOperation<T>(getNamespace(), true, writeConcern, asList(replaceRequest),
                    getDocumentCodec(), getCodec()));
        }

        boolean asBoolean(final Get get) {
            return get == Get.AfterChangeApplied;
        }

        @Override
        public MongoFuture<T> asyncOne() {
            final SingleResultFuture<T> retVal = new SingleResultFuture<T>();
            executeAsync(new QueryOperation<T>(getNamespace(), findOp.batchSize(-1), getDocumentCodec(), getCodec()))
            .register(new
                      SingleResultCallback<MongoAsyncCursor<T>>() {
                          @Override
                          public void onResult(
                                              final
                                              MongoAsyncCursor<T> cursor,
                                              final
                                              MongoException e) {
                              if (e != null) {
                                  retVal.init(null, e);
                              } else {
                                  cursor.start(new AsyncBlock<T>() {
                                      @Override
                                      public void done() {
                                          if (!retVal.isDone()) {
                                              retVal.init(null, null); // TODO: deal with errors
                                          }
                                      }

                                      @Override
                                      public void apply(final T t) {
                                          retVal.init(t, null);
                                      }
                                  });
                              }
                          }
                      });
            return retVal;
        }

        @Override
        public MongoFuture<Long> asyncCount() {
            return executeAsync(new CountOperation(getNamespace(), findOp, getDocumentCodec()));
        }

        private boolean getMultiFromLimit() {
            if (limitSet) {
                if (findOp.getLimit() == 1) {
                    return false;
                } else if (findOp.getLimit() == 0) {
                    return true;
                } else {
                    throw new IllegalArgumentException("Update currently only supports a limit of either none or 1");
                }
            } else {
                return true;
            }
        }

        @Override
        public void asyncForEach(final AsyncBlock<? super T> block) {
            executeAsync(new QueryOperation<T>(getNamespace(), findOp, getDocumentCodec(), getCodec()))
            .register(new SingleResultCallback<MongoAsyncCursor<T>>() {
                @Override
                public void onResult(final MongoAsyncCursor<T> cursor, final MongoException e) {
                    cursor.start(block);  // TODO: deal with exceptions
                }
            });
        }

        @Override
        public <A extends Collection<? super T>> MongoFuture<A> asyncInto(final A target) {
            SingleResultFuture<A> future = new SingleResultFuture<A>();

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
                public void apply(final T t) {
                    target.add(t);
                }
            });
        }
    }

    private class MongoCollectionPipeline implements MongoPipeline<T> {
        private final List<Document> pipeline;

        private MongoCollectionPipeline() {
            pipeline = new ArrayList<Document>();
        }

        public MongoCollectionPipeline(final MongoCollectionPipeline from) {
            pipeline = new ArrayList<Document>(from.pipeline);
        }

        @Override
        public MongoPipeline<T> find(final Document criteria) {
            MongoCollectionPipeline newPipeline = new MongoCollectionPipeline(this);
            newPipeline.pipeline.add(new Document("$match", criteria));
            return newPipeline;
        }

        @Override
        public MongoPipeline<T> sort(final Document sortCriteria) {
            MongoCollectionPipeline newPipeline = new MongoCollectionPipeline(this);
            newPipeline.pipeline.add(new Document("$sort", sortCriteria));
            return newPipeline;
        }

        @Override
        public MongoPipeline<T> skip(final long skip) {
            MongoCollectionPipeline newPipeline = new MongoCollectionPipeline(this);
            newPipeline.pipeline.add(new Document("$skip", skip));
            return newPipeline;
        }

        @Override
        public MongoPipeline<T> limit(final long limit) {
            MongoCollectionPipeline newPipeline = new MongoCollectionPipeline(this);
            newPipeline.pipeline.add(new Document("$limit", limit));
            return newPipeline;
        }

        @Override
        public MongoPipeline<T> project(final Document projection) {
            MongoCollectionPipeline newPipeline = new MongoCollectionPipeline(this);
            newPipeline.pipeline.add(new Document("$project", projection));
            return newPipeline;
        }

        @Override
        public MongoPipeline<T> group(final Document group) {
            MongoCollectionPipeline newPipeline = new MongoCollectionPipeline(this);
            newPipeline.pipeline.add(new Document("$group", group));
            return newPipeline;
        }

        @Override
        public MongoPipeline<T> unwind(final String field) {
            MongoCollectionPipeline newPipeline = new MongoCollectionPipeline(this);
            newPipeline.pipeline.add(new Document("$unwind", field));
            return newPipeline;
        }

        @Override
        public <U> MongoIterable<U> map(final Function<T, U> mapper) {
            return new MappingIterable<T, U>(this, mapper);
        }

        @Override
        public void asyncForEach(final AsyncBlock<? super T> block) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <A extends Collection<? super T>> MongoFuture<A> asyncInto(final A target) {
            throw new UnsupportedOperationException();
        }

        @Override
        @SuppressWarnings("unchecked")
        public MongoCursor<T> iterator() {
            return execute(new AggregateOperation<T>(getNamespace(), pipeline, codec, AggregationOptions.builder().build(),
                                                     options.getReadPreference()));
        }

        @Override
        public void forEach(final Block<? super T> block) {
            MongoCursor<T> cursor = iterator();
            try {
                while (cursor.hasNext()) {
                    block.apply(cursor.next());
                }
            } finally {
                cursor.close();
            }
        }

        @Override
        public <A extends Collection<? super T>> A into(final A target) {
            forEach(new Block<T>() {
                @Override
                public void apply(final T t) {
                    target.add(t);
                }
            });
            return target;
        }
    }
}
