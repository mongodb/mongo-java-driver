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

package com.mongodb.async.client;

import com.mongodb.Block;
import com.mongodb.CursorFlag;
import com.mongodb.Function;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.async.MongoAsyncCursor;
import com.mongodb.async.MongoFuture;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.SingleResultFuture;
import com.mongodb.client.model.FindModel;
import com.mongodb.codecs.CollectibleCodec;
import com.mongodb.operation.AsyncReadOperation;
import com.mongodb.operation.AsyncWriteOperation;
import com.mongodb.operation.CountOperation;
import com.mongodb.operation.InsertOperation;
import com.mongodb.operation.InsertRequest;
import com.mongodb.operation.QueryOperation;
import com.mongodb.operation.RemoveOperation;
import com.mongodb.operation.RemoveRequest;
import com.mongodb.operation.ReplaceOperation;
import com.mongodb.operation.ReplaceRequest;
import com.mongodb.operation.UpdateOperation;
import com.mongodb.operation.UpdateRequest;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.codecs.Codec;
import org.mongodb.ConvertibleToDocument;
import org.mongodb.Document;
import org.mongodb.WriteResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class MongoCollectionImpl<T> implements MongoCollection<T> {
    private final MongoNamespace namespace;
    private final Codec<T> codec;
    private final MongoCollectionOptions options;
    private final MongoClientImpl client;

    public MongoCollectionImpl(final MongoNamespace namespace, final Codec<T> codec, final MongoCollectionOptions options,
                               final MongoClientImpl client) {

        this.namespace = namespace;
        this.codec = codec;
        this.options = options;
        this.client = client;
    }

    @Override
    public String getName() {
        return namespace.getCollectionName();
    }

    @Override
    public MongoNamespace getNamespace() {
        return namespace;
    }

    @Override
    public MongoCollectionOptions getOptions() {
        return options;
    }

    @Override
    public Codec<T> getCodec() {
        return codec;
    }

    @Override
    public MongoView<T> find(final Document filter) {
        return new MongoCollectionView().find(filter);
    }

    @Override
    @SuppressWarnings("unchecked")
    public MongoFuture<WriteResult> insert(final T document) {
        notNull("document", document);
        return insert(asList(document));
    }

    @Override
    public MongoFuture<WriteResult> insert(final List<T> documents) {
        notNull("documents", documents);
        List<InsertRequest<T>> insertRequests = new ArrayList<InsertRequest<T>>();
        for (T document : documents) {
            if (getCodec() instanceof CollectibleCodec) {
                ((CollectibleCodec<T>) getCodec()).generateIdIfAbsentFromDocument(document);
            }
            insertRequests.add(new InsertRequest<T>(document));
        }
        return execute(new InsertOperation<T>(getNamespace(), true, options.getWriteConcern(), insertRequests,
                                              getCodec()));
    }

    @Override
    public MongoFuture<WriteResult> save(final T document) {
        if (!(codec instanceof CollectibleCodec)) {
            throw new UnsupportedOperationException();
        }
        CollectibleCodec<T> collectibleCodec = (CollectibleCodec<T>) codec;
        if (!collectibleCodec.documentHasId(document)) {
            return insert(document);
        } else {
            return new MongoCollectionView().find(new BsonDocument("_id", collectibleCodec.getDocumentId(document)))
                                            .upsert()
                                            .replace(document);
        }
    }

    @Override
    public CollectionAdministration tools() {
        return new CollectionAdministrationImpl(client, namespace);
    }

    <V> MongoFuture<V> execute(final AsyncWriteOperation<V> writeOperation) {
        return client.execute(writeOperation);
    }

    <V> MongoFuture<V> execute(final AsyncReadOperation<V> readOperation, final ReadPreference readPreference) {
        return client.execute(readOperation, readPreference);
    }

    private class MongoCollectionView implements MongoView<T> {
        private final FindModel<BsonDocument> find = new FindModel<BsonDocument>();
        private final ReadPreference readPreference = options.getReadPreference();
        private boolean upsert;

        @Override
        public MongoView<T> cursorFlags(final EnumSet<CursorFlag> flags) {
            find.cursorFlags(flags);
            return this;
        }

        @Override
        public MongoFuture<T> one() {
            QueryOperation<T> queryOperation = createQueryOperation();
            queryOperation.setBatchSize(-1);

            final SingleResultFuture<T> retVal = new SingleResultFuture<T>();
            execute(queryOperation, readPreference)
            .register(new
                      SingleResultCallback<MongoAsyncCursor<T>>() {
                          @Override
                          public void onResult(final MongoAsyncCursor<T> cursor, final MongoException e) {
                              if (e != null) {
                                  retVal.init(null, e);
                              } else {
                                  cursor.forEach(new Block<T>() {
                                      @Override
                                      public void apply(final T t) {
                                          retVal.init(t, null);
                                      }
                                  }).register(new SingleResultCallback<Void>() {
                                      @Override
                                      public void onResult(final Void result, final MongoException e) {
                                          if (!retVal.isDone()) {
                                              retVal.init(null, e);
                                          }
                                      }
                                  });
                              }
                          }
                      });
            return retVal;
        }

        @Override
        public MongoFuture<Long> count() {
            CountOperation operation = new CountOperation(getNamespace());
            operation.setCriteria(find.getCriteria());
            operation.setSkip(find.getSkip());
            operation.setLimit(find.getLimit());
            operation.setMaxTime(find.getMaxTime(MILLISECONDS), MILLISECONDS);
            return execute(operation, readPreference);
        }

        @Override
        public MongoView<T> find(final Document filter) {
            find.criteria(new BsonDocumentWrapper<Document>(filter, options.getDocumentCodec()));
            return this;
        }

        @Override
        public MongoView<T> find(final ConvertibleToDocument filter) {
            return find(filter.toDocument());
        }

        MongoView<T> find(final BsonDocument filter) {
            find.criteria(filter);
            return this;
        }

        @Override
        public MongoView<T> sort(final Document sortCriteria) {
            find.sort(new BsonDocumentWrapper<Document>(sortCriteria, options.getDocumentCodec()));
            return this;
        }

        @Override
        public MongoView<T> sort(final ConvertibleToDocument sortCriteria) {
            return sort(sortCriteria.toDocument());
        }

        @Override
        public MongoView<T> skip(final int skip) {
            find.skip(skip);
            return this;
        }

        @Override
        public MongoView<T> limit(final int limit) {
            find.limit(limit);
            return this;
        }

        @Override
        public MongoView<T> fields(final Document selector) {
            find.projection(new BsonDocumentWrapper<Document>(selector, options.getDocumentCodec()));
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
        public MongoFuture<Void> forEach(final Block<? super T> block) {
            final SingleResultFuture<Void> retVal = new SingleResultFuture<Void>();
            execute(createQueryOperation(), readPreference)
            .register(new
                      SingleResultCallback<MongoAsyncCursor<T>>() {
                          @Override
                          public void onResult(final MongoAsyncCursor<T> cursor, final MongoException e) {
                              if (e != null) {
                                  retVal.init(null, e);
                              } else {
                                  cursor.forEach(new Block<T>() {
                                      @Override
                                      public void apply(final T t) {
                                          block.apply(t);
                                      }
                                  }).register(new SingleResultCallback<Void>() {
                                      @Override
                                      public void onResult(final Void result, final MongoException e) {
                                          if (e != null) {
                                              retVal.init(null, e);
                                          } else {
                                              retVal.init(null, null);
                                          }
                                      }
                                  });
                              }
                          }
                      });
            return retVal;
        }

        @Override
        public <A extends Collection<? super T>> MongoFuture<A> into(final A target) {
            final SingleResultFuture<A> future = new SingleResultFuture<A>();
            forEach(new Block<T>() {
                @Override
                public void apply(final T t) {
                    target.add(t);
                }
            }).register(new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final MongoException e) {
                    if (e != null) {
                        future.init(null, e);
                    } else {
                        future.init(target, null);
                    }
                }
            });
            return future;
        }

        @Override
        public <U> MongoIterable<U> map(final Function<T, U> mapper) {
            return new MappingIterable<T, U>(this, mapper);
        }

        @Override
        @SuppressWarnings("unchecked")
        public MongoFuture<WriteResult> replace(final T replacement) {
            notNull("replacement", replacement);
            return execute(new ReplaceOperation<T>(getNamespace(), true, options.getWriteConcern(),
                                                   asList(new ReplaceRequest<T>(find.getCriteria(), replacement).upsert(upsert)),
                                                   getCodec()));
        }

        @Override
        public MongoFuture<WriteResult> update(final Document updateOperations) {
            notNull("updateOperations", updateOperations);
            return execute(new UpdateOperation(getNamespace(), true, options.getWriteConcern(),
                                               asList(new UpdateRequest(find.getCriteria(),
                                                                        new BsonDocumentWrapper<Document>(updateOperations,
                                                                                                          options.getDocumentCodec()))
                                                      .upsert(upsert).multi(true))
            ));
        }

        @Override
        public MongoFuture<WriteResult> updateOne(final Document updateOperations) {
            notNull("updateOperations", updateOperations);
            return execute(new UpdateOperation(getNamespace(), true, options.getWriteConcern(),
                                               asList(new UpdateRequest(find.getCriteria(),
                                                                        new BsonDocumentWrapper<Document>(updateOperations,
                                                                                                          options.getDocumentCodec()))
                                                      .upsert(upsert).multi(false))
            ));
        }

        @Override
        public MongoFuture<WriteResult> remove() {
            return execute(new RemoveOperation(getNamespace(), true, options.getWriteConcern(),
                                               asList(new RemoveRequest(find.getCriteria()).multi(true))));
        }

        @Override
        public MongoFuture<WriteResult> removeOne() {
            return execute(new RemoveOperation(getNamespace(), true, options.getWriteConcern(),
                                               asList(new RemoveRequest(find.getCriteria()).multi(false))));
        }

        private QueryOperation<T> createQueryOperation() {
            QueryOperation<T> operation = new QueryOperation<T>(getNamespace(), getCodec());
            operation.setCriteria(asBson(find.getCriteria()));
            operation.setBatchSize(find.getBatchSize());
            operation.setCursorFlags(find.getCursorFlags());
            operation.setSkip(find.getSkip());
            operation.setLimit(find.getLimit());
            operation.setMaxTime(find.getMaxTime(MILLISECONDS), MILLISECONDS);
            operation.setModifiers(asBson(find.getModifiers()));
            operation.setProjection(asBson(find.getProjection()));
            operation.setSort(asBson(find.getSort()));
            return operation;
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        private BsonDocument asBson(final Object document) {
            if (document == null) {
                return null;
            }
            if (document instanceof BsonDocument) {
                return (BsonDocument) document;
            } else if (document instanceof Document) {
                return new BsonDocumentWrapper(document, options.getDocumentCodec());
            } else {
                throw new IllegalArgumentException("No encoder for class " + document.getClass());
            }

        }

    }
}
