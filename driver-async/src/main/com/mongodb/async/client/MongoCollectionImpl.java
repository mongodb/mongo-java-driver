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
import com.mongodb.Function;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.async.MongoAsyncCursor;
import com.mongodb.async.MongoFuture;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.SingleResultFuture;
import com.mongodb.client.model.FindOptions;
import com.mongodb.codecs.CollectibleCodec;
import com.mongodb.operation.AsyncReadOperation;
import com.mongodb.operation.AsyncWriteOperation;
import com.mongodb.operation.CountOperation;
import com.mongodb.operation.DeleteOperation;
import com.mongodb.operation.DeleteRequest;
import com.mongodb.operation.FindOperation;
import com.mongodb.operation.InsertOperation;
import com.mongodb.operation.InsertRequest;
import com.mongodb.operation.UpdateOperation;
import com.mongodb.operation.UpdateRequest;
import com.mongodb.operation.WriteRequest;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.codecs.Codec;
import org.mongodb.ConvertibleToDocument;
import org.mongodb.Document;
import org.mongodb.WriteResult;

import java.util.ArrayList;
import java.util.Collection;
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
        List<InsertRequest> insertRequests = new ArrayList<InsertRequest>();
        for (T document : documents) {
            if (getCodec() instanceof CollectibleCodec) {
                ((CollectibleCodec<T>) getCodec()).generateIdIfAbsentFromDocument(document);
            }
            insertRequests.add(new InsertRequest(new BsonDocumentWrapper<T>(document, getCodec())));
        }
        return execute(new InsertOperation(getNamespace(), true, options.getWriteConcern(), insertRequests));
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
        private final FindOptions findOptions = new FindOptions();
        private final ReadPreference readPreference = options.getReadPreference();
        private BsonDocument criteria;
        private boolean upsert;

        @Override
        public MongoFuture<T> one() {
            FindOperation<T> findOperation = createQueryOperation().batchSize(-1);

            final SingleResultFuture<T> retVal = new SingleResultFuture<T>();
            execute(findOperation, readPreference)
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
            CountOperation operation = new CountOperation(getNamespace())
                                           .criteria(criteria)
                                           .skip(findOptions.getSkip())
                                           .limit(findOptions.getLimit())
                                           .maxTime(findOptions.getMaxTime(MILLISECONDS), MILLISECONDS);
            return execute(operation, readPreference);
        }

        @Override
        public MongoView<T> find(final Document filter) {
            criteria = new BsonDocumentWrapper<Document>(filter, options.getDocumentCodec());
            return this;
        }

        @Override
        public MongoView<T> find(final ConvertibleToDocument filter) {
            return find(filter.toDocument());
        }

        MongoView<T> find(final BsonDocument filter) {
            criteria = filter;
            return this;
        }

        @Override
        public MongoView<T> sort(final Document sortCriteria) {
            findOptions.sort(asBson(sortCriteria));
            return this;
        }

        @Override
        public MongoView<T> sort(final ConvertibleToDocument sortCriteria) {
            return sort(sortCriteria.toDocument());
        }

        @Override
        public MongoView<T> skip(final int skip) {
            findOptions.skip(skip);
            return this;
        }

        @Override
        public MongoView<T> limit(final int limit) {
            findOptions.limit(limit);
            return this;
        }

        @Override
        public MongoView<T> fields(final Document selector) {
            findOptions.projection(new BsonDocumentWrapper<Document>(selector, options.getDocumentCodec()));
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
            return execute(new UpdateOperation(getNamespace(), true, options.getWriteConcern(),
                                                   asList(new UpdateRequest(criteria, asBson(replacement), WriteRequest.Type.REPLACE)
                                                          .upsert(upsert))
            ));
        }

        @Override
        public MongoFuture<WriteResult> update(final Document updateOperations) {
            notNull("updateOperations", updateOperations);
            return execute(new UpdateOperation(getNamespace(), true, options.getWriteConcern(),
                                               asList(new UpdateRequest(criteria,
                                                                        new BsonDocumentWrapper<Document>(updateOperations,
                                                                                                          options.getDocumentCodec()),
                                                                        WriteRequest.Type.UPDATE)
                                                    .upsert(upsert).multi(true))
            ));
        }

        @Override
        public MongoFuture<WriteResult> updateOne(final Document updateOperations) {
            notNull("updateOperations", updateOperations);
            return execute(new UpdateOperation(getNamespace(), true, options.getWriteConcern(),
                                               asList(new UpdateRequest(criteria,
                                                                        new BsonDocumentWrapper<Document>(updateOperations,
                                                                                                          options.getDocumentCodec()),
                                                                        WriteRequest.Type.UPDATE)
                                                      .upsert(upsert).multi(false))
            ));
        }

        @Override
        public MongoFuture<WriteResult> remove() {
            return execute(new DeleteOperation(getNamespace(), true, options.getWriteConcern(),
                                               asList(new DeleteRequest(criteria).multi(true))));
        }

        @Override
        public MongoFuture<WriteResult> removeOne() {
            return execute(new DeleteOperation(getNamespace(), true, options.getWriteConcern(),
                                               asList(new DeleteRequest(criteria).multi(false))));
        }

        private FindOperation<T> createQueryOperation() {
            return new FindOperation<T>(getNamespace(), getCodec())
                       .criteria(criteria)
                       .batchSize(findOptions.getBatchSize())
                       .skip(findOptions.getSkip())
                       .limit(findOptions.getLimit())
                       .maxTime(findOptions.getMaxTime(MILLISECONDS), MILLISECONDS)
                       .modifiers(asBson(findOptions.getModifiers()))
                       .projection(asBson(findOptions.getProjection()))
                       .sort(asBson(findOptions.getSort()));
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
