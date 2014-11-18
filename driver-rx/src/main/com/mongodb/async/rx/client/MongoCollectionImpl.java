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

package com.mongodb.async.rx.client;

import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcernResult;
import com.mongodb.async.MongoFuture;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.AggregateOptions;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.DistinctOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.MapReduceOptions;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.options.OperationOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonDocument;
import org.bson.Document;
import rx.Observable;
import rx.functions.Func1;

import java.util.List;

class MongoCollectionImpl<T> implements MongoCollection<T> {

    private final com.mongodb.async.client.MongoCollection<T> wrapped;

    public MongoCollectionImpl(final com.mongodb.async.client.MongoCollection<T> wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public MongoNamespace getNamespace() {
        return wrapped.getNamespace();
    }

    @Override
    public OperationOptions getOptions() {
        return wrapped.getOptions();
    }

    @Override
    public Observable<Long> count() {
        return count(new BsonDocument(), new CountOptions());
    }

    @Override
    public Observable<Long> count(final Object filter) {
        return count(filter, new CountOptions());
    }

    @Override
    public Observable<Long> count(final Object filter, final CountOptions options) {
        return Observable.create(new OnSubscribeAdapter<Long>(new FutureBlock<Long>() {
            @Override
            public MongoFuture<Long> apply() {
                return wrapped.count(filter, options);
            }
        }));
    }

    @Override
    public Observable<Object> distinct(final String fieldName, final Object filter) {
        return distinct(fieldName, filter, new DistinctOptions());
    }

    @Override
    public Observable<Object> distinct(final String fieldName, final Object filter, final DistinctOptions distinctOptions) {
        return Observable.concat(
                                Observable.create(
                                                 new OnSubscribeAdapter<List<Object>>(
                                                                                     new FutureBlock<List<Object>>() {
                                                                                         @Override
                                                                                         public MongoFuture<List<Object>> apply() {
                                                                                             return wrapped.distinct(fieldName, filter,
                                                                                                                     distinctOptions);
                                                                                         }
                                                                                     })
                                                 ).map(new Func1<List<Object>, Observable<Object>>() {
                                    @Override
                                    public Observable<Object> call(final List<Object> distinctValues) {
                                        return Observable.from(distinctValues);
                                    }
                                }));
    }

    @Override
    public FindFluent<T> find() {
        return new FindFluentImpl<T>(wrapped.find());
    }

    @Override
    public <C> FindFluent<C> find(final Class<C> clazz) {
        return new FindFluentImpl<C>(wrapped.find(clazz));
    }

    @Override
    public FindFluent<T> find(final Object filter) {
        return new FindFluentImpl<T>(wrapped.find(filter));
    }

    @Override
    public <C> FindFluent<C> find(final Object filter, final Class<C> clazz) {
        return new FindFluentImpl<C>(wrapped.find(filter, clazz));
    }

    public MongoIterable<Document> aggregate(final List<?> pipeline) {
        return aggregate(pipeline, new AggregateOptions(), Document.class);
    }

    public <C> MongoIterable<C> aggregate(final List<?> pipeline, final Class<C> clazz) {
        return aggregate(pipeline, new AggregateOptions(), clazz);
    }

    public MongoIterable<Document> aggregate(final List<?> pipeline, final AggregateOptions options) {
        return aggregate(pipeline, options, Document.class);
    }

    public <C> MongoIterable<C> aggregate(final List<?> pipeline, final AggregateOptions options, final Class<C> clazz) {
        return new OperationIterable<C>(wrapped.aggregate(pipeline, options, clazz));
    }

    @Override
    public MongoIterable<Document> mapReduce(final String mapFunction, final String reduceFunction) {
        return mapReduce(mapFunction, reduceFunction, new MapReduceOptions());
    }

    @Override
    public MongoIterable<Document> mapReduce(final String mapFunction, final String reduceFunction, final MapReduceOptions options) {
        return mapReduce(mapFunction, reduceFunction, options, Document.class);
    }

    @Override
    public <C> MongoIterable<C> mapReduce(final String mapFunction, final String reduceFunction, final Class<C> clazz) {
        return mapReduce(mapFunction, reduceFunction, new MapReduceOptions(), clazz);
    }

    @Override
    public <C> MongoIterable<C> mapReduce(final String mapFunction, final String reduceFunction, final MapReduceOptions options,
                                          final Class<C> clazz) {
        return new OperationIterable<C>(wrapped.mapReduce(mapFunction, reduceFunction, options, clazz));
    }

    @Override
    public Observable<BulkWriteResult> bulkWrite(final List<? extends WriteModel<? extends T>> requests) {
        return bulkWrite(requests, new BulkWriteOptions());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Observable<BulkWriteResult> bulkWrite(final List<? extends WriteModel<? extends T>> requests, final BulkWriteOptions options) {
        return Observable.create(new OnSubscribeAdapter<BulkWriteResult>(new FutureBlock<BulkWriteResult>() {
            @Override
            public MongoFuture<BulkWriteResult> apply() {
                return wrapped.bulkWrite(requests, options);
            }
        }));
    }

    @Override
    public Observable<WriteConcernResult> insertOne(final T document) {
        return Observable.create(new OnSubscribeAdapter<WriteConcernResult>(new FutureBlock<WriteConcernResult>() {
            @Override
            public MongoFuture<WriteConcernResult> apply() {
                return wrapped.insertOne(document);
            }
        }));
    }

    @Override
    public Observable<WriteConcernResult> insertMany(final List<? extends T> documents) {
        return insertMany(documents, new InsertManyOptions());
    }

    @Override
    public Observable<WriteConcernResult> insertMany(final List<? extends T> documents, final InsertManyOptions options) {
        return Observable.create(new OnSubscribeAdapter<WriteConcernResult>(new FutureBlock<WriteConcernResult>() {
            @Override
            public MongoFuture<WriteConcernResult> apply() {
                return wrapped.insertMany(documents, options);
            }
        }));
    }

    @Override
    public Observable<DeleteResult> deleteOne(final Object filter) {
        return Observable.create(new OnSubscribeAdapter<DeleteResult>(new FutureBlock<DeleteResult>() {
            @Override
            public MongoFuture<DeleteResult> apply() {
                return wrapped.deleteOne(filter);
            }
        }));
    }

    @Override
    public Observable<DeleteResult> deleteMany(final Object filter) {
        return Observable.create(new OnSubscribeAdapter<DeleteResult>(new FutureBlock<DeleteResult>() {
            @Override
            public MongoFuture<DeleteResult> apply() {
                return wrapped.deleteMany(filter);
            }
        }));
    }

    @Override
    public Observable<UpdateResult> replaceOne(final Object filter, final T replacement) {
        return replaceOne(filter, replacement, new UpdateOptions());
    }

    @Override
    public Observable<UpdateResult> replaceOne(final Object filter, final T replacement, final UpdateOptions options) {
        return Observable.create(new OnSubscribeAdapter<UpdateResult>(new FutureBlock<UpdateResult>() {
            @Override
            public MongoFuture<UpdateResult> apply() {
                return wrapped.replaceOne(filter, replacement, options);
            }
        }));
    }

    @Override
    public Observable<UpdateResult> updateOne(final Object filter, final Object update) {
        return updateOne(filter, update, new UpdateOptions());
    }

    @Override
    public Observable<UpdateResult> updateOne(final Object filter, final Object update, final UpdateOptions options) {
        return Observable.create(new OnSubscribeAdapter<UpdateResult>(new FutureBlock<UpdateResult>() {
            @Override
            public MongoFuture<UpdateResult> apply() {
                return wrapped.updateOne(filter, update, options);
            }
        }));
    }

    @Override
    public Observable<UpdateResult> updateMany(final Object filter, final Object update) {
        return updateMany(filter, update, new UpdateOptions());
    }

    @Override
    public Observable<UpdateResult> updateMany(final Object filter, final Object update, final UpdateOptions options) {
        return Observable.create(new OnSubscribeAdapter<UpdateResult>(new FutureBlock<UpdateResult>() {
            @Override
            public MongoFuture<UpdateResult> apply() {
                return wrapped.updateMany(filter, update, options);
            }
        }));
    }

    @Override
    public Observable<T> findOneAndDelete(final Object filter) {
        return findOneAndDelete(filter, new FindOneAndDeleteOptions());
    }

    @Override
    public Observable<T> findOneAndDelete(final Object filter, final FindOneAndDeleteOptions options) {
        return Observable.create(new OnSubscribeAdapter<T>(new FutureBlock<T>() {
            @Override
            public MongoFuture<T> apply() {
                return wrapped.findOneAndDelete(filter, options);
            }
        }));
    }

    @Override
    public Observable<T> findOneAndReplace(final Object filter, final T replacement) {
        return findOneAndReplace(filter, replacement, new FindOneAndReplaceOptions());
    }

    @Override
    public Observable<T> findOneAndReplace(final Object filter, final T replacement, final FindOneAndReplaceOptions options) {
        return Observable.create(new OnSubscribeAdapter<T>(new FutureBlock<T>() {
            @Override
            public MongoFuture<T> apply() {
                return wrapped.findOneAndReplace(filter, replacement, options);
            }
        }));
    }

    @Override
    public Observable<T> findOneAndUpdate(final Object filter, final Object update) {
        return findOneAndUpdate(filter, update, new FindOneAndUpdateOptions());
    }

    @Override
    public Observable<T> findOneAndUpdate(final Object filter, final Object update, final FindOneAndUpdateOptions options) {
        return Observable.create(new OnSubscribeAdapter<T>(new FutureBlock<T>() {
            @Override
            public MongoFuture<T> apply() {
                return wrapped.findOneAndUpdate(filter, update, options);
            }
        }));
    }

    @Override
    public Observable<Void> dropCollection() {
        return Observable.create(new OnSubscribeAdapter<Void>(new FutureBlock<Void>() {
            @Override
            public MongoFuture<Void> apply() {
                return wrapped.dropCollection();
            }
        }));
    }

    @Override
    public Observable<Void> createIndex(final Object key) {
        return createIndex(key, new CreateIndexOptions());
    }

    @Override
    public Observable<Void> createIndex(final Object key, final CreateIndexOptions options) {
        return Observable.create(new OnSubscribeAdapter<Void>(new FutureBlock<Void>() {
            @Override
            public MongoFuture<Void> apply() {
                return wrapped.createIndex(key, options);
            }
        }));
    }

    @Override
    public Observable<Document> getIndexes() {
        return getIndexes(Document.class);
    }

    @Override
    public <C> Observable<C> getIndexes(final Class<C> clazz) {
        return Observable.concat(
                                Observable.create(
                                                 new OnSubscribeAdapter<List<C>>(
                                                                                new FutureBlock<List<C>>() {
                                                                                    @Override
                                                                                    public MongoFuture<List<C>> apply() {
                                                                                        return wrapped.getIndexes(clazz);
                                                                                    }
                                                                                })
                                                 ).map(new Func1<List<C>, Observable<C>>() {
                                    @Override
                                    public Observable<C> call(final List<C> indexes) {
                                        return Observable.from(indexes);
                                    }
                                }));
    }

    @Override
    public Observable<Void> dropIndex(final String indexName) {
        return Observable.create(new OnSubscribeAdapter<Void>(new FutureBlock<Void>() {
            @Override
            public MongoFuture<Void> apply() {
                return wrapped.dropIndex(indexName);
            }
        }));
    }

    @Override
    public Observable<Void> dropIndexes() {
        return dropIndex("*");
    }

    @Override
    public Observable<Void> renameCollection(final MongoNamespace newCollectionNamespace) {
        return renameCollection(newCollectionNamespace, new RenameCollectionOptions());
    }

    @Override
    public Observable<Void> renameCollection(final MongoNamespace newCollectionNamespace,
                                              final RenameCollectionOptions options) {
        return Observable.create(new OnSubscribeAdapter<Void>(new FutureBlock<Void>() {
            @Override
            public MongoFuture<Void> apply() {
                return wrapped.renameCollection(newCollectionNamespace, options);
            }
        }));
    }
}
