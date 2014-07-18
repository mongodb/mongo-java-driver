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

package org.mongodb.async.rxjava;

import com.mongodb.MongoException;
import com.mongodb.connection.SingleResultCallback;
import com.mongodb.operation.QueryFlag;
import org.bson.codecs.Codec;
import org.mongodb.Block;
import org.mongodb.ConvertibleToDocument;
import org.mongodb.Document;
import org.mongodb.MongoCollectionOptions;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteResult;
import rx.Observable;
import rx.Subscriber;

import java.util.EnumSet;
import java.util.List;

import static org.mongodb.async.rxjava.OnSubscribeAdapter.FutureFunction;
import static rx.Observable.OnSubscribe;

class MongoCollectionImpl<T> implements MongoCollection<T> {
    private final com.mongodb.async.client.MongoCollection<T> wrapped;

    public MongoCollectionImpl(final com.mongodb.async.client.MongoCollection<T> wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public String getName() {
        return wrapped.getName();
    }

    @Override
    public MongoNamespace getNamespace() {
        return wrapped.getNamespace();
    }

    @Override
    public MongoCollectionOptions getOptions() {
        return wrapped.getOptions();
    }

    @Override
    public Codec<T> getCodec() {
        return wrapped.getCodec();
    }

    @Override
    public MongoView<T> find(final Document filter) {
        return new MongoCollectionView(filter);
    }

    @Override
    public Observable<WriteResult> insert(final T document) {
        return Observable.create(new OnSubscribeAdapter<WriteResult>(new FutureFunction<WriteResult>() {
            @Override
            public MongoFuture<WriteResult> apply() {
                return wrapped.insert(document);
            }
        }));
    }

    @Override
    public Observable<WriteResult> insert(final List<T> documents) {
        return Observable.create(new OnSubscribeAdapter<WriteResult>(new FutureFunction<WriteResult>() {
            @Override
            public MongoFuture<WriteResult> apply() {
                return wrapped.insert(documents);
            }
        }));
    }

    @Override
    public Observable<WriteResult> save(final T document) {
        return Observable.create(new OnSubscribeAdapter<WriteResult>(new FutureFunction<WriteResult>() {
            @Override
            public MongoFuture<WriteResult> apply() {
                return wrapped.save(document);
            }
        }));
    }

    @Override
    public CollectionAdministration tools() {
        return new CollectionAdministrationImpl(wrapped.tools());
    }

    private final class MongoCollectionView implements MongoView<T> {
        private final com.mongodb.async.client.MongoView<T> wrappedView;

        private MongoCollectionView(final Document filter) {
            wrappedView = wrapped.find(filter);
        }

        @Override
        public MongoView<T> cursorFlags(final EnumSet<QueryFlag> flags) {
            wrappedView.cursorFlags(flags);
            return this;
        }

        @Override
        public MongoView<T> find(final Document filter) {
            wrappedView.find(filter);
            return this;
        }

        @Override
        public MongoView<T> find(final ConvertibleToDocument filter) {
            wrappedView.find(filter);
            return this;
        }

        @Override
        public MongoView<T> sort(final Document sortCriteria) {
            wrappedView.sort(sortCriteria);
            return this;
        }

        @Override
        public MongoView<T> sort(final ConvertibleToDocument sortCriteria) {
            wrappedView.sort(sortCriteria);
            return this;
        }

        @Override
        public MongoView<T> skip(final int skip) {
            wrappedView.skip(skip);
            return this;
        }

        @Override
        public MongoView<T> limit(final int limit) {
            wrappedView.limit(limit);
            return this;
        }

        @Override
        public MongoView<T> fields(final Document selector) {
            wrappedView.fields(selector);
            return this;
        }

        @Override
        public MongoView<T> fields(final ConvertibleToDocument selector) {
            wrappedView.fields(selector);
            return this;
        }

        @Override
        public MongoView<T> upsert() {
            wrappedView.upsert();
            return this;
        }

        @Override
        public Observable<T> one() {
            return Observable.create(new OnSubscribeAdapter<T>(new FutureFunction<T>() {
                @Override
                public MongoFuture<T> apply() {
                    return wrappedView.one();
                }
            }));
        }

        @Override
        public Observable<Long> count() {
            return Observable.create(new OnSubscribeAdapter<Long>(new FutureFunction<Long>() {
                @Override
                public MongoFuture<Long> apply() {
                    return wrappedView.count();
                }
            }));
        }

        @Override
        public Observable<T> forEach() {
            return Observable.create(new OnSubscribe<T>() {
                @Override
                public void call(final Subscriber<? super T> subscriber) {
                    wrappedView.forEach(new Block<T>() {
                        @Override
                        public void apply(final T t) {
                            subscriber.onNext(t);
                        }
                    }).register(new SingleResultCallback<Void>() {
                        @Override
                        public void onResult(final Void result, final MongoException e) {
                            if (e != null) {
                                subscriber.onError(e);
                            } else {
                                subscriber.onCompleted();
                            }
                        }
                    });
                }
            });
        }

        @Override
        public Observable<WriteResult> replace(final T replacement) {
            return Observable.create(new OnSubscribeAdapter<WriteResult>(new FutureFunction<WriteResult>() {
                @Override
                public MongoFuture<WriteResult> apply() {
                    return wrappedView.replace(replacement);
                }
            }));
        }

        @Override
        public Observable<WriteResult> update(final Document updateOperations) {
            return Observable.create(new OnSubscribeAdapter<WriteResult>(new FutureFunction<WriteResult>() {
                @Override
                public MongoFuture<WriteResult> apply() {
                    return wrappedView.update(updateOperations);
                }
            }));
        }

        @Override
        public Observable<WriteResult> updateOne(final Document updateOperations) {
            return Observable.create(new OnSubscribeAdapter<WriteResult>(new FutureFunction<WriteResult>() {
                @Override
                public MongoFuture<WriteResult> apply() {
                    return wrappedView.updateOne(updateOperations);
                }
            }));
        }

        @Override
        public Observable<WriteResult> remove() {
            return Observable.create(new OnSubscribeAdapter<WriteResult>(new FutureFunction<WriteResult>() {
                @Override
                public MongoFuture<WriteResult> apply() {
                    return wrappedView.remove();
                }
            }));
        }

        @Override
        public Observable<WriteResult> removeOne() {
            return Observable.create(new OnSubscribeAdapter<WriteResult>(new FutureFunction<WriteResult>() {
                @Override
                public MongoFuture<WriteResult> apply() {
                    return wrappedView.removeOne();
                }
            }));
        }
    }
}
