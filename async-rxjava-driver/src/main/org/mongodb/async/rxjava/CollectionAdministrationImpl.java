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

import org.mongodb.Document;
import org.mongodb.Index;
import org.mongodb.MongoFuture;
import rx.Observable;
import rx.functions.Func1;

import java.util.List;

/**
 * Provides the functionality for a collection that is useful for administration, but not necessarily in the course of normal use of a
 * collection.
 *
 * @since 3.0
 */
public class CollectionAdministrationImpl implements CollectionAdministration {

    private final org.mongodb.async.CollectionAdministration wrapped;

    CollectionAdministrationImpl(final org.mongodb.async.CollectionAdministration wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public Observable<Void> createIndexes(final List<Index> indexes) {
        return Observable.create(new OnSubscribeAdapter<Void>(new OnSubscribeAdapter.FutureFunction<Void>() {
            @Override
            public MongoFuture<Void> apply() {
                return wrapped.createIndexes(indexes);
            }
        }));
    }

    @Override
    public Observable<Document> getIndexes() {
        return Observable.concat(
            Observable.create(
                 new OnSubscribeAdapter<List<Document>>(
                     new OnSubscribeAdapter.FutureFunction<List<Document>>() {
                         @Override
                         public MongoFuture<List<Document>>
                         apply() {
                             return wrapped.getIndexes();
                         }
                     }
                 )
            ).map(new Func1<List<Document>, Observable<Document>>() {
                @Override
                public Observable<Document> call(final List<Document> documents) {
                    return Observable.from(documents);
                }
            })
        );
    }

    @Override
    public Observable<Boolean> isCapped() {
        return Observable.create(new OnSubscribeAdapter<Boolean>(new OnSubscribeAdapter.FutureFunction<Boolean>() {
            @Override
            public MongoFuture<Boolean> apply() {
                return wrapped.isCapped();
            }
        }));
    }

    @Override
    public Observable<Document> getStatistics() {
        return Observable.create(new OnSubscribeAdapter<Document>(new OnSubscribeAdapter.FutureFunction<Document>() {
            @Override
            public MongoFuture<Document> apply() {
                return wrapped.getStatistics();
            }
        }));
    }

    @Override
    public Observable<Void> drop() {
        return Observable.create(new OnSubscribeAdapter<Void>(new OnSubscribeAdapter.FutureFunction<Void>() {
            @Override
            public MongoFuture<Void> apply() {
                return wrapped.drop();
            }
        }));
    }

    @Override
    public Observable<Void> dropIndex(final Index index) {
        return Observable.create(new OnSubscribeAdapter<Void>(new OnSubscribeAdapter.FutureFunction<Void>() {
            @Override
            public MongoFuture<Void> apply() {
                return wrapped.dropIndex(index);
            }
        }));
    }

    @Override
    public Observable<Void> dropIndexes() {
        return Observable.create(new OnSubscribeAdapter<Void>(new OnSubscribeAdapter.FutureFunction<Void>() {
            @Override
            public MongoFuture<Void> apply() {
                return wrapped.dropIndexes();
            }
        }));
    }
}
