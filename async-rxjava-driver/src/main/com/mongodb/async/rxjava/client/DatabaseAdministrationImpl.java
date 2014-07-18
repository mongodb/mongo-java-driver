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

package com.mongodb.async.rxjava.client;

import org.mongodb.CreateCollectionOptions;
import org.mongodb.MongoFuture;
import rx.Observable;
import rx.functions.Func1;

import java.util.List;

/**
 * The administrative commands that can be run against a selected database.  Application developers should not normally need to call these
 * methods.
 *
 * @since 3.0
 */
public class DatabaseAdministrationImpl implements DatabaseAdministration {

    private final com.mongodb.async.client.DatabaseAdministration wrapped;

    DatabaseAdministrationImpl(final com.mongodb.async.client.DatabaseAdministration wrapped) {
        this.wrapped = wrapped;
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
    public Observable<String> getCollectionNames() {
        return Observable.concat(
            Observable.create(
                new OnSubscribeAdapter<List<String>>(
                    new OnSubscribeAdapter.FutureFunction<List<String>>() {
                        @Override
                        public MongoFuture<List<String>> apply() {
                            return wrapped.getCollectionNames();
                        }
                    }
                )
            ).map(new Func1<List<String>, Observable<String>>() {
                @Override
                public Observable<String> call(final List<String> strings) {
                    return Observable.from(strings);
                }
            })
        );
    }

    @Override
    public Observable<Void> createCollection(final String collectionName) {
        return Observable.create(new OnSubscribeAdapter<Void>(new OnSubscribeAdapter.FutureFunction<Void>() {
            @Override
            public MongoFuture<Void> apply() {
                return wrapped.createCollection(collectionName);
            }
        }));
    }

    @Override
    public Observable<Void> createCollection(final CreateCollectionOptions createCollectionOptions) {
        return Observable.create(new OnSubscribeAdapter<Void>(new OnSubscribeAdapter.FutureFunction<Void>() {
            @Override
            public MongoFuture<Void> apply() {
                return wrapped.createCollection(createCollectionOptions);
            }
        }));
    }

    @Override
    public Observable<Void> renameCollection(final String oldCollectionName, final String newCollectionName) {
        return Observable.create(new OnSubscribeAdapter<Void>(new OnSubscribeAdapter.FutureFunction<Void>() {
            @Override
            public MongoFuture<Void> apply() {
                return wrapped.renameCollection(oldCollectionName, newCollectionName);
            }
        }));
    }

    @Override
    public Observable<Void> renameCollection(final String oldCollectionName, final String newCollectionName, final boolean dropTarget) {
        return Observable.create(new OnSubscribeAdapter<Void>(new OnSubscribeAdapter.FutureFunction<Void>() {
            @Override
            public MongoFuture<Void> apply() {
                return wrapped.renameCollection(oldCollectionName, newCollectionName, dropTarget);
            }
        }));
    }

}
