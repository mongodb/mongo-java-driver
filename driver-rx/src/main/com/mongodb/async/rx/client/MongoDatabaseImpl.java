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

import com.mongodb.ReadPreference;
import com.mongodb.async.MongoFuture;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.options.OperationOptions;
import org.bson.Document;
import rx.Observable;
import rx.functions.Func1;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

class MongoDatabaseImpl implements MongoDatabase {
    private final com.mongodb.async.client.MongoDatabase wrapped;

    public MongoDatabaseImpl(final com.mongodb.async.client.MongoDatabase wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public String getName() {
        return wrapped.getName();
    }

    @Override
    public MongoCollection<Document> getCollection(final String collectionName) {
        return getCollection(collectionName, OperationOptions.builder().build());
    }

    @Override
    public MongoCollection<Document> getCollection(final String collectionName, final OperationOptions operationOptions) {
        return getCollection(collectionName, Document.class, operationOptions);
    }

    @Override
    public <T> MongoCollection<T> getCollection(final String collectionName, final Class<T> clazz) {
        return getCollection(collectionName, clazz, OperationOptions.builder().build());
    }

    @Override
    public <T> MongoCollection<T> getCollection(final String collectionName, final Class<T> clazz,
                                                final OperationOptions operationOptions) {
        return new MongoCollectionImpl<T>(wrapped.getCollection(collectionName, clazz, operationOptions));
    }

    @Override
    public Observable<Void> dropDatabase() {
        return Observable.create(new OnSubscribeAdapter<Void>(new FutureBlock<Void>() {
            @Override
            public MongoFuture<Void> apply() {
                return wrapped.dropDatabase();
            }
        }));
    }

    @Override
    public Observable<String> getCollectionNames() {
        return Observable.concat(
                                Observable.create(
                                                 new OnSubscribeAdapter<List<String>>(
                                                                                     new FutureBlock<List<String>>() {
                                                                                         @Override
                                                                                         public MongoFuture<List<String>> apply() {
                                                                                             return wrapped.getCollectionNames();
                                                                                         }
                                                                                     })
                                                 ).map(new Func1<List<String>, Observable<String>>() {
                                    @Override
                                    public Observable<String> call(final List<String> strings) {
                                        return Observable.from(strings);
                                    }
                                }));
    }

    @Override
    public Observable<Void> createCollection(final String collectionName) {
        return createCollection(collectionName, new CreateCollectionOptions());
    }

    @Override
    public Observable<Void> createCollection(final String collectionName, final CreateCollectionOptions createCollectionOptions) {
        return Observable.create(new OnSubscribeAdapter<Void>(new FutureBlock<Void>() {
            @Override
            public MongoFuture<Void> apply() {
                return wrapped.createCollection(collectionName, createCollectionOptions);
            }
        }));
    }

    @Override
    public Observable<Document> executeCommand(final Object command) {
        return executeCommand(command, Document.class);
    }

    @Override
    public Observable<Document> executeCommand(final Object command, final ReadPreference readPreference) {
        return executeCommand(command, readPreference, Document.class);
    }

    @Override
    public <T> Observable<T> executeCommand(final Object command, final Class<T> clazz) {
        notNull("command", command);
        return Observable.create(new OnSubscribeAdapter<T>(new FutureBlock<T>() {
            @Override
            public MongoFuture<T> apply() {
                return wrapped.executeCommand(command, clazz);
            }
        }));
    }

    @Override
    public <T> Observable<T> executeCommand(final Object command, final ReadPreference readPreference, final Class<T> clazz) {
        notNull("command", command);
        notNull("readPreference", readPreference);
        return Observable.create(new OnSubscribeAdapter<T>(new FutureBlock<T>() {
            @Override
            public MongoFuture<T> apply() {
                return wrapped.executeCommand(command, readPreference, clazz);
            }
        }));
    }

    @Override
    public OperationOptions getOptions() {
        return wrapped.getOptions();
    }

}
