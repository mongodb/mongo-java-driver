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

import com.mongodb.Block;
import com.mongodb.MongoException;
import com.mongodb.async.MongoFuture;
import com.mongodb.async.SingleResultCallback;
import rx.Observable;
import rx.Subscriber;

class OperationIterable<T> implements MongoIterable<T> {
    private final com.mongodb.async.client.MongoIterable<T> wrapped;

    public OperationIterable(final com.mongodb.async.client.MongoIterable<T> wrapped) {
        this.wrapped = wrapped;
    }
    @Override
    public Observable<T> first() {
        return Observable.create(new OnSubscribeAdapter<T>(new FutureBlock<T>() {
            @Override
            public MongoFuture<T> apply() {
                return wrapped.first();
            }
        }));
    }

    @Override
    public Observable<T> toObservable() {
        return Observable.create(new Observable.OnSubscribe<T>() {
            @Override
            public void call(final Subscriber<? super T> subscriber) {
                wrapped.forEach(new Block<T>() {
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

}
