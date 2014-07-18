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

import com.mongodb.MongoException;
import com.mongodb.connection.SingleResultCallback;
import org.mongodb.MongoFuture;
import rx.Observable;
import rx.Subscriber;

class OnSubscribeAdapter<R> implements Observable.OnSubscribe<R> {
    private final FutureFunction<R> func;

    public OnSubscribeAdapter(final FutureFunction<R> func) {
        this.func = func;
    }

    @Override
    public void call(final Subscriber<? super R> subscriber) {
        func.apply().register(new SingleResultCallback<R>() {
            @Override
            public void onResult(final R result, final MongoException e) {
                if (e != null) {
                    subscriber.onError(e);
                } else {
                    subscriber.onNext(result);
                    subscriber.onCompleted();
                }
            }
        });
    }

    /**
     * Apply a function yielding a future for an appropriate result object.
     *
     * @param <R> the type of result objects from the {@code apply} operation.
     */
    interface FutureFunction<R> {
        /**
         * Yield an appropriate result object
         *
         * @return the function result
         */
        MongoFuture<R> apply();
    }
}
