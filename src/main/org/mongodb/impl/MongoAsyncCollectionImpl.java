/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
 *
 */

package org.mongodb.impl;

import org.mongodb.MongoAsyncCollection;
import org.mongodb.MongoAsyncCursor;
import org.mongodb.MongoCollection;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoFindAndRemove;
import org.mongodb.operation.MongoFindAndReplace;
import org.mongodb.operation.MongoFindAndUpdate;
import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoRemove;
import org.mongodb.result.InsertResult;
import org.mongodb.result.RemoveResult;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MongoAsyncCollectionImpl<T> extends MongoCollectionBaseImpl<T> implements MongoAsyncCollection<T> {
    private final MongoCollection<T> proxiedCollection;

    public MongoAsyncCollectionImpl(final MongoCollection<T> proxiedCollection) {
        super(proxiedCollection);
        this.proxiedCollection = proxiedCollection;
    }

    @Override
    public MongoAsyncCursor<T> find(final MongoFind find) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<T> findOne(final MongoFind find) {
        return new AsyncFuture<T>() {
            @Override
            protected T doGet() {
                return proxiedCollection.findOne(find);
            }
        };
    }

    @Override
    public Future<Long> count() {
        return new AsyncFuture<Long>() {
            @Override
            protected Long doGet() {
                return proxiedCollection.count();
            }
        };
    }

    @Override
    public Future<Long> count(final MongoFind find) {
        return new AsyncFuture<Long>() {
            @Override
            protected Long doGet() {
                return proxiedCollection.count(find);
            }
        };
    }

    @Override
    public Future<T> findAndUpdate(final MongoFindAndUpdate findAndUpdate) {
        return new AsyncFuture<T>() {
            @Override
            protected T doGet() {
                return proxiedCollection.findAndUpdate(findAndUpdate);
            }
        };
    }

    @Override
    public Future<T> findAndReplace(final MongoFindAndReplace<T> findAndReplace) {
        return new AsyncFuture<T>() {
            @Override
            protected T doGet() {
                return proxiedCollection.findAndReplace(findAndReplace);
            }
        };
    }

    @Override
    public Future<T> findAndRemove(final MongoFindAndRemove findAndRemove) {
        return new AsyncFuture<T>() {
            @Override
            protected T doGet() {
                return proxiedCollection.findAndRemove(findAndRemove);
            }
        };
    }

    @Override
    public Future<InsertResult> insert(final MongoInsert<T> insert) {
        return new AsyncFuture<InsertResult>() {
            @Override
            protected InsertResult doGet() {
                return proxiedCollection.insert(insert);
            }
        };
    }

    @Override
    public Future<RemoveResult> remove(final MongoRemove remove) {
        return new AsyncFuture<RemoveResult>() {
            @Override
            protected RemoveResult doGet() {
                return proxiedCollection.remove(remove);
            }
        };
    }

    private abstract class AsyncFuture<T> implements Future<T> {
        private volatile boolean isCancelled = false;
        private volatile boolean isDone = false;

        @Override
        public boolean cancel(final boolean mayInterruptIfRunning) {
            isCancelled = true;
            return true;
        }

        @Override
        public boolean isCancelled() {
            return isCancelled;
        }

        @Override
        public boolean isDone() {
            return isDone;
        }

        @Override
        public T get() throws InterruptedException, ExecutionException {
            T result = doGet();
            isDone = true;
            return result;
        }

        @Override
        public T get(final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return get();
        }

        protected abstract T doGet();
    }
}
