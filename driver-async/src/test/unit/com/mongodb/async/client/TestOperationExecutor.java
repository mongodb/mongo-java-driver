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

import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.async.MongoFuture;
import com.mongodb.async.SingleResultFuture;
import com.mongodb.operation.AsyncOperationExecutor;
import com.mongodb.operation.AsyncReadOperation;
import com.mongodb.operation.AsyncWriteOperation;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("rawtypes")
public class TestOperationExecutor implements AsyncOperationExecutor {

    private final List<Object> responses;
    private final List<ReadPreference> readPreferences = new ArrayList<ReadPreference>();
    private final List<AsyncReadOperation> readOperations = new ArrayList<AsyncReadOperation>();
    private final List<AsyncWriteOperation> writeOperations = new ArrayList<AsyncWriteOperation>();

    TestOperationExecutor(final List<Object> responses) {
        this.responses = responses;
    }

    @Override
    public <T> MongoFuture<T> execute(final AsyncReadOperation<T> operation, final ReadPreference readPreference) {
        readOperations.add(operation);
        readPreferences.add(readPreference);
        return toFuture(responses.remove(0));
    }

    @Override
    public <T> MongoFuture<T> execute(final AsyncWriteOperation<T> operation) {
        writeOperations.add(operation);
        return toFuture(responses.remove(0));
    }

    @SuppressWarnings("unchecked")
    <T> MongoFuture<T> toFuture(final Object response) {
        SingleResultFuture<T> future = new SingleResultFuture<T>();
        if (response instanceof MongoException) {
            future.init(null, (MongoException) response);
        } else {
            future.init((T) response, null);
        }
        return future;
    }

    AsyncReadOperation getReadOperation() {
        return readOperations.isEmpty() ? null : readOperations.remove(0);
    }

    ReadPreference getReadPreference() {
        return readPreferences.isEmpty() ? null : readPreferences.remove(0);
    }

    AsyncWriteOperation getWriteOperation() {
        return writeOperations.isEmpty() ? null : writeOperations.remove(0);
    }

}