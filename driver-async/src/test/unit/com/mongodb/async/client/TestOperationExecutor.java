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

import com.mongodb.ReadPreference;
import com.mongodb.async.SingleResultCallback;
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
    public <T> void execute(final AsyncReadOperation<T> operation, final ReadPreference readPreference,
                            final SingleResultCallback<T> callback) {
        readOperations.add(operation);
        readPreferences.add(readPreference);
        callResult(callback);
    }


    @Override
    public <T> void execute(final AsyncWriteOperation<T> operation, final SingleResultCallback<T> callback) {
        writeOperations.add(operation);
        callResult(callback);
    }

    @SuppressWarnings("unchecked")
    <T> void callResult(final SingleResultCallback<T> callback) {
        Object response = responses.remove(0);
        if (response instanceof Throwable) {
            callback.onResult(null, (Throwable) response);
        } else {
            callback.onResult((T) response, null);
        }
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