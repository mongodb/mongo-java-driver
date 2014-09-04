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

package com.mongodb;

import com.mongodb.operation.OperationExecutor;
import com.mongodb.operation.ReadOperation;
import com.mongodb.operation.WriteOperation;

@SuppressWarnings("rawtypes")
class TestOperationExecutor implements OperationExecutor {

    private final Object response;
    private ReadPreference readPreference;
    private ReadOperation readOperation;
    private WriteOperation writeOperation;

    TestOperationExecutor(final Object response) {
        this.response = response;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T execute(final ReadOperation<T> operation, final ReadPreference readPreference) {
        this.readOperation = operation;
        this.readPreference = readPreference;
        return (T) response;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T execute(final WriteOperation<T> operation) {
        this.writeOperation = operation;
        return (T) response;
    }

    ReadOperation getReadOperation() {
        return readOperation;
    }

    ReadPreference getReadPreference() {
        return readPreference;
    }

    WriteOperation getWriteOperation() {
       return writeOperation;
    }
}