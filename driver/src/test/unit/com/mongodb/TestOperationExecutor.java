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

import com.mongodb.operation.ReadOperation;
import com.mongodb.operation.WriteOperation;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("rawtypes")
class TestOperationExecutor implements OperationExecutor {

    private final List<Object> responses;
    private List<ReadPreference> readPreferences = new ArrayList<ReadPreference>();
    private List<ReadOperation> readOperations = new ArrayList<ReadOperation>();
    private List<WriteOperation> writeOperations = new ArrayList<WriteOperation>();

    TestOperationExecutor(final List<Object> responses) {
        this.responses = responses;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T execute(final ReadOperation<T> operation, final ReadPreference readPreference) {
        readOperations.add(operation);
        readPreferences.add(readPreference);
        return (T) getResponse();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T execute(final WriteOperation<T> operation) {
        writeOperations.add(operation);
        return (T) getResponse();
    }

    private Object getResponse() {
        Object response = responses.remove(0);
        if (response instanceof RuntimeException) {
            throw (RuntimeException) response;
        }
        return response;
    }

    ReadOperation getReadOperation() {
        return readOperations.isEmpty() ? null : readOperations.remove(0);
    }

    ReadPreference getReadPreference() {
        return readPreferences.isEmpty() ? null : readPreferences.remove(0);
    }

    WriteOperation getWriteOperation() {
       return writeOperations.isEmpty() ? null : writeOperations.remove(0);
    }
}
