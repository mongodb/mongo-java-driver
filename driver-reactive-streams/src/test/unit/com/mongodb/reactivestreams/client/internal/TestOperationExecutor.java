/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.operation.ReadOperation;
import com.mongodb.internal.operation.WriteOperation;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ClientSession;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class TestOperationExecutor implements OperationExecutor {

    private final List<Object> responses;
    private final List<ClientSession> clientSessions = new ArrayList<>();
    private final List<ReadPreference> readPreferences = new ArrayList<>();

    private final List<ReadOperation> readOperations = new ArrayList<>();
    private final List<WriteOperation> writeOperations = new ArrayList<>();

    public TestOperationExecutor(final List<Object> responses) {
        this.responses = new ArrayList<>(responses);
    }

    @Override
    public <T> Mono<T> execute(final ReadOperation<?, T> operation, final ReadPreference readPreference, final ReadConcern readConcern,
                               @Nullable final ClientSession session) {
        readPreferences.add(readPreference);
        clientSessions.add(session);
        readOperations.add(operation);
        return createMono();
    }


    @Override
    public <T> Mono<T> execute(final WriteOperation<T> operation, final ReadConcern readConcern,
                               @Nullable final ClientSession session) {
        clientSessions.add(session);
        writeOperations.add(operation);
        return createMono();
    }

    @Override
    public OperationExecutor withTimeoutSettings(final TimeoutSettings timeoutSettings) {
        return this;
    }

    @Override
    public TimeoutSettings getTimeoutSettings() {
        throw new UnsupportedOperationException("Not supported");
    }

    <T> Mono<T> createMono() {
        return Mono.create(sink -> {
           Object response = responses.remove(0);
           if (response instanceof Throwable) {
               sink.error((Throwable) response);
           } else {
               if (response == null) {
                   sink.success();
               } else {
                   sink.success((T) response);
               }
           }
        }
        );
    }

    @Nullable
    ClientSession getClientSession() {
        return clientSessions.isEmpty() ? null : clientSessions.remove(0);
    }

    @Nullable
    ReadOperation getReadOperation() {
        return readOperations.isEmpty() ? null : readOperations.remove(0);
    }

    @Nullable
    ReadPreference getReadPreference() {
        return readPreferences.isEmpty() ? null : readPreferences.remove(0);
    }

    @Nullable
    WriteOperation getWriteOperation() {
        return writeOperations.isEmpty() ? null : writeOperations.remove(0);
    }

}
