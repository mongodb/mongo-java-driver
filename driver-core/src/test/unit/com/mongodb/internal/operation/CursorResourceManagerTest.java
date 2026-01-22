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
package com.mongodb.internal.operation;

import com.mongodb.MongoNamespace;
import com.mongodb.ServerCursor;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.binding.ReferenceCounted;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.mockito.MongoMockito;
import org.junit.jupiter.api.Test;

import static com.mongodb.ClusterFixture.getOperationContext;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.when;

final class CursorResourceManagerTest {
    @Test
    void doubleCloseExecutedConcurrentlyWithOperationBeingInProgressShouldNotFail() {
        CursorResourceManager<?, ?> cursorResourceManager = new CursorResourceManager<ReferenceCounted, ReferenceCounted>(
                new MongoNamespace("db", "coll"),
                MongoMockito.mock(AsyncConnectionSource.class, mock -> {
                    when(mock.retain()).thenReturn(mock);
                    when(mock.release()).thenReturn(1);
                }),
                null,
                MongoMockito.mock(ServerCursor.class)) {
            @Override
            void markAsPinned(final ReferenceCounted connectionToPin, final Connection.PinningMode pinningMode) {
            }

            @Override
            void doClose(final OperationContext operationContext) {
            }
        };
        cursorResourceManager.tryStartOperation();
        try {
            assertDoesNotThrow(() -> {
                cursorResourceManager.close(getOperationContext());
                cursorResourceManager.close(getOperationContext());
                cursorResourceManager.setServerCursor(null);
            });
        } finally {
            cursorResourceManager.endOperation(getOperationContext());
        }
    }
}
