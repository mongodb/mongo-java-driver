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

package com.mongodb.internal.async;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.mongodb.internal.async.AsyncRunnable.beginAsync;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DisplayName("The same thread async functions")
public class SameThreadAsyncFunctionsTest extends AsyncFunctionsAbstractTest {
    @Override
    public ExecutorService createAsyncExecutor() {
        return new SameThreadExecutorService();
    }

    @Test
    void testInvalid() {
        setIsTestingAbruptCompletion(false);
        setAsyncStep(true);
        IllegalStateException illegalStateException = new IllegalStateException("must not cause second callback invocation");

        assertThrows(IllegalStateException.class, () -> {
            beginAsync().thenRun(c -> {
                async(3, c);
                throw illegalStateException;
            }).finish((v, e) -> {
                assertNotEquals(e, illegalStateException);
            });
        });
        assertThrows(IllegalStateException.class, () -> {
            beginAsync().thenRun(c -> {
                async(3, c);
            }).finish((v, e) -> {
                throw illegalStateException;
            });
        });
    }

    private static class SameThreadExecutorService extends AbstractExecutorService {
        @Override
        public void execute(@NotNull final Runnable command) {
            command.run();
        }

        @Override
        public void shutdown() {
        }

        @NotNull
        @Override
        public List<Runnable> shutdownNow() {
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isTerminated() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean awaitTermination(final long timeout, @NotNull final TimeUnit unit) {
            return true;
        }
    }
}
