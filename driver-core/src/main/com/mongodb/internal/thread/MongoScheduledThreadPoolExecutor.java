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
package com.mongodb.internal.thread;

import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.lang.Nullable;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

import static com.mongodb.assertions.Assertions.assertNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.internal.thread.MongoThreadPoolExecutor.handleTaskExceptions;

/**
 * See {@link MongoThreadPoolExecutor}.
 */
final class MongoScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor {
    private final Logger logger;

    MongoScheduledThreadPoolExecutor(
            final int corePoolSize,
            final ThreadFactory threadFactory,
            final Logger logger) {
        super(corePoolSize, threadFactory);
        setRemoveOnCancelPolicy(true);
        this.logger = logger;
    }

    @Override
    protected void afterExecute(final Runnable r, @Nullable final Throwable t) {
        super.afterExecute(r, t);
        assertTrue(r instanceof Future<?>);
        handleTaskExceptions(r, assertNull(t), logger);
    }
}
