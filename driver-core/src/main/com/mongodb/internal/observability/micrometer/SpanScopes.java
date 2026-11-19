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

package com.mongodb.internal.observability.micrometer;

import com.mongodb.lang.Nullable;

/**
 * Runs an action while a {@link Span}'s scope is open on the current thread, so that anything that reads the
 * current tracing context during the action (log appenders, MDC bridges, listeners) observes that span.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class SpanScopes {

    /**
     * @param span   the span to make current, or {@code null} to run the action without touching any scope
     * @param action the action to run
     */
    public static void runInScope(@Nullable final Span span, final Runnable action) {
        if (span == null) {
            action.run();
            return;
        }
        span.openScope();
        try {
            action.run();
        } finally {
            span.closeScope();
        }
    }

    private SpanScopes() {
    }
}
