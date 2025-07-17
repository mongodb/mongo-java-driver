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

package com.mongodb.internal.tracing;

import com.mongodb.lang.Nullable;

/**
 * A Tracer interface that provides methods for tracing commands, operations and transactions.
 * <p>
 * This interface defines methods to retrieve the current trace context, create new spans, and check if tracing is enabled.
 * It also includes a no-operation (NO_OP) implementation for cases where tracing is not required.
 * </p>
 *
 * @since 5.6
 */
public interface Tracer {
    Tracer NO_OP = new Tracer() {

        @Override
        public TraceContext currentContext() {
            return TraceContext.EMPTY;
        }

        @Override
        public Span nextSpan(final String name) {
            return Span.EMPTY;
        }

        @Override
        public Span nextSpan(final String name, @Nullable final TraceContext parent) {
            return Span.EMPTY;
        }

        @Override
        public boolean enabled() {
            return false;
        }

        @Override
        public boolean includeCommandPayload() {
            return false;
        }
    };

    /**
     * Retrieves the current trace context from the Micrometer tracer.
     *
     * @return A {@link TraceContext} representing the underlying {@link io.micrometer.tracing.TraceContext}.
     * exists.
     */
    TraceContext currentContext();

    /**
     * Creates a new span with the specified name.
     *
     * @param name The name of the span.
     * @return A {@link Span} representing the newly created span.
     */
    Span nextSpan(String name); // uses current active span

    /**
     * Creates a new span with the specified name and optional parent trace context.
     *
     * @param name   The name of the span.
     * @param parent The parent {@link TraceContext}, or null if no parent context is provided.
     * @return A {@link Span} representing the newly created span.
     */
    Span nextSpan(String name, @Nullable TraceContext parent); // manually attach the next span to the provided parent

    /**
     * Indicates whether tracing is enabled.
     *
     * @return {@code true} if tracing is enabled, {@code false} otherwise.
     */
    boolean enabled();

    /**
     * Indicates whether command payloads are included in the trace context.
     *
     * @return {@code true} if command payloads are allowed, {@code false} otherwise.
     */
    boolean includeCommandPayload(); // whether the tracer allows command payloads in the trace context
}
