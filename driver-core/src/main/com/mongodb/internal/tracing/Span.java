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


/**
 * Represents a tracing span for the driver internal operations.
 * <p>
 * A span records information about a single operation, such as tags, events, errors, and its context.
 * Implementations can be used to propagate tracing information and record telemetry.
 * </p>
 * <p>
 * Spans can be used to trace different aspects of MongoDB driver activity:
 * <ul>
 *   <li><b>Command Spans</b>: Trace the execution of MongoDB commands (e.g., find, insert, update).</li>
 *   <li><b>Operation Spans</b>: Trace higher-level operations, which may include multiple commands or internal steps.</li>
 *   <li><b>Transaction Spans</b>: Trace the lifecycle of a transaction, including all operations and commands within it.</li>
 * </ul>
 * </p>
 *
 * @since 5.6
 */
public interface Span {
    /**
     * An empty / no-op implementation of the Span interface.
     * <p>
     * This implementation is used as a default when no actual tracing is required.
     * All methods in this implementation perform no operations and return default values.
     * </p>
     */
    Span EMPTY = new Span() {
        @Override
        public Span tag(final String key, final String value) {
            return this;
        }

        @Override
        public Span tag(final String key, final Long value) {
            return this;
        }

        @Override
        public void event(final String event) {
        }

        @Override
        public void error(final Throwable throwable) {
        }

        @Override
        public void end() {
        }

        @Override
        public TraceContext context() {
            return TraceContext.EMPTY;
        }
    };

    /**
     * Adds a tag to the span with a key-value pair.
     *
     * @param key   The tag key.
     * @param value The tag value.
     * @return The current instance of the span.
     */
    Span tag(String key, String value);

    /**
     * Adds a tag to the span with a key and a numeric value.
     *
     * @param key   The tag key.
     * @param value The numeric tag value.
     * @return The current instance of the span.
     */
    Span tag(String key, Long value);

    /**
     * Records an event in the span.
     *
     * @param event The event description.
     */
    void event(String event);

    /**
     * Records an error for this span.
     *
     * @param throwable The error to record.
     */
    void error(Throwable throwable);

    /**
     * Ends the span, marking it as complete.
     */
    void end();

    /**
     * Retrieves the context associated with the span.
     *
     * @return The trace context associated with the span.
     */
    TraceContext context();
}
