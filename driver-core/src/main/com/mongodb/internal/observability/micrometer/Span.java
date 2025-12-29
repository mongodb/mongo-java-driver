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

import com.mongodb.MongoNamespace;
import com.mongodb.lang.Nullable;
import io.micrometer.common.KeyValue;
import io.micrometer.common.KeyValues;
import org.bson.BsonDocument;

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
 *
 * @since 5.7
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
        public void tagLowCardinality(final KeyValue tag) {
        }

        @Override
        public void tagLowCardinality(final KeyValues keyValues) {
        }

        @Override
        public void tagHighCardinality(final String keyName, final BsonDocument value) {
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

        @Override
        @Nullable
        public MongoNamespace getNamespace() {
            return null;
        }
    };

    /**
     * Adds a low-cardinality tag to the span.
     *
     * @param keyValue The key-value pair representing the tag.
     */
    void tagLowCardinality(KeyValue keyValue);

    /**
     * Adds multiple low-cardinality tags to the span.
     *
     * @param keyValues The key-value pairs representing the tags.
     */
    void tagLowCardinality(KeyValues keyValues);

    /**
     * Adds a high-cardinality (highly variable values) tag to the span with a BSON document value.
     *
     * @param keyName The name of the tag.
     * @param value   The BSON document representing the value of the tag.
     */
    void tagHighCardinality(String keyName, BsonDocument value);

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

    /**
     * Retrieves the MongoDB namespace associated with the span, if any.
     *
     * @return The MongoDB namespace, or null if none is associated.
     */
    @Nullable
    MongoNamespace getNamespace();
}
