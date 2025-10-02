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

import com.mongodb.MongoNamespace;
import com.mongodb.lang.Nullable;

import static com.mongodb.tracing.MongodbObservation.LowCardinalityKeyNames.SYSTEM;
import static java.lang.System.getenv;

/**
 * Manages tracing spans for MongoDB driver activities.
 * <p>
 * This class provides methods to create and manage spans for commands, operations and transactions.
 * It integrates with a {@link Tracer} to propagate tracing information and record telemetry.
 * </p>
 */
public class TracingManager {
    /**
     * A no-op instance of the TracingManager used when tracing is disabled.
     */
    public static final TracingManager NO_OP = new TracingManager(Tracer.NO_OP);
    private static final String ENV_ALLOW_COMMAND_PAYLOAD = "MONGODB_TRACING_ALLOW_COMMAND_PAYLOAD";
    private final Tracer tracer;
    private final boolean enableCommandPayload;

    /**
     * Constructs a new TracingManager with the specified tracer and parent context.
     * Setting the environment variable {@code MONGODB_TRACING_ALLOW_COMMAND_PAYLOAD} to "true" will enable command payload tracing.
     *
     * @param tracer        The tracer to use for tracing operations.
     */
    public TracingManager(final Tracer tracer) {
        this.tracer = tracer;
        String envAllowCommandPayload = getenv(ENV_ALLOW_COMMAND_PAYLOAD);
        if (envAllowCommandPayload != null) {
            this.enableCommandPayload = Boolean.parseBoolean(envAllowCommandPayload);
        } else {
            this.enableCommandPayload = tracer.includeCommandPayload();
        }
    }

    /**
     * Creates a new span with the specified name and parent trace context.
     * <p>
     * This method is used to create a span that is linked to a parent context,
     * enabling hierarchical tracing of operations.
     * </p>
     *
     * @param name          The name of the span.
     * @param parentContext The parent trace context to associate with the span.
     * @return The created span.
     */
    public Span addSpan(final String name, @Nullable final TraceContext parentContext) {
        return tracer.nextSpan(name, parentContext, null);
    }

    /**
     * Creates a new span with the specified name, parent trace context, and MongoDB namespace.
     * <p>
     * This method is used to create a span that is linked to a parent context,
     * enabling hierarchical tracing of operations. The MongoDB namespace can be used
     * by nested spans to access the database and collection name (which might not be easily accessible at connection layer).
     * </p>
     *
     * @param name          The name of the span.
     * @param parentContext The parent trace context to associate with the span.
     * @param namespace     The MongoDB namespace associated with the operation.
     * @return The created span.
     */
    public Span addSpan(final String name, @Nullable final TraceContext parentContext, final MongoNamespace namespace) {
        return tracer.nextSpan(name, parentContext, namespace);
    }

    /**
     * Creates a new transaction span for the specified server session.
     *
     * @return The created transaction span.
     */
    public Span addTransactionSpan() {
        Span span = tracer.nextSpan("transaction", null, null);
        span.tagLowCardinality(SYSTEM.withValue("mongodb"));
        return span;
    }

    /**
     * Checks whether tracing is enabled.
     *
     * @return True if tracing is enabled, false otherwise.
     */
    public boolean isEnabled() {
        return tracer.enabled();
    }

    /**
     * Checks whether command payload tracing is enabled.
     *
     * @return True if command payload tracing is enabled, false otherwise.
     */
    public boolean isCommandPayloadEnabled() {
        return enableCommandPayload;
    }
}
