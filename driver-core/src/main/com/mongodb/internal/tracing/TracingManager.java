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
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.lang.Nullable;
import com.mongodb.session.ServerSession;
import org.bson.BsonDocument;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static com.mongodb.internal.tracing.Tags.COLLECTION;
import static com.mongodb.internal.tracing.Tags.CURSOR_ID;
import static com.mongodb.internal.tracing.Tags.NAMESPACE;
import static com.mongodb.internal.tracing.Tags.SYSTEM;

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

    private final Tracer tracer;
    private final TraceContext parentContext;
    private final Map<String, TraceContext> transactions = new ConcurrentHashMap<>();
    private final Map<Long, TraceContext> cursors = new ConcurrentHashMap<>();
    private final Map<Long, Span> operations = new ConcurrentHashMap<>();

    /**
     * Constructs a new TracingManager with the specified tracer.
     *
     * @param tracer The tracer to use for tracing operations.
     */
    public TracingManager(final Tracer tracer) {
        this(tracer, tracer.currentContext());
    }

    /**
     * Constructs a new TracingManager with the specified tracer and parent context.
     *
     * @param tracer        The tracer to use for tracing operations.
     * @param parentContext The parent trace context.
     */
    public TracingManager(final Tracer tracer, final TraceContext parentContext) {
        this.tracer = tracer;
        this.parentContext = parentContext;
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
        return tracer.nextSpan(name, parentContext);
    }

    /**
     * Creates a new span for the specified operation name and ID.
     *
     * @param name        The name of the operation.
     * @param operationId The ID of the operation.
     * @return The created span.
     */
    public Span addSpan(final String name, final Long operationId) {
        Span span = tracer.nextSpan(name);
        operations.put(operationId, span);
        return span;
    }

    /**
     * Creates a new transaction span for the specified server session.
     *
     * @param session The server session.
     * @return The created transaction span.
     */
    public Span addTransactionSpan(final ServerSession session) {
        Span span = tracer.nextSpan("transaction", parentContext);
        transactions.put(getTransactionId(session), span.context());
        return span;
    }

    /**
     * Cleans up the transaction context for the specified server session.
     *
     * @param serverSession The server session.
     */
    public void cleanupTransactionContext(final ServerSession serverSession) {
        transactions.remove(getTransactionId(serverSession));
    }

    /**
     * Cleans up the operation context for the specified operation ID.
     *
     * @param operationId The ID of the operation.
     */
    public void cleanContexts(final Long operationId) {
        operations.remove(operationId);
    }

    /**
     * Allows for the command spans to be linked to their parent operation spans.
     *
     * @param operationId The ID of the operation.
     * @return The parent trace context, or null if none exists.
     */
    @Nullable
    public TraceContext getParentContext(final Long operationId) {
        if (!operations.containsKey(operationId)) {
            return null;
        }
        return operations.get(operationId).context();
    }

    /**
     * Retrieves the parent trace context for the specified cursor ID.
     *
     * @param cursorId The ID of the cursor.
     * @return The parent trace context, or null if none exists.
     */
    @Nullable
    public TraceContext getCursorParentContext(final long cursorId) {
        return cursors.get(cursorId);
    }

    /**
     * Removes the cursor's parent context for the specified cursor ID.
     *
     * @param cursorId The ID of the cursor.
     */
    public void removeCursorParentContext(final long cursorId) {
        cursors.remove(cursorId);
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
     * Executes the specified supplier with tracing enabled.
     *
     * @param supplier         The supplier to execute.
     * @param operationContext The operation context.
     * @param opName           The name of the operation.
     * @param namespace        The MongoDB namespace, or null if none exists.
     * @param <T>              The type of the result.
     * @return The result of the supplier.
     */
    public static <T> T runWithTracing(final Supplier<T> supplier, final OperationContext operationContext, final String opName,
            @Nullable final MongoNamespace namespace) {
        TracingManager tracingManager = operationContext.getTracingManager();
        Span tracingSpan = tracingManager.addOperationSpan(buildSpanName(opName, namespace), operationContext);
        addNamespaceTags(tracingSpan, namespace);

        try {
            return supplier.get();
        } catch (Throwable t) {
            tracingSpan.error(t);
            throw t;
        } finally {
            tracingSpan.end();
            tracingManager.cleanContexts(operationContext.getId());
        }
    }

    /**
     * Links a cursor with its parent operation's context. This maintains the relationship between
     * the initial operation and subsequent 'getMore' commands that fetch the cursor's content.
     *
     * @param queryResult      The query result document.
     * @param operationContext The operation context.
     */
    public static void linkCursorWithOperation(final BsonDocument queryResult, final OperationContext operationContext) {
        long cursorId = queryResult.getDocument("cursor").getInt64("id").longValue();
        operationContext.getTracingManager().addCursorParentContext(cursorId, operationContext.getId());
    }

    /**
     * Checks whether command payload tracing is enabled.
     *
     * @return True if command payload tracing is enabled, false otherwise.
     */
    public boolean isCommandPayloadEnabled() {
        return this.tracer.includeCommandPayload();
    }

    /**
     * Creates a new span for the specified operation name and context.
     * If the operation is part of a transaction, the transaction's span context is used.
     *
     * @param name             The name of the operation.
     * @param operationContext The operation context.
     * @return The created span.
     */
    private Span addOperationSpan(final String name, final OperationContext operationContext) {
        // If this is part of a transaction, get the transaction's span context
        String sessionIdTransactionId = getTransactionId(operationContext);
        Span span = sessionIdTransactionId != null && transactions.containsKey(sessionIdTransactionId)
                ? tracer.nextSpan(name, transactions.get(sessionIdTransactionId))
                : tracer.nextSpan(name);
        operations.put(operationContext.getId(), span);
        return span;
    }

    /**
     * Links the cursor ID to its parent operation, allowing for traceability of 'getMore' commands
     *
     * @param cursorId    The ID of the cursor.
     * @param operationId The ID of the operation.
     */
    private void addCursorParentContext(final long cursorId, final long operationId) {
        if (!operations.containsKey(operationId)) {
            throw new IllegalArgumentException("Operation ID " + operationId + " does not exist.");
        }
        Span operationSpan = operations.get(operationId);
        operationSpan.tag(CURSOR_ID, cursorId);
        cursors.put(cursorId, operationSpan.context());
    }

    /**
     * Builds a span name based on the operation name and namespace.
     *
     * @param opName    The name of the operation.
     * @param namespace The MongoDB namespace, or null if none exists.
     * @return The span name.
     */
    private static String buildSpanName(final String opName, @Nullable final MongoNamespace namespace) {
        return namespace != null ? opName + " " + namespace.getFullName() : opName;
    }

    /**
     * Adds namespace-related tags to the specified span.
     *
     * @param span      The span to add tags to.
     * @param namespace The MongoDB namespace, or null if none exists.
     */
    private static void addNamespaceTags(final Span span, @Nullable final MongoNamespace namespace) {
        span.tag(SYSTEM, "mongodb");
        if (namespace != null) {
            span.tag(NAMESPACE, namespace.getDatabaseName());
            span.tag(COLLECTION, namespace.getCollectionName());
        }
    }

    /**
     * Retrieves the transaction ID for the specified operation context.
     * The ID is constructed from the session identifier and the transaction number.
     *
     * @param operationContext The operation context.
     * @return The transaction ID, or null if none exists.
     */
    @Nullable
    private String getTransactionId(final OperationContext operationContext) {
        if (operationContext.getSessionContext().hasSession()
                && !operationContext.getSessionContext().isImplicitSession()
                && operationContext.getSessionContext().hasActiveTransaction()) {
            return operationContext.getSessionContext().getSessionId()
                    .get(operationContext.getSessionContext().getSessionId().getFirstKey())
                    .asBinary()
                    .asUuid() + "-" + operationContext.getSessionContext().getTransactionNumber();
        }
        return null;
    }

    /**
     * Retrieves the transaction ID for the specified server session.
     * The ID is constructed from the session identifier and the transaction number.
     *
     * @param session The server session.
     * @return The transaction ID.
     */
    private String getTransactionId(final ServerSession session) {
        return session.getIdentifier()
                .get(session.getIdentifier().getFirstKey())
                .asBinary()
                .asUuid()
                .toString() + "-" + session.getTransactionNumber();
    }
}
