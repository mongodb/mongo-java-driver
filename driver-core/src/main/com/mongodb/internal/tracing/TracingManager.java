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
import com.mongodb.ServerAddress;
import com.mongodb.UnixServerAddress;
import com.mongodb.connection.ConnectionId;
import com.mongodb.internal.connection.CommandMessage;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.lang.Nullable;
import com.mongodb.observability.MicrometerObservabilitySettings;
import com.mongodb.observability.ObservabilitySettings;
import io.micrometer.common.KeyValues;
import io.micrometer.observation.ObservationRegistry;
import org.bson.BsonDocument;

import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.mongodb.internal.tracing.MongodbObservation.LowCardinalityKeyNames.CLIENT_CONNECTION_ID;
import static com.mongodb.internal.tracing.MongodbObservation.LowCardinalityKeyNames.COLLECTION;
import static com.mongodb.internal.tracing.MongodbObservation.LowCardinalityKeyNames.COMMAND_NAME;
import static com.mongodb.internal.tracing.MongodbObservation.LowCardinalityKeyNames.CURSOR_ID;
import static com.mongodb.internal.tracing.MongodbObservation.LowCardinalityKeyNames.NAMESPACE;
import static com.mongodb.internal.tracing.MongodbObservation.LowCardinalityKeyNames.NETWORK_TRANSPORT;
import static com.mongodb.internal.tracing.MongodbObservation.LowCardinalityKeyNames.QUERY_SUMMARY;
import static com.mongodb.internal.tracing.MongodbObservation.LowCardinalityKeyNames.SERVER_ADDRESS;
import static com.mongodb.internal.tracing.MongodbObservation.LowCardinalityKeyNames.SERVER_CONNECTION_ID;
import static com.mongodb.internal.tracing.MongodbObservation.LowCardinalityKeyNames.SERVER_PORT;
import static com.mongodb.internal.tracing.MongodbObservation.LowCardinalityKeyNames.SESSION_ID;
import static com.mongodb.internal.tracing.MongodbObservation.LowCardinalityKeyNames.SYSTEM;
import static com.mongodb.internal.tracing.MongodbObservation.LowCardinalityKeyNames.TRANSACTION_NUMBER;
import static com.mongodb.observability.MicrometerObservabilitySettings.ENV_OBSERVABILITY_ENABLED;
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
    public static final TracingManager NO_OP = new TracingManager(null);
    private final Tracer tracer;
    private final boolean enableCommandPayload;

    /**
     * Constructs a new TracingManager with the specified observation registry.
     * @param observationRegistry The observation registry to use for tracing operations, may be null.
     */
    public TracingManager(@Nullable final ObservabilitySettings observabilitySettings) {
        if (observabilitySettings == null) {
            tracer = Tracer.NO_OP;
            enableCommandPayload = false;

        } else {
            MicrometerObservabilitySettings settings;
            if (observabilitySettings instanceof MicrometerObservabilitySettings) {
                settings = (MicrometerObservabilitySettings) observabilitySettings;
            } else {
                throw new IllegalArgumentException("Only Micrometer based observability is currently supported");
            }

            String envOtelInstrumentationEnabled = getenv(ENV_OBSERVABILITY_ENABLED);
            boolean enableTracing = true;
            if (envOtelInstrumentationEnabled != null) {
                enableTracing = Boolean.parseBoolean(envOtelInstrumentationEnabled);
            }

            ObservationRegistry observationRegistry = settings.getObservationRegistry();
            tracer = enableTracing && observationRegistry != null
                    ? new MicrometerTracer(observationRegistry, settings.isEnableCommandPayloadTracing(), settings.getMaxQueryTextLength())
                    : Tracer.NO_OP;

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
        return tracer.isEnabled();
    }

    /**
     * Checks whether command payload tracing is enabled.
     *
     * @return True if command payload tracing is enabled, false otherwise.
     */
    public boolean isCommandPayloadEnabled() {
        return enableCommandPayload;
    }


    /** Create a tracing span for the given command message.
     * <p>
     * The span is only created if tracing is enabled and the command is not security-sensitive.
     * It attaches various tags to the span, such as database system, namespace, query summary, opcode,
     * server address, port, server type, client and server connection IDs, and, if applicable,
     * transaction number and session ID.
     * If command payload tracing is enabled, the command document is also attached as a tag.
     *
     * @param message          the command message to trace
     * @param operationContext the operation context containing tracing and session information
     * @param commandDocumentSupplier a supplier that provides the command document when needed
     * @param isSensitiveCommand a predicate that determines if a command is security-sensitive based on its name
     * @param serverAddressSupplier a supplier that provides the server address when needed
     * @param connectionIdSupplier a supplier that provides the connection ID when needed
     * @return the created {@link Span}, or {@code null} if tracing is not enabled or the command is security-sensitive
     */
    @Nullable
    public Span createTracingSpan(final CommandMessage message,
            final OperationContext operationContext,
            final Supplier<BsonDocument> commandDocumentSupplier,
            final Predicate<String> isSensitiveCommand,
            final Supplier<ServerAddress> serverAddressSupplier,
            final Supplier<ConnectionId> connectionIdSupplier
            ) {

       if (!isEnabled()) {
            return null;
        }
        BsonDocument command = commandDocumentSupplier.get();
        String commandName = command.getFirstKey();
        if (isSensitiveCommand.test(commandName)) {
            return null;
        }

        Span operationSpan = operationContext.getTracingSpan();
        Span span = addSpan(commandName,  operationSpan != null ? operationSpan.context() : null);

        if (command.containsKey("getMore")) {
            long cursorId = command.getInt64("getMore").longValue();
            span.tagLowCardinality(CURSOR_ID.withValue(String.valueOf(cursorId)));
            if (operationSpan != null) {
                operationSpan.tagLowCardinality(CURSOR_ID.withValue(String.valueOf(cursorId)));
            }
        }

        // Tag namespace
        String namespace;
        String collection = "";
        if (operationSpan != null) {
            MongoNamespace parentNamespace = operationSpan.getNamespace();
            if (parentNamespace != null) {
                namespace = parentNamespace.getDatabaseName();
                collection =
                        MongoNamespace.COMMAND_COLLECTION_NAME.equalsIgnoreCase(parentNamespace.getCollectionName()) ? ""
                                : parentNamespace.getCollectionName();
            } else {
                namespace = message.getDatabase();
            }
        } else {
            namespace = message.getDatabase();
        }
        String summary = commandName + " " + namespace + (collection.isEmpty() ? "" : "." + collection);

        KeyValues keyValues = KeyValues.of(
                SYSTEM.withValue("mongodb"),
                NAMESPACE.withValue(namespace),
                QUERY_SUMMARY.withValue(summary),
                COMMAND_NAME.withValue(commandName));

        if (!collection.isEmpty()) {
            keyValues = keyValues.and(COLLECTION.withValue(collection));
        }
        span.tagLowCardinality(keyValues);

        // tag server and connection info
        ServerAddress serverAddress = serverAddressSupplier.get();
        ConnectionId connectionId = connectionIdSupplier.get();
        span.tagLowCardinality(KeyValues.of(
                SERVER_ADDRESS.withValue(serverAddress.getHost()),
                SERVER_PORT.withValue(String.valueOf(serverAddress.getPort())),
                CLIENT_CONNECTION_ID.withValue(String.valueOf(connectionId.getLocalValue())),
                SERVER_CONNECTION_ID.withValue(String.valueOf(connectionId.getServerValue())),
                NETWORK_TRANSPORT.withValue(serverAddress instanceof UnixServerAddress ? "unix" : "tcp")
        ));

        // tag session and transaction info
        SessionContext sessionContext = operationContext.getSessionContext();
        if (sessionContext.hasSession() && !sessionContext.isImplicitSession()) {
            span.tagLowCardinality(KeyValues.of(
                    TRANSACTION_NUMBER.withValue(String.valueOf(sessionContext.getTransactionNumber())),
                    SESSION_ID.withValue(String.valueOf(sessionContext.getSessionId()
                            .get(sessionContext.getSessionId().getFirstKey())
                            .asBinary().asUuid()))
            ));
        }

        return span;
    }
}
