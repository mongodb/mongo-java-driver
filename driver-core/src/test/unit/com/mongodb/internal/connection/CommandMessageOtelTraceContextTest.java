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

package com.mongodb.internal.connection;

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.connection.MessageSequences.EmptyMessageSequences;
import com.mongodb.internal.observability.micrometer.OtelTracePropagationTestToggle;
import com.mongodb.internal.observability.micrometer.Span;
import com.mongodb.internal.observability.micrometer.TraceContext;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static com.mongodb.internal.mockito.MongoMockito.mock;
import static com.mongodb.internal.operation.ServerVersionHelper.LATEST_WIRE_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

class CommandMessageOtelTraceContextTest {

    private static final String TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    private static final MongoNamespace NAMESPACE = new MongoNamespace("db.test");
    private static final BsonDocument COMMAND = new BsonDocument("find", new BsonString(NAMESPACE.getCollectionName()));

    @Test
    void writesSectionWhenSupportedAndSampledSpanPresent() {
        CommandMessage message = buildCommandMessage(true);
        TraceContext traceContext = () -> TRACEPARENT;
        Span span = mock(Span.class, mock -> when(mock.context()).thenReturn(traceContext));
        OperationContext operationContext = buildOperationContext(span);

        byte[] encoded = encodeToBytes(message, operationContext);

        assertTrue(containsOtelSection(encoded, TRACEPARENT),
                "Encoded message should contain the OTel trace context section (kind byte 3 + traceparent C-string)");
    }

    @Test
    void getCommandDocumentIgnoresOtelSection() {
        // Regression guard: InternalStreamConnection calls getCommandDocument() on every send (logging/monitoring/
        // compression). The trailing kind-3 section must not corrupt command-document reconstruction.
        CommandMessage message = buildCommandMessage(true);
        TraceContext traceContext = () -> TRACEPARENT;
        Span span = mock(Span.class, mock -> when(mock.context()).thenReturn(traceContext));
        OperationContext operationContext = buildOperationContext(span);

        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            message.encode(output, operationContext);
            BsonDocument commandDocument = message.getCommandDocument(output);
            assertEquals("test", commandDocument.getString("find").getValue());
            assertEquals("db", commandDocument.getString("$db").getValue());
            // The reconstructed command is exactly the body section (find + $db); the trailing
            // kind-3 section must not leak any extra fields into it.
            assertEquals(2, commandDocument.size());
        }
    }

    @Test
    void omitsSectionWhenCapabilityAbsent() {
        CommandMessage message = buildCommandMessage(false);
        TraceContext traceContext = () -> TRACEPARENT;
        Span span = mock(Span.class, mock -> when(mock.context()).thenReturn(traceContext));
        OperationContext operationContext = buildOperationContext(span);

        byte[] encoded = encodeToBytes(message, operationContext);

        assertFalse(containsOtelSection(encoded, TRACEPARENT),
                "Encoded message should NOT contain the OTel trace context section when tracingSupported=false");
    }

    @Test
    void omitsSectionWhenSpanHasNoTraceParent() {
        CommandMessage message = buildCommandMessage(true);
        TraceContext traceContext = () -> null;
        Span span = mock(Span.class, mock -> when(mock.context()).thenReturn(traceContext));
        OperationContext operationContext = buildOperationContext(span);

        byte[] encoded = encodeToBytes(message, operationContext);

        assertFalse(containsOtelSection(encoded, TRACEPARENT),
                "Encoded message should NOT contain the OTel trace context section when traceParent is null");
    }

    @Test
    void omitsSectionWhenSpanIsNull() {
        CommandMessage message = buildCommandMessage(true);
        OperationContext operationContext = buildOperationContext(null);

        byte[] encoded = encodeToBytes(message, operationContext);

        assertFalse(containsOtelSection(encoded, TRACEPARENT),
                "Encoded message should NOT contain the OTel trace context section when there is no tracing span");
    }

    @Test
    void writesSectionWhenForcedEvenIfCapabilityAbsent() {
        OtelTracePropagationTestToggle.FORCE_PROPAGATION = true;
        try {
            CommandMessage message = buildCommandMessage(false); // server did NOT advertise tracingSupport
            TraceContext traceContext = () -> TRACEPARENT;
            Span span = mock(Span.class, mock -> when(mock.context()).thenReturn(traceContext));
            OperationContext operationContext = buildOperationContext(span);

            byte[] encoded = encodeToBytes(message, operationContext);

            assertTrue(containsOtelSection(encoded, TRACEPARENT),
                    "With FORCE_PROPAGATION the section must be sent even when the server did not advertise support");
        } finally {
            OtelTracePropagationTestToggle.FORCE_PROPAGATION = false;
        }
    }

    // --- helpers ---

    private static CommandMessage buildCommandMessage(final boolean tracingSupported) {
        return new CommandMessage(
                NAMESPACE.getDatabaseName(),
                COMMAND,
                NoOpFieldNameValidator.INSTANCE,
                ReadPreference.primary(),
                MessageSettings.builder()
                        .maxWireVersion(LATEST_WIRE_VERSION)
                        .serverType(ServerType.REPLICA_SET_PRIMARY)
                        .sessionSupported(true)
                        .tracingSupported(tracingSupported)
                        .build(),
                true,
                EmptyMessageSequences.INSTANCE,
                ClusterConnectionMode.MULTIPLE,
                null);
    }

    private static OperationContext buildOperationContext(final Span span) {
        SessionContext sessionContext = mock(SessionContext.class, mock -> {
            when(mock.getClusterTime()).thenReturn(null);
            when(mock.hasSession()).thenReturn(false);
            when(mock.getReadConcern()).thenReturn(ReadConcern.DEFAULT);
            when(mock.notifyMessageSent()).thenReturn(true);
            when(mock.hasActiveTransaction()).thenReturn(false);
            when(mock.isSnapshot()).thenReturn(false);
        });
        TimeoutContext timeoutContext = new TimeoutContext(TimeoutSettings.DEFAULT);
        return mock(OperationContext.class, mock -> {
            when(mock.getSessionContext()).thenReturn(sessionContext);
            when(mock.getTimeoutContext()).thenReturn(timeoutContext);
            when(mock.getTracingSpan()).thenReturn(span);
        });
    }

    private static byte[] encodeToBytes(final CommandMessage message, final OperationContext operationContext) {
        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            message.encode(output, operationContext);
            return output.toByteArray();
        }
    }

    /**
     * Searches for the needle: kind byte {@code 3} immediately followed by the UTF-8 bytes of
     * {@code traceparent} and a trailing null byte (C-string terminator).
     */
    private static boolean containsOtelSection(final byte[] encoded, final String traceparent) {
        byte[] traceparentBytes = traceparent.getBytes(StandardCharsets.UTF_8);
        // needle = [0x03, tp[0], tp[1], ..., tp[n-1], 0x00]
        int needleLen = 1 + traceparentBytes.length + 1;
        outer:
        for (int i = 0; i <= encoded.length - needleLen; i++) {
            if (encoded[i] != 3) {
                continue;
            }
            for (int j = 0; j < traceparentBytes.length; j++) {
                if (encoded[i + 1 + j] != traceparentBytes[j]) {
                    continue outer;
                }
            }
            if (encoded[i + 1 + traceparentBytes.length] == 0) {
                return true;
            }
        }
        return false;
    }
}
