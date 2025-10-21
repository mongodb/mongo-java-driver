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
import io.micrometer.common.KeyValue;
import io.micrometer.common.KeyValues;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.transport.Kind;
import io.micrometer.observation.transport.SenderContext;
import org.bson.BsonDocument;
import org.bson.BsonReader;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriter;
import org.bson.json.JsonWriterSettings;

import java.io.PrintWriter;
import java.io.StringWriter;

import static com.mongodb.internal.tracing.MongodbObservation.LowCardinalityKeyNames.EXCEPTION_MESSAGE;
import static com.mongodb.internal.tracing.MongodbObservation.LowCardinalityKeyNames.EXCEPTION_STACKTRACE;
import static com.mongodb.internal.tracing.MongodbObservation.LowCardinalityKeyNames.EXCEPTION_TYPE;
import static com.mongodb.internal.tracing.MongodbObservation.MONGODB_OBSERVATION;
import static com.mongodb.internal.tracing.TracingManager.ENV_OBSERVABILITY_QUERY_TEXT_MAX_LENGTH;
import static java.lang.System.getenv;
import static java.util.Optional.ofNullable;


/**
 * A {@link Tracer} implementation that delegates tracing operations to a Micrometer {@link ObservationRegistry}.
 * <p>
 * This class enables integration of MongoDB driver tracing with Micrometer-based tracing systems.
 * It provides integration with Micrometer to propagate observations into tracing API.
 * </p>
 *
 * @since 5.7
 */
public class MicrometerTracer implements Tracer {
    private final ObservationRegistry observationRegistry;
    private final boolean allowCommandPayload;
    private final int textMaxLength;
    private static final String QUERY_TEXT_LENGTH_CONTEXT_KEY = "QUERY_TEXT_MAX_LENGTH";

    /**
     * Constructs a new {@link MicrometerTracer} instance.
     *
     * @param observationRegistry The Micrometer {@link ObservationRegistry} to delegate tracing operations to.
     */
    public MicrometerTracer(final ObservationRegistry observationRegistry) {
        this(observationRegistry, false, 0);
    }

    /**
     * Constructs a new {@link MicrometerTracer} instance with an option to allow command payloads.
     *
     * @param observationRegistry The Micrometer {@link ObservationRegistry} to delegate tracing operations to.
     * @param allowCommandPayload Whether to allow command payloads in the trace context.
     */
    public MicrometerTracer(final ObservationRegistry observationRegistry, final boolean allowCommandPayload, final int textMaxLength) {
        this.allowCommandPayload = allowCommandPayload;
        this.observationRegistry = observationRegistry;
        this.textMaxLength = ofNullable(getenv(ENV_OBSERVABILITY_QUERY_TEXT_MAX_LENGTH))
                .map(Integer::parseInt)
                .orElse(textMaxLength);
    }

    @Override
    public Span nextSpan(final String name, @Nullable final TraceContext parent, @Nullable final MongoNamespace namespace) {
        Observation observation = getObservation(name);

        if (parent instanceof MicrometerTraceContext) {
            Observation parentObservation = ((MicrometerTraceContext) parent).observation;
            if (parentObservation != null) {
                observation.parentObservation(parentObservation);
            }
        }

        return new MicrometerSpan(observation.start(), namespace);
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public boolean includeCommandPayload() {
        return allowCommandPayload;
    }

    private Observation getObservation(final String name) {
        Observation observation = MONGODB_OBSERVATION.observation(observationRegistry,
                        () -> new SenderContext<>((carrier, key, value) -> {}, Kind.CLIENT))
                .contextualName(name);
        observation.getContext().put(QUERY_TEXT_LENGTH_CONTEXT_KEY, textMaxLength);
        return observation;
    }
    /**
     * Represents a Micrometer-based trace context.
     */
    private static class MicrometerTraceContext implements TraceContext {
        private final Observation observation;

        /**
         * Constructs a new {@link MicrometerTraceContext} instance with an associated Observation.
         *
         * @param observation The Micrometer {@link Observation}, or null if none exists.
         */
        MicrometerTraceContext(@Nullable final Observation observation) {
            this.observation = observation;
        }
    }

    /**
     * Represents a Micrometer-based span.
     */
    private static class MicrometerSpan implements Span {
        private final Observation observation;
        @Nullable
        private final MongoNamespace namespace;
        private final int queryTextLength;

        /**
         * Constructs a new {@link MicrometerSpan} instance with an associated Observation and MongoDB namespace.
         *
         * @param observation The Micrometer {@link Observation}, or null if none exists.
         * @param namespace   The MongoDB namespace associated with the span.
         */
        MicrometerSpan(final Observation observation, @Nullable final MongoNamespace namespace) {
            this.namespace = namespace;
            this.observation = observation;
            this.queryTextLength = ofNullable(observation.getContext().get(QUERY_TEXT_LENGTH_CONTEXT_KEY))
                    .filter(Integer.class::isInstance)
                    .map(Integer.class::cast)
                    .orElse(Integer.MAX_VALUE);
        }

        @Override
        public void tagLowCardinality(final KeyValue keyValue) {
            observation.lowCardinalityKeyValue(keyValue);
        }

        @Override
        public void tagLowCardinality(final KeyValues keyValues) {
            observation.lowCardinalityKeyValues(keyValues);
        }

        @Override
        public void tagHighCardinality(final String keyName, final BsonDocument value) {
            observation.highCardinalityKeyValue(keyName,
                    (queryTextLength < Integer.MAX_VALUE) // truncate values that are too long
                            ? getTruncatedBsonDocument(value)
                            : value.toString());
        }

        @Override
        public void event(final String event) {
            observation.event(() -> event);
        }

        @Override
        public void error(final Throwable throwable) {
            observation.lowCardinalityKeyValues(KeyValues.of(
                    EXCEPTION_MESSAGE.withValue(throwable.getMessage()),
                    EXCEPTION_TYPE.withValue(throwable.getClass().getName()),
                    EXCEPTION_STACKTRACE.withValue(getStackTraceAsString(throwable))
            ));
            observation.error(throwable);
        }

        @Override
        public void end() {
            observation.stop();
        }

        @Override
        public TraceContext context() {
            return new MicrometerTraceContext(observation);
        }

        @Override
        @Nullable
        public MongoNamespace getNamespace() {
            return namespace;
        }

        private String getStackTraceAsString(final Throwable throwable) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            throwable.printStackTrace(pw);
            return sw.toString();
        }

        private String getTruncatedBsonDocument(final BsonDocument commandDocument) {
            StringWriter writer = new StringWriter();

            try (BsonReader bsonReader = commandDocument.asBsonReader()) {
                JsonWriter jsonWriter = new JsonWriter(writer,
                        JsonWriterSettings.builder().outputMode(JsonMode.RELAXED)
                                .maxLength(queryTextLength)
                                .build());

                jsonWriter.pipe(bsonReader);

                if (jsonWriter.isTruncated()) {
                    writer.append(" ...");
                }

                return writer.toString();
            }
        }
    }
}
