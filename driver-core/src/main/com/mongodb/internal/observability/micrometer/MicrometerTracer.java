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
import com.mongodb.observability.micrometer.DefaultMongodbObservationConvention;
import com.mongodb.observability.micrometer.MongodbObservation;
import com.mongodb.observability.micrometer.MongodbObservationContext;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationConvention;
import io.micrometer.observation.ObservationRegistry;
import org.bson.BsonDocument;
import org.bson.BsonReader;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriter;
import org.bson.json.JsonWriterSettings;

import java.io.StringWriter;

import static com.mongodb.internal.observability.micrometer.TracingManager.ENV_OBSERVABILITY_QUERY_TEXT_MAX_LENGTH;
import static com.mongodb.internal.EnvironmentProvider.getEnv;
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
    /**
     * {@code micrometer-tracing} is an optional dependency: consumers may have only {@code micrometer-observation}
     * on the classpath (e.g. metrics/logging-only observability setups). Guard any use of {@code io.micrometer.tracing.*}
     * types with this flag so that such setups never trigger a {@link NoClassDefFoundError} or {@link LinkageError}.
     */
    static final boolean MICROMETER_TRACING_ON_CLASSPATH = isMicrometerTracingOnClasspath();

    private final ObservationRegistry observationRegistry;
    private final boolean allowCommandPayload;
    private final int textMaxLength;
    private final ObservationConvention<MongodbObservationContext> convention;

    private static boolean isMicrometerTracingOnClasspath() {
        try {
            Class.forName("io.micrometer.tracing.handler.TracingObservationHandler",
                    false, MicrometerTracer.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException | LinkageError e) {
            return false;
        }
    }

    /**
     * Constructs a new {@link MicrometerTracer} instance.
     *
     * @param observationRegistry The Micrometer {@link ObservationRegistry} to delegate tracing operations to.
     * @param allowCommandPayload Whether to allow command payloads in the trace context.
     * @param textMaxLength       The maximum length for query text truncation.
     * @param customConvention    A custom observation convention, or null to use the default.
     */
    public MicrometerTracer(final ObservationRegistry observationRegistry, final boolean allowCommandPayload,
            final int textMaxLength, @Nullable final ObservationConvention<MongodbObservationContext> customConvention) {
        this.allowCommandPayload = allowCommandPayload;
        this.observationRegistry = observationRegistry;
        this.textMaxLength = ofNullable(getEnv(ENV_OBSERVABILITY_QUERY_TEXT_MAX_LENGTH))
                .map(Integer::parseInt)
                .orElse(textMaxLength);
        this.convention = customConvention != null ? customConvention : new DefaultMongodbObservationConvention();
    }

    @Override
    public Span nextSpan(final MongodbObservation observationType, final String name,
            @Nullable final TraceContext parent, @Nullable final MongoNamespace namespace) {
        Observation observation = getObservation(observationType, name);

        if (parent instanceof MicrometerTraceContext) {
            Observation parentObservation = ((MicrometerTraceContext) parent).observation;
            if (parentObservation != null) {
                observation.parentObservation(parentObservation);
            }
        }

        return new MicrometerSpan(observation.start(), namespace, textMaxLength);
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public boolean includeCommandPayload() {
        return allowCommandPayload;
    }

    private Observation getObservation(final MongodbObservation observationType, final String name) {
        return observationType.observation(observationRegistry, () -> {
            MongodbObservationContext ctx = new MongodbObservationContext();
            ctx.setObservationType(observationType);
            return ctx;
        }).observationConvention(convention).contextualName(name);
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

        @Override
        @Nullable
        public String traceParent() {
            if (observation == null) {
                return null;
            }
            if (!MICROMETER_TRACING_ON_CLASSPATH) {
                return null;
            }
            return MicrometerTracingSupport.traceParent(observation);
        }

        /**
         * The server ({@code validateW3CTraceparent}) rejects ids that are not exactly the expected
         * length of lowercase hex, or that are all zeroes. Never emit a traceparent it would reject.
         */
        static boolean isValidNonZeroLowercaseHex(@Nullable final String value, final int expectedLength) {
            if (value == null || value.length() != expectedLength) {
                return false;
            }
            boolean nonZero = false;
            for (int i = 0; i < value.length(); i++) {
                char c = value.charAt(i);
                if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
                    return false;
                }
                if (c != '0') {
                    nonZero = true;
                }
            }
            return nonZero;
        }
    }

    /**
     * Isolates all hard references to {@code io.micrometer.tracing.*} types. This class must only be loaded/invoked
     * after {@link #MICROMETER_TRACING_ON_CLASSPATH} has been confirmed {@code true}, so that JVMs which resolve
     * classes referenced by bytecode eagerly (e.g. during verification) do not trigger a {@link NoClassDefFoundError}
     * when {@code micrometer-tracing} is absent from the classpath.
     */
    private static final class MicrometerTracingSupport {
        @Nullable
        static String traceParent(final Observation observation) {
            io.micrometer.tracing.handler.TracingObservationHandler.TracingContext tracingContext =
                    observation.getContextView().get(io.micrometer.tracing.handler.TracingObservationHandler.TracingContext.class);
            if (tracingContext == null || tracingContext.getSpan() == null) {
                return null;
            }
            io.micrometer.tracing.TraceContext ctx = tracingContext.getSpan().context();
            if (ctx == null) {
                return null;
            }
            String traceId = ctx.traceId();
            String spanId = ctx.spanId();
            if (!MicrometerTraceContext.isValidNonZeroLowercaseHex(traceId, 32)
                    || !MicrometerTraceContext.isValidNonZeroLowercaseHex(spanId, 16)) {
                return null;
            }
            Boolean sampled = ctx.sampled();
            return "00-" + traceId + "-" + spanId + (sampled != null && sampled ? "-01" : "-00");
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
        @Nullable
        private Observation.Scope scope;

        /**
         * Constructs a new {@link MicrometerSpan} instance with an associated Observation and MongoDB namespace.
         *
         * @param observation     The Micrometer {@link Observation}, or null if none exists.
         * @param namespace       The MongoDB namespace associated with the span.
         * @param queryTextLength The maximum length for query text truncation.
         */
        MicrometerSpan(final Observation observation, @Nullable final MongoNamespace namespace, final int queryTextLength) {
            this.namespace = namespace;
            this.observation = observation;
            this.queryTextLength = queryTextLength;
        }

        @Override
        public void openScope() {
            this.scope = observation.openScope();
        }

        @Override
        public void closeScope() {
            if (scope != null) {
                scope.close();
                scope = null;
            }
        }

        @Override
        public void setQueryText(final BsonDocument commandDocument) {
            MongodbObservationContext ctx = getMongodbObservationContext();
            if (ctx != null) {
                ctx.setQueryText((queryTextLength < Integer.MAX_VALUE)
                        ? getTruncatedBsonDocument(commandDocument)
                        : commandDocument.toString());
            }
        }

        @Override
        public void event(final String event) {
            observation.event(() -> event);
        }

        @Override
        public void error(final Throwable throwable) {
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
        public MongodbObservationContext getMongodbObservationContext() {
            if (observation.getContext() instanceof MongodbObservationContext) {
                return (MongodbObservationContext) observation.getContext();
            }
            return null;
        }

        @Override
        @Nullable
        public MongoNamespace getNamespace() {
            return namespace;
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
