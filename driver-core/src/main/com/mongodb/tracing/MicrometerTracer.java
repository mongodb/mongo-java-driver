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

package com.mongodb.tracing;

import com.mongodb.MongoNamespace;
import com.mongodb.internal.tracing.Span;
import com.mongodb.internal.tracing.TraceContext;
import com.mongodb.internal.tracing.Tracer;
import com.mongodb.lang.Nullable;
import io.micrometer.common.KeyValue;
import io.micrometer.common.KeyValues;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;

import java.io.PrintWriter;
import java.io.StringWriter;

import static com.mongodb.tracing.MongodbObservation.LowCardinalityKeyNames.EXCEPTION_MESSAGE;
import static com.mongodb.tracing.MongodbObservation.LowCardinalityKeyNames.EXCEPTION_STACKTRACE;
import static com.mongodb.tracing.MongodbObservation.LowCardinalityKeyNames.EXCEPTION_TYPE;
import static com.mongodb.tracing.MongodbObservation.MONGODB_OBSERVATION;


/**
 * A {@link Tracer} implementation that delegates tracing operations to a Micrometer {@link io.micrometer.observation.ObservationRegistry}.
 * <p>
 * This class enables integration of MongoDB driver tracing with Micrometer-based tracing systems.
 * It provides integration with Micrometer to propagate observations into tracing API.
 * </p>
 *
 * @since 5.7
 */
public class MicrometerTracer implements Tracer {
    private final boolean allowCommandPayload;
    private final ObservationRegistry observationRegistry;

    /**
     * Constructs a new {@link MicrometerTracer} instance.
     *
     * @param observationRegistry The Micrometer {@link ObservationRegistry} to delegate tracing operations to.
     */
    public MicrometerTracer(final ObservationRegistry observationRegistry) {
        this(observationRegistry, false);
    }

    /**
     * Constructs a new {@link MicrometerTracer} instance with an option to allow command payloads.
     *
     * @param observationRegistry The Micrometer {@link ObservationRegistry} to delegate tracing operations to.
     * @param allowCommandPayload Whether to allow command payloads in the trace context.
     */
    public MicrometerTracer(final ObservationRegistry observationRegistry, final boolean allowCommandPayload) {
        this.allowCommandPayload = allowCommandPayload;
        this.observationRegistry = observationRegistry;
    }

    @Override
    public Span nextSpan(final String name, @Nullable final TraceContext parent, @Nullable final MongoNamespace namespace) {
        if (parent instanceof MicrometerTraceContext) {
            Observation parentObservation = ((MicrometerTraceContext) parent).observation;
            if (parentObservation != null) {
                return new MicrometerSpan(MONGODB_OBSERVATION
                        .observation(observationRegistry)
                        .contextualName(name)
                        .parentObservation(parentObservation)
                        .start(), namespace);
            }
        }
        return new MicrometerSpan(MONGODB_OBSERVATION.observation(observationRegistry).contextualName(name).start(), namespace);
    }

    @Override
    public boolean enabled() {
        return true;
    }

    @Override
    public boolean includeCommandPayload() {
        return allowCommandPayload;
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

        /**
         * Constructs a new {@link MicrometerSpan} instance with an associated Observation.
         *
         * @param observation The Micrometer {@link Observation}, or null if none exists.
         */
        MicrometerSpan(final Observation observation) {
            this.observation = observation;
            this.namespace = null;
        }

        /**
         * Constructs a new {@link MicrometerSpan} instance with an associated Observation and MongoDB namespace.
         *
         * @param observation The Micrometer {@link Observation}, or null if none exists.
         * @param namespace   The MongoDB namespace associated with the span.
         */
        MicrometerSpan(final Observation observation, @Nullable final MongoNamespace namespace) {
            this.namespace = namespace;
            this.observation = observation;
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
        public void tagHighCardinality(final KeyValue keyValue) {
            observation.highCardinalityKeyValue(keyValue);
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
    }
}
