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

import com.mongodb.internal.tracing.Span;
import com.mongodb.internal.tracing.TraceContext;
import com.mongodb.internal.tracing.Tracer;
import com.mongodb.lang.Nullable;

import java.io.PrintWriter;
import java.io.StringWriter;

import static com.mongodb.internal.tracing.Tags.EXCEPTION_MESSAGE;
import static com.mongodb.internal.tracing.Tags.EXCEPTION_STACKTRACE;
import static com.mongodb.internal.tracing.Tags.EXCEPTION_TYPE;

/**
 * A {@link Tracer} implementation that delegates tracing operations to a Micrometer {@link io.micrometer.tracing.Tracer}.
 * <p>
 * This class enables integration of MongoDB driver tracing with Micrometer-based tracing systems.
 * It provides methods to create and manage spans using the Micrometer tracing API.
 * </p>
 *
 * @since 5.6
 */
public class MicrometerTracer implements Tracer {
    private final io.micrometer.tracing.Tracer tracer;
    private final boolean allowCommandPayload;

    /**
     * Constructs a new {@link MicrometerTracer} instance.
     *
     * @param tracer The Micrometer {@link io.micrometer.tracing.Tracer} to delegate tracing operations to.
     */
    public MicrometerTracer(final io.micrometer.tracing.Tracer tracer) {
        this(tracer, false);
    }

    /**
     * Constructs a new {@link MicrometerTracer} instance with an option to allow command payloads.
     *
     * @param tracer              The Micrometer {@link io.micrometer.tracing.Tracer} to delegate tracing operations to.
     * @param allowCommandPayload Whether to allow command payloads in the trace context.
     */
    public MicrometerTracer(final io.micrometer.tracing.Tracer tracer, final boolean allowCommandPayload) {
        this.tracer = tracer;
        this.allowCommandPayload = allowCommandPayload;
    }

    @Override
    public TraceContext currentContext() {
        return new MicrometerTraceContext(tracer.currentTraceContext().context());
    }

    @Override
    public Span nextSpan(final String name) {
        return new MicrometerSpan(tracer.nextSpan().name(name).start());
    }

    @Override
    public Span nextSpan(final String name, @Nullable final TraceContext parent) {
        if (parent instanceof MicrometerTraceContext) {
            io.micrometer.tracing.TraceContext micrometerContext = ((MicrometerTraceContext) parent).getTraceContext();
            if (micrometerContext != null) {
                return new MicrometerSpan(tracer.spanBuilder()
                        .name(name)
                        .setParent(micrometerContext)
                        .start());
            }
        }
        return nextSpan(name);
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
        private final io.micrometer.tracing.TraceContext traceContext;

        /**
         * Constructs a new {@link MicrometerTraceContext} instance.
         *
         * @param traceContext The Micrometer {@link io.micrometer.tracing.TraceContext}, or null if none exists.
         */
        MicrometerTraceContext(@Nullable final io.micrometer.tracing.TraceContext traceContext) {
            this.traceContext = traceContext;
        }

        /**
         * Retrieves the underlying Micrometer trace context.
         *
         * @return The Micrometer {@link io.micrometer.tracing.TraceContext}, or null if none exists.
         */
        @Nullable
        public io.micrometer.tracing.TraceContext getTraceContext() {
            return traceContext;
        }
    }

    /**
     * Represents a Micrometer-based span.
     */
    private static class MicrometerSpan implements Span {
        private final io.micrometer.tracing.Span span;

        /**
         * Constructs a new {@link MicrometerSpan} instance.
         *
         * @param span The Micrometer {@link io.micrometer.tracing.Span} to delegate operations to.
         */
        MicrometerSpan(final io.micrometer.tracing.Span span) {
            this.span = span;
        }

        @Override
        public Span tag(final String key, final String value) {
            span.tag(key, value);
            return this;
        }

        @Override
        public Span tag(final String key, final Long value) {
            span.tag(key, value);
            return this;
        }

        @Override
        public void event(final String event) {
            span.event(event);
        }

        @Override
        public void error(final Throwable throwable) {
            span.tag(EXCEPTION_MESSAGE, throwable.getMessage());
            span.tag(EXCEPTION_TYPE, throwable.getClass().getName());
            span.tag(EXCEPTION_STACKTRACE, getStackTraceAsString(throwable));
            span.error(throwable);
        }

        @Override
        public void end() {
            span.end();
        }

        @Override
        public TraceContext context() {
            return new MicrometerTraceContext(span.context());
        }

        private String getStackTraceAsString(final Throwable throwable) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            throwable.printStackTrace(pw);
            return sw.toString();
        }
    }
}
