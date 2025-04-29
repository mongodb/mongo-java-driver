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

public class MicrometerTracer implements Tracer {
    private final io.micrometer.tracing.Tracer tracer;

    public MicrometerTracer(final io.micrometer.tracing.Tracer tracer) {
        this.tracer = tracer;
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
    public Span nextSpan(final String name, final TraceContext parent) {
        if (parent != null) {
            io.micrometer.tracing.Span span = tracer.spanBuilder()
                    .name(name)
                    .setParent(((MicrometerTraceContext) parent).getTraceContext())
                    .start();
            return new MicrometerSpan(span);
        } else {
            return nextSpan(name);
        }
    }

    @Override
    public boolean enabled() {
        return true;
    }

    private static class MicrometerTraceContext implements TraceContext {
        private final io.micrometer.tracing.TraceContext traceContext;

        MicrometerTraceContext(final io.micrometer.tracing.TraceContext traceContext) {
            this.traceContext = traceContext;
        }

        public io.micrometer.tracing.TraceContext getTraceContext() {
            return traceContext;
        }
    }

    private static class MicrometerSpan implements Span {
        private final io.micrometer.tracing.Span span;

        MicrometerSpan(final io.micrometer.tracing.Span span) {
            this.span = span;
        }

        @Override
        public void tag(final String key, final String value) {
            span.tag(key, value);
        }

        // TODO add variant with TimeUnit
        @Override
        public void event(final String event) {
            span.event(event);
        }

        @Override
        public void error(final Throwable throwable) {
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
    }
}
