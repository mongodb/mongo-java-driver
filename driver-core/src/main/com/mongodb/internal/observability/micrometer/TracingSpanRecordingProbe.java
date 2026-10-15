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

import com.mongodb.lang.Nullable;
import io.micrometer.observation.Observation;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

/**
 * Reads, from a started {@link Observation}, whether the tracing span that Micrometer Tracing attached to it is being
 * recorded by the tracing SDK.
 * <p>
 * The driver depends only on {@code micrometer-observation}. When {@code micrometer-tracing} is present, its
 * {@code TracingObservationHandler} stores the span in the observation context under the
 * {@code TracingObservationHandler.TracingContext} class key during {@code onStart}, and the span's {@code isNoop()}
 * reports {@code true} when the SDK is not recording it. Both are resolved reflectively once; when either is absent,
 * every span is reported as recording, which is the pre-existing behavior.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
final class TracingSpanRecordingProbe {
    @Nullable
    private static final Class<?> TRACING_CONTEXT_CLASS;
    @Nullable
    private static final MethodHandle GET_SPAN;
    @Nullable
    private static final MethodHandle IS_NOOP;

    static {
        Class<?> tracingContextClass = null;
        MethodHandle getSpan = null;
        MethodHandle isNoop = null;
        try {
            ClassLoader classLoader = TracingSpanRecordingProbe.class.getClassLoader();
            tracingContextClass = Class.forName("io.micrometer.tracing.handler.TracingObservationHandler$TracingContext",
                    false, classLoader);
            Class<?> spanClass = Class.forName("io.micrometer.tracing.Span", false, classLoader);
            MethodHandles.Lookup lookup = MethodHandles.publicLookup();
            getSpan = lookup.findVirtual(tracingContextClass, "getSpan", MethodType.methodType(spanClass));
            isNoop = lookup.findVirtual(spanClass, "isNoop", MethodType.methodType(boolean.class));
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | LinkageError e) {
            tracingContextClass = null;
            getSpan = null;
            isNoop = null;
        }
        TRACING_CONTEXT_CLASS = tracingContextClass;
        GET_SPAN = getSpan;
        IS_NOOP = isNoop;
    }

    private TracingSpanRecordingProbe() {
    }

    /**
     * @return {@code false} only when Micrometer Tracing is present, has attached a span to the observation, and
     * reports that span as not recording; {@code true} in every other case.
     */
    static boolean isRecording(final Observation observation) {
        if (TRACING_CONTEXT_CLASS == null || GET_SPAN == null || IS_NOOP == null) {
            return true;
        }
        Object tracingContext = observation.getContext().get(TRACING_CONTEXT_CLASS);
        if (tracingContext == null) {
            return true;
        }
        try {
            Object span = GET_SPAN.invoke(tracingContext);
            return span == null || !(boolean) IS_NOOP.invoke(span);
        } catch (Throwable t) {
            return true;
        }
    }
}
