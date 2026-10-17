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

import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.test.simple.SimpleTracer;
import io.micrometer.tracing.test.simple.SimpleTraceContext;
import io.micrometer.tracing.handler.DefaultTracingObservationHandler;
import com.mongodb.observability.micrometer.MongodbObservation;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MicrometerTraceParentTest {
    private static final String VALID_TRACE_ID = "0af7651916cd43dd8448eb211c80319c";
    private static final String VALID_SPAN_ID = "b7ad6b7169203331";

    @Test
    void shouldFormatSampledTraceParent() {
        String traceParent = traceParentFor(VALID_TRACE_ID, VALID_SPAN_ID, true);
        assertEquals("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", traceParent);
        assertEquals(55, traceParent.length());
    }

    @Test
    void shouldFormatUnsampledTraceParentWithZeroFlags() {
        assertEquals("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00",
                traceParentFor(VALID_TRACE_ID, VALID_SPAN_ID, false));
        assertEquals("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00",
                traceParentFor(VALID_TRACE_ID, VALID_SPAN_ID, null));
    }

    @Test
    void shouldReturnNullForInvalidIds() {
        assertNull(traceParentFor("00000000000000000000000000000000", VALID_SPAN_ID, true));
        assertNull(traceParentFor(VALID_TRACE_ID, "0000000000000000", true));
        assertNull(traceParentFor("abc", VALID_SPAN_ID, true));
        assertNull(traceParentFor(VALID_TRACE_ID, "abc", true));
        assertNull(traceParentFor("0AF7651916CD43DD8448EB211C80319C", VALID_SPAN_ID, true));
        assertNull(traceParentFor(null, VALID_SPAN_ID, true));
        assertNull(traceParentFor(VALID_TRACE_ID, null, true));
    }

    /**
     * {@code micrometer-tracing} is an optional dependency. Consumers who only have {@code micrometer-observation}
     * on the classpath (metrics/logging-only observability setups) must not see a {@link NoClassDefFoundError} or
     * any other {@link LinkageError} the first time {@code traceParent()} is called.
     */
    @Test
    void shouldExposeMicrometerTracingClasspathGuard() {
        // Sanity check on the current test classpath (micrometer-tracing IS present here).
        assertTrue(MicrometerTracer.MICROMETER_TRACING_ON_CLASSPATH);
    }

    /**
     * Simulates a consumer that has only {@code micrometer-observation} on the classpath (no
     * {@code micrometer-tracing}), by loading {@link MicrometerTracer} (and its dependency classes) through an
     * isolated classloader that refuses to resolve {@code io.micrometer.tracing.*}. Asserts that {@code
     * traceParent()} returns {@code null} rather than throwing {@link NoClassDefFoundError}.
     */
    @Test
    void shouldReturnNullWhenMicrometerTracingIsAbsentFromClasspath() throws Exception {
        List<URL> urls = new ArrayList<>();
        String[] classpathEntries = System.getProperty("java.class.path").split(System.getProperty("path.separator"));
        for (String entry : classpathEntries) {
            if (entry.contains("micrometer-tracing")) {
                continue;
            }
            urls.add(new java.io.File(entry).toURI().toURL());
        }

        try (URLClassLoader isolatedLoader = new URLClassLoader(urls.toArray(new URL[0]), null) {
            @Override
            protected Class<?> loadClass(final String name, final boolean resolve) throws ClassNotFoundException {
                if (name.startsWith("io.micrometer.tracing.")) {
                    throw new ClassNotFoundException(name);
                }
                if (name.startsWith("java.") || name.startsWith("javax.") || name.startsWith("jdk.")) {
                    return Class.forName(name, resolve, null);
                }
                return super.loadClass(name, resolve);
            }
        }) {
            Class<?> tracerClass = Class.forName(
                    "com.mongodb.internal.observability.micrometer.MicrometerTracer", true, isolatedLoader);

            Field guard = tracerClass.getDeclaredField("MICROMETER_TRACING_ON_CLASSPATH");
            guard.setAccessible(true);
            assertFalse((Boolean) guard.get(null),
                    "guard should detect that io.micrometer.tracing is not resolvable in the isolated loader");

            Class<?> registryClass = Class.forName("io.micrometer.observation.ObservationRegistry", true, isolatedLoader);
            Object registry = registryClass.getMethod("create").invoke(null);

            Constructor<?> tracerCtor = tracerClass.getDeclaredConstructor(
                    registryClass, boolean.class, int.class,
                    Class.forName("io.micrometer.observation.ObservationConvention", true, isolatedLoader));
            Object micrometerTracer = tracerCtor.newInstance(registry, false, 1000, null);

            Class<?> observationTypeClass = Class.forName(
                    "com.mongodb.observability.micrometer.MongodbObservation", true, isolatedLoader);
            Object observationType = observationTypeClass.getField("MONGODB_COMMAND").get(null);

            Class<?> tracerInterface = Class.forName(
                    "com.mongodb.internal.observability.micrometer.Tracer", true, isolatedLoader);
            Method nextSpan = tracerInterface.getMethod("nextSpan", observationTypeClass, String.class,
                    Class.forName("com.mongodb.internal.observability.micrometer.TraceContext", true, isolatedLoader),
                    Class.forName("com.mongodb.MongoNamespace", true, isolatedLoader));
            Object span = nextSpan.invoke(micrometerTracer, observationType, "find", null, null);

            Class<?> spanInterface = Class.forName(
                    "com.mongodb.internal.observability.micrometer.Span", true, isolatedLoader);
            spanInterface.getMethod("openScope").invoke(span);

            Object traceContext = assertDoesNotThrow(() -> spanInterface.getMethod("context").invoke(span),
                    "obtaining the trace context must not throw when micrometer-tracing is absent");

            Class<?> traceContextInterface = Class.forName(
                    "com.mongodb.internal.observability.micrometer.TraceContext", true, isolatedLoader);
            Object traceParent = assertDoesNotThrow(
                    () -> traceContextInterface.getMethod("traceParent").invoke(traceContext),
                    "traceParent() must not throw NoClassDefFoundError when micrometer-tracing is absent");

            assertNull(traceParent);

            spanInterface.getMethod("closeScope").invoke(span);
            spanInterface.getMethod("end").invoke(span);
        }
    }

    private static String traceParentFor(final String traceId, final String spanId, final Boolean sampled) {
        ObservationRegistry registry = ObservationRegistry.create();
        SimpleTracer tracer = new SimpleTracer();
        registry.observationConfig().observationHandler(new DefaultTracingObservationHandler(tracer));

        MicrometerTracer micrometerTracer = new MicrometerTracer(registry, false, 1000, null);
        Span span = micrometerTracer.nextSpan(MongodbObservation.MONGODB_COMMAND, "find", null, null);
        span.openScope();
        try {
            SimpleTraceContext context = (SimpleTraceContext) tracer.lastSpan().context();
            context.setTraceId(traceId);
            context.setSpanId(spanId);
            context.setSampled(sampled);
            return span.context().traceParent();
        } finally {
            span.closeScope();
            span.end();
        }
    }
}
