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

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.tck.TestObservationRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class MicrometerSpanScopeTest {

    @Test
    void nestedOpenAndCloseRestoreTheOuterScope() {
        TestObservationRegistry registry = TestObservationRegistry.create();
        Observation observation = Observation.start("op", registry);
        Span span = new MicrometerTracer.MicrometerSpan(observation, null, Integer.MAX_VALUE);

        span.openScope();                      // outer, e.g. MongoClusterImpl
        assertSame(observation, registry.getCurrentObservation());
        span.openScope();                      // inner, e.g. DefaultConnectionPool at log time
        assertSame(observation, registry.getCurrentObservation());
        span.closeScope();                     // inner close
        assertSame(observation, registry.getCurrentObservation(), "outer scope must still be current");
        span.closeScope();                     // outer close
        assertNull(registry.getCurrentObservation(), "no scope may remain after balanced closes");
        observation.stop();
    }

    @Test
    void openScopeAfterEndDoesNotThrow() {
        ObservationRegistry registry = TestObservationRegistry.create();
        Observation observation = Observation.start("op", registry);
        Span span = new MicrometerTracer.MicrometerSpan(observation, null, Integer.MAX_VALUE);
        span.end();
        assertDoesNotThrow(span::openScope);
        assertDoesNotThrow(span::closeScope);
    }

    @Test
    void extraCloseIsNoOp() {
        ObservationRegistry registry = TestObservationRegistry.create();
        Observation observation = Observation.start("op", registry);
        Span span = new MicrometerTracer.MicrometerSpan(observation, null, Integer.MAX_VALUE);
        span.closeScope();
        span.openScope();
        span.closeScope();
        assertDoesNotThrow(span::closeScope);
        observation.stop();
    }
}
