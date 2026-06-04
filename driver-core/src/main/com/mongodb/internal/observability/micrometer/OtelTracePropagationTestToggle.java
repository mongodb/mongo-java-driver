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

/**
 * TEST-ONLY switch (DRIVERS-3454): when {@code true}, the driver writes the OP_MSG OpenTelemetry
 * trace-context section even if the server did not advertise {@code tracingSupport} in its
 * {@code hello} response. The sampled-{@code traceparent} requirement still applies.
 *
 * <p>This exists only to exercise end-to-end propagation against a server that does not yet advertise
 * the capability. Remove before any production use.</p>
 */
public final class OtelTracePropagationTestToggle {
    public static volatile boolean FORCE_PROPAGATION = false;

    private OtelTracePropagationTestToggle() {
    }
}
