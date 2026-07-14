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

@SuppressWarnings("InterfaceIsType")
public interface TraceContext {
    TraceContext EMPTY = new TraceContext() {
        @Override
        public String traceParent() {
            return null;
        }
    };

    /**
     * The 55-char W3C {@code traceparent} string for this context
     * ({@code 00-<32 hex traceId>-<16 hex spanId>-<flags>}; flags {@code 01} sampled / {@code 00} unsampled),
     * or {@code null} when there is no valid context (no-op span, missing/zero/malformed ids).
     * Never includes tracestate.
     */
    @Nullable
    String traceParent();
}
