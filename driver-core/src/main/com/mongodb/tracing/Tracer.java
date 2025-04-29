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

public interface Tracer {
    Tracer NO_OP = new Tracer() {

        @Override
        public TraceContext currentContext() {
            return TraceContext.EMPTY;
        }

        @Override
        public Span nextSpan(final String name) {
            return Span.EMPTY;
        }

        @Override
        public Span nextSpan(final String name, final TraceContext parent) {
            return Span.EMPTY;
        }

        @Override
        public boolean enabled() {
            return false;
        }
    };

    TraceContext currentContext();

    Span nextSpan(String name); // uses current active span

    Span nextSpan(String name, TraceContext parent); // manually attach the next span to the provided parent

    boolean enabled();
}
