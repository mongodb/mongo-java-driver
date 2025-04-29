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

package com.mongodb.internal.tracing;

import com.mongodb.tracing.Tracer;

import java.util.HashMap;

public class TracingManager {
    public static final TracingManager NO_OP = new TracingManager(Tracer.NO_OP);

    private final Tracer tracer;
    private final TraceContext parentContext;

    // Map a cursor id to its parent context (useful for folding getMore commands under the parent operation)
    private final HashMap<Long, TraceContext> cursors = new HashMap<>();

    // Map an operation's span context so the subsequent commands spans can fold under the parent operation
    private final HashMap<Long, TraceContext> operationContexts = new HashMap<>();

    public TracingManager(final Tracer tracer) {
        this.tracer = tracer;
        this.parentContext = tracer.currentContext();
    }

    public TracingManager(final Tracer tracer, final TraceContext parentContext) {
        this.tracer = tracer;
        this.parentContext = parentContext;
    }

    public Span addSpan(final String name, final Long operationId) {
        Span span = tracer.nextSpan(name);
        operationContexts.put(operationId, span.context());
        return span;
    }

    public Span addSpan(final String name, final TraceContext parentContext) {
        return tracer.nextSpan(name, parentContext);
    }

    public void cleanContexts(final Long operationId) {
        operationContexts.remove(operationId);
    }

    public TraceContext getParentContext(final Long operationId) {
        assert operationContexts.containsKey(operationId);
        return operationContexts.get(operationId);
    }

    public void addCursorParentContext(final long cursorId, final long operationId) {
        assert operationContexts.containsKey(operationId);
        cursors.put(cursorId, operationContexts.get(operationId));
    }

    public TraceContext getCursorParentContext(final long cursorId) {
        return cursors.get(cursorId);
    }

    public void removeCursorParentContext(final long cursorId) {
        cursors.remove(cursorId);
    }

    public boolean isEnabled() {
        return tracer.enabled();
    }
}
