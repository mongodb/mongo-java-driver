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

package com.mongodb.internal.connection;

import com.mongodb.internal.observability.micrometer.Span;

/**
 * Emits command events while the command span's scope is open on the emitting thread, so that command
 * listeners and log appenders observe the command span (for example as {@code traceId}/{@code spanId} in
 * the MDC). The scope is opened and closed around each event; {@link Span#closeScope()} is idempotent.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
final class TracingScopedCommandEventSender implements CommandEventSender {
    private final CommandEventSender delegate;
    private final Span span;

    TracingScopedCommandEventSender(final CommandEventSender delegate, final Span span) {
        this.delegate = delegate;
        this.span = span;
    }

    @Override
    public void sendStartedEvent() {
        span.openScope();
        try {
            delegate.sendStartedEvent();
        } finally {
            span.closeScope();
        }
    }

    @Override
    public void sendFailedEvent(final Throwable t) {
        span.openScope();
        try {
            delegate.sendFailedEvent(t);
        } finally {
            span.closeScope();
        }
    }

    @Override
    public void sendSucceededEvent(final ResponseBuffers responseBuffers) {
        span.openScope();
        try {
            delegate.sendSucceededEvent(responseBuffers);
        } finally {
            span.closeScope();
        }
    }

    @Override
    public void sendSucceededEventForOneWayCommand() {
        span.openScope();
        try {
            delegate.sendSucceededEventForOneWayCommand();
        } finally {
            span.closeScope();
        }
    }
}
