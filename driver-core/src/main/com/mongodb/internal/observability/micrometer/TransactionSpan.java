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

/**
 * State class for transaction tracing.
 *
 * @since 5.7
 */
public class TransactionSpan {
    private boolean isConvenientTransaction = false;
    private final Span span;
    @Nullable
    private Throwable reportedError;

    public TransactionSpan(final TracingManager tracingManager) {
        this.span = tracingManager.addTransactionSpan();
    }

    /**
     * Handles a transaction span error.
     * <p>
     * If the transaction is convenient, the error is reported as an event. This is done since
     * the error is not fatal and the transaction may be retried.
     * <p>
     * If the transaction is not convenient, the error is reported as a span error and the
     * transaction context is cleaned up.
     *
     * @param e The error to report.
     */
    public void handleTransactionSpanError(final Throwable e) {
        if (isConvenientTransaction) {
            // report error as event (since subsequent retries might succeed, also keep track of the last event
            span.event(e.toString());
            reportedError = e;
        } else {
            span.error(e);
        }

        if (!isConvenientTransaction) {
            span.end();
        }
    }

    /**
     * Finalizes the transaction span by logging the specified status as an event and ending the span.
     *
     * @param status The status to log as an event.
     */
    public void finalizeTransactionSpan(final String status) {
        span.event(status);
        // clear previous commit error if any
        if (!isConvenientTransaction) {
            span.end();
        }
        reportedError = null; // clear previous commit error if any
    }

    /**
     * Finalizes the transaction span by logging any last span event as an error and ending the span.
     * Optionally cleans up the transaction context if specified.
     *
     * @param cleanupTransactionContext A boolean indicating whether to clean up the transaction context.
     */
    public void spanFinalizing(final boolean cleanupTransactionContext) {
        if (reportedError != null) {
            span.error(reportedError);
        }
        span.end();
        reportedError = null;
        // Don't clean up transaction context if we're still retrying (we want the retries to fold under the original transaction span)
        if (cleanupTransactionContext) {
            isConvenientTransaction = false;
        }
    }

    /**
     * Indicates that the transaction is a convenient transaction.
     * <p>
     * This has an impact on how the transaction span is handled. If the transaction is convenient, any errors that occur
     * during the transaction are reported as events. If the transaction is not convenient, errors are reported as span
     * errors and the transaction context is cleaned up.
     */
    public void setIsConvenientTransaction() {
        this.isConvenientTransaction = true;
    }

    /**
     * Retrieves the trace context associated with the transaction span.
     *
     * @return The trace context associated with the transaction span.
     */
    public TraceContext getContext() {
        return span.context();
    }
}
