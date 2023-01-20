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

package com.mongodb.internal.logging;

import com.mongodb.connection.ClusterId;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.internal.logging.StructuredLogMessage.Entry;

import java.util.HashMap;
import java.util.Map;

import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;
import static java.lang.String.format;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class StructuredLogger {

    private final Logger logger;

    private static final Map<String, StructuredLoggingInterceptor> INTERCEPTORS = new HashMap<>();

    @VisibleForTesting(otherwise = PRIVATE)
    public static void addInterceptor(final String applicationName, final StructuredLoggingInterceptor interceptor) {
        INTERCEPTORS.put(applicationName, interceptor);
    }

    @VisibleForTesting(otherwise = PRIVATE)
    public static void removeInterceptor(final String applicationName) {
        INTERCEPTORS.remove(applicationName);
    }

    private static boolean hasInterceptor(final ClusterId clusterId) {
        return INTERCEPTORS.containsKey(clusterId.getDescription());
    }

    private static StructuredLoggingInterceptor getInterceptor(final ClusterId clusterId) {
        return INTERCEPTORS.get(clusterId.getDescription());
    }

    public StructuredLogger(final String suffix) {
        this(Loggers.getLogger(suffix));
    }

    @VisibleForTesting(otherwise = PRIVATE)
    public StructuredLogger(final Logger logger) {
        this.logger = logger;
    }

    public boolean isDebugRequired(final ClusterId clusterId) {
        return logger.isDebugEnabled() || hasInterceptor(clusterId);
    }

    public void debug(final String message, final ClusterId clusterId, final String format,
            final String k1, final Object v1,
            final String k2, final Object v2,
            final String k3, final Object v3,
            final String k4, final Object v4,
            final String k5, final Object v5,
            final String k6, final Object v6,
            final String k7, final Object v7,
            final String k8, final Object v8) {
        if (hasInterceptor(clusterId)) {
            getInterceptor(clusterId).intercept(new StructuredLogMessage(logger.getName(), "debug", message, clusterId,
                    new Entry(k1, v1), new Entry(k2, v2), new Entry(k3, v3), new Entry(k4, v4), new Entry(k5, v5), new Entry(k6, v6),
                    new Entry(k7, v7), new Entry(k8, v8)));
        }
        if (logger.isDebugEnabled()) {
            logger.debug(format(format, v1, v2, v3, v4, v5, v6, v7, v8));
        }
    }

    public void debug(final String message, final ClusterId clusterId, final String format,
            final String k1, final Object v1,
            final String k2, final Object v2,
            final String k3, final Object v3,
            final String k4, final Object v4,
            final String k5, final Object v5,
            final String k6, final Object v6,
            final String k7, final Object v7,
            final String k8, final Object v8,
            final String k9, final Object v9) {
        if (hasInterceptor(clusterId)) {
            getInterceptor(clusterId).intercept(new StructuredLogMessage(logger.getName(), "debug", message, clusterId,
                    new Entry(k1, v1), new Entry(k2, v2), new Entry(k3, v3), new Entry(k4, v4), new Entry(k5, v5), new Entry(k6, v6),
                    new Entry(k7, v7), new Entry(k8, v8), new Entry(k9, v9)));
        }
        if (logger.isDebugEnabled()) {
            logger.debug(format(format, v1, v2, v3, v4, v5, v6, v7, v8, v9));
        }
    }

    public void debug(final String message, final ClusterId clusterId, final Throwable exception, final String format,
            final String k1, final Object v1,
            final String k2, final Object v2,
            final String k3, final Object v3,
            final String k4, final Object v4,
            final String k5, final Object v5,
            final String k6, final Object v6,
            final String k7, final Object v7) {
        if (hasInterceptor(clusterId)) {
            getInterceptor(clusterId).intercept(new StructuredLogMessage(logger.getName(), "debug", message, clusterId, exception,
                    new Entry(k1, v1), new Entry(k2, v2), new Entry(k3, v3), new Entry(k4, v4), new Entry(k5, v5), new Entry(k6, v6),
                    new Entry(k7, v7)));
        }
        if (logger.isDebugEnabled()) {
            logger.debug(format(format, v1, v2, v3, v4, v5, v6, v7), exception);
        }
    }

    public void debug(final String message, final ClusterId clusterId, final Throwable exception, final String format,
            final String k1, final Object v1,
            final String k2, final Object v2,
            final String k3, final Object v3,
            final String k4, final Object v4,
            final String k5, final Object v5,
            final String k6, final Object v6,
            final String k7, final Object v7,
            final String k8, final Object v8) {
        if (hasInterceptor(clusterId)) {
            getInterceptor(clusterId).intercept(new StructuredLogMessage(logger.getName(), "debug", message, clusterId, exception,
                    new Entry(k1, v1), new Entry(k2, v2), new Entry(k3, v3), new Entry(k4, v4), new Entry(k5, v5), new Entry(k6, v6),
                    new Entry(k7, v7), new Entry(k8, v8)));
        }
        if (logger.isDebugEnabled()) {
            logger.debug(format(format, v1, v2, v3, v4, v5, v6, v7, v8), exception);
        }
    }
}
