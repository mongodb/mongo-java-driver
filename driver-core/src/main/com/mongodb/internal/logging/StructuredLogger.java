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
import com.mongodb.internal.logging.StructuredLogMessage.Level;
import com.mongodb.lang.Nullable;

import java.util.concurrent.ConcurrentHashMap;

import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;
import static java.lang.String.format;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class StructuredLogger {

    private static final ConcurrentHashMap<String, StructuredLoggingInterceptor> INTERCEPTORS = new ConcurrentHashMap<>();

    private final Logger logger;

    @VisibleForTesting(otherwise = PRIVATE)
    public static void addInterceptor(final String clusterDescription, final StructuredLoggingInterceptor interceptor) {
        INTERCEPTORS.put(clusterDescription, interceptor);
    }

    @VisibleForTesting(otherwise = PRIVATE)
    public static void removeInterceptor(final String clusterDescription) {
        INTERCEPTORS.remove(clusterDescription);
    }

    @Nullable
    private static StructuredLoggingInterceptor getInterceptor(@Nullable final String clusterDescription) {
        if (clusterDescription == null) {
            return null;
        }
        return INTERCEPTORS.get(clusterDescription);
    }

    public StructuredLogger(final String suffix) {
        this(Loggers.getLogger(suffix));
    }

    @VisibleForTesting(otherwise = PRIVATE)
    public StructuredLogger(final Logger logger) {
        this.logger = logger;
    }

    public boolean isRequired(final Level level, final ClusterId clusterId) {
        if (getInterceptor(clusterId.getDescription()) != null) {
            return true;
        }

        //noinspection SwitchStatementWithTooFewBranches
        switch (level) {
            case DEBUG:
                return logger.isDebugEnabled();
            default:
                throw new UnsupportedOperationException();
        }
   }

    public void log(final StructuredLogMessage message, final String format) {
        StructuredLoggingInterceptor interceptor = getInterceptor(message.getClusterId().getDescription());
        if (interceptor != null) {
            interceptor.intercept(message);
        }
        //noinspection SwitchStatementWithTooFewBranches
        switch (message.getLevel()) {
            case DEBUG:
                if (logger.isDebugEnabled()) {
                    Throwable exception = message.getException();
                    if (exception == null) {
                        logger.debug(format(format, message.getEntries().stream().map(Entry::getValue).toArray()));
                    } else {
                        logger.debug(format(format, message.getEntries().stream().map(Entry::getValue).toArray()), exception);
                    }
                }
                break;
            default:
                throw new UnsupportedOperationException();
        }
    }
}
