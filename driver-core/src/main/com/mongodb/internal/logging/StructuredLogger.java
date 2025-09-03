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
import com.mongodb.internal.logging.LogMessage.Level;
import com.mongodb.lang.Nullable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class StructuredLogger {

    private static final ConcurrentHashMap<String, LoggingInterceptor> INTERCEPTORS = new ConcurrentHashMap<>();

    private final Logger logger;

    @VisibleForTesting(otherwise = PRIVATE)
    public static void addInterceptor(final String clusterDescription, final LoggingInterceptor interceptor) {
        INTERCEPTORS.put(clusterDescription, interceptor);
    }

    @VisibleForTesting(otherwise = PRIVATE)
    public static void removeInterceptor(final String clusterDescription) {
        INTERCEPTORS.remove(clusterDescription);
    }

    @Nullable
    private static LoggingInterceptor getInterceptor(@Nullable final String clusterDescription) {
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

        switch (level) {
            case DEBUG:
                return logger.isDebugEnabled();
            case INFO:
                return logger.isInfoEnabled();
            default:
                throw new UnsupportedOperationException();
        }
   }

    public void log(final LogMessage logMessage) {
        LoggingInterceptor interceptor = getInterceptor(logMessage.getClusterId().getDescription());
        if (interceptor != null) {
            interceptor.intercept(logMessage);
        }
        switch (logMessage.getLevel()) {
            case DEBUG:
                logUnstructured(logMessage, logger::isDebugEnabled, logger::debug, logger::debug);
                break;
            case INFO:
                logUnstructured(logMessage, logger::isInfoEnabled, logger::info, logger::info);
                break;
            default:
                throw new UnsupportedOperationException();
        }
    }

    private static void logUnstructured(
            final LogMessage logMessage,
            final Supplier<Boolean> loggingEnabled,
            final Consumer<String> doLog,
            final BiConsumer<String, Throwable> doLogWithException) {
        if (loggingEnabled.get()) {
            LogMessage.UnstructuredLogMessage unstructuredLogMessage = logMessage.toUnstructuredLogMessage();
            String message = unstructuredLogMessage.interpolate();
            Throwable exception = logMessage.getException();
            if (exception == null) {
                doLog.accept(message);
            } else {
                doLogWithException.accept(message, exception);
            }
        }
    }
}
