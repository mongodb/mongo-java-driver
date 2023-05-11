/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.unified;

import com.mongodb.internal.logging.LogMessage;
import com.mongodb.internal.logging.StructuredLogger;
import com.mongodb.internal.logging.StructuredLoggingInterceptor;
import com.mongodb.lang.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class TestLoggingInterceptor implements StructuredLoggingInterceptor, AutoCloseable {

    private final List<LogMessage.StructuredLogMessage> messages = new ArrayList<>();
    private final String applicationName;
    private final LoggingFilter filter;

    public TestLoggingInterceptor(final String applicationName, final LoggingFilter filter) {
        this.applicationName = requireNonNull(applicationName);
        this.filter = requireNonNull(filter);
        StructuredLogger.addInterceptor(applicationName, this);
    }

    @Override
    public synchronized void intercept(@NonNull final LogMessage.StructuredLogMessage message) {
        if (filter.match(message)) {
            messages.add(message);
        }
    }

    @Override
    public void close() {
        StructuredLogger.removeInterceptor(applicationName);
    }

    public synchronized List<LogMessage.StructuredLogMessage> getMessages() {
        return new ArrayList<>(messages);
    }

    public static final class LoggingFilter{
        private final Map<LogMessage.Component, LogMessage.Level> filterConfig;

        public LoggingFilter(Map<LogMessage.Component, LogMessage.Level> filterConfig){
            this.filterConfig = filterConfig;
        }
        boolean match(final LogMessage.StructuredLogMessage message){
            LogMessage.Level expectedLevel = filterConfig.get(message.getComponent());
            if(expectedLevel!=null) {
                return message.getLevel().compareTo(expectedLevel) <= 0;
            }
            return false;
        }
    }
}
