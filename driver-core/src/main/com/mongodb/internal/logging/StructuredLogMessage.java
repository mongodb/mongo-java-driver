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

import java.util.Arrays;
import java.util.List;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class StructuredLogMessage {

    private final String loggerName;
    private final String level;
    private final String messageId;
    private final ClusterId clusterId;
    private final Throwable exception;
    private final List<Entry> entries;

    public static final class Entry {
        private final String name;
        private final Object value;

        public Entry(final String name, final Object value) {
            this.name = name;
            this.value = value;
        }

        public String getName() {
            return name;
        }

        public Object getValue() {
            return value;
        }
    }

    public StructuredLogMessage(final String loggerName, final String level, final String messageId, final ClusterId clusterId,
            final Entry... entries) {
        this(loggerName, level, messageId, clusterId, null, entries);
    }

    public StructuredLogMessage(final String loggerName, final String level, final String messageId, final ClusterId clusterId,
            final Throwable exception, final Entry... entries) {
        this.loggerName = loggerName;
        this.level = level;
        this.messageId = messageId;
        this.clusterId = clusterId;
        this.exception = exception;
        this.entries = Arrays.asList(entries);
    }

    public String getLoggerName() {
        return loggerName;
    }

    public String getLevel() {
        return level;
    }

    public String getMessageId() {
        return messageId;
    }

    public ClusterId getClusterId() {
        return clusterId;
    }

    public Throwable getException() {
        return exception;
    }

    public List<Entry> getEntries() {
        return entries;
    }
}
