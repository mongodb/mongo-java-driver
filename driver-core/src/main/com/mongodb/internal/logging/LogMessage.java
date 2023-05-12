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
import com.mongodb.lang.Nullable;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class LogMessage {

    private final Component component;
    private final Level level;
    private final String messageId;
    private final ClusterId clusterId;
    private final Throwable exception;
    private final Collection<Entry> entries;
    private final String format;

    public enum Component {
        COMMAND,
        CONNECTION
    }

    public enum Level {
        DEBUG
    }

    public static final class Entry {
        private final String name;
        private final Object value;

        public Entry(final String name, final @Nullable Object value) {
            this.name = name;
            this.value = value;
        }

        public String getName() {
            return name;
        }

        @Nullable
        public Object getValue() {
            return value;
        }

        public enum Name {
            SERVER_HOST("serverHost"),
            SERVER_PORT("serverPort"),
            COMMAND_NAME("commandName"),
            REQUEST_ID("requestId"),
            OPERATION_ID("operationId"),
            SERVICE_ID("serviceId"),
            SERVER_CONNECTION_ID("serverConnectionId"),
            DRIVER_CONNECTION_ID("driverConnectionId"),
            DURATION_MS("durationMS"),
            DATABASE_NAME("databaseName"),
            REPLY("reply"),
            COMMAND_CONTENT("command"),
            REASON_DESCRIPTION("reason"),
            ERROR_DESCRIPTION("error");

            private final String value;

            public String getValue() {
                return value;
            }

            Name(final String value) {
                this.value = value;
            }
        }
    }

    public LogMessage(final Component component, final Level level, final String messageId, final ClusterId clusterId,
            final List<Entry> entries, final String format) {
        this(component, level, messageId, clusterId, null, entries, format);
    }

    public LogMessage(final Component component, final Level level, final String messageId, final ClusterId clusterId,
                                @Nullable final Throwable exception, final Collection<Entry> entries, final String format) {
        this.component = component;
        this.level = level;
        this.messageId = messageId;
        this.clusterId = clusterId;
        this.exception = exception;
        this.entries = entries;
        this.format = format;
    }

    public ClusterId getClusterId() {
        return clusterId;
    }

    public LogMessage.StructuredLogMessage toStructuredLogMessage() {
        List<LogMessage.Entry> nullableEntries = entries.stream()
                .filter(entry -> entry.getValue() != null)
                .collect(Collectors.toList());
        return new LogMessage.StructuredLogMessage(nullableEntries);
    }

    public LogMessage.UnstructuredLogMessage toUnstructuredLogMessage() {
        return new LogMessage.UnstructuredLogMessage();
    }

    public final class StructuredLogMessage {
        private final Collection<LogMessage.Entry> entries;

        public StructuredLogMessage(final Collection<LogMessage.Entry> entries) {
            entries.forEach(entry -> notNull(entry.getName(), entry.getValue()));
            this.entries = entries;
        }

        public LogMessage.Component getComponent() {
            return component;
        }

        public LogMessage.Level getLevel() {
            return level;
        }

        public String getMessageId() {
            return messageId;
        }
        @Nullable
        public Throwable getException() {
            return exception;
        }

        public Collection<LogMessage.Entry> getEntries() {
            return entries;
        }
    }

    public final class UnstructuredLogMessage {
        /**
         * Interpolates the specified string format with the values in the entries collection.
         * The format string can contain {} placeholders for values and [] placeholders for conditionals.
         * <p>
         * For example, [ with service-id {}] will wrap the sentence with 'service-id {}' within the conditionals.
         * If the corresponding {@link LogMessage.Entry#getValue()} for the placeholder is null, the entire sentence within the conditionals will be
         * omitted.
         * <p>
         * If the {@link LogMessage.Entry#getValue()} for the {} placeholder is null outside of conditionals, then null will be placed instead of
         * placeholder.
         * <p>
         * The method will iterate through the values in the entries collection and fill the placeholders in the order specified.
         * If the number of placeholders does not correspond to the number of entries in the collection, a NoSuchElementException will be thrown.
         *
         * @return the interpolated string with the values from the entries collection filled in the placeholders.
         * @throws NoSuchElementException – if the iteration has no more elements.
         */
        public String interpolate() {
            Iterator<LogMessage.Entry> iterator = entries.iterator();
            StringBuilder builder = new StringBuilder();
            int s = 0, i = 0;
            while (i < format.length()) {
                char curr = format.charAt(i);
                if (curr == '[' || curr == '{') {
                    Object value = iterator.next().getValue();
                    builder.append(format, s, i);
                    if (curr == '{') {
                        builder.append(value);
                    } else if (value == null) {
                        i = format.indexOf(']', i);
                    } else {
                        int openBrace = format.indexOf('{', i);
                        builder.append(format, i + 1, openBrace);
                        builder.append(value);
                        i = openBrace + 1;
                    }
                    s = i + 1;
                } else if (curr == ']' || curr == '}') {
                    if (curr == ']') {
                        builder.append(format, s, i);
                    }
                    s = i + 1;
                }
                i++;
            }
            builder.append(format, s, format.length());
            return builder.toString();
        }
    }
}
