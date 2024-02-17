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
package com.mongodb.connection;

import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;

/**
 * The server monitoring mode, which defines the monitoring protocol to use.
 *
 * @since 5.1
 */
public enum ServerMonitoringMode {
    /**
     * Use the streaming protocol whe the server supports it or fall back to the polling protocol otherwise.
     */
    STREAM("stream"),
    /**
     * Use the polling protocol.
     */
    POLL("poll"),
    /**
     * Behave the same as {@link #POLL} if running in a FaaS environment, otherwise behave as {@link #STREAM}.
     * This is the default.
     */
    AUTO("auto");

    private final String value;

    ServerMonitoringMode(final String value) {
        this.value = value;
    }

    /**
     * Parses a string into {@link ServerMonitoringMode}.
     *
     * @param serverMonitoringMode A server monitoring mode string.
     * @return The corresponding {@link ServerMonitoringMode} value.
     * @see #getValue()
     */
    public static ServerMonitoringMode fromString(final String serverMonitoringMode) {
        notNull("serverMonitoringMode", serverMonitoringMode);
        for (ServerMonitoringMode mode : ServerMonitoringMode.values()) {
            if (serverMonitoringMode.equalsIgnoreCase(mode.value)) {
                return mode;
            }
        }
        throw new IllegalArgumentException(format("'%s' is not a valid %s",
                serverMonitoringMode, ServerMonitoringMode.class.getSimpleName()));
    }

    /**
     * The string value.
     *
     * @return The string value.
     * @see #fromString(String)
     */
    public String getValue() {
        return value;
    }
}
