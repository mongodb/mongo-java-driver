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

import com.mongodb.connection.ServerMonitoringMode;

import static java.lang.String.format;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class ServerMonitoringModeUtil {
    /**
     * Returns the string value of the provided {@code serverMonitoringMode}.
     *
     * @return The string value.
     * @see #fromString(String)
     */
    public static String getValue(final ServerMonitoringMode serverMonitoringMode) {
        return serverMonitoringMode.name().toLowerCase();
    }

    /**
     * Parses a string into {@link ServerMonitoringMode}.
     *
     * @param serverMonitoringMode A server monitoring mode string.
     * @return The corresponding {@link ServerMonitoringMode} value.
     * @see #getValue(ServerMonitoringMode)
     */
    public static ServerMonitoringMode fromString(final String serverMonitoringMode) {
        for (ServerMonitoringMode mode : ServerMonitoringMode.values()) {
            if (serverMonitoringMode.equalsIgnoreCase(mode.name())) {
                return mode;
            }
        }
        throw new IllegalArgumentException(format("'%s' is not a valid %s",
                serverMonitoringMode, ServerMonitoringMode.class.getSimpleName()));
    }

    private ServerMonitoringModeUtil() {
    }
}
