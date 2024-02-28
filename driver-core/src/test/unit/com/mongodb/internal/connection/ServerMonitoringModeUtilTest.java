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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ServerMonitoringModeUtilTest {
    @Test
    public void fromString() {
        assertAll(
                () -> assertEquals(ServerMonitoringMode.STREAM, ServerMonitoringModeUtil.fromString("stream")),
                () -> assertEquals(ServerMonitoringMode.POLL, ServerMonitoringModeUtil.fromString("poll")),
                () -> assertEquals(ServerMonitoringMode.AUTO, ServerMonitoringModeUtil.fromString("auto")),
                () -> assertThrows(IllegalArgumentException.class, () -> ServerMonitoringModeUtil.fromString("invalid"))
        );
    }

    @Test
    public void getValue() {
        assertAll(
                () -> assertEquals("stream", ServerMonitoringModeUtil.getValue(ServerMonitoringMode.STREAM)),
                () -> assertEquals("poll", ServerMonitoringModeUtil.getValue(ServerMonitoringMode.POLL)),
                () -> assertEquals("auto", ServerMonitoringModeUtil.getValue(ServerMonitoringMode.AUTO))
        );
    }
}
