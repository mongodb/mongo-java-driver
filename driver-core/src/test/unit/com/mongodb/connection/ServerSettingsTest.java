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

import com.mongodb.ConnectionString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

final class ServerSettingsTest {
    private static final String DEFAULT_OPTIONS = "mongodb://localhost/?";

    @Test
    void defaults() {
        ServerSettings defaultServerSettings = ServerSettings.builder().build();
        assertAll(() -> assertEquals(ServerMonitoringMode.AUTO, defaultServerSettings.getServerMonitoringMode()));
    }

    @ParameterizedTest
    @MethodSource("equalAndHashCodeArgs")
    void equalAndHashCode(final ServerSettings.Builder serverSettingsBuilder) {
        ServerSettings default1 = ServerSettings.builder().build();
        ServerSettings default2 = ServerSettings.builder().build();
        ServerSettings actual1 = serverSettingsBuilder.build();
        ServerSettings actual2 = serverSettingsBuilder.build();
        assertAll(
                () -> assertEquals(default1, default2),
                () -> assertEquals(default1.hashCode(), default2.hashCode()),
                () -> assertEquals(actual1, actual2),
                () -> assertEquals(actual1.hashCode(), actual2.hashCode()),
                () -> assertNotEquals(default1, actual1)
        );
    }

    private static Stream<Arguments> equalAndHashCodeArgs() {
        return Stream.of(
                Arguments.of(ServerSettings.builder().serverMonitoringMode(ServerMonitoringMode.POLL))
        );
    }

    @Test
    void serverMonitoringMode() {
        assertAll(
                () -> assertEquals(
                        ServerMonitoringMode.POLL,
                        ServerSettings.builder()
                                .serverMonitoringMode(ServerMonitoringMode.POLL)
                                .build()
                                .getServerMonitoringMode(),
                        "should set"),
                () -> assertEquals(
                        ServerMonitoringMode.STREAM,
                        ServerSettings.builder()
                                .applySettings(ServerSettings.builder()
                                        .serverMonitoringMode(ServerMonitoringMode.STREAM)
                                        .build())
                                .build()
                                .getServerMonitoringMode(),
                        "should apply from settings"),
                () -> assertEquals(
                        ServerMonitoringMode.AUTO,
                        ServerSettings.builder()
                                .serverMonitoringMode(ServerMonitoringMode.STREAM)
                                .applySettings(ServerSettings.builder()
                                        .build())
                                .build()
                                .getServerMonitoringMode(),
                        "should apply unset from settings"),
                () -> assertEquals(
                        ServerMonitoringMode.POLL,
                        ServerSettings.builder()
                                .applyConnectionString(new ConnectionString(DEFAULT_OPTIONS + "serverMonitoringMode=POLL"))
                                .build()
                                .getServerMonitoringMode(),
                        "should apply from connection string"),
                () -> assertEquals(
                        ServerMonitoringMode.STREAM,
                        ServerSettings.builder()
                                .serverMonitoringMode(ServerMonitoringMode.STREAM)
                                .applyConnectionString(new ConnectionString(DEFAULT_OPTIONS))
                                .build()
                                .getServerMonitoringMode(),
                        "should not apply unset from connection string")
        );
    }
}
