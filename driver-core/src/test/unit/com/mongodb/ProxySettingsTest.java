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

package com.mongodb;

import com.mongodb.connection.ProxySettings;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.stream.Stream;

class ProxySettingsTest {

    private static final String PASSWORD = "password";
    private static final String USERNAME = "username";
    private static final String HOST = "proxy.example.com";
    private static final int VALID_PORT = 1080;

    static Stream<Arguments> shouldThrowExceptionWhenProxySettingsAreInInvalid() {
        return Stream.of(
                Arguments.of(ProxySettings.builder()
                        .port(VALID_PORT), "state should be: proxyPort can only be specified with proxyHost"),
                Arguments.of(ProxySettings.builder()
                        .port(VALID_PORT)
                        .username(USERNAME)
                        .password(PASSWORD), "state should be: proxyPort can only be specified with proxyHost"),
                Arguments.of(ProxySettings.builder()
                        .username(USERNAME), "state should be: proxyUsername can only be specified with proxyHost"),
                Arguments.of(ProxySettings.builder()
                        .password(PASSWORD), "state should be: proxyPassword can only be specified with proxyHost"),
                Arguments.of(ProxySettings.builder()
                                .host(HOST)
                                .username(USERNAME),
                        "state should be: Both proxyUsername and proxyPassword must be set together. They cannot be set individually"),
                Arguments.of(ProxySettings.builder()
                                .host(HOST)
                                .password(PASSWORD),
                        "state should be: Both proxyUsername and proxyPassword must be set together. They cannot be set individually")
        );
    }

    @ParameterizedTest
    @MethodSource
    void shouldThrowExceptionWhenProxySettingsAreInInvalid(final ProxySettings.Builder builder, final String expectedErrorMessage) {
        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class, builder::build);
        Assertions.assertEquals(expectedErrorMessage, exception.getMessage());
    }

    static Stream<Arguments> shouldThrowExceptionWhenInvalidValueIsProvided() {
        byte[] byteData = new byte[256];
        Arrays.fill(byteData, (byte) 1);
        return Stream.of(
                Arguments.of((Executable) () -> ProxySettings.builder()
                        .port(-1), "state should be: proxyPort is within the valid range (0 to 65535)"),
                Arguments.of((Executable) () -> ProxySettings.builder()
                        .port(65536), "state should be: proxyPort is within the valid range (0 to 65535)"),
                Arguments.of((Executable) () -> ProxySettings.builder()
                        .host(""), "state should be: proxyHost is not empty"),
                Arguments.of((Executable) () -> ProxySettings.builder()
                        .username(""), "state should be: username is not empty"),
                Arguments.of((Executable) () -> ProxySettings.builder()
                        .username(new String(byteData)), "state should be: username's length in bytes is not greater than 255"),
                Arguments.of((Executable) () -> ProxySettings.builder()
                        .password(""), "state should be: password is not empty"),
                Arguments.of((Executable) () -> ProxySettings.builder()
                        .password(new String(byteData)), "state should be: password's length in bytes is not greater than 255"),
                Arguments.of((Executable) () -> ProxySettings.builder()
                        .host(null), "proxyHost can not be null"),
                Arguments.of((Executable) () -> ProxySettings.builder()
                        .username(null), "username can not be null"),
                Arguments.of((Executable) () -> ProxySettings.builder()
                        .password(null), "password can not be null")
        );
    }

    @ParameterizedTest
    @MethodSource
    void shouldThrowExceptionWhenInvalidValueIsProvided(final Executable action, final String expectedMessage) {
        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class, action);
        Assertions.assertEquals(expectedMessage, exception.getMessage());
    }

    static Stream<Arguments> shouldNotThrowExceptionWhenProxySettingAreValid() {
        return Stream.of(
                Arguments.of(ProxySettings.builder()
                        .host(HOST)
                        .port(VALID_PORT)),
                Arguments.of(ProxySettings.builder()
                        .host(HOST)),
                Arguments.of(ProxySettings.builder()
                        .host(HOST)
                        .port(VALID_PORT)
                        .host(USERNAME)
                        .host(PASSWORD)),
                Arguments.of(ProxySettings.builder()
                        .host(HOST)
                        .host(USERNAME)
                        .host(PASSWORD))
        );
    }

    @ParameterizedTest
    @MethodSource
    void shouldNotThrowExceptionWhenProxySettingAreValid(final ProxySettings.Builder builder) {
        builder.build();
    }

    @Test
    void shouldGetExpectedValues() {
        //given
        ProxySettings proxySettings = ProxySettings.builder()
                .host(HOST)
                .port(VALID_PORT)
                .username(USERNAME)
                .password(PASSWORD)
                .build();

        Assertions.assertEquals(HOST, proxySettings.getHost());
        Assertions.assertEquals(VALID_PORT, proxySettings.getPort());
        Assertions.assertEquals(USERNAME, proxySettings.getUsername());
        Assertions.assertEquals(PASSWORD, proxySettings.getPassword());
    }
}
