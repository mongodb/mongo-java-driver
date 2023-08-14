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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

class ProxySettingsTest {
    static Stream<Arguments> shouldThrowExceptionWhenProxySettingIsInInvalidState() {
        return Stream.of(
                Arguments.of(ProxySettings.builder()
                        .port(1080), "state should be: proxyPort can only be specified with proxyHost"),
                Arguments.of(ProxySettings.builder()
                        .port(1080)
                        .username("test")
                        .password("test"), "state should be: proxyPort can only be specified with proxyHost"),
                Arguments.of(ProxySettings.builder()
                        .username("test"), "state should be: proxyUsername can only be specified with proxyHost"),
                Arguments.of(ProxySettings.builder()
                        .password("test"), "state should be: proxyPassword can only be specified with proxyHost"),
                Arguments.of(ProxySettings.builder()
                                .host("test")
                                .username("test"),
                        "state should be: Both proxyUsername and proxyPassword must be set together. They cannot be set individually"),
                Arguments.of(ProxySettings.builder()
                                .host("test")
                                .password("test"),
                        "state should be: Both proxyUsername and proxyPassword must be set together. They cannot be set individually")
        );
    }

    @ParameterizedTest
    @MethodSource
    void shouldThrowExceptionWhenProxySettingIsInInvalidState(final ProxySettings.Builder builder, final String expectedErrorMessage) {
        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class, builder::build);
        Assertions.assertEquals(expectedErrorMessage, exception.getMessage());
    }

    static Stream<Arguments> shouldNotThrowExceptionWhenProxySettingIsInValidState() {
        return Stream.of(
                Arguments.of(ProxySettings.builder()
                        .host("test")
                        .port(1080)),
                Arguments.of(ProxySettings.builder()
                        .host("test")),
                Arguments.of(ProxySettings.builder()
                        .host("test")
                        .port(1080)
                        .username("test")
                        .password("test")),
                Arguments.of(ProxySettings.builder()
                        .host("test")
                        .username("test")
                        .password("test"))
        );
    }

    @ParameterizedTest
    @MethodSource
    void shouldNotThrowExceptionWhenProxySettingIsInValidState(final ProxySettings.Builder builder) {
        builder.build();
    }
}
