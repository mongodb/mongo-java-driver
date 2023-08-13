package com.mongodb;

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