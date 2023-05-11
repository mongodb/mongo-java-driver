package com.mongodb.internal.logging;

import com.mongodb.connection.ClusterId;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

class UnstructuredLogMessageTest {

    @ParameterizedTest
    @MethodSource("provideExpectedWithEntries")
    void shouldInterpolateMessage(String format, String expectedMessage, List<LogMessage.Entry> entries) {
        LogMessage.UnstructuredLogMessage unstructuredLogMessage = new LogMessage(LogMessage.Component.COMMAND, LogMessage.Level.DEBUG,
                "Connection id", new ClusterId(), entries, format).toUnstructuredLogMessage();

        String actualMessage = unstructuredLogMessage.interpolate();
        Assertions.assertEquals(expectedMessage, actualMessage);
    }

    private static Stream<Arguments> provideExpectedWithEntries() {

       String format =  "Command \"{}\" succeeded in {} ms using a connection with driver-generated ID {}" +
                "[ and server-generated ID {}] to {}:{}[ with service ID {}]. The requestID is {} and the " +
                "operation ID is {}. Command reply: {}]";
        return Stream.of(
                Arguments.of(format, "Command \"create\" succeeded in 5000 ms using a connection with driver-generated ID 1" +
                        " and server-generated ID 2 to localhost:8080 with service ID 3. The requestID is 333 and the " +
                        "operation ID is 444. Command reply: create", createEntries(
                        entry("commandName", "create"),
                        entry("durationMS", 5000),
                        entry("driverConnectionId", 1),
                        entry("serverConnectionId", 2),
                        entry("serverHost", "localhost"),
                        entry("serverPort", 8080),
                        entry("serviceId", 3),
                        entry("requestId", 333),
                        entry("operationId", 444),
                        entry("commandReply", "create")
                )),
                Arguments.of(format, "Command \"null\" succeeded in null ms using a connection with driver-generated ID null" +
                        " and server-generated ID 2 to localhost:8080 with service ID 3. The requestID is null and the " +
                        "operation ID is null. Command reply: null", createEntries(
                        entry("commandName", null),
                        entry("durationMS", null),
                        entry("driverConnectionId", null),
                        entry("serverConnectionId", 2),
                        entry("serverHost", "localhost"),
                        entry("serverPort", 8080),
                        entry("serviceId", 3),
                        entry("requestId", null),
                        entry("operationId", null),
                        entry("commandReply", null)
                )),
                Arguments.of(format, "Command \"null\" succeeded in null ms using a connection with driver-generated ID null" +
                        " to localhost:8080 with service ID 3. The requestID is null and the " +
                        "operation ID is null. Command reply: null", createEntries(
                        entry("commandName", null),
                        entry("durationMS", null),
                        entry("driverConnectionId", null),
                        entry("serverConnectionId", null),
                        entry("serverHost", "localhost"),
                        entry("serverPort", 8080),
                        entry("serviceId", 3),
                        entry("requestId", null),
                        entry("operationId", null),
                        entry("commandReply", null)
                )),
                Arguments.of(format, "Command \"null\" succeeded in null ms using a connection with driver-generated ID null" +
                        " to localhost:8080. The requestID is null and the " +
                        "operation ID is null. Command reply: null", createEntries(
                        entry("commandName", null),
                        entry("durationMS", null),
                        entry("driverConnectionId", null),
                        entry("serverConnectionId", null),
                        entry("serverHost", "localhost"),
                        entry("serverPort", 8080),
                        entry("serviceId", null),
                        entry("requestId", null),
                        entry("operationId", null),
                        entry("commandReply", null)
                )),
                Arguments.of(format, "Command \"create\" succeeded in 5000 ms using a connection with driver-generated ID 1" +
                        " to localhost:8080. The requestID is 333 and the " +
                        "operation ID is 444. Command reply: create", createEntries(
                        entry("commandName", "create"),
                        entry("durationMS", 5000),
                        entry("driverConnectionId", 1),
                        entry("serverConnectionId", null),
                        entry("serverHost", "localhost"),
                        entry("serverPort", 8080),
                        entry("serviceId", null),
                        entry("requestId", 333),
                        entry("operationId", 444),
                        entry("commandReply", "create")
                )),
                Arguments.of("Command \"{}\" succeeded in {} ms using a connection with driver-generated ID {}" +
                                "[ and server-generated ID {}] to {}:{}[ with service ID {}]. The requestID is {} and the " +
                                "operation ID is {}. Command reply: {}. Command finished",

                        "Command \"create\" succeeded in 5000 ms using a connection with driver-generated ID 1" +
                                " to localhost:8080. The requestID is 333 and the " +
                                "operation ID is 444. Command reply: create. Command finished", createEntries(
                                entry("commandName", "create"),
                                entry("durationMS", 5000),
                                entry("driverConnectionId", 1),
                                entry("serverConnectionId", null),
                                entry("serverHost", "localhost"),
                                entry("serverPort", 8080),
                                entry("serviceId", null),
                                entry("requestId", 333),
                                entry("operationId", 444),
                                entry("commandReply", "create")
                        )),
                Arguments.of("Command \"{}\" succeeded in {} ms using a connection with driver-generated ID {}" +
                                "[ and server-generated ID {}] to {}:{}[ with service ID {} generated]. The requestID is {} and the " +
                                "operation ID is {}. Command reply: {}.",

                        "Command \"create\" succeeded in 5000 ms using a connection with driver-generated ID 1" +
                                " to localhost:8080 with service ID 1 generated. The requestID is 333 and the " +
                                "operation ID is 444. Command reply: create.", createEntries(
                                entry("commandName", "create"),
                                entry("durationMS", 5000),
                                entry("driverConnectionId", 1),
                                entry("serverConnectionId", null),
                                entry("serverHost", "localhost"),
                                entry("serverPort", 8080),
                                entry("serviceId", 1),
                                entry("requestId", 333),
                                entry("operationId", 444),
                                entry("commandReply", "create")
                        )),
                Arguments.of("Command succeeded.", "Command succeeded.", createEntries(
                        entry("commandName", "create"),
                        entry("durationMS", 5000),
                        entry("driverConnectionId", 1),
                        entry("serverConnectionId", null),
                        entry("serverHost", "localhost"),
                        entry("serverPort", 8080),
                        entry("serviceId", null),
                        entry("requestId", 333),
                        entry("operationId", 444),
                        entry("commandReply", "create")
                ))
        );
    }


    private static LogMessage.Entry entry(String name, @Nullable Object key) {
        return new LogMessage.Entry(name, key);
    }

    @NotNull
    private static List<LogMessage.Entry> createEntries(LogMessage.Entry... entry) {
        return Arrays.asList(entry);
    }
}
