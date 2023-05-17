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
import com.mongodb.lang.NonNull;
import com.mongodb.lang.Nullable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static com.mongodb.internal.logging.LogMessage.Entry.Name.COMMAND_NAME;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.DRIVER_CONNECTION_ID;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.DURATION_MS;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.OPERATION_ID;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.REPLY;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.REQUEST_ID;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.SERVER_CONNECTION_ID;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.SERVER_HOST;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.SERVER_PORT;
import static com.mongodb.internal.logging.LogMessage.Entry.Name.SERVICE_ID;

class UnstructuredLogMessageTest {

    @ParameterizedTest
    @MethodSource("provideExpectedWithEntries")
    void shouldInterpolateMessage(final String format, final String expectedMessage, final List<LogMessage.Entry> entries) {
        LogMessage.UnstructuredLogMessage unstructuredLogMessage = new LogMessage(LogMessage.Component.COMMAND, LogMessage.Level.DEBUG,
                "Connection id", new ClusterId(), entries, format).toUnstructuredLogMessage();

        String actualMessage = unstructuredLogMessage.interpolate();
        Assertions.assertEquals(expectedMessage, actualMessage);
    }

    private static Stream<Arguments> provideExpectedWithEntries() {

        String format = "Command \"{}\" succeeded in {} ms using a connection with driver-generated ID {}"
                + "[ and server-generated ID {}] to {}:{}[ with service ID {}]. The requestID is {} and the "
                + "operation ID is {}. Command reply: {}";
        return Stream.of(
                Arguments.of(format, "Command \"create\" succeeded in 5000 ms using a connection with driver-generated ID 1"
                        + " and server-generated ID 2 to localhost:8080 with service ID 3. The requestID is 333 and the "
                        + "operation ID is 444. Command reply: create", createEntries(
                        entry(COMMAND_NAME, "create"),
                        entry(DURATION_MS, 5000),
                        entry(DRIVER_CONNECTION_ID, 1),
                        entry(SERVER_CONNECTION_ID, 2),
                        entry(SERVER_HOST, "localhost"),
                        entry(SERVER_PORT, 8080),
                        entry(SERVICE_ID, 3),
                        entry(REQUEST_ID, 333),
                        entry(OPERATION_ID, 444),
                        entry(REPLY, "create")
                )),
                Arguments.of(format, "Command \"null\" succeeded in null ms using a connection with driver-generated ID null"
                        + " and server-generated ID 2 to localhost:8080 with service ID 3. The requestID is null and the "
                        + "operation ID is null. Command reply: null", createEntries(
                        entry(COMMAND_NAME, null),
                        entry(DURATION_MS, null),
                        entry(DRIVER_CONNECTION_ID, null),
                        entry(SERVER_CONNECTION_ID, 2),
                        entry(SERVER_HOST, "localhost"),
                        entry(SERVER_PORT, 8080),
                        entry(SERVICE_ID, 3),
                        entry(REQUEST_ID, null),
                        entry(OPERATION_ID, null),
                        entry(REPLY, null)
                )), Arguments.of(format, "Command \"null\" succeeded in null ms using a connection with driver-generated ID null"
                        + " to localhost:8080 with service ID 3. The requestID is null and the "
                        + "operation ID is null. Command reply: null", createEntries(
                        entry(COMMAND_NAME, null),
                        entry(DURATION_MS, null),
                        entry(DRIVER_CONNECTION_ID, null),
                        entry(SERVER_CONNECTION_ID, null),
                        entry(SERVER_HOST, "localhost"),
                        entry(SERVER_PORT, 8080),
                        entry(SERVICE_ID, 3),
                        entry(REQUEST_ID, null),
                        entry(OPERATION_ID, null),
                        entry(REPLY, null)
                )),
                Arguments.of(format, "Command \"null\" succeeded in null ms using a connection with driver-generated ID null"
                        + " to localhost:8080. The requestID is null and the "
                        + "operation ID is null. Command reply: null", createEntries(
                        entry(COMMAND_NAME, null),
                        entry(DURATION_MS, null),
                        entry(DRIVER_CONNECTION_ID, null),
                        entry(SERVER_CONNECTION_ID, null),
                        entry(SERVER_HOST, "localhost"),
                        entry(SERVER_PORT, 8080),
                        entry(SERVICE_ID, null),
                        entry(REQUEST_ID, null),
                        entry(OPERATION_ID, null),
                        entry(REPLY, null)
                )),
                Arguments.of(format, "Command \"create\" succeeded in 5000 ms using a connection with driver-generated ID 1"
                        + " to localhost:8080. The requestID is 333 and the "
                        + "operation ID is 444. Command reply: create", createEntries(
                        entry(COMMAND_NAME, "create"),
                        entry(DURATION_MS, 5000),
                        entry(DRIVER_CONNECTION_ID, 1),
                        entry(SERVER_CONNECTION_ID, null),
                        entry(SERVER_HOST, "localhost"),
                        entry(SERVER_PORT, 8080),
                        entry(SERVICE_ID, null),
                        entry(REQUEST_ID, 333),
                        entry(OPERATION_ID, 444),
                        entry(REPLY, "create")
                )),
                Arguments.of("Command \"{}\" succeeded in {} ms using a connection with driver-generated ID {}"
                                + "[ and server-generated ID {}] to {}:{}[ with service ID {}]. The requestID is {} and the "
                                + "operation ID is {}. Command reply: {}. Command finished",

                        "Command \"create\" succeeded in 5000 ms using a connection with driver-generated ID 1"
                                + " to localhost:8080. The requestID is 333 and the "
                                + "operation ID is 444. Command reply: create. Command finished", createEntries(
                                entry(COMMAND_NAME, "create"),
                                entry(DURATION_MS, 5000),
                                entry(DRIVER_CONNECTION_ID, 1),
                                entry(SERVER_CONNECTION_ID, null),
                                entry(SERVER_HOST, "localhost"),
                                entry(SERVER_PORT, 8080),
                                entry(SERVICE_ID, null),
                                entry(REQUEST_ID, 333),
                                entry(OPERATION_ID, 444),
                                entry(REPLY, "create")
                        )),
                Arguments.of("Command \"{}\" succeeded in {} ms using a connection with driver-generated ID {}"
                                + "[ and server-generated ID {}] to {}:{}[ with service ID {} generated]. The requestID is {} and the "
                                + "operation ID is {}. Command reply: {}.",

                        "Command \"create\" succeeded in 5000 ms using a connection with driver-generated ID 1"
                                + " to localhost:8080 with service ID 1 generated. The requestID is 333 and the "
                                + "operation ID is 444. Command reply: create.", createEntries(
                                entry(COMMAND_NAME, "create"),
                                entry(DURATION_MS, 5000),
                                entry(DRIVER_CONNECTION_ID, 1),
                                entry(SERVER_CONNECTION_ID, null),
                                entry(SERVER_HOST, "localhost"),
                                entry(SERVER_PORT, 8080),
                                entry(SERVICE_ID, 1),
                                entry(REQUEST_ID, 333),
                                entry(OPERATION_ID, 444),
                                entry(REPLY, "create")
                        )),
                Arguments.of("Command succeeded.", "Command succeeded.", createEntries(
                        entry(COMMAND_NAME, "create"),
                        entry(DURATION_MS, 5000),
                        entry(DRIVER_CONNECTION_ID, 1),
                        entry(SERVER_CONNECTION_ID, null),
                        entry(SERVER_HOST, "localhost"),
                        entry(SERVER_PORT, 8080),
                        entry(SERVICE_ID, null),
                        entry(REQUEST_ID, 333),
                        entry(OPERATION_ID, 444),
                        entry(REPLY, "create")
                ))
        );
    }


    private static LogMessage.Entry entry(final LogMessage.Entry.Name name, final @Nullable Object key) {
        return new LogMessage.Entry(name, key);
    }

    @NonNull
    private static List<LogMessage.Entry> createEntries(final LogMessage.Entry... entry) {
        return Arrays.asList(entry);
    }
}
