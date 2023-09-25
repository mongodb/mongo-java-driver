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
package com.mongodb.internal;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.Collection;

import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS_WITH_MAX_AWAIT_TIME;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS_WITH_MAX_COMMIT;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS_WITH_MAX_TIME;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS_WITH_MAX_TIME_AND_AWAIT_TIME;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS_WITH_TIMEOUT;
import static com.mongodb.ClusterFixture.sleep;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

final class ClientSideOperationTimeoutTest {

    @TestFactory
    Collection<DynamicTest> clientSideOperationTimeoutTest() {
        return asList(
                dynamicTest("test defaults", () -> {
                    ClientSideOperationTimeout clientSideOperationTimeout = new ClientSideOperationTimeout(TIMEOUT_SETTINGS);
                    assertAll(
                            () -> assertFalse(clientSideOperationTimeout.hasTimeoutMS()),
                            () -> assertEquals(0, clientSideOperationTimeout.getMaxTimeMS()),
                            () -> assertEquals(0, clientSideOperationTimeout.getMaxAwaitTimeMS()),
                            () -> assertEquals(0, clientSideOperationTimeout.getMaxCommitTimeMS())
                    );
                }),
                dynamicTest("Uses timeoutMS if set", () -> {
                    ClientSideOperationTimeout clientSideOperationTimeout =
                            new ClientSideOperationTimeout(TIMEOUT_SETTINGS_WITH_TIMEOUT.withMaxAwaitTimeMS(9));
                    assertAll(
                            () -> assertTrue(clientSideOperationTimeout.hasTimeoutMS()),
                            () -> assertTrue(clientSideOperationTimeout.getMaxTimeMS() > 0),
                            () -> assertTrue(clientSideOperationTimeout.getMaxAwaitTimeMS() > 0),
                            () -> assertTrue(clientSideOperationTimeout.getMaxCommitTimeMS() > 0)
                    );
                }),
                dynamicTest("MaxTimeMS set", () -> {
                    ClientSideOperationTimeout clientSideOperationTimeout = new ClientSideOperationTimeout(TIMEOUT_SETTINGS_WITH_MAX_TIME);
                    assertAll(
                            () -> assertEquals(100, clientSideOperationTimeout.getMaxTimeMS()),
                            () -> assertEquals(0, clientSideOperationTimeout.getMaxAwaitTimeMS()),
                            () -> assertEquals(0, clientSideOperationTimeout.getMaxCommitTimeMS())
                    );
                }),
                dynamicTest("MaxAwaitTimeMS set", () -> {
                    ClientSideOperationTimeout clientSideOperationTimeout =
                            new ClientSideOperationTimeout(TIMEOUT_SETTINGS_WITH_MAX_AWAIT_TIME);
                    assertAll(
                            () -> assertEquals(0, clientSideOperationTimeout.getMaxTimeMS()),
                            () -> assertEquals(101, clientSideOperationTimeout.getMaxAwaitTimeMS()),
                            () -> assertEquals(0, clientSideOperationTimeout.getMaxCommitTimeMS())
                    );
                }),
                dynamicTest("MaxTimeMS and MaxAwaitTimeMS set", () -> {
                    ClientSideOperationTimeout clientSideOperationTimeout =
                            new ClientSideOperationTimeout(TIMEOUT_SETTINGS_WITH_MAX_TIME_AND_AWAIT_TIME);
                    assertAll(
                            () -> assertEquals(101, clientSideOperationTimeout.getMaxTimeMS()),
                            () -> assertEquals(1001, clientSideOperationTimeout.getMaxAwaitTimeMS()),
                            () -> assertEquals(0, clientSideOperationTimeout.getMaxCommitTimeMS())
                    );
                }),
                dynamicTest("MaxCommitTimeMS set", () -> {
                    ClientSideOperationTimeout clientSideOperationTimeout =
                            new ClientSideOperationTimeout(TIMEOUT_SETTINGS_WITH_MAX_COMMIT);
                    assertAll(
                            () -> assertEquals(0, clientSideOperationTimeout.getMaxTimeMS()),
                            () -> assertEquals(0, clientSideOperationTimeout.getMaxAwaitTimeMS()),
                            () -> assertEquals(999L, clientSideOperationTimeout.getMaxCommitTimeMS())
                    );
                }),
                dynamicTest("All deprecated options set", () -> {
                    ClientSideOperationTimeout clientSideOperationTimeout =
                            new ClientSideOperationTimeout(TIMEOUT_SETTINGS_WITH_MAX_TIME_AND_AWAIT_TIME
                                    .withMaxCommitMS(999L));
                    assertAll(
                            () -> assertEquals(101, clientSideOperationTimeout.getMaxTimeMS()),
                            () -> assertEquals(1001, clientSideOperationTimeout.getMaxAwaitTimeMS()),
                            () -> assertEquals(999, clientSideOperationTimeout.getMaxCommitTimeMS())
                    );
                }),
                dynamicTest("Use timeout if available or the alternative", () -> assertAll(
                        () -> {
                            ClientSideOperationTimeout clientSideOperationTimeout =
                                    new ClientSideOperationTimeout(TIMEOUT_SETTINGS);
                            assertEquals(99L, clientSideOperationTimeout.timeoutOrAlternative(99));
                        },
                        () -> {
                            ClientSideOperationTimeout clientSideOperationTimeout =
                                    new ClientSideOperationTimeout(TIMEOUT_SETTINGS.withTimeoutMS(0));
                            assertEquals(0L, clientSideOperationTimeout.timeoutOrAlternative(99));
                        },
                        () -> {
                            ClientSideOperationTimeout clientSideOperationTimeout =
                                    new ClientSideOperationTimeout(TIMEOUT_SETTINGS.withTimeoutMS(999));
                            assertTrue(clientSideOperationTimeout.timeoutOrAlternative(0) <= 999);
                        },
                        () -> {
                            ClientSideOperationTimeout clientSideOperationTimeout =
                                    new ClientSideOperationTimeout(TIMEOUT_SETTINGS.withTimeoutMS(999));
                            assertTrue(clientSideOperationTimeout.timeoutOrAlternative(999999) <= 999);
                        }
                )),
                dynamicTest("Calculate min works as expected", () -> assertAll(
                        () -> {
                            ClientSideOperationTimeout clientSideOperationTimeout =
                                    new ClientSideOperationTimeout(TIMEOUT_SETTINGS);
                            assertEquals(99L, clientSideOperationTimeout.calculateMin(99));
                        },
                        () -> {
                            ClientSideOperationTimeout clientSideOperationTimeout =
                                    new ClientSideOperationTimeout(TIMEOUT_SETTINGS.withTimeoutMS(0));
                            assertEquals(99L, clientSideOperationTimeout.calculateMin(99));
                        },
                        () -> {
                            ClientSideOperationTimeout clientSideOperationTimeout =
                                    new ClientSideOperationTimeout(TIMEOUT_SETTINGS.withTimeoutMS(999));
                            assertTrue(clientSideOperationTimeout.calculateMin(0) <= 999);
                        },
                        () -> {
                            ClientSideOperationTimeout clientSideOperationTimeout =
                                    new ClientSideOperationTimeout(TIMEOUT_SETTINGS.withTimeoutMS(999));
                            assertTrue(clientSideOperationTimeout.calculateMin(999999) <= 999);
                        }
                )),
                dynamicTest("Expired works as expected", () -> {
                    ClientSideOperationTimeout smallTimeout = new ClientSideOperationTimeout(TIMEOUT_SETTINGS.withTimeoutMS(1));
                    ClientSideOperationTimeout longTimeout =
                            new ClientSideOperationTimeout(TIMEOUT_SETTINGS.withTimeoutMS(9999999));
                    ClientSideOperationTimeout noTimeout = new ClientSideOperationTimeout(TIMEOUT_SETTINGS);
                    sleep(100);
                    assertAll(
                            () -> assertFalse(noTimeout.hasExpired()),
                            () -> assertFalse(longTimeout.hasExpired()),
                            () -> assertTrue(smallTimeout.hasExpired())
                    );
                })
        );
    }

    private ClientSideOperationTimeoutTest() {
    }
}
