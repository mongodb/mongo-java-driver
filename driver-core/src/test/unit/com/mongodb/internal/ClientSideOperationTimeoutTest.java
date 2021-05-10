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

import static com.mongodb.ClusterFixture.sleep;
import static com.mongodb.internal.ClientSideOperationTimeoutFactories.NO_TIMEOUT;
import static com.mongodb.internal.ClientSideOperationTimeoutFactories.create;
import static com.mongodb.internal.ClientSideOperationTimeoutFactories.shared;
import static com.mongodb.internal.ClientSideOperationTimeoutFactories.withMaxCommitMS;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

final class ClientSideOperationTimeoutTest {


    @TestFactory
    Collection<DynamicTest> clientSideOperationTimeoutFactoriesTest() {
        return asList(
                dynamicTest("test defaults", () -> {
                    ClientSideOperationTimeout clientSideOperationTimeout = NO_TIMEOUT.create();
                    assertAll(
                            () -> assertFalse(clientSideOperationTimeout.hasTimeoutMS()),
                            () -> assertEquals(0, clientSideOperationTimeout.getMaxTimeMS()),
                            () -> assertEquals(0, clientSideOperationTimeout.getMaxAwaitTimeMS()),
                            () -> assertEquals(0, clientSideOperationTimeout.getMaxCommitTimeMS())
                    );
                }),
                dynamicTest("Uses timeoutMS if set", () -> {
                    long altTimeout = 9;
                    ClientSideOperationTimeout clientSideOperationTimeout = create(99999999L, altTimeout, altTimeout, altTimeout).create();
                    assertAll(
                            () -> assertTrue(clientSideOperationTimeout.hasTimeoutMS()),
                            () -> assertTrue(clientSideOperationTimeout.getMaxTimeMS() > 0),
                            () -> assertTrue(clientSideOperationTimeout.getMaxAwaitTimeMS() > 0),
                            () -> assertTrue(clientSideOperationTimeout.getMaxCommitTimeMS() > 0)
                    );
                }),
                dynamicTest("MaxTimeMS set", () -> {
                    ClientSideOperationTimeout clientSideOperationTimeout = create(null, 9).create();
                    assertAll(
                            () -> assertEquals(9, clientSideOperationTimeout.getMaxTimeMS()),
                            () -> assertEquals(0, clientSideOperationTimeout.getMaxAwaitTimeMS()),
                            () -> assertEquals(0, clientSideOperationTimeout.getMaxCommitTimeMS())
                    );
                }),
                dynamicTest("MaxTimeMS and MaxAwaitTimeMS set", () -> {
                    ClientSideOperationTimeout clientSideOperationTimeout = create(null, 9, 99).create();
                    assertAll(
                            () -> assertEquals(9, clientSideOperationTimeout.getMaxTimeMS()),
                            () -> assertEquals(99, clientSideOperationTimeout.getMaxAwaitTimeMS()),
                            () -> assertEquals(0, clientSideOperationTimeout.getMaxCommitTimeMS())
                    );
                }),
                dynamicTest("MaxCommitTimeMS set", () -> {
                    ClientSideOperationTimeout clientSideOperationTimeout = withMaxCommitMS(null, 9L).create();
                    assertAll(
                            () -> assertEquals(0, clientSideOperationTimeout.getMaxTimeMS()),
                            () -> assertEquals(0, clientSideOperationTimeout.getMaxAwaitTimeMS()),
                            () -> assertEquals(9L, clientSideOperationTimeout.getMaxCommitTimeMS())
                    );
                }),
                dynamicTest("All deprecated options set", () -> {
                    ClientSideOperationTimeout clientSideOperationTimeout = create(null, 99, 9L, 999).create();
                    assertAll(
                            () -> assertEquals(9, clientSideOperationTimeout.getMaxAwaitTimeMS()),
                            () -> assertEquals(99, clientSideOperationTimeout.getMaxTimeMS()),
                            () -> assertEquals(999, clientSideOperationTimeout.getMaxCommitTimeMS())
                    );
                }),
                dynamicTest("shared ClientSideOperationTimeout", () -> {
                    ClientSideOperationTimeoutFactory normalFactory = create(null, 99);
                    ClientSideOperationTimeoutFactory sharedFactory = shared(normalFactory);
                    assertAll(
                            () -> assertNotEquals(normalFactory.create(), normalFactory.create()),
                            () -> assertEquals(sharedFactory.create(), sharedFactory.create())
                    );
                }),
                dynamicTest("Use timeout if available or the alternative", () -> {
                    assertAll(
                            () -> assertEquals(99L, NO_TIMEOUT.create().timeoutOrAlternative(99)),
                            () -> assertEquals(0L, ClientSideOperationTimeoutFactories.create(0L).create().timeoutOrAlternative(99)),
                            () -> assertTrue(ClientSideOperationTimeoutFactories.create(999L).create().timeoutOrAlternative(0) <= 999),
                            () -> assertTrue(ClientSideOperationTimeoutFactories.create(999L).create().timeoutOrAlternative(999999) <= 999)
                    );
                }),
                dynamicTest("Calculate min works as expected", () -> {
                    assertAll(
                            () -> assertEquals(99L, NO_TIMEOUT.create().calculateMin(99)),
                            () -> assertEquals(99L, ClientSideOperationTimeoutFactories.create(0L).create().calculateMin(99)),
                            () -> assertTrue(ClientSideOperationTimeoutFactories.create(999L).create().calculateMin(0) <= 999),
                            () -> assertTrue(ClientSideOperationTimeoutFactories.create(999L).create().calculateMin(999999) <= 999)
                    );
                }),
                dynamicTest("Expired works as expected", () -> {
                    ClientSideOperationTimeout smallTimeout = ClientSideOperationTimeoutFactories.create(1L).create();
                    ClientSideOperationTimeout longTimeout = ClientSideOperationTimeoutFactories.create(999999999L).create();
                    ClientSideOperationTimeout noTimeout = NO_TIMEOUT.create();
                    sleep(100);
                    assertAll(
                            () -> assertFalse(noTimeout.expired()),
                            () -> assertFalse(longTimeout.expired()),
                            () -> assertTrue(smallTimeout.expired())
                    );
                })
        );
    }

    private ClientSideOperationTimeoutTest() {
    }
}
