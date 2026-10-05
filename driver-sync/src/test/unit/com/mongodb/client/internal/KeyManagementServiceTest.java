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

package com.mongodb.client.internal;

import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.internal.time.Timeout;
import org.junit.jupiter.api.Test;

import static com.mongodb.internal.time.Timeout.ZeroSemantics.ZERO_DURATION_MEANS_EXPIRED;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The specification requires that a driver supporting CSOT pass the remaining {@code timeoutMS} when establishing a
 * connection to a KMS host. Prose test 28 asserts this by observing a {@code kmsConnectCallback}; this covers the same
 * requirement directly, including the branches that prose test does not reach.
 */
final class KeyManagementServiceTest {

    private static final int CONNECT_TIMEOUT_MILLIS = 10_000;

    private final KeyManagementService keyManagementService =
            new KeyManagementService(emptyMap(), null, CONNECT_TIMEOUT_MILLIS);

    @Test
    void shouldUseConfiguredConnectTimeoutWhenNoOperationTimeoutApplies() {
        assertEquals(CONNECT_TIMEOUT_MILLIS, keyManagementService.remainingMillis(null));
    }

    @Test
    void shouldPassRemainingOperationTimeoutWhenItIsShorter() {
        Timeout operationTimeout = Timeout.expiresIn(500, MILLISECONDS, ZERO_DURATION_MEANS_EXPIRED);

        long remaining = keyManagementService.remainingMillis(operationTimeout);

        assertTrue(remaining > 0 && remaining <= 500,
                () -> "expected the remaining operation timeout to be passed, but got " + remaining);
    }

    @Test
    void shouldNotExceedConfiguredConnectTimeoutWhenOperationTimeoutIsLonger() {
        Timeout operationTimeout = Timeout.expiresIn(60_000, MILLISECONDS, ZERO_DURATION_MEANS_EXPIRED);

        assertEquals(CONNECT_TIMEOUT_MILLIS, keyManagementService.remainingMillis(operationTimeout));
    }

    @Test
    void shouldThrowWhenOperationTimeoutHasExpired() {
        Timeout expired = Timeout.expiresIn(0, MILLISECONDS, ZERO_DURATION_MEANS_EXPIRED);

        assertThrows(MongoOperationTimeoutException.class, () -> keyManagementService.remainingMillis(expired));
    }
}
