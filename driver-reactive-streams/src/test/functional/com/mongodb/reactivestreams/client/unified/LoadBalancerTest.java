/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client.unified;

import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.junit.After;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient.disableSleep;
import static com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient.enableSleepAfterCursorClose;
import static com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient.enableSleepAfterCursorOpen;
import static org.junit.Assume.assumeFalse;

public class LoadBalancerTest extends UnifiedReactiveStreamsTest {

    private static final List<String> CURSOR_OPEN_TIMING_SENSITIVE_TESTS =
            Arrays.asList(
                    "pinned connections are returned when the cursor is drained",
                    "only connections for a specific serviceId are closed when pools are cleared",
                    "pinned connections are returned to the pool when the cursor is closed",
                    "no connection is pinned if all documents are returned in the initial batch",
                    "stale errors are ignored",
                    "a connection can be shared by a transaction and a cursor",
                    "wait queue timeout errors include cursor statistics");

    private static final List<String> CURSOR_CLOSE_TIMING_SENSITIVE_TESTS =
            Arrays.asList(
                    "pinned connections are returned to the pool when the cursor is closed",
                    "only connections for a specific serviceId are closed when pools are cleared",
                    "pinned connections are returned after a network error during a killCursors request",
                    "a connection can be shared by a transaction and a cursor");

    public LoadBalancerTest(final String fileDescription,
                            final String testDescription,
                            final String schemaVersion, @Nullable final BsonArray runOnRequirements, final BsonArray entities,
                            final BsonArray initialData, final BsonDocument definition) {
        super(fileDescription, schemaVersion, runOnRequirements, entities, initialData, definition);
        // Reactive streams driver can't implement these tests because the underlying cursor is closed on error, which
        // breaks assumption in the tests that closing the cursor is something that happens under user control
        assumeFalse(testDescription.equals("pinned connections are not returned after an network error during getMore"));
        assumeFalse(testDescription.equals("pinned connections are not returned to the pool after a non-network error on getMore"));
        // Reactive streams driver can't implement this test because there is no way to tell that a change stream cursor
        // that has not yet received any results has even initiated the change stream
        assumeFalse(testDescription.equals("change streams pin to a connection"));

        if (CURSOR_OPEN_TIMING_SENSITIVE_TESTS.contains(testDescription)) {
            enableSleepAfterCursorOpen(256);
        }

        if (CURSOR_CLOSE_TIMING_SENSITIVE_TESTS.contains(testDescription)) {
            enableSleepAfterCursorClose(256);
        }
    }

    @After
    public void cleanUp() {
        super.cleanUp();
        disableSleep();
    }

    @Parameterized.Parameters(name = "{0}: {1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        return getTestData("unified-test-format/load-balancers");
    }
}
