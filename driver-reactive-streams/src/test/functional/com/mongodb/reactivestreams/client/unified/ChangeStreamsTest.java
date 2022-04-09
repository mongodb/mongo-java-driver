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
import static com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient.enableSleepAfterCursorOpen;
import static org.junit.Assume.assumeFalse;

public final class ChangeStreamsTest extends UnifiedReactiveStreamsTest {

    @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
    private static final List<String> ERROR_REQUIRED_FROM_CHANGE_STREAM_INITIALIZATION_TESTS =
            Arrays.asList(
                    "Test with document comment - pre 4.4"
            );

    private static final List<String> EVENT_SENSITIVE_TESTS =
            Arrays.asList(
                    "Test that comment is set on getMore",
                    "Test that comment is not set on getMore - pre 4.4"
            );

    private static final List<String> CURSOR_OPEN_TIMING_SENSITIVE_TESTS =
            Arrays.asList(
                    "Test with document comment",
                    "Test with document comment - pre 4.4",
                    "Test with string comment",
                    "Test that comment is set on getMore"
            );

    public ChangeStreamsTest(@SuppressWarnings("unused") final String fileDescription,
                             @SuppressWarnings("unused") final String testDescription,
                             final String schemaVersion, @Nullable final BsonArray runOnRequirements, final BsonArray entities,
                             final BsonArray initialData, final BsonDocument definition) {
        super(schemaVersion, runOnRequirements, entities, initialData, definition);

        assumeFalse(ERROR_REQUIRED_FROM_CHANGE_STREAM_INITIALIZATION_TESTS.contains(testDescription));
        assumeFalse(EVENT_SENSITIVE_TESTS.contains(testDescription));

        if (CURSOR_OPEN_TIMING_SENSITIVE_TESTS.contains(testDescription)) {
            enableSleepAfterCursorOpen(256);
        }
    }

    @After
    public void cleanUp() {
        super.cleanUp();
        disableSleep();
    }

    @Parameterized.Parameters(name = "{0}: {1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        return getTestData("unified-test-format/change-streams");
    }
}
