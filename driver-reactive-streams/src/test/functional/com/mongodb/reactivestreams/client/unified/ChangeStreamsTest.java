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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.provider.Arguments;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient.disableSleep;
import static com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient.disableWaitForBatchCursorCreation;
import static com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient.enableSleepAfterCursorOpen;
import static com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient.enableWaitForBatchCursorCreation;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

final class ChangeStreamsTest extends UnifiedReactiveStreamsTest {

    private static final List<String> ERROR_REQUIRED_FROM_CHANGE_STREAM_INITIALIZATION_TESTS =
            Arrays.asList(
                    "Test with document comment - pre 4.4"
            );

    private static final List<String> EVENT_SENSITIVE_TESTS =
            Arrays.asList(
                    "Test that comment is set on getMore",
                    "Test that comment is not set on getMore - pre 4.4"
            );

    private static final List<String> REQUIRES_BATCH_CURSOR_CREATION_WAITING =
            Arrays.asList(
                    "Change Stream should error when an invalid aggregation stage is passed in",
                    "The watch helper must not throw a custom exception when executed against a single server topology, "
                            + "but instead depend on a server error"
            );

    @Override
    protected void skips(final String fileDescription, final String testDescription) {
        assumeFalse(ERROR_REQUIRED_FROM_CHANGE_STREAM_INITIALIZATION_TESTS.contains(testDescription));
        assumeFalse(EVENT_SENSITIVE_TESTS.contains(testDescription));
    }

    @BeforeEach
    @Override
    public void setUp(@Nullable final String fileDescription,
            @Nullable final String testDescription,
            final String schemaVersion,
            @Nullable final BsonArray runOnRequirements,
            final BsonArray entitiesArray,
            final BsonArray initialData,
            final BsonDocument definition) {
        super.setUp(
                fileDescription,
                testDescription,
                schemaVersion,
                runOnRequirements,
                entitiesArray,
                initialData,
                definition);
        enableSleepAfterCursorOpen(256);
        if (REQUIRES_BATCH_CURSOR_CREATION_WAITING.contains(testDescription)) {
            enableWaitForBatchCursorCreation();
        }
    }

    @AfterEach
    @Override
    public void cleanUp() {
        super.cleanUp();
        disableSleep();
        disableWaitForBatchCursorCreation();
    }

    private static Collection<Arguments> data() throws URISyntaxException, IOException {
        return getTestData("change-streams/tests/unified");
    }
}
