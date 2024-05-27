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

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.junit.After;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;

import static com.mongodb.client.unified.UnifiedRetryableReadsTest.customSkips;
import static com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient.disableWaitForBatchCursorCreation;
import static com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient.enableWaitForBatchCursorCreation;

public class UnifiedRetryableReadsTest extends UnifiedReactiveStreamsTest {
    public UnifiedRetryableReadsTest(final String fileDescription, final String testDescription, final String schemaVersion,
            final BsonArray runOnRequirements, final BsonArray entitiesArray, final BsonArray initialData, final BsonDocument definition) {
        super(schemaVersion, runOnRequirements, entitiesArray, initialData, definition);
        customSkips(fileDescription, testDescription);
        if (fileDescription.startsWith("changeStreams") || testDescription.contains("ChangeStream")) {
            // Several reactive change stream tests fail if we don't block waiting for batch cursor creation.
            enableWaitForBatchCursorCreation();
            // The reactive driver will execute extra getMore commands for change streams.  Ignore them.
            ignoreExtraEvents();
        }
    }

    @After
    public void cleanUp() {
        super.cleanUp();
        disableWaitForBatchCursorCreation();
    }

    @Parameterized.Parameters(name = "{0}: {1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        return getTestData("unified-test-format/retryable-reads");
    }
}
