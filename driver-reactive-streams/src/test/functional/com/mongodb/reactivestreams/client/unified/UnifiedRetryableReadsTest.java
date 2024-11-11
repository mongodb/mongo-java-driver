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
import java.util.Collection;

import static com.mongodb.client.unified.UnifiedRetryableReadsTest.doSkips;
import static com.mongodb.client.unified.UnifiedTestSkips.testDef;
import static com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient.disableWaitForBatchCursorCreation;
import static com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient.enableWaitForBatchCursorCreation;

final class UnifiedRetryableReadsTest extends UnifiedReactiveStreamsTest {
    @Override
    protected void skips(final String fileDescription, final String testDescription) {
        doSkips(testDef("unified-test-format/retryable-reads", fileDescription, testDescription));
    }

    @Override
    @BeforeEach
    public void setUp(
            final String fileDescription,
            final String testDescription,
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
        if (fileDescription.startsWith("changeStreams") || testDescription.contains("ChangeStream")) {
            // Several reactive change stream tests fail if we don't block waiting for batch cursor creation.
            enableWaitForBatchCursorCreation();
            // The reactive driver will execute extra getMore commands for change streams.  Ignore them.
            ignoreExtraEvents();
        }
    }

    @Override
    @AfterEach
    public void cleanUp() {
        super.cleanUp();
        disableWaitForBatchCursorCreation();
    }

    private static Collection<Arguments> data() throws URISyntaxException, IOException {
        return getTestData("unified-test-format/retryable-reads");
    }
}
