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

package com.mongodb.client;

import com.mongodb.ClusterFixture;
import com.mongodb.client.unified.UnifiedSyncTest;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;

import static org.junit.Assume.assumeFalse;

// See https://github.com/mongodb/specifications/tree/master/source/client-side-operation-timeout/tests
@RunWith(Parameterized.class)
public class ClientSideOperationTimeoutTest extends UnifiedSyncTest {

    public ClientSideOperationTimeoutTest(final String fileDescription, final String testDescription,
            final String schemaVersion, @Nullable final BsonArray runOnRequirements, final BsonArray entities,
            final BsonArray initialData, final BsonDocument definition) {
        super(schemaVersion, runOnRequirements, entities, initialData, definition);
        skipOperationTimeoutTests(fileDescription, testDescription);
    }

    @Parameterized.Parameters(name = "{0}: {1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        return getTestData("unified-test-format/client-side-operation-timeout");
    }

    @Test
    @Override
    public void shouldPassAllOutcomes() {
        super.shouldPassAllOutcomes();
    }

    public static void skipOperationTimeoutTests(final String fileDescription, final String testDescription) {

        if (ClusterFixture.isServerlessTest()) {

            // It is not possible to create capped collections on serverless instances.
            assumeFalse(fileDescription.equals("timeoutMS behaves correctly for tailable awaitData cursors"));
            assumeFalse(fileDescription.equals("timeoutMS behaves correctly for tailable non-awaitData cursors"));

            /* Drivers MUST NOT execute a killCursors command because the pinned connection is no longer under a load balancer. */
            assumeFalse(testDescription.equals("timeoutMS is refreshed for close"));

            /* Flaky tests. We have to retry them once we have a Junit5 rule. */
            assumeFalse(testDescription.equals("remaining timeoutMS applied to getMore if timeoutMode is unset"));
            assumeFalse(testDescription.equals("remaining timeoutMS applied to getMore if timeoutMode is cursor_lifetime"));
            assumeFalse(testDescription.equals("timeoutMS is refreshed for getMore if timeoutMode is iteration - success"));
            assumeFalse(testDescription.equals("timeoutMS is refreshed for getMore if timeoutMode is iteration - failure"));
        }
        assumeFalse("No maxTimeMS parameter for createIndex() method",
                testDescription.contains("maxTimeMS is ignored if timeoutMS is set - createIndex on collection"));
        assumeFalse("No run cursor command", fileDescription.startsWith("runCursorCommand"));
        assumeFalse("No special handling of runCommand", testDescription.contains("runCommand on database"));
        assumeFalse("No count command helper", testDescription.endsWith("count on collection"));
        assumeFalse("No operation based overrides", fileDescription.equals("timeoutMS can be overridden for an operation"));
        assumeFalse("No operation session based overrides",
                testDescription.equals("timeoutMS can be overridden for commitTransaction")
                        || testDescription.equals("timeoutMS applied to abortTransaction"));
        assumeFalse("No operation based overrides", fileDescription.equals("timeoutMS behaves correctly when closing cursors")
                && testDescription.equals("timeoutMS can be overridden for close"));
    }
}
