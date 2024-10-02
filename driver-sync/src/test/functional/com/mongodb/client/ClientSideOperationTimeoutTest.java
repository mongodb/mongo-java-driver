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
import org.junit.jupiter.params.provider.Arguments;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;

import static org.junit.jupiter.api.Assumptions.assumeFalse;


// See https://github.com/mongodb/specifications/tree/master/source/client-side-operation-timeout/tests
public class ClientSideOperationTimeoutTest extends UnifiedSyncTest {

    private static Collection<Arguments> data() throws URISyntaxException, IOException {
        return getTestData("client-side-operations-timeout/tests");
    }

    @Override
    protected void skips(final String fileDescription, final String testDescription) {
        skipOperationTimeoutTests(fileDescription, testDescription);
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
        assumeFalse(testDescription.contains("maxTimeMS is ignored if timeoutMS is set - createIndex on collection"),
                "No maxTimeMS parameter for createIndex() method");
        assumeFalse(fileDescription.startsWith("runCursorCommand"), "No run cursor command");
        assumeFalse(testDescription.contains("runCommand on database"), "No special handling of runCommand");
        assumeFalse(testDescription.endsWith("count on collection"), "No count command helper");
        assumeFalse(fileDescription.equals("timeoutMS can be overridden for an operation"), "No operation based overrides");
        assumeFalse(testDescription.equals("timeoutMS can be overridden for commitTransaction")
                        || testDescription.equals("timeoutMS applied to abortTransaction"),
                "No operation session based overrides");
        assumeFalse(fileDescription.equals("timeoutMS behaves correctly when closing cursors")
                && testDescription.equals("timeoutMS can be overridden for close"), "No operation based overrides");
    }
}
