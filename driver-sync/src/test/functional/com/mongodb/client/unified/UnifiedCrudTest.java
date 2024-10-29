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

package com.mongodb.client.unified;

import org.junit.jupiter.params.provider.Arguments;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;

import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

public final class UnifiedCrudTest extends UnifiedSyncTest {
    public static void doSkips(final String fileDescription, final String testDescription) {
        assumeFalse(testDescription.equals("Deprecated count with empty collection"));
        assumeFalse(testDescription.equals("Deprecated count with collation"));
        assumeFalse(testDescription.equals("Deprecated count without a filter"));
        assumeFalse(testDescription.equals("Deprecated count with a filter"));
        assumeFalse(testDescription.equals("Deprecated count with skip and limit"));
        assumeFalse(testDescription.equals("Unacknowledged findOneAndReplace with hint string on 4.4+ server"));
        assumeFalse(testDescription.equals("Unacknowledged findOneAndReplace with hint document on 4.4+ server"));
        assumeFalse(testDescription.equals("Unacknowledged findOneAndUpdate with hint string on 4.4+ server"));
        assumeFalse(testDescription.equals("Unacknowledged findOneAndUpdate with hint document on 4.4+ server"));
        assumeFalse(testDescription.equals("Unacknowledged findOneAndDelete with hint string on 4.4+ server"));
        assumeFalse(testDescription.equals("Unacknowledged findOneAndDelete with hint document on 4.4+ server"));
        if (isDiscoverableReplicaSet() && serverVersionAtLeast(8, 0)) {
            assumeFalse(testDescription.equals("Aggregate with $out includes read preference for 5.0+ server"));
            assumeFalse(testDescription.equals("Database-level aggregate with $out includes read preference for 5.0+ server"));
        }
        assumeFalse(testDescription.endsWith("updateOne with sort option unsupported (server-side error)"), "Skipping until JAVA-5622 is " +
                "implemented");
        assumeFalse(testDescription.endsWith("replaceOne with sort option unsupported (server-side error)"), "Skipping until " +
                "JAVA-5622 is implemented");
        assumeFalse(testDescription.equals("BulkWrite with string comment"), "Skipping until we decide " +
                "whether to update the spec or test https://mongodb.slack.com/archives/C72LB5RPV/p1730225657999979");
        assumeFalse(testDescription.equals("BulkWrite with document comment"), "Skipping until we decide " +
                "whether to update the spec or test https://mongodb.slack.com/archives/C72LB5RPV/p1730225657999979");
    }

    @Override
    protected void skips(final String fileDescription, final String testDescription) {
        doSkips(fileDescription, testDescription);
    }

    private static Collection<Arguments> data() throws URISyntaxException, IOException {
        return getTestData("unified-test-format/crud");
    }
}
