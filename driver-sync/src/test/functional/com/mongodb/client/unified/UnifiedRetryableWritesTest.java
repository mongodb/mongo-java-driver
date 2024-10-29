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

import com.mongodb.client.unified.UnifiedTestSkips.TestDef;
import org.junit.jupiter.params.provider.Arguments;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;

import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.ClusterFixture.serverVersionLessThan;
import static com.mongodb.client.unified.UnifiedTestSkips.testDef;

public final class UnifiedRetryableWritesTest extends UnifiedSyncTest {
    @Override
    protected void skips(final String fileDescription, final String testDescription) {
        doSkips(testDef("unified-test-format/retryable-writes", fileDescription, testDescription));
    }

    public static void doSkips(final TestDef def) {
        if (isSharded() && serverVersionLessThan(5, 0)) {
            def.skipJira("https://jira.mongodb.org/browse/JAVA-5125")
                    .testContains("retryable-writes", "succeeds after WriteConcernError")
                    .testContains("retryable-writes", "succeeds after retryable writeConcernError");
        }
        if (isDiscoverableReplicaSet() && serverVersionLessThan(4, 4)) {
            def.skipJira("https://jira.mongodb.org/browse/JAVA-5341")
                    .test("retryable-writes", "insertOne-serverErrors", "RetryableWriteError label is added based on writeConcernError in pre-4.4 mongod response");
        }
        def.skipJira("https://jira.mongodb.org/browse/JAVA-4586")
                .testContains("retryable-writes", "client bulkWrite")
                .testContains("retryable-writes", "client.clientBulkWrite");
    }

    private static Collection<Arguments> data() throws URISyntaxException, IOException {
        return getTestData("unified-test-format/retryable-writes");
    }
}
