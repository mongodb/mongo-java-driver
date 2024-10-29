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

import static com.mongodb.client.unified.UnifiedTestSkips.TestDef;
import static com.mongodb.client.unified.UnifiedTestSkips.testDef;

public final class UnifiedRetryableReadsTest extends UnifiedSyncTest {
    @Override
    protected void skips(final String fileDescription, final String testDescription) {
        doSkips(testDef("unified-test-format/retryable-reads", fileDescription, testDescription));
    }

    public static void doSkips(final TestDef def) {
        def.skipDeprecated("Deprecated feature removed")
                .file("retryable-reads", "count")
                .file("retryable-reads", "count-serverErrors");

        def.skipDeprecated("Deprecated feature never implemented")
                .file("retryable-reads", "listDatabaseObjects")
                .file("retryable-reads", "listDatabaseObjects-serverErrors")
                .file("retryable-reads", "listCollectionObjects")
                .file("retryable-reads", "listCollectionObjects-serverErrors");

        def.skipJira("https://jira.mongodb.org/browse/JAVA-5224")
                .test("retryable-reads", "ReadConcernMajorityNotAvailableYet is a retryable read", "Find succeeds on second attempt after ReadConcernMajorityNotAvailableYet");
    }

    private static Collection<Arguments> data() throws URISyntaxException, IOException {
        return getTestData("unified-test-format/retryable-reads");
    }
}
