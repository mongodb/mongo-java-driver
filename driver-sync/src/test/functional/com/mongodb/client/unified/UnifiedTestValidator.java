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

import static com.mongodb.ClusterFixture.serverVersionLessThan;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

final class UnifiedTestValidator extends UnifiedSyncTest {
    @Override
    protected void skips(final String fileDescription, final String testDescription) {
        assumeFalse(testDescription.equals("InsertOne fails after multiple retryable writeConcernErrors") && serverVersionLessThan(4, 4),
                "MongoDB releases prior to 4.4 incorrectly add errorLabels as a field within the writeConcernError document "
                        + "instead of as a top-level field.  Rather than handle that in code, we skip the test on older server versions.");
        // Feature to be implemented in scope of JAVA-5389
        assumeFalse(fileDescription.equals("expectedEventsForClient-topologyDescriptionChangedEvent"));
        // Feature to be implemented in scope JAVA-4862
        assumeFalse(fileDescription.equals("entity-commandCursor"));
    }

    private static Collection<Arguments> data() throws URISyntaxException, IOException {
        return getTestData("unified-test-format/valid-pass");
    }
}
