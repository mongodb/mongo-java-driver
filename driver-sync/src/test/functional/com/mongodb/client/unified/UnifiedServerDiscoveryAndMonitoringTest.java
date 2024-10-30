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

import static org.junit.jupiter.api.Assumptions.assumeFalse;

public final class UnifiedServerDiscoveryAndMonitoringTest extends UnifiedSyncTest {
    private static Collection<Arguments> data() throws URISyntaxException, IOException {
        return getTestData("unified-test-format/server-discovery-and-monitoring");
    }

    @Override
    protected void skips(final String fileDescription, final String testDescription) {
        doSkips(fileDescription, testDescription);
    }

    public static void doSkips(final String fileDescription, final String testDescription) {
        assumeFalse(testDescription.equals("connect with serverMonitoringMode=auto >=4.4"),
                "Skipping because our server monitoring events behave differently for now");
        assumeFalse(testDescription.equals("connect with serverMonitoringMode=stream >=4.4"),
                "Skipping because our server monitoring events behave differently for now");

        assumeFalse(fileDescription.equals("standalone-logging"), "Skipping until JAVA-4770 is implemented");
        assumeFalse(fileDescription.equals("replicaset-logging"), "Skipping until JAVA-4770 is implemented");
        assumeFalse(fileDescription.equals("sharded-logging"), "Skipping until JAVA-4770 is implemented");
        assumeFalse(fileDescription.equals("loadbalanced-logging"), "Skipping until JAVA-4770 is implemented");

        assumeFalse(fileDescription.equals("standalone-emit-topology-description-changed-before-close"),
                "Skipping until JAVA-5229 is implemented");
        assumeFalse(fileDescription.equals("replicaset-emit-topology-description-changed-before-close"),
                "Skipping until JAVA-5229 is implemented");
        assumeFalse(fileDescription.equals("sharded-emit-topology-description-changed-before-close"),
                "Skipping until JAVA-5229 is implemented");
        assumeFalse(fileDescription.equals("loadbalanced-emit-topology-description-changed-before-close"),
                "Skipping until JAVA-5229 is implemented");

        assumeFalse(testDescription.equals("poll waits after successful heartbeat"), "Skipping until JAVA-5564 is implemented");
        assumeFalse(fileDescription.equals("interruptInUse"), "Skipping until JAVA-4536 is implemented");
    }
}
