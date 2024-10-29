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

public final class UnifiedServerDiscoveryAndMonitoringTest extends UnifiedSyncTest {
    private static Collection<Arguments> data() throws URISyntaxException, IOException {
        return getTestData("unified-test-format/server-discovery-and-monitoring");
    }

    @Override
    protected void skips(final String fileDescription, final String testDescription) {
        doSkips(testDef("unified-test-format/server-discovery-and-monitoring", fileDescription, testDescription));
    }

    public static void doSkips(final TestDef def) {
        def.skipJira("https://jira.mongodb.org/browse/JAVA-5230")
                .file("server-discovery-and-monitoring", "connect with serverMonitoringMode=auto >=4.4")
                .file("server-discovery-and-monitoring", "connect with serverMonitoringMode=stream >=4.4");

        def.skipJira("https://jira.mongodb.org/browse/JAVA-4770")
                .file("server-discovery-and-monitoring", "standalone-logging")
                .file("server-discovery-and-monitoring", "replicaset-logging")
                .file("server-discovery-and-monitoring", "sharded-logging")
                .file("server-discovery-and-monitoring", "loadbalanced-logging");

        def.skipJira("https://jira.mongodb.org/browse/JAVA-5229")
                .file("server-discovery-and-monitoring", "standalone-emit-topology-description-changed-before-close")
                .file("server-discovery-and-monitoring", "replicaset-emit-topology-description-changed-before-close")
                .file("server-discovery-and-monitoring", "sharded-emit-topology-description-changed-before-close")
                .file("server-discovery-and-monitoring", "loadbalanced-emit-topology-description-changed-before-close");

        def.skipJira("https://jira.mongodb.org/browse/JAVA-5564")
                .test("server-discovery-and-monitoring", "serverMonitoringMode", "poll waits after successful heartbeat");

        def.skipJira("https://jira.mongodb.org/browse/JAVA-4536")
                .file("server-discovery-and-monitoring", "interruptInUse");
    }
}
