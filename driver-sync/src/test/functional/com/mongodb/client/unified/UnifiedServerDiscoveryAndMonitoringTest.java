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

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.jupiter.params.provider.Arguments;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;

import static org.junit.jupiter.api.Assumptions.assumeFalse;

public final class UnifiedServerDiscoveryAndMonitoringTest extends UnifiedSyncTest {
    private static Collection<Arguments> data() throws URISyntaxException, IOException {
        return getTestData("server-discovery-and-monitoring/tests/unified");
    }

    @Override
    protected void skips(final String fileDescription, final String testDescription) {
        doSkips(getDefinition());
    }

    public static void doSkips(final BsonDocument definition) {
        String description = definition.getString("description", new BsonString("")).getValue();
        assumeFalse(description.equals("connect with serverMonitoringMode=auto >=4.4"),
                "Skipping because our server monitoring events behave differently for now");
        assumeFalse(description.equals("connect with serverMonitoringMode=stream >=4.4"),
                "Skipping because our server monitoring events behave differently for now");
    }
}
