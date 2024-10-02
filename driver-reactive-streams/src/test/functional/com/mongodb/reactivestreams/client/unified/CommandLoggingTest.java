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

import org.junit.jupiter.params.provider.Arguments;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;

import static org.junit.jupiter.api.Assumptions.assumeFalse;

final class CommandLoggingTest extends UnifiedReactiveStreamsTest {
    @Override
    protected void skips(final String fileDescription, final String testDescription) {
        // The driver has a hack where getLastError command is executed as part of the handshake in order to get a connectionId
        // even when the hello command response doesn't contain it.
        assumeFalse(fileDescription.equals("pre-42-server-connection-id"));
    }

    private static Collection<Arguments> data() throws URISyntaxException, IOException {
        return getTestData("command-logging-and-monitoring/tests/logging");
    }
}
