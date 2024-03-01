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

import com.mongodb.ClusterFixture;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.connection.TransportSettings;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.junit.After;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import reactor.core.publisher.Hooks;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.client.ClientSideOperationTimeoutTest.checkSkipCSOTTest;
import static com.mongodb.client.ClientSideOperationTimeoutTest.racyTestAssertion;
import static com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient.disableSleep;
import static com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient.enableSleepAfterCursorError;
import static org.junit.Assume.assumeFalse;

// See https://github.com/mongodb/specifications/tree/master/source/client-side-operation-timeout/tests
@RunWith(Parameterized.class)
public class ClientSideOperationTimeoutTest extends UnifiedReactiveStreamsTest {
    private final String testDescription;
    private final AtomicReference<Throwable> atomicReferenceThrowable = new AtomicReference<>();

    public ClientSideOperationTimeoutTest(final String fileDescription, final String testDescription,
            final String schemaVersion, @Nullable final BsonArray runOnRequirements, final BsonArray entities,
            final BsonArray initialData, final BsonDocument definition) {
        super(schemaVersion, runOnRequirements, entities, initialData, definition);
        this.testDescription = testDescription;
        // Time sensitive - cannot just create a cursor with publishers

        assumeFalse("No iterateOnce support. There is alternative prose test for it.",
                testDescription.equals("timeoutMS is refreshed for getMore if maxAwaitTimeMS is not set"));

        assumeFalse("No iterateOnce support. There is alternative prose test for it.",
                testDescription.equals("timeoutMS is refreshed for getMore if maxAwaitTimeMS is set"));
        /*
           The Reactive Streams specification prevents us from allowing a subsequent next call (event in reactive terms) after a timeout error,
           conflicting with the CSOT spec requirement not to invalidate the change stream and to try resuming and establishing a new change
           stream on the server. We immediately let users know about a timeout error, which then closes the stream/publisher.
         */
        assumeFalse("It is not possible due to a conflict with the Reactive Streams specification .",
                testDescription.equals("change stream can be iterated again if previous iteration times out"));

        assumeFalse("Flaky and racy due to asynchronous behaviour. There is alternative prose test for it.",
                testDescription.equals("timeoutMS applies to full resume attempt in a next call"));

        assumeFalse(testDescription.endsWith("createChangeStream on client"));
        assumeFalse(testDescription.endsWith("createChangeStream on database"));
        assumeFalse(testDescription.endsWith("createChangeStream on collection"));
        assumeFalse("TODO (CSOT) - JAVA-5104", fileDescription.equals("timeoutMS behaves correctly during command execution")
                &&  testDescription.equals("command is not sent if RTT is greater than timeoutMS"));

        checkSkipCSOTTest(fileDescription, testDescription);

        if (testDescription.equals("timeoutMS is refreshed for close")) {
            enableSleepAfterCursorError(256);
        }

        Hooks.onOperatorDebug();
        Hooks.onErrorDropped(atomicReferenceThrowable::set);
    }

    @Parameterized.Parameters(name = "{0}: {1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        return getTestData("unified-test-format/client-side-operation-timeout");
    }

    @Override
    public void shouldPassAllOutcomes() {
        try {
            super.shouldPassAllOutcomes();
        } catch (AssertionError e) {
            if (racyTestAssertion(testDescription, e)) {
                // Ignore failure - racy test often no time to do the getMore
                return;
            }
            throw e;
        }
        Throwable droppedError = atomicReferenceThrowable.get();
        if (droppedError != null) {
            throw new AssertionError("Test passed but there was a dropped error; `onError` called with no handler.", droppedError);
        }
    }

    @Override
    protected MongoClient createMongoClient(final MongoClientSettings settings) {
        TransportSettings overriddenTransportSettings = ClusterFixture.getOverriddenTransportSettings();
        MongoClientSettings clientSettings = overriddenTransportSettings == null ? settings
                : MongoClientSettings.builder(settings).transportSettings(overriddenTransportSettings).build();
        return new SyncMongoClient(MongoClients.create(clientSettings));
    }

    @After
    public void cleanUp() {
        super.cleanUp();
        disableSleep();
        Hooks.resetOnOperatorDebug();
        Hooks.resetOnErrorDropped();
    }
}
