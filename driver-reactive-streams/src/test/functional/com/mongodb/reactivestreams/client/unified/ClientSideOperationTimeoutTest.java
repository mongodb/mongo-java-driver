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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Hooks;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.client.ClientSideOperationTimeoutTest.skipOperationTimeoutTests;
import static com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient.disableSleep;
import static com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient.enableSleepAfterCursorError;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assumptions.assumeFalse;


// See https://github.com/mongodb/specifications/tree/master/source/client-side-operation-timeout/tests
public class ClientSideOperationTimeoutTest extends UnifiedReactiveStreamsTest {

    private final AtomicReference<Throwable> atomicReferenceThrowable = new AtomicReference<>();

    private static Collection<Arguments> data() throws URISyntaxException, IOException {
        return getTestData("client-side-operations-timeout/tests");
    }

    @Override
    protected void skips(final String fileDescription, final String testDescription) {
        skipOperationTimeoutTests(fileDescription, testDescription);

        assumeFalse(testDescription.equals("timeoutMS is refreshed for getMore if maxAwaitTimeMS is not set"),
                "No iterateOnce support. There is alternative prose test for it.");
        assumeFalse(testDescription.equals("timeoutMS is refreshed for getMore if maxAwaitTimeMS is set"),
                "No iterateOnce support. There is alternative prose test for it.");
        /*
           The Reactive Streams specification prevents us from allowing a subsequent next call (event in reactive terms) after a timeout error,
           conflicting with the CSOT spec requirement not to invalidate the change stream and to try resuming and establishing a new change
           stream on the server. We immediately let users know about a timeout error, which then closes the stream/publisher.
         */
        assumeFalse(testDescription.equals("change stream can be iterated again if previous iteration times out"),
                "It is not possible due to a conflict with the Reactive Streams specification .");
        assumeFalse(testDescription.equals("timeoutMS applies to full resume attempt in a next call"),
                "Flaky and racy due to asynchronous behaviour. There is alternative prose test for it.");
        assumeFalse(testDescription.equals("timeoutMS applied to initial aggregate"),
                "No way to catch an error on BarchCursor creation. There is alternative prose test for it.");

        assumeFalse(testDescription.endsWith("createChangeStream on client"));
        assumeFalse(testDescription.endsWith("createChangeStream on database"));
        assumeFalse(testDescription.endsWith("createChangeStream on collection"));

        // No withTransaction support
        assumeFalse(fileDescription.contains("withTransaction") || testDescription.contains("withTransaction"));

        if (testDescription.equals("timeoutMS is refreshed for close")) {
            enableSleepAfterCursorError(256);
        }

        /*
         * The test is occasionally racy. The "killCursors" command may appear as an additional event. This is unexpected in unified tests,
         * but anticipated in reactive streams because an operation timeout error triggers the closure of the stream/publisher.
         */
        ignoreExtraCommandEvents(testDescription.contains("timeoutMS is refreshed for getMore - failure"));

        Hooks.onOperatorDebug();
        Hooks.onErrorDropped(atomicReferenceThrowable::set);
    }

    @ParameterizedTest(name = "{0}: {1}")
    @MethodSource("data")
    @Override
    public void shouldPassAllOutcomes(
            @Nullable final String fileDescription,
            @Nullable final String testDescription,
            final String schemaVersion,
            @Nullable final BsonArray runOnRequirements,
            final BsonArray entitiesArray,
            final BsonArray initialData,
            final BsonDocument definition) {
        try {
            super.shouldPassAllOutcomes(fileDescription,
                    testDescription,
                    schemaVersion,
                    runOnRequirements,
                    entitiesArray,
                    initialData,
                    definition);

        } catch (AssertionError e) {
            assertNoDroppedError(format("%s failed due to %s.\n"
                            + "The test also caused a dropped error; `onError` called with no handler.",
                    testDescription, e.getMessage()));
            if (racyTestAssertion(testDescription, e)) {
                // Ignore failure - racy test often no time to do the getMore
                return;
            }
            throw e;
        }
        assertNoDroppedError(format("%s passed but there was a dropped error; `onError` called with no handler.", testDescription));
    }
    @Override
    protected MongoClient createMongoClient(final MongoClientSettings settings) {
        TransportSettings overriddenTransportSettings = ClusterFixture.getOverriddenTransportSettings();
        MongoClientSettings clientSettings = overriddenTransportSettings == null ? settings
                : MongoClientSettings.builder(settings).transportSettings(overriddenTransportSettings).build();
        return new SyncMongoClient(MongoClients.create(clientSettings));
    }

    @AfterEach
    public void cleanUp() {
        super.cleanUp();
        disableSleep();
        Hooks.resetOnOperatorDebug();
        Hooks.resetOnErrorDropped();
    }

    public static boolean racyTestAssertion(final String testDescription, final AssertionError e) {
        return RACY_GET_MORE_TESTS.contains(testDescription) && e.getMessage().startsWith("Number of events must be the same");
    }

    private static final List<String> RACY_GET_MORE_TESTS = asList(
            "remaining timeoutMS applied to getMore if timeoutMode is cursor_lifetime",
            "remaining timeoutMS applied to getMore if timeoutMode is unset");

    private void assertNoDroppedError(final String message) {
        Throwable droppedError = atomicReferenceThrowable.get();
        if (droppedError != null) {
            throw new AssertionError(message, droppedError);
        }
    }
}
