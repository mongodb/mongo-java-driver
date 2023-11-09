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

package com.mongodb.client;

import com.mongodb.client.unified.UnifiedSyncTest;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assume.assumeFalse;

// See https://github.com/mongodb/specifications/tree/master/source/client-side-operation-timeout/tests
@RunWith(Parameterized.class)
public class ClientSideOperationTimeoutTest extends UnifiedSyncTest {
    public ClientSideOperationTimeoutTest(final String fileDescription, final String testDescription,
            final String schemaVersion, @Nullable final BsonArray runOnRequirements, final BsonArray entities,
            final BsonArray initialData, final BsonDocument definition) {
        super(schemaVersion, runOnRequirements, entities, initialData, definition);
        checkSkipCSOTTest(fileDescription, testDescription);
    }

    @Parameterized.Parameters(name = "{0}: {1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        return getTestData("unified-test-format/client-side-operation-timeout");
    }

    public static void checkSkipCSOTTest(final String fileDescription, final String testDescription) {
        assumeFalse("No run cursor command", fileDescription.startsWith("runCursorCommand"));
        assumeFalse("No count command helper", testDescription.endsWith("count on collection"));
        assumeFalse("No operation based overrides", fileDescription.equals("timeoutMS can be overridden for an operation"));
        assumeFalse("No iterateOnce support", testDescription.equals("timeoutMS is refreshed for getMore if maxAwaitTimeMS is not set"));

        checkOperationSupportsMaxTimeMS(fileDescription, testDescription);
        checkTransactionSessionSupport(fileDescription, testDescription);
        checkCommandExecutionSupport(fileDescription, testDescription);


        assumeFalse("TODO (CSOT) - JAVA-4054", fileDescription.equals("timeoutMS behaves correctly for change streams"));
        assumeFalse("TODO (CSOT) - JAVA-4052", fileDescription.startsWith("timeoutMS behaves correctly for retryable operations"));

        assumeFalse("TODO (CSOT) - JAVA-4063", testDescription.contains("RTT"));
        assumeFalse("TODO (CSOT) - JAVA-4059", fileDescription.contains("GridFS"));

        assumeFalse("TODO (CSOT) - JAVA-5248",
                fileDescription.equals("MaxTimeMSExpired server errors are transformed into a custom timeout error"));

        assumeFalse("TODO (CSOT) - JAVA-4062", testDescription.contains("wTimeoutMS is ignored"));

        assumeFalse("TODO (CSOT) - JAVA-4057", fileDescription.contains("bulkWrite") || testDescription.contains("bulkWrite"));
        assumeFalse("TODO (CSOT) JAVA-4057 ",
                fileDescription.equals("timeoutMS behaves correctly for non-tailable cursors")
                && testDescription.equals("timeoutMS is refreshed for getMore if timeoutMode is iteration - success"));

        assumeFalse("TODO (CSOT) - Tests need to create a capped collection - not in json",
                fileDescription.startsWith("timeoutMS behaves correctly for tailable awaitData cursors"));
    }

    private static void checkOperationSupportsMaxTimeMS(final String fileDescription, final String testDescription) {
        // TODO (CSOT) JAVA-4057 - check if maxTimeMS is set when using timeoutMS
        assumeFalse("No maxTimeMS support", NO_MAX_TIME_MS_SUPPORT.stream().anyMatch(testDescription::endsWith));
    }

    private static void checkTransactionSessionSupport(final String fileDescription, final String testDescription) {
        assumeFalse("TODO (CSOT) - JAVA-4066", fileDescription.contains("timeoutMS behaves correctly for the withTransaction API"));
        assumeFalse("TODO (CSOT) - JAVA-4067", testDescription.contains("Transaction"));
        assumeFalse("TODO (CSOT) - JAVA-4067", fileDescription.contains("timeoutMS can be overridden at the level of a ClientSession"));
        assumeFalse("TODO (CSOT) - JAVA-4067", fileDescription.startsWith("sessions inherit timeoutMS from their parent MongoClient"));
    }

    private static void checkCommandExecutionSupport(final String fileDescription, final String testDescription) {
        assumeFalse("TODO (CSOT) - JAVA-5232", testDescription.startsWith("socketTimeoutMS is ignored if timeoutMS is set"));
        assumeFalse("TODO (CSOT) - JAVA-5232", fileDescription.startsWith("timeoutMS behaves correctly during command execution"));
        assumeFalse("TODO (CSOT) - JAVA-5232",
                fileDescription.equals("timeoutMS behaves correctly for non-tailable cursors")
                        && testDescription.equals("remaining timeoutMS applied to getMore if timeoutMode is cursor_lifetime"));
        assumeFalse("TODO (CSOT) - JAVA-5232",
                fileDescription.equals("timeoutMS behaves correctly for non-tailable cursors")
                        && testDescription.equals("remaining timeoutMS applied to getMore if timeoutMode is unset"));
        assumeFalse("TODO (CSOT) - JAVA-5232",
                fileDescription.equals("timeoutMS behaves correctly for non-tailable cursors")
                        && testDescription.equals("timeoutMS applied to find if timeoutMode is cursor_lifetime"));
        assumeFalse("TODO (CSOT) - JAVA-5232",
                fileDescription.equals("timeoutMS behaves correctly for non-tailable cursors")
                        && testDescription.equals("timeoutMS applied to find if timeoutMode is iteration"));
        assumeFalse("TODO (CSOT) - JAVA-5232",
                fileDescription.equals("timeoutMS behaves correctly for non-tailable cursors")
                        && testDescription.equals("timeoutMS is refreshed for getMore if timeoutMode is iteration - failure"));
        assumeFalse("TODO (CSOT) - JAVA-5232",
                fileDescription.equals("timeoutMS behaves correctly for tailable non-awaitData cursors")
                        && testDescription.equals("timeoutMS applied to find"));
        assumeFalse("TODO (CSOT) - JAVA-5232",
                fileDescription.equals("timeoutMS behaves correctly for tailable non-awaitData cursors")
                        && testDescription.equals("timeoutMS is refreshed for getMore - failure"));
        assumeFalse("TODO (CSOT) - JAVA-5232",
                fileDescription.equals("timeoutMS behaves correctly for tailable non-awaitData cursors")
                        && testDescription.equals("timeoutMS is refreshed for getMore - success"));
        assumeFalse("TODO (CSOT) - JAVA-5232", testDescription.equals("timeoutMS can be overridden for close"));
        assumeFalse("TODO (CSOT) - JAVA-5232", testDescription.equals("timeoutMS is refreshed for close"));
        assumeFalse("TODO (CSOT) - JAVA-5232", testDescription.contains("timeoutMS can be configured on a MongoClient -"));
        assumeFalse("TODO (CSOT) - JAVA-5232", testDescription.contains("timeoutMS can be configured on a MongoDatabase -"));
    }

    private static final List<String> NO_MAX_TIME_MS_SUPPORT = asList(
            "createIndex on collection",
            "deleteMany on collection",
            "deleteOne on collection",
            "distinct on collection",
            "dropIndex on collection",
            "dropIndexes on collection",
            "dropIndexes on collection",
            "findOneAndDelete on collection",
            "findOneAndReplace on collection",
            "findOneAndUpdate on collection",
            "insertMany on collection",
            "insertOne on collection",
            "listIndexNames on collection",
            "listIndexes on collection",
            "replaceOne on collection",
            "updateMany on collection",
            "updateOne on collection"
    );
}
