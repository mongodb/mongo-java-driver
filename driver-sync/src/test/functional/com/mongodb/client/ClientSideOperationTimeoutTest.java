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
import org.junit.Test;
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
    private final String testDescription;
    public ClientSideOperationTimeoutTest(final String fileDescription, final String testDescription,
            final String schemaVersion, @Nullable final BsonArray runOnRequirements, final BsonArray entities,
            final BsonArray initialData, final BsonDocument definition) {
        super(schemaVersion, runOnRequirements, entities, initialData, definition);
        this.testDescription = testDescription;
        checkSkipCSOTTest(fileDescription, testDescription);
    }

    @Parameterized.Parameters(name = "{0}: {1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        return getTestData("unified-test-format/client-side-operation-timeout");
    }

    @Test
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
    }


    public static boolean racyTestAssertion(final String testDescription, final AssertionError e) {
        return RACY_GET_MORE_TESTS.contains(testDescription) && e.getMessage().startsWith("Number of events must be the same");
    }

    public static void checkSkipCSOTTest(final String fileDescription, final String testDescription) {
        assumeFalse("No run cursor command", fileDescription.startsWith("runCursorCommand")
                || testDescription.contains("runCommand on database"));
        assumeFalse("No count command helper", testDescription.endsWith("count on collection"));
        assumeFalse("No operation based overrides", fileDescription.equals("timeoutMS can be overridden for an operation"));
        assumeFalse("No iterateOnce support", testDescription.equals("timeoutMS is refreshed for getMore if maxAwaitTimeMS is not set"));

        assumeFalse("TODO (CSOT) - JAVA-5259 No client.withTimeout", testDescription.endsWith("on client"));

        checkOperationSupportsMaxTimeMS(fileDescription, testDescription);
        checkTransactionSessionSupport(fileDescription, testDescription);

        assumeFalse("TODO (CSOT) - JAVA-4054", fileDescription.equals("timeoutMS behaves correctly for change streams"));
        assumeFalse("TODO (CSOT) - JAVA-4052", fileDescription.startsWith("timeoutMS behaves correctly for retryable operations"));
        assumeFalse("TODO (CSOT) - JAVA-4052", fileDescription.startsWith("legacy timeouts behave correctly for retryable operations"));

        assumeFalse("TODO (CSOT) - JAVA-5248",
                fileDescription.equals("MaxTimeMSExpired server errors are transformed into a custom timeout error"));

        assumeFalse("TODO (CSOT) - JAVA-4062", testDescription.contains("wTimeoutMS is ignored"));


        if (fileDescription.contains("GridFS")) { //TODO (CSOT) - JAVA-4057
            assumeFalse("TODO (CSOT) - JAVA-4057", testDescription.contains("chunk insertion"));
            assumeFalse("TODO (CSOT) - JAVA-4057", testDescription.contains("creation of files document"));
            assumeFalse("TODO (CSOT) - JAVA-4057", testDescription.contains("delete against the files collection"));
            assumeFalse("TODO (CSOT) - JAVA-4057", testDescription.contains("delete against the chunks collection"));
            assumeFalse("TODO (CSOT) - JAVA-4057", testDescription.contains("overridden for a rename"));
            assumeFalse("TODO (CSOT) - JAVA-4057", testDescription.contains("update during a rename"));
            assumeFalse("TODO (CSOT) - JAVA-4057", testDescription.contains("collection drop"));
            assumeFalse("TODO (CSOT) - JAVA-4057", testDescription.contains("drop as a whole"));
            assumeFalse("TODO (CSOT) - JAVA-4057", testDescription.contains("entire delete"));
        }

        assumeFalse("TODO (CSOT) - JAVA-4057", testDescription.equals("maxTimeMS value in the command is less than timeoutMS"));
        assumeFalse("TODO (CSOT) - JAVA-4057", fileDescription.contains("bulkWrite") || testDescription.contains("bulkWrite"));

        // TEST BUGS / ISSUES
        assumeFalse("TODO (CSOT) - Tests need to create a capped collection - not in json",
                fileDescription.startsWith("timeoutMS behaves correctly for tailable awaitData cursors"));
        assumeFalse("TODO (CSOT) - Tests need to create a capped collection - not in json",
                fileDescription.startsWith("timeoutMS behaves correctly for tailable non-await cursors"));
        assumeFalse("TODO (CSOT) - Tests need to create a capped collection - not in json",
                fileDescription.startsWith("timeoutMS behaves correctly for tailable non-awaitData cursors"));

        assumeFalse("TODO (CSOT) - Invalid collection name in the test",
             testDescription.equals("timeoutMS can be overridden for close"));
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

    private static final List<String> RACY_GET_MORE_TESTS = asList(
            "remaining timeoutMS applied to getMore if timeoutMode is cursor_lifetime",
            "remaining timeoutMS applied to getMore if timeoutMode is unset");

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
