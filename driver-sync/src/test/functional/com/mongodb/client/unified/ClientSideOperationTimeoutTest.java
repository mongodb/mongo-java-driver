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

import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;

import static org.junit.Assume.assumeFalse;

public class ClientSideOperationTimeoutTest extends UnifiedSyncTest {

    public ClientSideOperationTimeoutTest(@SuppressWarnings("unused") final String fileDescription,
                                          @SuppressWarnings("unused") final String testDescription,
                                          final String schemaVersion,
                                          @Nullable final BsonArray runOnRequirements, final BsonArray entities, final BsonArray initialData,
                                          final BsonDocument definition) {
        super(schemaVersion, runOnRequirements, entities, initialData, definition);

        // Ignored until timeout per operation is implemented on Client level. //TODO
        assumeFalse(testDescription.endsWith("createChangeStream on client"));
        assumeFalse(testDescription.endsWith("listDatabases on client"));
        assumeFalse(testDescription.endsWith("listDatabaseNames on client"));

        // No count command helper
        assumeFalse(testDescription.endsWith("count on collection"));

        // TODO JAVA-5232 maxTimeMs is expected to be present in command definition
        assumeFalse("operation is retried multiple times for non-zero timeoutMS - insertOne on collection".equals(testDescription));
        assumeFalse("operation is retried multiple times for non-zero timeoutMS - insertMany on collection".equals(testDescription));
        assumeFalse("operation is retried multiple times for non-zero timeoutMS - deleteOne on collection".equals(testDescription));
        assumeFalse("operation is retried multiple times for non-zero timeoutMS - replaceOne on collection".equals(testDescription));
        assumeFalse("operation is retried multiple times for non-zero timeoutMS - updateOne on collection".equals(testDescription));
        assumeFalse("operation is retried multiple times for non-zero timeoutMS - bulkWrite on collection".equals(testDescription));

    }

    @Parameterized.Parameters(name = "{0}: {1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        return getTestData("unified-test-format/client-side-operations-timeout");
    }
}
