/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client.unified;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.unified.UnifiedTest;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;

import static com.mongodb.ClusterFixture.isServerlessTest;
import static org.junit.Assume.assumeFalse;

public class SessionsTest extends UnifiedTest {
    public SessionsTest(@SuppressWarnings("unused") final String fileDescription, @SuppressWarnings("unused") final String testDescription,
                        final String schemaVersion, @Nullable final BsonArray runOnRequirements, final BsonArray entities,
                        final BsonArray initialData, final BsonDocument definition) {
        super(schemaVersion, runOnRequirements, entities, initialData, definition);
        // This test is currently failing due to an issue with the serverless proxy.  It can be re-enabled once the issue is addressed.
        assumeFalse((
                testDescription.equals("Server returns an error on listCollections with snapshot")
                        || testDescription.equals("Server returns an error on runCommand with snapshot"))
                && isServerlessTest());
    }

    @Override
    protected MongoClient createMongoClient(final MongoClientSettings settings) {
        return new SyncMongoClient(MongoClients.create(settings));
    }

    @Parameterized.Parameters(name = "{0}: {1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        return getTestData("unified-test-format/sessions");
    }
}
