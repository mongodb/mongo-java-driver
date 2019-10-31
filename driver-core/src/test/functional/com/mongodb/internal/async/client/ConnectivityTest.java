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

package com.mongodb.internal.async.client;

import com.mongodb.async.FutureResultCallback;
import org.bson.Document;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class ConnectivityTest {
    // the test succeeds if no exception is thrown, and fail otherwise
    @Test
    public void testConnectivity() throws InterruptedException {
        AsyncMongoClient client = AsyncMongoClients.create(Fixture.getMongoClientSettings());

        try {
            FutureResultCallback<Document> commandCallback = new FutureResultCallback<Document>();
            // test that a command that doesn't require auth completes normally
            client.getDatabase("admin").runCommand(new Document("ismaster", 1), commandCallback);

            commandCallback.get(10, TimeUnit.SECONDS);

            FutureResultCallback<Long> countCallback = new FutureResultCallback<Long>();

            // test that a command that requires auth completes normally
            client.getDatabase("test").getCollection("test").estimatedDocumentCount(countCallback);

            countCallback.get(10, TimeUnit.SECONDS);
        } finally {
            client.close();
        }
    }

}
