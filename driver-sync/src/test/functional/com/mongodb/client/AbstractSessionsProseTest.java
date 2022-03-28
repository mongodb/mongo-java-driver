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

package com.mongodb.client;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.Updates;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.getConnectionString;
import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

// Prose tests from https://github.com/mongodb/specifications/tree/master/source/sessions
public abstract class AbstractSessionsProseTest {

    protected abstract MongoClient getMongoClient(MongoClientSettings settings);

    // Test 13 from https://github.com/mongodb/specifications/blob/master/source/sessions/driver-sessions.rst#test-plan"
    @Test
    public void shouldCreateServerSessionOnlyAfterConnectionCheckout() throws InterruptedException {
        assumeTrue(serverVersionAtLeast(3, 6));

        int minLsidSetSize = Integer.MAX_VALUE;

        for (int i = 0; i < 5; i++) {
            // given
            Set<BsonDocument> lsidSet = ConcurrentHashMap.newKeySet();
            MongoClient client = getMongoClient(MongoClientSettings.builder()
                    .applyConnectionString(getConnectionString())
                    .applyToConnectionPoolSettings(builder -> builder.maxSize(1))
                    .addCommandListener(new CommandListener() {
                        @Override
                        public void commandStarted(final CommandStartedEvent event) {
                            lsidSet.add(event.getCommand().getDocument("lsid"));
                        }
                    })
                    .build());
            MongoCollection<Document> collection = client.getDatabase(getDefaultDatabaseName()).getCollection(getClass().getName());

            // when executing 8 operations concurrently
            ExecutorService executor = Executors.newFixedThreadPool(3);
            executor.submit(() -> {
                collection.insertOne(new Document());
            });
            executor.submit(() -> {
                collection.deleteOne(Filters.eq("_id", 1));
            });
            executor.submit(() -> {
                collection.updateOne(Filters.eq("_id", 1), Updates.set("x", 1));
            });
            executor.submit(() -> {
                collection.bulkWrite(Collections.singletonList(
                        new UpdateOneModel<>(Filters.eq("_id", 1), Updates.set("x", 1))));
            });
            executor.submit(() -> {
                // Test fineOneAndDelete, since there is specialized code for retryable writes, that uses sessions
                collection.findOneAndDelete(Filters.eq("_id", 1));
            });
            executor.submit(() -> {
                // Test fineOneAndDelete, since there is specialized code for retryable writes, that uses sessions
                collection.findOneAndUpdate(Filters.eq("_id", 1), Updates.set("x", 1));
            });
            executor.submit(() -> {
                // Test fineOneAndDelete, since there is specialized code for retryable writes, that uses sessions
                collection.findOneAndReplace(Filters.eq("_id", 1), new Document("_id", 1));
            });
            executor.submit(() -> {
                collection.find().first();
            });

            executor.shutdown();
            boolean terminated = executor.awaitTermination(5, TimeUnit.SECONDS);

            // then
            assertTrue(terminated);
            assertTrue(lsidSet.size() < 8);
            minLsidSetSize = Math.min(minLsidSetSize, lsidSet.size());
        }
        assertEquals(1, minLsidSetSize);
    }
}
