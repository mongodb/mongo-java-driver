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

package com.mongodb.reactivestreams.client;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import static com.mongodb.reactivestreams.client.Fixture.dropDatabase;
import static com.mongodb.reactivestreams.client.Fixture.getDefaultDatabase;
import static com.mongodb.reactivestreams.client.Fixture.isReplicaSet;
import static com.mongodb.reactivestreams.client.Fixture.serverVersionAtLeast;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class ChangeStreamsCancellationTest {

    private MongoDatabase database;

    @BeforeEach
    public void setup() {
        assumeTrue(isReplicaSet() && serverVersionAtLeast(3, 6));
        database = getDefaultDatabase();
    }

    @AfterEach
    public void tearDown() {
        if (database != null) {
            dropDatabase(database.getName());
        }
    }

    @Test
    public void testCancelReleasesSessions() {
        MongoCollection<Document> collection = database.getCollection("changeStreamsCancellationTest");
        Mono.from(collection.insertOne(new Document()));

        TestSubscriber<ChangeStreamDocument<Document>> subscriber = new TestSubscriber<>();
        subscriber.doOnSubscribe(sub -> {
            sub.request(Integer.MAX_VALUE);
            new Thread(() -> {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Sleep interrupted");
                }
                sub.cancel();
            }).start();
        });
        collection.watch().subscribe(subscriber);

        assertDoesNotThrow(Fixture::waitForLastServerSessionPoolRelease);
    }

}
