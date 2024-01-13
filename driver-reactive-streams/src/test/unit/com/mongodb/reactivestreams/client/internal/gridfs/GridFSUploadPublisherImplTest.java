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

package com.mongodb.reactivestreams.client.internal.gridfs;

import com.mongodb.ClusterFixture;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.Fixture;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.event.CommandEvent;
import com.mongodb.internal.connection.TestCommandListener;
import com.mongodb.reactivestreams.client.EventPublisher;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.reactivestreams.client.TestSubscriber;
import com.mongodb.reactivestreams.client.gridfs.GridFSBucket;
import com.mongodb.reactivestreams.client.gridfs.GridFSBuckets;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;


class GridFSUploadPublisherTest {
    private static final String GRID_FS_BUCKET_NAME = "db.fs";
    private TestCommandListener commandListener;

    protected MongoClientSettings.Builder getMongoClientSettingsBuilder() {
        commandListener.reset();
        return Fixture.getMongoClientSettingsBuilder()
                .readConcern(ReadConcern.MAJORITY)
                .writeConcern(WriteConcern.MAJORITY)
                .readPreference(ReadPreference.primary())
                .addCommandListener(commandListener);
    }

    @Test
    void shouldTimeoutWhenSourcePublisherCompletionExceedsOverallOperationTimeout() {
        assumeTrue(serverVersionAtLeast(4, 4));
        long rtt = ClusterFixture.getPrimaryRTT();

        //given
        try (MongoClient client = MongoClients.create(getMongoClientSettingsBuilder()
                .timeout(rtt + 800, TimeUnit.MILLISECONDS).build())) {
            MongoDatabase database = client.getDatabase(getDefaultDatabaseName());
            GridFSBucket gridFsBucket = GridFSBuckets.create(database, GRID_FS_BUCKET_NAME);

            EventPublisher<ByteBuffer> eventPublisher = new EventPublisher<>();
            TestSubscriber<ObjectId> testSubscriber = new TestSubscriber<>();

            //when
            gridFsBucket.uploadFromPublisher("filename", eventPublisher.getEventStream())
                    .subscribe(testSubscriber);
            testSubscriber.requestMore(1);

            //then
            testSubscriber.assertTerminalEvent();

            List<Throwable> onErrorEvents = testSubscriber.getOnErrorEvents();
            assertEquals(1, onErrorEvents.size());

            Throwable throwable = onErrorEvents.get(0);
            assertEquals(MongoOperationTimeoutException.class, throwable.getClass());
            assertEquals("GridFS timeout out waiting for data"
                    + " from provided source Publisher", throwable.getMessage());

            //assert no chunk has been inserted as we have not sent any data from source publisher.
            for (CommandEvent event : commandListener.getEvents()) {
                assertNotEquals("insert", event.getCommandName());
            }
        }
    }

    @Test
    void shouldCancelSubscriptionToSourceWhenOperationTimeoutOccurs() {
        assumeTrue(serverVersionAtLeast(4, 4));
        long rtt = ClusterFixture.getPrimaryRTT();

        //given
        try (MongoClient client = MongoClients.create(getMongoClientSettingsBuilder()
                .timeout(rtt + 800, TimeUnit.MILLISECONDS).build())) {
            MongoDatabase database = client.getDatabase(getDefaultDatabaseName());
            GridFSBucket gridFsBucket = GridFSBuckets.create(database, GRID_FS_BUCKET_NAME);

            EventPublisher<ByteBuffer> eventPublisher = new EventPublisher<>();
            TestSubscriber<ObjectId> testSubscriber = new TestSubscriber<>();

            //when
            gridFsBucket.uploadFromPublisher("filename", eventPublisher.getEventStream())
                    .subscribe(testSubscriber);
            testSubscriber.requestMore(1);

            assertEquals(1, eventPublisher.currentSubscriberCount());
            testSubscriber.cancelSubscription();

            //then
            assertEquals(0, eventPublisher.currentSubscriberCount());
        }
    }

    @BeforeEach
    public void setUp() {
        commandListener = new TestCommandListener();
    }

    @AfterEach
    public void tearDown() {
        CollectionHelper.dropDatabase(getDefaultDatabaseName());
    }
}
