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

package com.mongodb.reactivestreams.client;

import com.mongodb.ClusterFixture;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.client.AbstractClientSideOperationsTimeoutProseTest;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.reactivestreams.client.gridfs.GridFSBucket;
import com.mongodb.reactivestreams.client.gridfs.GridFSBuckets;
import com.mongodb.reactivestreams.client.syncadapter.SyncGridFSBucket;
import com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Hooks;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;


/**
 * See https://github.com/mongodb/specifications/blob/master/source/client-side-operations-timeout/tests/README.rst#prose-tests
 */
public final class ClientSideOperationTimeoutProseTest extends AbstractClientSideOperationsTimeoutProseTest {
    private MongoClient wrapped;

    @Override
    protected com.mongodb.client.MongoClient createMongoClient(final MongoClientSettings mongoClientSettings) {
        wrapped = createReactiveClient(mongoClientSettings);
        return new SyncMongoClient(wrapped);
    }

    private static MongoClient createReactiveClient(final MongoClientSettings.Builder builder) {
        return MongoClients.create(builder.build());
    }

    private static MongoClient createReactiveClient(final MongoClientSettings mongoClientSettings) {
        return MongoClients.create(mongoClientSettings);
    }

    @Override
    protected com.mongodb.client.gridfs.GridFSBucket createGridFsBucket(final com.mongodb.client.MongoDatabase mongoDatabase,
                                                                        final String bucketName) {
        return new SyncGridFSBucket(GridFSBuckets.create(wrapped.getDatabase(mongoDatabase.getName()), bucketName));
    }

    private GridFSBucket createReaciveGridFsBucket(final MongoDatabase mongoDatabase, final String bucketName) {
        return GridFSBuckets.create(mongoDatabase, bucketName);
    }

    @Override
    protected boolean isAsync() {
        return true;
    }

    @Tag("setsFailPoint")
    @DisplayName("6. GridFS Upload - uploads via openUploadStream can be timed out")
    @Test
    public void testGridFSUploadViaOpenUploadStreamTimeout() {
        assumeTrue(serverVersionAtLeast(4, 4));
        long rtt = ClusterFixture.getPrimaryRTT();

        //given
        collectionHelper.runAdminCommand("{"
                + "  configureFailPoint: \"failCommand\","
                + "  mode: { times: 1 },"
                + "  data: {"
                + "    failCommands: [\"insert\"],"
                + "    blockConnection: true,"
                + "    blockTimeMS: " + (rtt + 405)
                + "  }"
                + "}");

        try (MongoClient client = createReactiveClient(getMongoClientSettingsBuilder()
                .timeout(rtt + 400, TimeUnit.MILLISECONDS))) {
            MongoDatabase database = client.getDatabase(gridFsFileNamespace.getDatabaseName());
            GridFSBucket gridFsBucket = createReaciveGridFsBucket(database, GRID_FS_BUCKET_NAME);


            TestEventPublisher<ByteBuffer> eventPublisher = new TestEventPublisher<>();
            TestSubscriber<ObjectId> testSubscriber = new TestSubscriber<>();

            gridFsBucket.uploadFromPublisher("filename", eventPublisher.getEventStream())
                    .subscribe(testSubscriber);

            //when
            eventPublisher.sendEvent(ByteBuffer.wrap(new byte[]{0x12}));
            testSubscriber.requestMore(1);
            /*
             By prose spec definition we have to close GridFSUploadStream when we don't have more data to submit and want to flux internal buffers.
              However, in Reactive streams that would be equivalent to calling propagating complete signal from the source publisher.
            */
            eventPublisher.complete();

            //then
            testSubscriber.assertTerminalEvent();

            List<Throwable> onErrorEvents = testSubscriber.getOnErrorEvents();
            assertEquals(1, onErrorEvents.size());

            Throwable operationTimeoutError = onErrorEvents.get(0);
            Throwable operationTimeoutErrorCase = operationTimeoutError.getCause();
            assertEquals(MongoOperationTimeoutException.class, operationTimeoutError.getClass());
            assertEquals(MongoSocketReadTimeoutException.class, operationTimeoutErrorCase.getClass());

            assertEquals("Operation timed out: Timeout while receiving message", operationTimeoutError.getMessage());
            assertEquals("Timeout while receiving message", operationTimeoutErrorCase.getMessage());

            CommandFailedEvent chunkInsertFailedEvent = commandListener.getCommandFailedEvent("insert");
            assertNotNull(chunkInsertFailedEvent);
            assertEquals(operationTimeoutError, commandListener.getCommandFailedEvent("insert").getThrowable());
        }
    }

    @Tag("setsFailPoint")
    @DisplayName("6. GridFS Upload - Aborting an upload stream can be timed out")
    @Test
    public void testAbortingGridFsUploadStreamTimeout() throws ExecutionException, InterruptedException, TimeoutException {
        assumeTrue(serverVersionAtLeast(4, 4));
        long rtt = ClusterFixture.getPrimaryRTT();

        //given
        CompletableFuture<Throwable> droppedErrorFuture = new CompletableFuture<>();
        Hooks.onErrorDropped(droppedErrorFuture::complete);

        collectionHelper.runAdminCommand("{"
                + "  configureFailPoint: \"failCommand\","
                + "  mode: { times: 1 },"
                + "  data: {"
                + "    failCommands: [\"delete\"],"
                + "    blockConnection: true,"
                + "    blockTimeMS: " + (rtt + 405)
                + "  }"
                + "}");

        try (MongoClient client = createReactiveClient(getMongoClientSettingsBuilder()
                .timeout(rtt + 400, TimeUnit.MILLISECONDS))) {
            MongoDatabase database = client.getDatabase(gridFsFileNamespace.getDatabaseName());
            GridFSBucket gridFsBucket = createReaciveGridFsBucket(database, GRID_FS_BUCKET_NAME);


            TestEventPublisher<ByteBuffer> eventPublisher = new TestEventPublisher<>();
            TestSubscriber<ObjectId> testSubscriber = new TestSubscriber<>();

            gridFsBucket.uploadFromPublisher("filename", eventPublisher.getEventStream())
                    .subscribe(testSubscriber);

            //when
            eventPublisher.sendEvent(ByteBuffer.wrap(new byte[]{0x01, 0x02, 0x03, 0x04}));
            testSubscriber.requestMore(1);
            /*
             By prose spec definition we have to abort GridFSUploadStream.
              However, in Reactive streams that would be equivalent to calling subscription to propagate cancellation signal.
            */
            testSubscriber.cancelSubscription();

            //then
            Throwable droppedError = droppedErrorFuture.get(TIMEOUT_DURATION.toMillis(), TimeUnit.MILLISECONDS);
            Throwable operationTimeoutError = droppedError.getCause();
            Throwable operationTimeoutErrorCause = operationTimeoutError.getCause();
            assertEquals(MongoOperationTimeoutException.class, operationTimeoutError.getClass());
            assertEquals(MongoSocketReadTimeoutException.class, operationTimeoutErrorCause.getClass());

            assertEquals("Operation timed out: Timeout while receiving message", operationTimeoutError.getMessage());
            assertEquals("Timeout while receiving message", operationTimeoutErrorCause.getMessage());

            CommandFailedEvent deleteFailedEvent = commandListener.getCommandFailedEvent("delete");
            assertNotNull(deleteFailedEvent);
            assertEquals(operationTimeoutError, commandListener.getCommandFailedEvent("delete").getThrowable());
            // When subscription is cancelled, we should not receive any more events.
            testSubscriber.assertNoTerminalEvent();
        }
    }
}
