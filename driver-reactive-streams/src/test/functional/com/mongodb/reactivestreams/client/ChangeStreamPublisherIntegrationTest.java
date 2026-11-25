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

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.ConnectionCheckedOutEvent;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.internal.connection.TestCommandListener;
import com.mongodb.reactivestreams.client.internal.BatchCursorPublisher;
import com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;
import static com.mongodb.internal.thread.InterruptionUtil.interruptAndCreateMongoInterruptedException;
import static com.mongodb.reactivestreams.client.Fixture.getMongoClientSettingsBuilder;
import static com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient.disableWaitForBatchCursorCreation;
import static com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient.enableWaitForBatchCursorCreation;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ChangeStreamPublisherIntegrationTest extends com.mongodb.client.ChangeStreamIterableIntegrationTest {
    @BeforeEach
    void beforeEach() {
        enableWaitForBatchCursorCreation();
    }

    @AfterEach
    void afterEach() {
        disableWaitForBatchCursorCreation();
    }

    @Override
    protected MongoClient createClient(final MongoClientSettings settings) {
        return new SyncMongoClient(settings);
    }

    /**
     * {@link ChangeStreamPublisher#withDocumentClass(Class)} returns a new {@link BatchCursorPublisher},
     * which must handle batch size correctly.
     */
    @Test
    @Override
    protected void batchSizeWorksForWithDocumentClass() {
        super.batchSizeWorksForWithDocumentClass();
    }

    /**
     * @see #batchSizeWorksForWithDocumentClass()
     */
    @Test
    @Override
    protected void commandOptionWorksForWithDocumentClass() {
        super.commandOptionWorksForWithDocumentClass();
    }

    @ParameterizedTest
    @EnumSource(PublisherExecutor.class)
    void commandOptionIsSeparateForSeparateSubscriptions(final PublisherExecutor publisherExecutor)
            throws Exception {
        TestCommandListener commandListener = new TestCommandListener();
        CountDownLatch commandOptionChanged = new CountDownLatch(1);
        try (com.mongodb.reactivestreams.client.MongoClient client = MongoClients.create(getMongoClientSettingsBuilder()
                .addCommandListener(commandListener)
                .applyToConnectionPoolSettings(builder -> builder.addConnectionPoolListener(new ConnectionPoolListener() {
                    @Override
                    public void connectionCheckedOut(final ConnectionCheckedOutEvent event) {
                        try {
                            // block subscriptions from executing commands
                            com.mongodb.assertions.Assertions.assertTrue(
                                    commandOptionChanged.await(TIMEOUT_DURATION.toMillis(), MILLISECONDS));
                        } catch (InterruptedException e) {
                            throw interruptAndCreateMongoInterruptedException(null, e);
                        }
                    }
                }))
                .build())) {
            MongoDatabase database = client.getDatabase(NAMESPACE.getDatabaseName());
            database.drop();
            ChangeStreamPublisher<Document> publisher = database.getCollection(NAMESPACE.getCollectionName()).watch();
            publisher.commandOption("allowDiskUse", true);
            Disposable firstSubscription = publisherExecutor.apply(publisher);
            try {
                publisher.commandOption("allowDiskUse", false);
                // unblock subscriptions from executing commands
                commandOptionChanged.countDown();
                assertCommand(commandListener, command -> assertTrue(command.getBoolean("allowDiskUse").getValue()));
            } finally {
                firstSubscription.dispose();
            }
            commandListener.reset();
            Disposable secondSubscription = publisherExecutor.apply(publisher);
            try {
                assertCommand(commandListener, command -> assertFalse(command.getBoolean("allowDiskUse").getValue()));
            } finally {
                secondSubscription.dispose();
            }
        }
    }

    private static void assertCommand(final TestCommandListener commandListener, final Consumer<BsonDocument> asserter) throws Exception {
        commandListener.waitForEvents(CommandStartedEvent.class, event -> event.getCommandName().equals("aggregate"), 1);
        asserter.accept(commandListener.getCommandStartedEvent("aggregate").getCommand());
    }

    private enum PublisherExecutor implements Function<ChangeStreamPublisher<?>, Disposable> {
        FIRST,
        SUBSCRIBE;

        @Override
        public Disposable apply(final ChangeStreamPublisher<?> publisher) {
            Publisher<?> publisherVariant;
            switch (this) {
                case FIRST: {
                    publisherVariant = publisher.first();
                    break;
                }
                case SUBSCRIBE: {
                    publisherVariant = publisher;
                    break;
                }
                default: {
                    throw com.mongodb.assertions.Assertions.fail();
                }
            }
            return Mono.from(publisherVariant).subscribe();
        }
    }
}
