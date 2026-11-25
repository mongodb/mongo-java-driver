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
import com.mongodb.event.ConnectionCheckedOutEvent;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.internal.connection.TestCommandListener;
import com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;
import static com.mongodb.client.model.Aggregates.merge;
import static com.mongodb.client.model.Aggregates.out;
import static com.mongodb.internal.thread.InterruptionUtil.interruptAndCreateMongoInterruptedException;
import static com.mongodb.reactivestreams.client.Fixture.getMongoClientSettingsBuilder;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class AggregatePublisherIntegrationTest extends com.mongodb.client.AggregateIterableIntegrationTest {
    @Override
    protected MongoClient createClient(final MongoClientSettings settings) {
        return new SyncMongoClient(settings);
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
            AggregatePublisher<Document> publisher = client.getDatabase(NAMESPACE.getDatabaseName())
                    .getCollection(NAMESPACE.getCollectionName())
                    .aggregate(publisherExecutor.getPipeline());
            publisher.commandOption("allowDiskUse", true);
            CompletableFuture<?> firstSubscription = publisherExecutor.apply(publisher);
            publisher.commandOption("allowDiskUse", false);
            // unblock subscriptions from executing commands
            commandOptionChanged.countDown();
            firstSubscription.get(TIMEOUT_DURATION.toMillis(), MILLISECONDS);
            assertTrue(publisherExecutor.getCommand(commandListener).getBoolean("allowDiskUse").getValue());
            commandListener.reset();
            CompletableFuture<?> secondSubscription = publisherExecutor.apply(publisher);
            secondSubscription.get(TIMEOUT_DURATION.toMillis(), MILLISECONDS);
            assertFalse(publisherExecutor.getCommand(commandListener).getBoolean("allowDiskUse").getValue());
        }
    }

    private enum PublisherExecutor implements Function<AggregatePublisher<?>, CompletableFuture<?>> {
        FIRST,
        SUBSCRIBE,
        SUBSCRIBE_WITH_OUT,
        SUBSCRIBE_WITH_MERGE,
        TO_COLLECTION_WITH_OUT,
        TO_COLLECTION_WITH_MERGE,
        EXPLAIN;

        List<Bson> getPipeline() {
            switch (this) {
                case SUBSCRIBE_WITH_OUT:
                case TO_COLLECTION_WITH_OUT: {
                    return singletonList(out(NAMESPACE.getCollectionName()));
                }
                case SUBSCRIBE_WITH_MERGE:
                case TO_COLLECTION_WITH_MERGE: {
                    return singletonList(merge(NAMESPACE.getCollectionName() + "Merged"));
                }
                default: {
                    return emptyList();
                }
            }
        }

        @Override
        public CompletableFuture<?> apply(final AggregatePublisher<?> publisher) {
            Publisher<?> publisherVariant;
            switch (this) {
                case FIRST: {
                    publisherVariant = publisher.first();
                    break;
                }
                case SUBSCRIBE:
                case SUBSCRIBE_WITH_OUT:
                case SUBSCRIBE_WITH_MERGE: {
                    publisherVariant = publisher;
                    break;
                }
                case TO_COLLECTION_WITH_OUT:
                case TO_COLLECTION_WITH_MERGE: {
                    publisherVariant = publisher.toCollection();
                    break;
                }
                case EXPLAIN: {
                    publisherVariant = publisher.explain();
                    break;
                }
                default: {
                    throw com.mongodb.assertions.Assertions.fail();
                }
            }
            return Mono.from(publisherVariant).toFuture();
        }

        BsonDocument getCommand(final TestCommandListener commandListener) {
            return this == PublisherExecutor.EXPLAIN
                    ? commandListener.getCommandStartedEvent("explain").getCommand().getDocument("explain")
                    : commandListener.getCommandStartedEvent("aggregate").getCommand();
        }
    }
}
