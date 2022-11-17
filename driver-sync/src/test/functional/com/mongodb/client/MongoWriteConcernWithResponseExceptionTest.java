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

import com.mongodb.Function;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.ServerAddress;
import com.mongodb.assertions.Assertions;
import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandSucceededEvent;
import com.mongodb.internal.connection.MongoWriteConcernWithResponseException;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertThrows;
import static org.junit.Assume.assumeTrue;

/**
 * Tests in this class check that the internal {@link MongoWriteConcernWithResponseException} does not leak from our API.
 */
public final class MongoWriteConcernWithResponseExceptionTest {
    /**
     * This test is similar to {@link RetryableWritesProseTest#originalErrorMustBePropagatedIfNoWritesPerformed()}.
     * The difference is in the assertions, it also verifies situations when `writeConcernError` happens on the first attempt
     * and on the last attempt.
     */
    @Test
    public void doesNotLeak() throws InterruptedException {
        doesNotLeak(MongoClients::create);
    }

    public static void doesNotLeak(final Function<MongoClientSettings, MongoClient> clientCreator) throws InterruptedException {
        BsonDocument writeConcernErrorFpDoc = new BsonDocument()
                .append("configureFailPoint", new BsonString("failCommand"))
                .append("mode", new BsonDocument()
                        .append("times", new BsonInt32(2)))
                .append("data", new BsonDocument()
                        .append("writeConcernError", new BsonDocument()
                                .append("code", new BsonInt32(91))
                                .append("errorLabels", new BsonArray(Stream.of("RetryableWriteError")
                                        .map(BsonString::new).collect(Collectors.toList())))
                                .append("errmsg", new BsonString(""))
                        )
                        .append("failCommands", new BsonArray(singletonList(new BsonString("insert")))));
        BsonDocument noWritesPerformedFpDoc = new BsonDocument()
                .append("configureFailPoint", new BsonString("failCommand"))
                .append("mode", new BsonDocument()
                        .append("times", new BsonInt32(1)))
                .append("data", new BsonDocument()
                        .append("failCommands", new BsonArray(singletonList(new BsonString("insert"))))
                        .append("errorCode", new BsonInt32(10107))
                        .append("errorLabels", new BsonArray(Stream.of("RetryableWriteError", "NoWritesPerformed")
                                .map(BsonString::new).collect(Collectors.toList()))));
        doesNotLeak(clientCreator, writeConcernErrorFpDoc, true, noWritesPerformedFpDoc);
        doesNotLeak(clientCreator, noWritesPerformedFpDoc, false, writeConcernErrorFpDoc);
    }

    @SuppressWarnings("try")
    private static void doesNotLeak(
            final Function<MongoClientSettings, MongoClient> clientCreator,
            final BsonDocument firstAttemptFpDoc,
            final boolean firstAttemptCommandSucceededEvent,
            final BsonDocument lastAttemptFpDoc) throws InterruptedException {
        assumeTrue(serverVersionAtLeast(6, 0) && isDiscoverableReplicaSet());
        ServerAddress primaryServerAddress = Fixture.getPrimary();
        CompletableFuture<FailPoint> futureFailPointFromListener = new CompletableFuture<>();
        CommandListener commandListener = new CommandListener() {
            private final AtomicBoolean configureFailPoint = new AtomicBoolean(true);

            @Override
            public void commandSucceeded(final CommandSucceededEvent event) {
                if (firstAttemptCommandSucceededEvent) {
                    enableLastAttemptFp(event);
                }
            }

            @Override
            public void commandFailed(final CommandFailedEvent event) {
                if (!firstAttemptCommandSucceededEvent) {
                    enableLastAttemptFp(event);
                }
            }

            private void enableLastAttemptFp(final CommandEvent event) {
                if (event.getCommandName().equals("insert") && configureFailPoint.compareAndSet(true, false)) {
                    Assertions.assertTrue(futureFailPointFromListener.complete(FailPoint.enable(lastAttemptFpDoc, primaryServerAddress)));
                }
            }
        };
        try (MongoClient client = clientCreator.apply(getMongoClientSettingsBuilder()
                .retryWrites(true)
                .addCommandListener(commandListener)
                .applyToServerSettings(builder -> builder.heartbeatFrequency(50, TimeUnit.MILLISECONDS))
                .build());
             FailPoint ignored = FailPoint.enable(firstAttemptFpDoc, primaryServerAddress)) {
            MongoCollection<Document> collection = client.getDatabase(getDefaultDatabaseName())
                    .getCollection("originalErrorMustBePropagatedIfNoWritesPerformed");
            collection.drop();
            assertThrows(MongoWriteConcernException.class, () -> {
                // We want to see an exception indicating `writeConcernError`,
                // but not in the form of `MongoWriteConcernWithResponseException`.
                try {
                    collection.insertOne(new Document());
                } catch (MongoWriteConcernWithResponseException e) {
                    throw new AssertionError("The internal exception leaked.", e);
                }
            });
        } finally {
            futureFailPointFromListener.thenAccept(FailPoint::close);
        }
    }
}
