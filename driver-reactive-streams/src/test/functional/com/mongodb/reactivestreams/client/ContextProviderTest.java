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
import com.mongodb.RequestContext;
import com.mongodb.WriteConcern;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import com.mongodb.lang.Nullable;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static com.mongodb.ClusterFixture.getConnectionString;
import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static com.mongodb.client.model.Updates.inc;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

public class ContextProviderTest {

    @Test
    public void contextShouldBeNullByDefaultInCommandEvents() {

        TestCommandListener commandListener = new TestCommandListener(null);
        try (MongoClient client = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(getConnectionString())
                .addCommandListener(commandListener)
                .build())) {

            // given
            MongoCollection<Document> collection = client.getDatabase(getDefaultDatabaseName())
                    .getCollection("ContextProviderTest");
            Mono.from(collection.drop()).block();
            Mono.from(collection.insertMany(asList(new Document(), new Document(), new Document(), new Document()))).block();
            commandListener.reset();

            // when
            Mono.from(collection.countDocuments()).block();

            // then
            assertEquals(1, commandListener.numCommandStartedEventsWithExpectedContext);
            assertEquals(1, commandListener.numCommandSucceededEventsWithExpectedContext);
        }
    }

    @Test
    public void contextShouldBeAvailableInCommandEvents() {
        RequestContext requestContext = mock(RequestContext.class);

        TestCommandListener commandListener = new TestCommandListener(requestContext);
        try (MongoClient client = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(getConnectionString())
                .contextProvider(new ReactiveContextProvider() {
                    @Override
                    public RequestContext getContext(final Subscriber<?> subscriber) {
                        return requestContext;
                    }
                })
                .addCommandListener(commandListener)
                .build())) {

            // given
            MongoCollection<Document> collection = client.getDatabase(getDefaultDatabaseName())
                    .getCollection("ContextProviderTest");
            Mono.from(collection.drop()).block();
            Mono.from(collection.insertMany(asList(new Document(), new Document(), new Document(), new Document()))).block();
            commandListener.reset();

            // when
            Mono.from(collection.countDocuments()).block();

            // then
            assertEquals(1, commandListener.numCommandStartedEventsWithExpectedContext);
            assertEquals(1, commandListener.numCommandSucceededEventsWithExpectedContext);

            // given
            commandListener.reset();
            Document document = new Document();

            // when
            Mono.from(collection.insertOne(document)).block();

            // then
            assertEquals(1, commandListener.numCommandStartedEventsWithExpectedContext);
            assertEquals(1, commandListener.numCommandSucceededEventsWithExpectedContext);

            // given
            commandListener.reset();

            // when
            Mono.from(collection.updateOne(document, inc("x", 1))).block();

            // then
            assertEquals(1, commandListener.numCommandStartedEventsWithExpectedContext);
            assertEquals(1, commandListener.numCommandSucceededEventsWithExpectedContext);

            // given
            commandListener.reset();
            Document documentTwo = new Document();

            // when
            Mono.from(collection.withWriteConcern(WriteConcern.UNACKNOWLEDGED).insertOne(documentTwo)).block();

            // then
            assertEquals(1, commandListener.numCommandStartedEventsWithExpectedContext);
            assertEquals(1, commandListener.numCommandSucceededEventsWithExpectedContext);

            // given
            commandListener.reset();

            // when
            Mono.from(collection.withWriteConcern(WriteConcern.UNACKNOWLEDGED).updateOne(documentTwo, inc("x", 1)))
                    .block();

            // then
            assertEquals(1, commandListener.numCommandStartedEventsWithExpectedContext);
            assertEquals(1, commandListener.numCommandSucceededEventsWithExpectedContext);

            // given
            commandListener.reset();

            // when
            Mono.from(collection.withWriteConcern(WriteConcern.UNACKNOWLEDGED).deleteOne(documentTwo)).block();

            // then
            assertEquals(1, commandListener.numCommandStartedEventsWithExpectedContext);
            assertEquals(1, commandListener.numCommandSucceededEventsWithExpectedContext);

            // given
            commandListener.reset();

            // when
            Flux<Document> findFlux = Flux.from(collection.find().batchSize(4));
            findFlux.blockLast();

            // then
            assertEquals(2, commandListener.numCommandStartedEventsWithExpectedContext);
            assertEquals(2, commandListener.numCommandSucceededEventsWithExpectedContext);

            // given
            commandListener.reset();

            // when
            try {
                Mono.from(client.getDatabase("admin").runCommand(new Document("notRealCommand", 1))).block();
                fail();
            } catch (Exception e) {
                // then
                assertEquals(1, commandListener.numCommandStartedEventsWithExpectedContext);
                assertEquals(1, commandListener.numCommandFailedEventsWithExpectedContext);
            }
        }
    }

    private static final class TestCommandListener implements CommandListener {
        private int numCommandStartedEventsWithExpectedContext;
        private int numCommandSucceededEventsWithExpectedContext;
        private int numCommandFailedEventsWithExpectedContext;
        private final RequestContext expectedContext;

        private TestCommandListener(@Nullable final RequestContext expectedContext) {
            this.expectedContext = expectedContext;
        }

        public void reset() {
            numCommandStartedEventsWithExpectedContext = 0;
            numCommandSucceededEventsWithExpectedContext = 0;
            numCommandFailedEventsWithExpectedContext = 0;
        }

        @Override
        public void commandStarted(final CommandStartedEvent event) {
            if (event.getRequestContext() == expectedContext) {
                numCommandStartedEventsWithExpectedContext++;
            }
        }

        @Override
        public void commandSucceeded(final CommandSucceededEvent event) {
            if (event.getRequestContext() == expectedContext) {
                numCommandSucceededEventsWithExpectedContext++;
            }

        }

        @Override
        public void commandFailed(final CommandFailedEvent event) {
            if (event.getRequestContext() == expectedContext) {
                numCommandFailedEventsWithExpectedContext++;
            }
        }
    }
}
