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

import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.DropIndexOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexModel;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.internal.connection.TestCommandListener;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;
import static com.mongodb.reactivestreams.client.Fixture.getConnectionString;
import static com.mongodb.reactivestreams.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.reactivestreams.client.Fixture.getMongoClientBuilderFromConnectionString;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Verifies that the per-call {@code maxTime} option is sent to the server as {@code maxTimeMS} by the reactive driver.
 */
public class MaxTimeCommandTest {
    private static final String COLLECTION_NAME = "maxTimeCommandTest";
    private static final long MAX_TIME_MS = 5000L;

    private TestCommandListener commandListener;
    private MongoClient mongoClient;
    private MongoCollection<Document> collection;

    @BeforeEach
    public void setUp() {
        // A client-level timeoutMS overrides the legacy maxTime option under test
        assumeTrue(getConnectionString().getTimeout() == null, "client timeoutMS overrides per-call maxTime");
        commandListener = new TestCommandListener();
        mongoClient = MongoClients.create(getMongoClientBuilderFromConnectionString()
                .retryWrites(false)
                .addCommandListener(commandListener)
                .build());
        collection = mongoClient.getDatabase(getDefaultDatabaseName()).getCollection(COLLECTION_NAME);
        Mono.from(collection.drop()).block(TIMEOUT_DURATION);
        Mono.from(collection.insertOne(new Document("_id", 1).append("a", 1))).block(TIMEOUT_DURATION);
        Mono.from(collection.createIndex(new Document("a", 1))).block(TIMEOUT_DURATION);
        commandListener.reset();
    }

    @AfterEach
    public void tearDown() {
        if (collection != null) {
            Mono.from(collection.drop()).block(TIMEOUT_DURATION);
        }
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    static Stream<Arguments> maxTimeOperations() {
        final long ms = MAX_TIME_MS;
        final TimeUnit unit = TimeUnit.MILLISECONDS;
        return Stream.of(
                op("findOneAndDelete", "findAndModify", c -> c.findOneAndDelete(new Document("_id", 1),
                        new FindOneAndDeleteOptions().maxTime(ms, unit))),
                op("findOneAndReplace", "findAndModify", c -> c.findOneAndReplace(new Document("_id", 1), new Document("a", 2),
                        new FindOneAndReplaceOptions().maxTime(ms, unit))),
                op("findOneAndUpdate", "findAndModify", c -> c.findOneAndUpdate(new Document("_id", 1),
                        new Document("$set", new Document("a", 2)), new FindOneAndUpdateOptions().maxTime(ms, unit))),
                op("findOneAndUpdate with pipeline", "findAndModify", c -> c.findOneAndUpdate(new Document("_id", 1),
                        singletonList(new Document("$set", new Document("a", 2))), new FindOneAndUpdateOptions().maxTime(ms, unit))),
                op("createIndexes", "createIndexes", c -> c.createIndexes(singletonList(new IndexModel(new Document("b", 1))),
                        new CreateIndexOptions().maxTime(ms, unit))),
                op("dropIndex by name", "dropIndexes", c -> c.dropIndex("a_1", new DropIndexOptions().maxTime(ms, unit))),
                op("dropIndex by keys", "dropIndexes", c -> c.dropIndex(new Document("a", 1), new DropIndexOptions().maxTime(ms, unit)))
        );
    }

    private static Arguments op(final String name, final String commandName,
                                final Function<MongoCollection<Document>, Publisher<?>> operation) {
        return Arguments.of(name, commandName, operation);
    }

    @ParameterizedTest(name = "{0} should send maxTimeMS")
    @MethodSource("maxTimeOperations")
    void shouldSendMaxTimeMS(final String name, final String commandName,
                             final Function<MongoCollection<Document>, Publisher<?>> operation) {
        Mono.from(operation.apply(collection)).block(TIMEOUT_DURATION);
        List<CommandStartedEvent> events = commandListener.getCommandStartedEvents(commandName);
        assertFalse(events.isEmpty(), "no " + commandName + " command observed");
        BsonDocument command = events.get(events.size() - 1).getCommand();
        assertTrue(command.containsKey("maxTimeMS"), "maxTimeMS missing from " + command.toJson());
        assertEquals(MAX_TIME_MS, command.getNumber("maxTimeMS").longValue());
    }

    @Test
    void findOneAndUpdateWithoutMaxTimeShouldNotSendMaxTimeMS() {
        Mono.from(collection.findOneAndUpdate(new Document("_id", 1), new Document("$set", new Document("a", 2))))
                .block(TIMEOUT_DURATION);
        List<CommandStartedEvent> events = commandListener.getCommandStartedEvents("findAndModify");
        assertFalse(events.isEmpty(), "no findAndModify command observed");
        assertFalse(events.get(events.size() - 1).getCommand().containsKey("maxTimeMS"));
    }
}
