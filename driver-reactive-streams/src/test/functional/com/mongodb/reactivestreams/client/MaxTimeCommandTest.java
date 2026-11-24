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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;
import static com.mongodb.reactivestreams.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.reactivestreams.client.Fixture.getMongoClientBuilderFromConnectionString;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Verifies that the per-call {@code maxTime} option is sent to the server as {@code maxTimeMS} by the reactive driver.
 */
public class MaxTimeCommandTest {
    private static final String COLLECTION_NAME = "maxTimeCommandTest";
    private static final long MAX_TIME_MS = 5000L;

    private TestCommandListener commandListener;
    private MongoClient mongoClient;
    private MongoCollection<Document> collection;

    @Before
    public void setUp() {
        commandListener = new TestCommandListener();
        mongoClient = MongoClients.create(getMongoClientBuilderFromConnectionString()
                .addCommandListener(commandListener)
                .build());
        collection = mongoClient.getDatabase(getDefaultDatabaseName()).getCollection(COLLECTION_NAME);
        Mono.from(collection.drop()).block(TIMEOUT_DURATION);
        Mono.from(collection.insertOne(new Document("_id", 1).append("a", 1))).block(TIMEOUT_DURATION);
        commandListener.reset();
    }

    @After
    public void tearDown() {
        if (mongoClient != null) {
            Mono.from(collection.drop()).block(TIMEOUT_DURATION);
            mongoClient.close();
        }
    }

    @Test
    public void findOneAndDeleteShouldSendMaxTimeMS() {
        Mono.from(collection.findOneAndDelete(new Document("_id", 1),
                new FindOneAndDeleteOptions().maxTime(MAX_TIME_MS, TimeUnit.MILLISECONDS))).block(TIMEOUT_DURATION);
        assertMaxTimeMS("findAndModify");
    }

    @Test
    public void findOneAndReplaceShouldSendMaxTimeMS() {
        Mono.from(collection.findOneAndReplace(new Document("_id", 1), new Document("a", 2),
                new FindOneAndReplaceOptions().maxTime(MAX_TIME_MS, TimeUnit.MILLISECONDS))).block(TIMEOUT_DURATION);
        assertMaxTimeMS("findAndModify");
    }

    @Test
    public void findOneAndUpdateShouldSendMaxTimeMS() {
        Mono.from(collection.findOneAndUpdate(new Document("_id", 1), new Document("$set", new Document("a", 2)),
                new FindOneAndUpdateOptions().maxTime(MAX_TIME_MS, TimeUnit.MILLISECONDS))).block(TIMEOUT_DURATION);
        assertMaxTimeMS("findAndModify");
    }

    @Test
    public void findOneAndUpdateWithPipelineShouldSendMaxTimeMS() {
        Mono.from(collection.findOneAndUpdate(new Document("_id", 1),
                singletonList(new Document("$set", new Document("a", 2))),
                new FindOneAndUpdateOptions().maxTime(MAX_TIME_MS, TimeUnit.MILLISECONDS))).block(TIMEOUT_DURATION);
        assertMaxTimeMS("findAndModify");
    }

    @Test
    public void createIndexesShouldSendMaxTimeMS() {
        Mono.from(collection.createIndexes(singletonList(new IndexModel(new Document("a", 1))),
                new CreateIndexOptions().maxTime(MAX_TIME_MS, TimeUnit.MILLISECONDS))).block(TIMEOUT_DURATION);
        assertMaxTimeMS("createIndexes");
    }

    @Test
    public void dropIndexByNameShouldSendMaxTimeMS() {
        Mono.from(collection.createIndex(new Document("a", 1))).block(TIMEOUT_DURATION);
        commandListener.reset();
        Mono.from(collection.dropIndex("a_1", new DropIndexOptions().maxTime(MAX_TIME_MS, TimeUnit.MILLISECONDS)))
                .block(TIMEOUT_DURATION);
        assertMaxTimeMS("dropIndexes");
    }

    @Test
    public void dropIndexByKeysShouldSendMaxTimeMS() {
        Mono.from(collection.createIndex(new Document("a", 1))).block(TIMEOUT_DURATION);
        commandListener.reset();
        Mono.from(collection.dropIndex(new Document("a", 1), new DropIndexOptions().maxTime(MAX_TIME_MS, TimeUnit.MILLISECONDS)))
                .block(TIMEOUT_DURATION);
        assertMaxTimeMS("dropIndexes");
    }

    @Test
    public void findOneAndUpdateWithoutMaxTimeShouldNotSendMaxTimeMS() {
        Mono.from(collection.findOneAndUpdate(new Document("_id", 1), new Document("$set", new Document("a", 2))))
                .block(TIMEOUT_DURATION);
        List<CommandStartedEvent> events = commandListener.getCommandStartedEvents("findAndModify");
        assertEquals(1, events.size());
        assertFalse(events.get(0).getCommand().containsKey("maxTimeMS"));
    }

    private void assertMaxTimeMS(final String commandName) {
        List<CommandStartedEvent> events = commandListener.getCommandStartedEvents(commandName);
        assertEquals("expected exactly one " + commandName + " command", 1, events.size());
        BsonDocument command = events.get(0).getCommand();
        assertTrue("maxTimeMS missing from " + command.toJson(), command.containsKey("maxTimeMS"));
        assertEquals(MAX_TIME_MS, command.getNumber("maxTimeMS").longValue());
    }
}
