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
import com.mongodb.MongoNamespace;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.internal.connection.TestCommandListener;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class ChangeStreamIterableIntegrationTest {
    protected static final MongoNamespace NAMESPACE = new MongoNamespace(ChangeStreamIterableIntegrationTest.class.getSimpleName(), "test");

    private MongoClient client;
    private MongoCollection<Document> collection;
    private TestCommandListener commandListener;

    @BeforeEach
    void beforeEach() {
        assumeTrue(isDiscoverableReplicaSet());
        commandListener = new TestCommandListener();
        client = createClient(getMongoClientSettingsBuilder()
                .addCommandListener(commandListener)
                .build());
        MongoDatabase database = client.getDatabase(NAMESPACE.getDatabaseName());
        database.drop();
        collection = database.getCollection(NAMESPACE.getCollectionName());
    }

    @AfterEach
    void afterEach() {
        if (client != null) {
            client.close();
        }
    }

    protected MongoClient createClient(final MongoClientSettings settings) {
        return MongoClients.create(settings);
    }

    /**
     * {@link ChangeStreamIterable#withDocumentClass(Class)} returns a new {@link MongoIterable},
     * which must handle batch size correctly.
     */
    @Test
    protected void batchSizeWorksForWithDocumentClass() {
        assertAll(
                () -> assertBatchSizeWorksForWithDocumentClass(true, true),
                () -> assertBatchSizeWorksForWithDocumentClass(true, false),
                () -> assertBatchSizeWorksForWithDocumentClass(false, true),
                () -> assertBatchSizeWorksForWithDocumentClass(false, false));
    }

    private void assertBatchSizeWorksForWithDocumentClass(final boolean chaining, final boolean batchSizeBeforeWithDocumentClass) {
        commandListener.reset();
        int batchSize = 3;
        int batchCount = 2;
        ChangeStreamIterable<Document> iterable = collection.watch();
        MongoIterable<Document> configuredIterable;
        if (chaining) {
            if (batchSizeBeforeWithDocumentClass) {
                configuredIterable = iterable.batchSize(batchSize).withDocumentClass(Document.class);
            } else {
                configuredIterable = iterable.withDocumentClass(Document.class).batchSize(batchSize);
            }
        } else {
            if (batchSizeBeforeWithDocumentClass) {
                iterable.batchSize(batchSize);
                configuredIterable = iterable.withDocumentClass(Document.class);
            } else {
                configuredIterable = iterable.withDocumentClass(Document.class);
                iterable.batchSize(batchSize);
            }
        }
        try (MongoCursor<Document> cursor = configuredIterable.cursor()) {
            List<Document> insertedDocuments = Stream.generate(Document::new).limit(batchSize * batchCount).collect(toList());
            collection.insertMany(insertedDocuments);
            for (int i = 0; i < insertedDocuments.size(); i++) {
                assertEquals(OperationType.INSERT.getValue(), cursor.next().get("operationType"));
            }
            commandListener.getCommandStartedEvents("getMore").stream()
                    .map(CommandStartedEvent::getCommand)
                    .forEach(command -> assertEquals(batchSize, command.getInt32("batchSize").intValue()));
        }
    }

    /**
     * @see #batchSizeWorksForWithDocumentClass()
     */
    @Test
    protected void commandOptionWorksForWithDocumentClass() {
        assertAll(
                () -> commandOptionWorksForWithDocumentClass(true, true),
                () -> commandOptionWorksForWithDocumentClass(false, true),
                () -> commandOptionWorksForWithDocumentClass(false, false));
    }

    private void commandOptionWorksForWithDocumentClass(final boolean chaining, final boolean commandOptionBeforeWithDocumentClass) {
        commandListener.reset();
        assertCommand(
                iterable -> {
                    Document collation = new Document("locale", "en");
                    MongoIterable<Document> configuredIterable;
                    if (chaining) {
                        if (commandOptionBeforeWithDocumentClass) {
                            configuredIterable = iterable.commandOption("collation", collation).withDocumentClass(Document.class);
                        } else {
                            throw com.mongodb.assertions.Assertions.fail("this variant is not expressible in code");
                        }
                    } else {
                        if (commandOptionBeforeWithDocumentClass) {
                            iterable.commandOption("collation", collation);
                            configuredIterable = iterable.withDocumentClass(Document.class);
                        } else {
                            configuredIterable = iterable.withDocumentClass(Document.class);
                            iterable.commandOption("collation", collation);
                        }
                    }
                    return configuredIterable;
                },
                command -> {
                    BsonValue actualCollation = command.get("collation");
                    assertEquals(new BsonDocument("locale", new BsonString("en")), actualCollation);
                });
    }

    @Test
    void commandOption() {
        assertCommand(
                iterable -> iterable
                        .commandOption("collation", new BsonDocument("locale", new BsonString("en")))
                        .collation(Collation.builder().locale("overridden").build()),
                command -> {
                    assertEquals(new BsonDocument("locale", new BsonString("en")), command.getDocument("collation"));
                });
    }

    private void assertCommand(
            final Function<ChangeStreamIterable<?>, MongoIterable<?>> iterableTuner,
            final Consumer<BsonDocument> asserter) {
        MongoIterable<?> iterable = iterableTuner.apply(collection.watch());
        iterable.cursor().close();
        asserter.accept(commandListener.getCommandStartedEvent("aggregate").getCommand());
    }
}
