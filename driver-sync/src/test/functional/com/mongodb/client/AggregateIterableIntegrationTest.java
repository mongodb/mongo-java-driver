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
import com.mongodb.internal.connection.TestCommandListener;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static com.mongodb.client.model.Aggregates.merge;
import static com.mongodb.client.model.Aggregates.out;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class AggregateIterableIntegrationTest {
    protected static final MongoNamespace NAMESPACE = new MongoNamespace(AggregateIterableIntegrationTest.class.getSimpleName(), "test");

    private MongoClient client;
    private MongoCollection<Document> collection;
    private TestCommandListener commandListener;

    @BeforeEach
    void beforeEach() {
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

    @ParameterizedTest
    @EnumSource(IterableExecutor.class)
    void commandOption(final IterableExecutor iterableExecutor) {
        // `let` requires MongoDB 5.0
        assumeTrue(serverVersionAtLeast(5, 0));
        assertCommand(
                iterable -> iterable
                        .commandOption("allowDiskUse", true)
                        .commandOption("let", new Document("overriding", 2))
                        .let(new Document("overridden", 1)),
                iterableExecutor,
                command -> {
                    assertEquals(BsonBoolean.TRUE, command.get("allowDiskUse"));
                    assertEquals(new BsonDocument("overriding", new BsonInt32(2)), command.getDocument("let"));
                });
    }

    private void assertCommand(
            final Consumer<AggregateIterable<?>> iterableTuner,
            final IterableExecutor iterableExecutor,
            final Consumer<BsonDocument> asserter) {
        AggregateIterable<Document> iterable = collection.aggregate(iterableExecutor.getPipeline());
        iterableTuner.accept(iterable);
        iterableExecutor.accept(iterable);
        asserter.accept(iterableExecutor.getCommand(commandListener));
    }

    private enum IterableExecutor implements Consumer<AggregateIterable<?>> {
        FIRST,
        INTO,
        CURSOR,
        CURSOR_WITH_OUT,
        CURSOR_WITH_MERGE,
        TO_COLLECTION_WITH_OUT,
        TO_COLLECTION_WITH_MERGE,
        EXPLAIN;

        List<Bson> getPipeline() {
            switch (this) {
                case CURSOR_WITH_OUT:
                case TO_COLLECTION_WITH_OUT: {
                    return singletonList(out(NAMESPACE.getCollectionName()));
                }
                case CURSOR_WITH_MERGE:
                case TO_COLLECTION_WITH_MERGE: {
                    return singletonList(merge(NAMESPACE.getCollectionName() + "Merged"));
                }
                default: {
                    return emptyList();
                }
            }
        }

        @Override
        public void accept(final AggregateIterable<?> iterable) {
            switch (this) {
                case FIRST: {
                    iterable.first();
                    break;
                }
                case INTO: {
                    iterable.into(new ArrayList<>());
                    break;
                }
                case CURSOR:
                case CURSOR_WITH_OUT:
                case CURSOR_WITH_MERGE: {
                    try (MongoCursor<?> cursor = iterable.cursor()) {
                        cursor.hasNext();
                    }
                    break;
                }
                case TO_COLLECTION_WITH_OUT:
                case TO_COLLECTION_WITH_MERGE: {
                    iterable.toCollection();
                    break;
                }
                case EXPLAIN: {
                    iterable.explain();
                    break;
                }
                default: {
                    com.mongodb.assertions.Assertions.fail();
                }
            }
        }

        BsonDocument getCommand(final TestCommandListener commandListener) {
            return this == IterableExecutor.EXPLAIN
                    ? commandListener.getCommandStartedEvent("explain").getCommand().getDocument("explain")
                    : commandListener.getCommandStartedEvent("aggregate").getCommand();
        }
    }
}
